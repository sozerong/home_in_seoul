"""서울교통공사 지하철 혼잡도 레이크 XML(API)을 정제·검증해 stg.congestion과 stg.congestion_quarantine에 넣는다.

규칙은 docs/rules.md 3절. 시간대 39칸을 세로로 풀어 넣는다.
처리 단위는 수집일 하나이고, 그 수집일의 기존 행을 지우고 다시 넣는다(멱등).
사용법: python -m transform.congestion 수집일YYYY-MM-DD
예:     python -m transform.congestion 2026-10-21
"""
import argparse
import collections
import datetime
import re
import xml.etree.ElementTree as ET

import psycopg
from psycopg.types.json import Jsonb

from common import ROOT, read_env

ID_FIELDS = ["DOW_SE", "LINE", "STTN_NO", "DPTRE_STTN", "UP_DOWN_SE"]
# 05:30부터 30분 간격 39칸. 마지막 두 칸은 다음 날 00:00, 00:30 (TIME0000, TIME0030)
TIME_FIELDS = [f"TIME{(330 + 30 * i) // 60 % 24:02d}{(330 + 30 * i) % 60:02d}" for i in range(39)]
FIELDS = set(ID_FIELDS + TIME_FIELDS)  # CF1
DOWS = {"평일", "토요일", "일요일"}
DIRECTIONS = {"상선", "하선", "내선", "외선"}
LINES = {f"{n}호선" for n in range(1, 9)}
MAX_CONGESTION = 300          # CV5
MAX_QUARANTINE_RATIO = 0.05   # CF3
MAX_SHRINK_RATIO = 0.10       # CF4
COLUMNS = ["collected_date", "source_file", "row_no", "dow", "line", "sttn_no", "sttn_no4", "sttn_nm",
           "direction", "slot", "congestion"]
QUARANTINE_COLUMNS = ["collected_date", "source_file", "row_no", "reasons", "raw"]


def clean(raw):
    """원본 한 행을 (세로로 푼 행 목록, 격리할 것 목록)으로 바꾼다.

    격리할 것은 (사유 목록, 원본 조각)의 목록이다. 신원 필드가 잘못되면(CV1~CV4) 원본 행 전체를,
    값만 잘못되면(CV5) 그 칸만 격리한다.
    """
    v = {k: s.strip() for k, s in raw.items()}
    reasons = set()
    if any(not v[f] for f in ID_FIELDS):
        reasons.add("CV1")
    if (v["DOW_SE"] and v["DOW_SE"] not in DOWS) or (v["UP_DOWN_SE"] and v["UP_DOWN_SE"] not in DIRECTIONS):
        reasons.add("CV2")
    if v["LINE"] and v["LINE"] not in LINES:
        reasons.add("CV3")
    if v["STTN_NO"] and not re.fullmatch(r"\d{3,4}", v["STTN_NO"]):
        reasons.add("CV4")
    if reasons:
        return [], [(sorted(reasons), raw)]

    identity = {"dow": v["DOW_SE"], "line": v["LINE"], "sttn_no": v["STTN_NO"],
                "sttn_no4": v["STTN_NO"].zfill(4),  # CC2
                "sttn_nm": v["DPTRE_STTN"], "direction": v["UP_DOWN_SE"]}
    rows, quarantined = [], []
    for field in TIME_FIELDS:  # CC3: 세로로 풀기
        try:
            value = float(v[field])  # CC5
        except ValueError:
            value = None
        if value is None or not 0 <= value <= MAX_CONGESTION:
            quarantined.append((["CV5"], {**{f: raw[f] for f in ID_FIELDS}, field: raw[field]}))
        else:
            slot = datetime.time(int(field[4:6]), int(field[6:8]))  # CC4: TIME0730 -> 07:30
            rows.append({**identity, "slot": slot, "congestion": value})
    return rows, quarantined


def insert_sql(table, columns):
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(f'%({c})s' for c in columns)})"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("collected_date", type=datetime.date.fromisoformat)
    args = parser.parse_args()
    day = args.collected_date

    folder = ROOT / "lake" / "seoul_subway_congestion" / f"collected_date={day}"
    files = sorted(folder.glob("subwConfusion_p*.xml"))
    if not files:
        raise SystemExit(f"레이크에 파일이 없습니다: {folder / 'subwConfusion_p*.xml'}")

    good, bad, source_rows, bad_cells = [], [], 0, 0
    for path in files:
        for row_no, element in enumerate(ET.parse(path).getroot().iter("row"), start=1):
            raw = {child.tag: child.text or "" for child in element}
            if set(raw) != FIELDS:  # CF1
                raise SystemExit(f"CF1 스키마 변경: {path.name} {row_no}번째 row, 다른 필드 {sorted(set(raw) ^ FIELDS)}")
            source_rows += 1
            meta = {"collected_date": day, "source_file": path.name, "row_no": row_no}
            rows, quarantined = clean(raw)
            good += [{**meta, **r} for r in rows]
            for reasons, piece in quarantined:
                bad.append({**meta, "reasons": reasons, "raw": Jsonb(piece)})
                bad_cells += 1 if reasons == ["CV5"] else len(TIME_FIELDS)  # 행 전체 격리는 39칸으로 센다

    keys = collections.Counter((r["dow"], r["line"], r["sttn_no"], r["direction"], r["slot"]) for r in good)
    duplicated = [k for k, n in keys.items() if n > 1]
    if duplicated:  # CF2
        raise SystemExit(f"CF2 (요일, 호선, 역번호, 방향) 중복: {duplicated[:5]}")
    total = len(good) + bad_cells
    if total == 0:
        raise SystemExit("row가 0개입니다")
    ratio = bad_cells / total
    counts = collections.Counter(r for b in bad for r in b["reasons"])
    print(f"혼잡도 (수집일 {day}): 원본 {source_rows}행 -> 세로 {total}칸, 통과 {len(good)}, 격리 {bad_cells}칸 "
          f"({ratio:.2%}), 사유 {dict(sorted(counts.items()))}")
    if ratio > MAX_QUARANTINE_RATIO:  # CF3
        raise SystemExit(f"CF3 격리 비율 {ratio:.2%}가 기준 {MAX_QUARANTINE_RATIO:.0%}를 넘었습니다")

    with psycopg.connect(read_env("DATABASE_URL")) as conn:  # 블록이 정상으로 끝나면 커밋, 예외면 롤백
        previous = conn.execute(
            "SELECT collected_date, count(DISTINCT (source_file, row_no)) FROM stg.congestion"
            " WHERE collected_date < %s GROUP BY collected_date ORDER BY collected_date DESC LIMIT 1",
            (day,)).fetchone()
        if previous and source_rows < previous[1] * (1 - MAX_SHRINK_RATIO):  # CF4
            raise SystemExit(f"CF4 원본 행 수 급감: 직전 {previous[0]} {previous[1]}행 -> 이번 {source_rows}행")
        for table in ("stg.congestion", "stg.congestion_quarantine"):
            conn.execute(f"DELETE FROM {table} WHERE collected_date = %s", (day,))
        with conn.cursor() as cur:
            cur.executemany(insert_sql("stg.congestion", COLUMNS), good)
            cur.executemany(insert_sql("stg.congestion_quarantine", QUARANTINE_COLUMNS), bad)

        if previous is None:
            print("변경 알림: 직전 수집일 스냅샷이 없어 비교하지 않았습니다")
        else:  # 결정 7: API가 갱신됐는지 알린다. 바뀌어도 실패시키지 않는다
            same, changed, one_side = conn.execute(
                "SELECT count(*) FILTER (WHERE a.congestion = b.congestion),"
                "       count(*) FILTER (WHERE a.congestion <> b.congestion),"
                "       count(*) FILTER (WHERE a.congestion IS NULL OR b.congestion IS NULL)"
                " FROM (SELECT * FROM stg.congestion WHERE collected_date = %s) a"
                " FULL JOIN (SELECT * FROM stg.congestion WHERE collected_date = %s) b"
                " USING (dow, line, sttn_no, direction, slot)", (day, previous[0])).fetchone()
            state = "직전과 같음 (API 미갱신)" if changed == 0 and one_side == 0 else "바뀜 (API 갱신 가능성)"
            print(f"변경 알림: 직전 {previous[0]} 대비 같은 값 {same}, 다른 값 {changed}, 한쪽에만 {one_side} -> {state}")
    print("적재 완료")


if __name__ == "__main__":
    main()
    