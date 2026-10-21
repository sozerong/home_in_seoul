"""서울시 역사마스터 레이크 XML을 정제·검증해 stg.station_master와 stg.station_master_quarantine에 넣는다.

규칙은 docs/rules.md 2절. 처리 단위는 수집일 하나(역 전체 목록)이고, 그 수집일의 기존 행을 지우고 다시 넣는다(멱등).
사용법: python -m transform.station_master 수집일YYYY-MM-DD
예:     python -m transform.station_master 2026-10-21
"""
import argparse
import collections
import datetime
import re
import xml.etree.ElementTree as ET

import psycopg
from psycopg.types.json import Jsonb

from common import ROOT, read_env

FIELDS = {"BLDN_ID", "BLDN_NM", "ROUTE", "LAT", "LOT"}  # SF1
MAX_QUARANTINE_RATIO = 0.05                            # SF3
MAX_SHRINK_RATIO = 0.10                                # SF4
COLUMNS = ["collected_date", "source_file", "row_no", "bldn_id", "bldn_nm", "route", "lat", "lot"]
QUARANTINE_COLUMNS = ["collected_date", "source_file", "row_no", "reasons", "raw"]


def clean(raw):
    """row 하나에 정제(SC2, SC3)와 검증(SV1~SV3)을 적용해 (정제된 값, 어긋난 규칙 번호 목록)을 돌려준다."""
    v = {k: s.strip() or None for k, s in raw.items()}
    reasons = set()
    if None in v.values():
        reasons.add("SV1")
    if v["BLDN_ID"] is not None and not re.fullmatch(r"\d{4}", v["BLDN_ID"]):
        reasons.add("SV2")
    lat = lot = None
    if v["LAT"] is not None and v["LOT"] is not None:
        try:
            lat, lot = float(v["LAT"]), float(v["LOT"])
        except ValueError:
            reasons.add("SV3")
        else:
            if not (33 <= lat <= 39 and 124 <= lot <= 132):  # 위도·경도가 뒤바뀌어도 여기서 걸린다
                reasons.add("SV3")
    row = {"bldn_id": v["BLDN_ID"], "bldn_nm": v["BLDN_NM"], "route": v["ROUTE"], "lat": lat, "lot": lot}
    return row, sorted(reasons)


def insert_sql(table, columns):
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(f'%({c})s' for c in columns)})"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("collected_date", type=datetime.date.fromisoformat)
    args = parser.parse_args()

    folder = ROOT / "lake" / "seoul_station_master" / f"collected_date={args.collected_date}"
    files = sorted(folder.glob("subwayStationMaster_p*.xml"))
    if not files:
        raise SystemExit(f"레이크에 파일이 없습니다: {folder / 'subwayStationMaster_p*.xml'}")

    good, bad = [], []
    for path in files:
        for row_no, element in enumerate(ET.parse(path).getroot().iter("row"), start=1):
            raw = {child.tag: child.text or "" for child in element}
            if set(raw) != FIELDS:  # SF1
                raise SystemExit(f"SF1 스키마 변경: {path.name} {row_no}번째 row, 다른 필드 {sorted(set(raw) ^ FIELDS)}")
            meta = {"collected_date": args.collected_date, "source_file": path.name, "row_no": row_no}
            row, reasons = clean(raw)
            if reasons:
                bad.append({**meta, "reasons": reasons, "raw": Jsonb(raw)})
            else:
                good.append({**meta, **row})

    duplicated = [k for k, n in collections.Counter(r["bldn_id"] for r in good).items() if n > 1]
    if duplicated:  # SF2
        raise SystemExit(f"SF2 역사_ID 중복: {duplicated[:10]}")
    total = len(good) + len(bad)
    if total == 0:
        raise SystemExit("row가 0개입니다")
    ratio = len(bad) / total
    counts = collections.Counter(r for b in bad for r in b["reasons"])
    print(f"역사마스터 (수집일 {args.collected_date}): row {total}, 통과 {len(good)}, 격리 {len(bad)} "
          f"({ratio:.2%}), 사유 {dict(sorted(counts.items()))}")
    if ratio > MAX_QUARANTINE_RATIO:  # SF3
        raise SystemExit(f"SF3 격리 비율 {ratio:.2%}가 기준 {MAX_QUARANTINE_RATIO:.0%}를 넘었습니다")

    with psycopg.connect(read_env("DATABASE_URL")) as conn:  # 블록이 정상으로 끝나면 커밋, 예외면 롤백
        previous = conn.execute(
            "SELECT collected_date, count(*) FROM stg.station_master WHERE collected_date < %s"
            " GROUP BY collected_date ORDER BY collected_date DESC LIMIT 1", (args.collected_date,)).fetchone()
        if previous and len(good) < previous[1] * (1 - MAX_SHRINK_RATIO):  # SF4
            raise SystemExit(f"SF4 행 수 급감: 직전 {previous[0]} {previous[1]}행 -> 이번 {len(good)}행")
        for table in ("stg.station_master", "stg.station_master_quarantine"):
            conn.execute(f"DELETE FROM {table} WHERE collected_date = %s", (args.collected_date,))
        with conn.cursor() as cur:
            cur.executemany(insert_sql("stg.station_master", COLUMNS), good)
            cur.executemany(insert_sql("stg.station_master_quarantine", QUARANTINE_COLUMNS), bad)
    print("적재 완료")


if __name__ == "__main__":
    main()