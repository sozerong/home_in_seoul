"""서울 생활이동(행정동) 레이크 zip을 정제·검증해 stg.living_migration과 stg.living_migration_quarantine에 넣는다.

규칙은 docs/rules_living_migration.md. 범위 C(HW, 월~금, 도착 07·08·09시, 출발·도착 모두 서울 행정동)만 넣는다.
처리 단위(수집일, 대상연월)마다 두 테이블의 기존 행을 지우고 다시 넣는다(멱등).
사용법: python -m transform.living_migration 수집일YYYY-MM-DD 대상연월YYYYMM
예:     python -m transform.living_migration 2026-10-21 202607
"""
import argparse
import collections
import csv
import datetime
import io
import xml.etree.ElementTree as ET
import zipfile
import math

import psycopg
from psycopg.types.json import Jsonb

from common import ROOT, read_env

HEADER = ["대상연월", "요일", "도착시간", "출발 행정동 코드", "도착 행정동 코드", "성별", "나이", "이동유형",
          "평균 이동 시간(분)", "이동인구(합)"]                          # F1
HOURS = ("07", "08", "09")                                              # 범위 C, F3
WEEKDAYS = {"월", "화", "수", "목", "금", "토", "일"}                   # V3
WORKDAYS = {"월", "화", "수", "목", "금"}                               # 범위 C
SEXES = {"M", "F"}                                                      # V5
AGES = {"00"} | {str(a) for a in range(10, 81, 5)}                      # V5: 00, 10, 15, ..., 80
MOVE_TYPES = {"HW", "HE", "EW", "EE", "EH", "WW", "WE", "WH", "HH"}     # V5
MAX_MINUTES = 240                                                       # V6
MAX_QUARANTINE_RATIO = 0.05                                             # F2
LAKE = ROOT / "lake" / "seoul_living_migration"
SHEET_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
GOOD_COLUMNS = ["collected_date", "target_ym", "dest_cd", "origin_cd", "weekday", "arrive_hour", "sex", "age",
                "avg_minutes", "population", "source_file", "row_no"]
QUARANTINE_COLUMNS = ["collected_date", "target_ym", "source_file", "row_no", "reasons", "raw"]


def load_codes():
    """가장 최근 코드표 xlsx의 읍면동 열(C열)에서 코드 집합을 만든다 (V4)."""
    path = sorted(LAKE.glob("collected_date=*/서울생활이동데이터_행정동코드_*.xlsx"))[-1]
    with zipfile.ZipFile(path) as zf:
        sheet = ET.fromstring(zf.read("xl/worksheets/sheet1.xml"))
    # 코드 칸은 숫자 칸(t 속성 없음)이고 머리글 칸은 문자열 칸(t="s")이라 저절로 빠진다
    return {c.find(f"{SHEET_NS}v").text for c in sheet.iter(f"{SHEET_NS}c")
            if c.get("r").startswith("C") and c.get("t") is None}



def out_of_scope(raw, codes):
    """올바른 값만 보고 범위 C 밖으로 확정되면 True. 값이 깨진 열은 판정에 쓰지 않고 검증으로 보낸다."""
    if raw["요일"] in WEEKDAYS and raw["요일"] not in WORKDAYS:
        return True
    if raw["이동유형"] in MOVE_TYPES and raw["이동유형"] != "HW":
        return True
    return any(code in codes and not (len(code) == 7 and code.startswith("11"))
               for code in (raw["출발 행정동 코드"], raw["도착 행정동 코드"]))



def clean(raw, target_ym, hour, codes):
    """행 하나에 정제(C2, C3)와 검증(V1~V7)을 적용해 (정제된 값, 어긋난 규칙 번호 목록)을 돌려준다.

    hour는 파일 이름의 도착시간("07" 등). 숫자로 바꾸지 못한 값은 C3을 사유로 남긴다.
    """
    reasons = set()

    def number(field, parse):
        if raw[field] == "":
            return None  # 빈 값은 V1이 잡는다
        try:
            x = parse(raw[field])
            if math.isfinite(x):  # float("nan"), float("inf")는 숫자로 치지 않는다
                return x
        except ValueError:
            pass
        reasons.add("C3")
        return None

    minutes = number("평균 이동 시간(분)", int)                                                   # C3
    population = None if raw["이동인구(합)"] == "*" else number("이동인구(합)", float)            # C2, C3

    if "" in raw.values():
        reasons.add("V1")
    if raw["대상연월"] != target_ym:
        reasons.add("V2")
    if raw["요일"] not in WEEKDAYS or raw["도착시간"] != hour:
        reasons.add("V3")
    if raw["출발 행정동 코드"] not in codes or raw["도착 행정동 코드"] not in codes:
        reasons.add("V4")
    if raw["성별"] not in SEXES or raw["나이"] not in AGES or raw["이동유형"] not in MOVE_TYPES:
        reasons.add("V5")
    if minutes is not None and not 1 <= minutes <= MAX_MINUTES:
        reasons.add("V6")
    if population is not None and population < 3:
        reasons.add("V7")

    row = {"dest_cd": raw["도착 행정동 코드"], "origin_cd": raw["출발 행정동 코드"], "weekday": raw["요일"],
           "arrive_hour": int(hour), "sex": raw["성별"], "age": raw["나이"], "avg_minutes": minutes,
           "population": population}
    return row, sorted(reasons)



def read_month(path, target_ym, codes):
    """zip 안 07·08·09시 CSV를 읽어 (통과 행 목록, 격리 행 목록, 건수)를 돌려준다.

    F1·F3을 어기면 바로 멈춘다. C4(완전 중복은 하나만)와 V8(키는 같은데 값이 다름 → 모두 격리)을 여기서 한다.
    """
    good, conflicts, bad, counts = {}, {}, [], collections.Counter()
    with zipfile.ZipFile(path, metadata_encoding="cp949") as zf:
        for hour in HOURS:
            names = [n for n in zf.namelist() if n.endswith(f"_{hour}시.csv")]
            if len(names) != 1:  # F3
                raise SystemExit(f"F3 {path.name} 안에 {hour}시 CSV가 {len(names)}개입니다")
            name = names[0]
            with zf.open(name) as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="cp949", newline=""))  # C1
                header = next(reader)
                if header != HEADER:  # F1
                    raise SystemExit(f"F1 스키마 변경: {name} 헤더 {header}")
                for row_no, values in enumerate(reader, start=2):
                    counts["읽은 행"] += 1
                    if len(values) != len(HEADER):
                        bad.append({"source_file": name, "row_no": row_no, "reasons": ["V1"], "raw": values})
                        continue
                    raw = dict(zip(HEADER, values))
                    if out_of_scope(raw, codes):
                        counts["범위 밖"] += 1
                        continue
                    row, reasons = clean(raw, target_ym, hour, codes)
                    if reasons:
                        bad.append({"source_file": name, "row_no": row_no, "reasons": reasons, "raw": raw})
                        continue
                    item = {"source_file": name, "row_no": row_no, **row}
                    key = tuple(values[1:8])  # 요일 ~ 이동유형
                    if key in conflicts:
                        conflicts[key].append((item, values))
                    elif key not in good:
                        good[key] = (item, values)
                    elif good[key][1] == values:  # C4
                        counts["C4 완전 중복"] += 1
                    else:  # V8
                        conflicts[key] = [good.pop(key), (item, values)]
    for pairs in conflicts.values():
        for item, values in pairs:
            bad.append({"source_file": item["source_file"], "row_no": item["row_no"], "reasons": ["V8"],
                        "raw": dict(zip(HEADER, values))})
    return [item for item, _ in good.values()], bad, counts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("collected_date", type=datetime.date.fromisoformat)
    parser.add_argument("target_ym")
    args = parser.parse_args()

    path = LAKE / f"collected_date={args.collected_date}" / f"생활이동_행정동_{args.target_ym}.zip"
    if not path.exists():
        raise SystemExit(f"레이크에 파일이 없습니다: {path}")
    good, bad, counts = read_month(path, args.target_ym, load_codes())

    total = len(good) + len(bad)
    if total == 0:
        raise SystemExit("범위 안 행이 0개입니다")
    ratio = len(bad) / total
    reasons = collections.Counter(r for b in bad for r in b["reasons"])
    print(f"생활이동 {args.target_ym} (수집일 {args.collected_date}): {dict(counts)}, "
          f"통과 {len(good)}, 격리 {len(bad)} ({ratio:.2%}), 사유 {dict(sorted(reasons.items()))}")
    if ratio > MAX_QUARANTINE_RATIO:  # F2: DB에 아무것도 쓰지 않고 실패
        raise SystemExit(f"F2 격리 비율 {ratio:.2%}가 기준 {MAX_QUARANTINE_RATIO:.0%}를 넘었습니다")

    unit = [args.collected_date, args.target_ym]
    with psycopg.connect(read_env("DATABASE_URL")) as conn:  # 블록이 정상으로 끝나면 커밋, 예외면 롤백
        for table in ("stg.living_migration", "stg.living_migration_quarantine"):
            conn.execute(f"DELETE FROM {table} WHERE collected_date = %s AND target_ym = %s", unit)
        with conn.cursor() as cur:
            with cur.copy(f"COPY stg.living_migration ({', '.join(GOOD_COLUMNS)}) FROM STDIN") as copy:
                for row in good:
                    copy.write_row(unit + [row[c] for c in GOOD_COLUMNS[2:]])
            with cur.copy(f"COPY stg.living_migration_quarantine ({', '.join(QUARANTINE_COLUMNS)}) FROM STDIN") as copy:
                for b in bad:
                    copy.write_row(unit + [b["source_file"], b["row_no"], b["reasons"], Jsonb(b["raw"])])
    print("적재 완료")


if __name__ == "__main__":
    main()