"""전월세 실거래가(오피스텔, 연립다세대) 레이크 XML을 파싱만 해서 raw.rent에 원천 모양 그대로 넣는다 (결정 42, 60, 72).

item 하나 = jsonb 한 칸(자식 태그: 텍스트, 공백 그대로). 정제(C1~C6)·검증(V1~V9)·격리·F1·F2는 dbt의 stg_dbt.rent_* 모델과
테스트가 한다(docs/rules.md 1절). 여기서 하는 검사는 둘뿐이다: 파일이 없으면 실패, item이 0개면 실패.
처리 단위는 (출처, 수집일, 구, 계약년월)이고, 그 단위 행을 지우고 다시 넣는다(멱등).
사용법: python -m transform.rent_raw {offi|rh} 수집일YYYY-MM-DD 구코드5자리 계약년월YYYYMM
        python -m transform.rent_raw --all    레이크에 있는 모든 처리 단위 (처음 채울 때)
"""
import argparse
import datetime
import re
import xml.etree.ElementTree as ET

import psycopg
from psycopg.types.json import Jsonb

from common import ROOT, read_env

KINDS = ("offi", "rh")


def load(conn, kind, day, gu, ym):
    folder = ROOT / "lake" / f"molit_{kind}_rent" / f"collected_date={day}"
    files = sorted(folder.glob(f"{gu}_{ym}_p*.xml"))
    if not files:
        raise SystemExit(f"레이크에 파일이 없습니다: {folder / f'{gu}_{ym}_p*.xml'}")
    rows = [(kind, day, gu, ym, path.name, item_no, Jsonb(raw))
            for path in files
            for item_no, raw in enumerate(({c.tag: c.text or "" for c in item}
                                           for item in ET.parse(path).getroot().iter("item")), start=1)]
    if not rows:
        raise SystemExit(f"item이 0개입니다: {kind} {gu}_{ym} (수집일 {day})")
    with conn.transaction():  # 단위 하나가 한 트랜잭션: 지우고 넣기가 함께 되거나 함께 안 된다
        conn.execute("DELETE FROM raw.rent WHERE source_type = %s AND collected_date = %s AND sgg_cd = %s "
                     "AND deal_ymd = %s", (kind, day, gu, ym))
        with conn.cursor().copy("COPY raw.rent (source_type, collected_date, sgg_cd, deal_ymd, source_file, item_no, "
                                "raw) FROM STDIN") as copy:
            for row in rows:
                copy.write_row(row)
    return len(rows)


def lake_units():
    """레이크의 모든 처리 단위 (출처, 수집일, 구, 계약년월)."""
    units = set()
    for kind in KINDS:
        for path in ROOT.glob(f"lake/molit_{kind}_rent/collected_date=*/*_p*.xml"):
            gu, ym = re.fullmatch(r"(\d{5})_(\d{6})_p\d+\.xml", path.name).groups()
            units.add((kind, datetime.date.fromisoformat(path.parent.name.split("=")[1]), gu, ym))
    return sorted(units)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true")
    parser.add_argument("kind", nargs="?", choices=KINDS)
    parser.add_argument("collected_date", nargs="?", type=datetime.date.fromisoformat)
    parser.add_argument("lawd_cd", nargs="?")
    parser.add_argument("deal_ymd", nargs="?")
    args = parser.parse_args()
    units = lake_units() if args.all else [(args.kind, args.collected_date, args.lawd_cd, args.deal_ymd)]
    if None in units[0]:
        parser.error("처리 단위 네 값을 모두 주거나 --all을 쓰세요")
    with psycopg.connect(read_env("DATABASE_URL"), autocommit=True) as conn:
        total = sum(load(conn, *unit) for unit in units)
    print(f"전월세 원본: 처리 단위 {len(units)}개, item {total}개 적재")


if __name__ == "__main__":
    main()
