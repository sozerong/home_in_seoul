"""전월세 실거래가(오피스텔, 연립다세대) 레이크 XML을 정제·검증해 stg.rent와 stg.rent_quarantine에 넣는다.

규칙은 docs/rules.md 1절. 처리 단위(출처, 수집일, 구, 계약년월)마다 기존 행을 지우고 다시 넣는다(멱등).
사용법: python -m transform.rent {offi|rh} 수집일YYYY-MM-DD 구코드5자리 계약년월YYYYMM
예:     python -m transform.rent offi 2026-10-21 11680 202609
"""
import argparse
import collections
import csv
import datetime
import io
import re
import xml.etree.ElementTree as ET
import zipfile

import psycopg
from psycopg.types.json import Jsonb

from common import ROOT, read_env

COMMON_FIELDS = {"buildYear", "contractTerm", "contractType", "dealDay", "dealMonth", "dealYear", "deposit",
                 "excluUseAr", "floor", "jibun", "monthlyRent", "preDeposit", "preMonthlyRent", "sggCd",
                 "umdNm", "useRRRight"}
EXPECTED_FIELDS = {"offi": COMMON_FIELDS | {"offiNm", "sggNm"},     # F1: data_sources.md 1절
                   "rh": COMMON_FIELDS | {"mhouseNm", "houseType"}}  # F1: data_sources.md 2절
NAME_FIELD = {"offi": "offiNm", "rh": "mhouseNm"}                   # C1
REQUIRED = ("deposit", "monthlyRent", "dealYear", "dealMonth", "dealDay", "excluUseAr", "umdNm", "jibun")  # V1
JIBUN = re.compile(r"(\d+)(?:-(\d+))?")                              # V9: 본번 또는 본번-부번
MAX_QUARANTINE_RATIO = 0.05                                          # F2

META = ["source_type", "collected_date", "source_file", "item_no", "deal_ymd", "sgg_cd"]
RENT_COLUMNS = META + ["umd_nm", "bjd_cd", "jibun", "pnu", "building_name", "house_type", "build_year", "floor",
                       "excl_area", "deal_date", "deposit", "monthly_rent", "contract_type", "contract_term",
                       "pre_deposit", "pre_monthly_rent", "use_rr_right"]
QUARANTINE_COLUMNS = META + ["reasons", "raw"]


def load_bjd_codes():
    """가장 최근 상가정보 zip의 서울 파일에서 (시군구코드, 법정동명) -> 법정동코드 대응표를 만든다 (C6)."""
    # ponytail: 실행마다 상가정보 서울 CSV(약 300MB)를 읽는다. 단계 5 지역 차원 테이블이 생기면 그것으로 바꾼다.
    path = sorted(ROOT.glob("lake/sbiz_store/collected_date=*/*.zip"))[-1]
    with zipfile.ZipFile(path, metadata_encoding="cp949") as zf:
        name = next(n for n in zf.namelist() if "서울" in n and n.endswith(".csv"))
        rows = csv.DictReader(io.TextIOWrapper(zf.open(name), encoding="utf-8", newline=""))
        return {(r["시군구코드"], r["법정동명"]): r["법정동코드"] for r in rows}


def read_items(path):
    """XML 파일의 item을 {필드 이름: 원본 문자열} 목록으로 읽는다."""
    return [{child.tag: child.text or "" for child in item} for item in ET.parse(path).getroot().iter("item")]


def clean(raw, kind, deal_ymd, sgg_cd, bjd_codes):
    """item 하나에 정제(C1~C6)와 검증(V1~V9)을 적용해 (정제된 값, 어긋난 규칙 번호 목록)을 돌려준다.

    정제 규칙을 적용하지 못한 값(예: 숫자 칸에 숫자가 아닌 값)은 그 정제 규칙 번호(C3, C4, C5)를 사유로 남긴다.
    """
    v = {k: s.strip() or None for k, s in raw.items()}  # C2: 공백 한 칸도 None
    reasons = set()

    def number(field, rule, parse=int):
        if v[field] is None:
            return None
        try:
            return parse(v[field].replace(",", ""))  # C3: 쉼표 제거
        except ValueError:
            reasons.add(rule)
            return None

    row = {
        "umd_nm": v["umdNm"],
        "jibun": v["jibun"],
        "building_name": v[NAME_FIELD[kind]],  # C1
        "house_type": v.get("houseType"),
        "build_year": number("buildYear", "C5"),
        "floor": number("floor", "C5"),
        "excl_area": number("excluUseAr", "C5", float),
        "deposit": number("deposit", "C3"),
        "monthly_rent": number("monthlyRent", "C3"),
        "pre_deposit": number("preDeposit", "C3"),
        "pre_monthly_rent": number("preMonthlyRent", "C3"),
        "contract_type": v["contractType"],
        "contract_term": v["contractTerm"],
        "use_rr_right": v["useRRRight"],
        "deal_date": None,
        "bjd_cd": None,
        "pnu": None,
    }

    if any(v[f] is None for f in REQUIRED):
        reasons.add("V1")

    y, m, d = number("dealYear", "C4"), number("dealMonth", "C4"), number("dealDay", "C4")
    if None not in (y, m, d):
        try:
            row["deal_date"] = datetime.date(y, m, d)  # C4
        except ValueError:
            reasons.add("V3")
        if f"{y:04d}{m:02d}" != deal_ymd:
            reasons.add("V2")

    if (row["deposit"] or 0) < 0 or (row["monthly_rent"] or 0) < 0:
        reasons.add("V4")
    if row["excl_area"] is not None and not 0 < row["excl_area"] <= 300:
        reasons.add("V5")
    if row["floor"] is not None and not -5 <= row["floor"] <= 80:
        reasons.add("V6")
    if row["contract_type"] not in (None, "신규", "갱신"):
        reasons.add("V7")

    if v["umdNm"] is not None:
        row["bjd_cd"] = bjd_codes.get((sgg_cd, v["umdNm"]))  # C6
        if row["bjd_cd"] is None:
            reasons.add("V8")
    if v["jibun"] is not None:
        match = JIBUN.fullmatch(v["jibun"])
        if match is None:
            reasons.add("V9")
        elif row["bjd_cd"] is not None:
            main_no, sub_no = match.groups()
            row["pnu"] = row["bjd_cd"] + "1" + main_no.zfill(4) + (sub_no or "0").zfill(4)  # C6: 대지구분 1

    return row, sorted(reasons)


def insert_sql(table, columns):
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(f'%({c})s' for c in columns)})"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=EXPECTED_FIELDS)
    parser.add_argument("collected_date", type=datetime.date.fromisoformat)
    parser.add_argument("lawd_cd")
    parser.add_argument("deal_ymd")
    args = parser.parse_args()

    folder = ROOT / "lake" / f"molit_{args.kind}_rent" / f"collected_date={args.collected_date}"
    files = sorted(folder.glob(f"{args.lawd_cd}_{args.deal_ymd}_p*.xml"))
    if not files:
        raise SystemExit(f"레이크에 파일이 없습니다: {folder / f'{args.lawd_cd}_{args.deal_ymd}_p*.xml'}")

    bjd_codes = load_bjd_codes()
    good, bad = [], []
    for path in files:
        for item_no, raw in enumerate(read_items(path), start=1):
            if set(raw) != EXPECTED_FIELDS[args.kind]:  # F1
                diff = sorted(set(raw) ^ EXPECTED_FIELDS[args.kind])
                raise SystemExit(f"F1 스키마 변경: {path.name} {item_no}번째 item, 다른 필드 {diff}")
            meta = {"source_type": args.kind, "collected_date": args.collected_date, "source_file": path.name,
                    "item_no": item_no, "deal_ymd": args.deal_ymd, "sgg_cd": args.lawd_cd}
            row, reasons = clean(raw, args.kind, args.deal_ymd, args.lawd_cd, bjd_codes)
            if reasons:
                bad.append({**meta, "reasons": reasons, "raw": Jsonb(raw)})
            else:
                good.append({**meta, **row})

    total = len(good) + len(bad)
    if total == 0:
        raise SystemExit("item이 0개입니다")
    ratio = len(bad) / total
    counts = collections.Counter(r for b in bad for r in b["reasons"])
    print(f"{args.kind} {args.lawd_cd}_{args.deal_ymd} (수집일 {args.collected_date}): "
          f"item {total}, 통과 {len(good)}, 격리 {len(bad)} ({ratio:.2%}), 사유 {dict(sorted(counts.items()))}")
    if ratio > MAX_QUARANTINE_RATIO:  # F2: DB에 아무것도 쓰지 않고 실패
        raise SystemExit(f"F2 격리 비율 {ratio:.2%}가 기준 {MAX_QUARANTINE_RATIO:.0%}를 넘었습니다")

    key = {k: getattr(args, k) for k in ("kind", "collected_date", "deal_ymd", "lawd_cd")}
    with psycopg.connect(read_env("DATABASE_URL")) as conn:  # 블록이 정상으로 끝나면 커밋, 예외면 롤백
        for table in ("stg.rent", "stg.rent_quarantine"):
            conn.execute(f"DELETE FROM {table} WHERE source_type = %(kind)s AND collected_date = %(collected_date)s"
                         f" AND deal_ymd = %(deal_ymd)s AND sgg_cd = %(lawd_cd)s", key)
        with conn.cursor() as cur:
            cur.executemany(insert_sql("stg.rent", RENT_COLUMNS), good)
            cur.executemany(insert_sql("stg.rent_quarantine", QUARANTINE_COLUMNS), bad)
    print("적재 완료")


if __name__ == "__main__":
    main()
