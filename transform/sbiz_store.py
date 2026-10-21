"""상가(상권)정보 레이크 zip의 서울 CSV를 정제·검증해 stg.sbiz_store와 stg.sbiz_store_quarantine에 넣는다.

규칙은 docs/rules_sbiz_store.md. 범위는 생활 편의 필수 5종 소분류 6코드만(결정 초안 1).
처리 단위는 수집일 하나(zip 하나)이고, 그 수집일의 기존 행을 지우고 다시 넣는다(멱등).
사용법: python -m transform.sbiz_store 수집일YYYY-MM-DD
예:     python -m transform.sbiz_store 2026-10-21
"""
import argparse
import collections
import csv
import datetime
import io
import re
import zipfile

import psycopg
from psycopg.types.json import Jsonb

from common import ROOT, read_env
from transform.rent import insert_sql

HEADER = ["상가업소번호", "상호명", "지점명", "상권업종대분류코드", "상권업종대분류명", "상권업종중분류코드",
          "상권업종중분류명", "상권업종소분류코드", "상권업종소분류명", "표준산업분류코드", "표준산업분류명",
          "시도코드", "시도명", "시군구코드", "시군구명", "행정동코드", "행정동명", "법정동코드", "법정동명",
          "지번코드", "대지구분코드", "대지구분명", "지번본번지", "지번부번지", "지번주소", "도로명코드", "도로명",
          "건물본번지", "건물부번지", "건물관리번호", "건물명", "도로명주소", "구우편번호", "신우편번호",
          "동정보", "층정보", "호정보", "경도", "위도"]  # MF2: 39열
ESSENTIAL = {"G20405": "편의점", "G20404": "슈퍼마켓", "G21501": "약국", "Q10201": "내과/소아과 의원",
             "S20901": "세탁소", "S20902": "셀프 빨래방"}  # MF3, 범위 A (question.md 3절)
REQUIRED = ("상가업소번호", "상권업종소분류코드", "상권업종소분류명", "시도코드", "시군구코드",
            "행정동코드", "법정동코드", "경도", "위도")  # MV1
LON_RANGE = (126.7, 127.3)    # MV3
LAT_RANGE = (37.4, 37.8)      # MV3
MAX_QUARANTINE_RATIO = 0.05   # MF4
MAX_SHRINK_RATIO = 0.10       # MF5
COLUMNS = ["collected_date", "base_ym", "source_file", "row_no", "store_id", "store_name", "sub_cd", "sub_nm",
           "sido_cd", "sgg_cd", "adm_cd", "bjd_cd", "lon", "lat"]
QUARANTINE_COLUMNS = ["collected_date", "source_file", "row_no", "reasons", "raw"]


def clean(raw):
    """행 하나에 정제(MC2, MC3)와 행 단위 검증(MV1, MV3, MV4)을 적용해 (정제된 값, 어긋난 규칙 번호 목록)을 돌려준다."""
    v = {k: s.strip() or None for k, s in raw.items()}  # MC2: 앞뒤 공백 제거, 빈 값은 None
    reasons = set()
    if any(v[f] is None for f in REQUIRED):
        reasons.add("MV1")

    row = {"store_id": v["상가업소번호"], "store_name": v["상호명"], "sub_cd": v["상권업종소분류코드"],
           "sub_nm": v["상권업종소분류명"], "sido_cd": v["시도코드"], "sgg_cd": v["시군구코드"],
           "adm_cd": v["행정동코드"], "bjd_cd": v["법정동코드"], "lon": None, "lat": None}

    for column, field, (low, high) in (("lon", "경도", LON_RANGE), ("lat", "위도", LAT_RANGE)):
        if v[field] is None:
            continue
        try:
            row[column] = float(v[field])  # MC3
        except ValueError:
            reasons.add("MC3")
            continue
        if not low <= row[column] <= high:  # nan도 여기서 걸린다
            reasons.add("MV3")

    sgg = v["시군구코드"]
    if v["시도코드"] not in (None, "11"):
        reasons.add("MV4")
    codes = ((sgg, 5), (v["행정동코드"], 8), (v["법정동코드"], 10))
    if sgg is not None and any(c is not None and not (len(c) == n and c.isdigit() and c.startswith(sgg))
                               for c, n in codes):
        reasons.add("MV4")

    return row, sorted(reasons)


def split(rows):
    """(행 번호, 원본) 목록을 (통과 목록, 격리 목록)으로 나눈다. MV2는 여러 행을 함께 봐야 해서 여기서 본다."""
    ids = collections.Counter(raw["상가업소번호"].strip() for _, raw in rows)
    good, bad = [], []
    for row_no, raw in rows:
        row, reasons = clean(raw)
        if row["store_id"] is not None and ids[row["store_id"]] > 1:
            reasons = sorted(reasons + ["MV2"])  # MV2: 중복된 행은 모두 격리
        if reasons:
            bad.append({"row_no": row_no, "reasons": reasons, "raw": raw})
        else:
            good.append({"row_no": row_no, **row})
    return good, bad


def read_seoul(path):
    """zip 안의 서울 CSV를 압축을 풀지 않고 읽어 (파일 이름, 기준월, 필수 6코드 행 목록)을 돌려준다.

    파일 단위 검사 MF1~MF3은 6코드만이 아니라 서울 파일 전체를 본다.
    """
    with zipfile.ZipFile(path, metadata_encoding="cp949") as zf:  # zip 안 파일 이름은 CP949
        names = [n for n in zf.namelist() if "_서울_" in n and n.endswith(".csv")]
        if len(names) != 1:  # MF1
            raise SystemExit(f"MF1 zip 안 서울 CSV가 {len(names)}개입니다: {path.name}")
        match = re.search(r"_(\d{6})\.csv$", names[0])
        if match is None:
            raise SystemExit(f"파일 이름에서 기준월을 찾지 못했습니다: {names[0]}")
        with io.TextIOWrapper(zf.open(names[0]), encoding="utf-8", newline="") as f:  # 실패해도 CSV 핸들을 닫는다
            reader = csv.DictReader(f)  # MC1: 모두 문자열
            if sorted(reader.fieldnames or []) != sorted(HEADER):  # MF2
                raise SystemExit(f"MF2 스키마 변경: 다른 열 {sorted(set(reader.fieldnames or []) ^ set(HEADER))}")
            total, pairs, rows = 0, set(), []
            for row_no, raw in enumerate(reader, start=1):
                if None in raw or None in raw.values():  # MF2: 열 수가 39가 아닌 행
                    raise SystemExit(f"MF2 {row_no}번째 행의 열 수가 {len(HEADER)}개가 아닙니다")
                total += 1
                code = raw["상권업종소분류코드"].strip()
                pairs.add((code, raw["상권업종소분류명"].strip()))
                if code in ESSENTIAL:
                    rows.append((row_no, raw))
    if total == 0:  # MF1
        raise SystemExit(f"MF1 서울 CSV가 0행입니다: {names[0]}")
    missing = sorted(set(ESSENTIAL.items()) - pairs)
    if missing:  # MF3
        raise SystemExit(f"MF3 필수 업종 (코드, 이름)이 없습니다. 분류 체계가 바뀌었는지 보세요: {missing}")
    return names[0], match.group(1), rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("collected_date", type=datetime.date.fromisoformat)
    args = parser.parse_args()

    folder = ROOT / "lake" / "sbiz_store" / f"collected_date={args.collected_date}"
    zips = sorted(folder.glob("*.zip"))
    if len(zips) != 1:
        raise SystemExit(f"레이크에 zip이 정확히 하나가 아닙니다({len(zips)}개): {folder}")

    source_file, base_ym, rows = read_seoul(zips[0])
    good, bad = split(rows)
    meta = {"collected_date": args.collected_date, "source_file": source_file}
    good = [{**meta, "base_ym": base_ym, **r} for r in good]
    bad = [{**meta, **b, "raw": Jsonb(b["raw"])} for b in bad]

    total = len(good) + len(bad)  # MF3을 지났으면 0이 아니다
    ratio = len(bad) / total
    counts = collections.Counter(r for b in bad for r in b["reasons"])
    print(f"상가정보 서울 {base_ym} (수집일 {args.collected_date}): 필수 6코드 행 {total}, 통과 {len(good)}, "
          f"격리 {len(bad)} ({ratio:.2%}), 사유 {dict(sorted(counts.items()))}")
    if ratio > MAX_QUARANTINE_RATIO:  # MF4: DB에 아무것도 쓰지 않고 실패
        raise SystemExit(f"MF4 격리 비율 {ratio:.2%}가 기준 {MAX_QUARANTINE_RATIO:.0%}를 넘었습니다")

    with psycopg.connect(read_env("DATABASE_URL")) as conn:  # 블록이 정상으로 끝나면 커밋, 예외면 롤백
        previous = conn.execute(
            "SELECT collected_date, count(*) FROM stg.sbiz_store WHERE collected_date < %s"
            " GROUP BY collected_date ORDER BY collected_date DESC LIMIT 1", (args.collected_date,)).fetchone()
        if previous and len(good) < previous[1] * (1 - MAX_SHRINK_RATIO):  # MF5
            raise SystemExit(f"MF5 행 수 급감: 직전 {previous[0]} {previous[1]}행 -> 이번 {len(good)}행")
        for table in ("stg.sbiz_store", "stg.sbiz_store_quarantine"):
            conn.execute(f"DELETE FROM {table} WHERE collected_date = %s", (args.collected_date,))
        with conn.cursor() as cur:
            cur.executemany(insert_sql("stg.sbiz_store", COLUMNS), good)
            cur.executemany(insert_sql("stg.sbiz_store_quarantine", QUARANTINE_COLUMNS), bad)
    print("적재 완료")


if __name__ == "__main__":
    main()