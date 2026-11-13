"""상가(상권)정보 레이크 zip의 서울 CSV를 파싱만 해서 raw.sbiz_store에 원천 모양 그대로 넣는다 (결정 42, 60, 78).

행 하나 = jsonb 한 칸(헤더 이름: 문자열 그대로). 서울 전체 업소, 모든 열. 업종 고르기(옛 MF3 범위), 정제, 검증, 격리는
dbt가 한다(design_remaining_work.md 2.2). 여기서 하는 검사는 파일을 읽을 수 있는지뿐이다:
zip 안 서울 CSV가 정확히 하나(MF1), 0행이 아님(MF1), 모든 행의 열 수가 헤더와 같음(깨진 행이면 jsonb 키를 정할 수 없다).
헤더가 옛 39열과 같은지(MF2)는 보지 않는다. 새 열은 jsonb 안에 남고, 쓰는 열이 빠졌는지는 dbt 관문이 본다(결정 60, 76).
배치는 (수집일, 원천 파일)이고, 그 배치 행을 지우고 다시 넣는다(멱등).
사용법: python -m transform.sbiz_store_raw 수집일YYYY-MM-DD
        python -m transform.sbiz_store_raw --all    레이크에 있는 모든 수집일
"""
import argparse
import csv
import datetime
import io
import zipfile

import psycopg
from psycopg.types.json import Jsonb

from common import ROOT, read_env


def read_rows(path):
    """zip 안 서울 CSV를 압축을 풀지 않고 읽어 (파일 이름, [(행 번호, 행 dict)])를 돌려준다."""
    with zipfile.ZipFile(path, metadata_encoding="cp949") as zf:  # zip 안 파일 이름은 CP949
        names = [n for n in zf.namelist() if "_서울_" in n and n.endswith(".csv")]
        if len(names) != 1:
            raise SystemExit(f"MF1 zip 안 서울 CSV가 {len(names)}개입니다: {path.name}")
        with io.TextIOWrapper(zf.open(names[0]), encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            rows = []
            for row_no, raw in enumerate(reader, start=1):
                if None in raw or None in raw.values():  # 열이 남거나 모자란 행
                    raise SystemExit(f"{names[0]} {row_no}번째 행의 열 수가 헤더({len(reader.fieldnames)}개)와 다릅니다")
                rows.append((row_no, raw))
    if not rows:
        raise SystemExit(f"MF1 서울 CSV가 0행입니다: {names[0]}")
    return names[0], rows


def load(conn, day):
    folder = ROOT / "lake" / "sbiz_store" / f"collected_date={day}"
    zips = sorted(folder.glob("*.zip"))
    if len(zips) != 1:
        raise SystemExit(f"레이크에 zip이 정확히 하나가 아닙니다({len(zips)}개): {folder}")
    source_file, rows = read_rows(zips[0])
    with conn.transaction():  # 배치 하나가 한 트랜잭션: 지우고 넣기가 함께 되거나 함께 안 된다
        conn.execute("DELETE FROM raw.sbiz_store WHERE collected_date = %s AND source_file = %s", (day, source_file))
        with conn.cursor().copy("COPY raw.sbiz_store (collected_date, source_file, row_no, raw) FROM STDIN") as copy:
            for row_no, raw in rows:
                copy.write_row((day, source_file, row_no, Jsonb(raw)))
    print(f"상가정보 원본 {day} {source_file}: {len(rows)}행 적재")
    return len(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true")
    parser.add_argument("collected_date", nargs="?", type=datetime.date.fromisoformat)
    args = parser.parse_args()
    if args.all:
        days = sorted(datetime.date.fromisoformat(p.name.split("=")[1])
                      for p in (ROOT / "lake" / "sbiz_store").glob("collected_date=*"))
    elif args.collected_date:
        days = [args.collected_date]
    else:
        parser.error("수집일을 주거나 --all을 쓰세요")
    with psycopg.connect(read_env("DATABASE_URL"), autocommit=True) as conn:
        for day in days:
            load(conn, day)


if __name__ == "__main__":
    main()
