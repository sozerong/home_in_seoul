"""서울교통공사 지하철 혼잡도 레이크 XML을 파싱만 해서 raw.congestion에 원천 모양 그대로 넣는다 (결정 42, 60).

row 하나 = jsonb 한 칸(자식 태그: 텍스트, 공백 그대로). 이름 대응·정제·검증·격리는 dbt의 stg_dbt 모델이 한다(설계서 2.3, 2.5).
여기서 하는 검사는 둘뿐이다: 파일이 없으면 실패, row가 0개면 실패. 필드 검사(CF1)는 SQL 쪽 테스트다.
처리 단위는 수집일 하나이고, 그 수집일 행을 지우고 다시 넣는다(멱등).
사용법: python -m transform.congestion_raw 수집일YYYY-MM-DD
"""
import argparse
import datetime
import xml.etree.ElementTree as ET

import psycopg
from psycopg.types.json import Jsonb

from common import ROOT, read_env


def read_rows(day):
    folder = ROOT / "lake" / "seoul_subway_congestion" / f"collected_date={day}"
    files = sorted(folder.glob("subwConfusion_p*.xml"))
    if not files:
        raise SystemExit(f"레이크에 파일이 없습니다: {folder / 'subwConfusion_p*.xml'}")
    for path in files:
        for row_no, element in enumerate(ET.parse(path).getroot().iter("row"), start=1):
            yield path.name, row_no, {child.tag: child.text or "" for child in element}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("collected_date", type=datetime.date.fromisoformat)
    day = parser.parse_args().collected_date
    rows = [(day, name, row_no, Jsonb(raw)) for name, row_no, raw in read_rows(day)]
    if not rows:
        raise SystemExit(f"row가 0개입니다 (수집일 {day})")
    with psycopg.connect(read_env("DATABASE_URL")) as conn:  # 정상 종료면 커밋, 예외면 롤백
        conn.execute("DELETE FROM raw.congestion WHERE collected_date = %s", (day,))
        with conn.cursor().copy("COPY raw.congestion (collected_date, source_file, row_no, raw) FROM STDIN") as copy:
            for row in rows:
                copy.write_row(row)
    print(f"혼잡도 원본 (수집일 {day}): {len(rows)}행 적재")


if __name__ == "__main__":
    main()
