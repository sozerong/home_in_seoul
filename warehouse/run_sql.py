"""warehouse/sql/의 SQL 파일 하나를 DATABASE_URL의 DB에 그대로 실행한다. Airflow 컨테이너에는 psql이 없어서 쓴다.

파일 안의 BEGIN/COMMIT과 RAISE가 그대로 동작하도록 autocommit으로 연결한다. 오류가 나면 종료 코드 1로 멈춘다.
사용법: python -m warehouse.run_sql warehouse/sql/01_stg_rent.sql (스테이징 DDL 01~06. wh·mart는 dbt가 만든다)
"""
import sys
import time
from pathlib import Path

import psycopg

from common import ROOT, read_env


def main():
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    path = ROOT / sys.argv[1]
    sql = path.read_text(encoding="utf-8")
    if "\\gexec" in sql or "\\set" in sql:
        raise SystemExit(f"psql 전용 명령이 들어 있어 이 실행기로는 돌릴 수 없습니다: {path.name}")
    started = time.perf_counter()
    with psycopg.connect(read_env("DATABASE_URL"), autocommit=True) as conn:
        conn.execute(sql)  # 매개변수가 없으므로 여러 문장을 한 번에 보낸다
    print(f"{path.name} 실행 완료 ({time.perf_counter() - started:.1f}초)")


if __name__ == "__main__":
    main()
