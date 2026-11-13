"""5절 체크포인트: stg_dbt.raw_shape가 같은 수집일은 한 줄로, 키가 바뀐 배치는 새 해시와 그 수집일로 보이는지 (결정 82).

raw를 고치지 않고 raw_t 스키마에 뷰를 만든다. 혼잡도 뷰에는 (1) 가장 최근 수집일 D를 한 번 더 붙이고(같은 수집일을 두 번 넣은 상황)
(2) D를 2099-01-01로 복사하면서 키 LINE을 LINE_NM으로 바꾼 배치를 붙인다. raw_shape만 raw_schema=raw_t로 빌드해 본다.
실행(Airflow 컨테이너): python -m measure.raw_shape_check
"""
import json
import subprocess

import psycopg

from common import read_env

DBT = "/opt/airflow/dbt-venv/bin/dbt"
DBT_DIR = "/opt/project/warehouse/dbt"


def main():
    with psycopg.connect(read_env("DATABASE_URL"), autocommit=True) as conn:
        day = conn.execute("SELECT max(collected_date) FROM raw.congestion").fetchone()[0]
        try:
            conn.execute("DROP SCHEMA IF EXISTS raw_t CASCADE; CREATE SCHEMA raw_t")
            conn.execute("CREATE VIEW raw_t.rent AS SELECT * FROM raw.rent")
            conn.execute("CREATE VIEW raw_t.sbiz_store AS SELECT * FROM raw.sbiz_store")
            conn.execute(f"""CREATE VIEW raw_t.congestion AS
                SELECT * FROM raw.congestion
                UNION ALL SELECT * FROM raw.congestion WHERE collected_date = '{day}'
                UNION ALL SELECT '2099-01-01'::date, source_file, row_no, (raw - 'LINE') || jsonb_build_object('LINE_NM', raw -> 'LINE'),
                                 loaded_at FROM raw.congestion WHERE collected_date = '{day}'""")
            out = subprocess.run([DBT, "run", "--select", "raw_shape", "--vars", json.dumps({"raw_schema": "raw_t", "build_suffix": "_t"}),
                                  "--target-path", "target/t", "--log-path", "logs/t"], cwd=DBT_DIR, capture_output=True, text=True)
            assert out.returncode == 0, out.stdout[-2000:]
            real = conn.execute("SELECT collected_date, keyset_hash, items FROM stg_dbt.raw_shape WHERE source = 'congestion' "
                                "ORDER BY 1").fetchall()
            test = conn.execute("SELECT collected_date, keyset_hash, items, 'LINE_NM' = ANY(keys) FROM stg_dbt_t.raw_shape "
                                "WHERE source = 'congestion' ORDER BY 1").fetchall()
            print("실제 raw_shape(혼잡도):", [(str(d), h[:8], n) for d, h, n in real])
            print("시험 raw_shape(혼잡도):", [(str(d), h[:8], n, new) for d, h, n, new in test])
            d_rows = [r for r in test if r[0] == day]
            new_rows = [r for r in test if str(r[0]) == "2099-01-01"]
            real_d = [r for r in real if r[0] == day]
            assert len(d_rows) == len(real_d) and [r[2] for r in d_rows] == [2 * r[2] for r in real_d], "같은 수집일이 한 줄로 모이지 않음"
            assert len(new_rows) == 1 and new_rows[0][3] and new_rows[0][1] not in {r[1] for r in real}, "키 변경이 새 해시로 안 나옴"
            print(f"통과: {day}는 {len(d_rows)}줄(items 두 배), 2099-01-01은 새 해시 {new_rows[0][1][:8]}")
        finally:
            conn.execute("DROP SCHEMA IF EXISTS raw_t CASCADE; DROP SCHEMA IF EXISTS stg_dbt_t CASCADE")


if __name__ == "__main__":
    main()
