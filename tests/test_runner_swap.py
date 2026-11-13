"""transform/spark_sql_runner.swap 시험 (F-13 처방 A). 실제 표를 건드리지 않고 임시 스키마 swap_test에서 돈다.
1) 표를 읽는 뷰가 있어도 교체되고, 뷰가 새 행을 본다. 2) 첫 표가 0행이면 실패하고 아무것도 바뀌지 않는다(두 번째 표 포함).
실행(Airflow 컨테이너): /opt/airflow/spark-venv/bin/python -m tests.test_runner_swap
"""
import psycopg

from common import read_env
from transform.spark_sql_runner import swap

URL = read_env("DATABASE_URL")
with psycopg.connect(URL, autocommit=True) as conn:
    conn.execute("DROP SCHEMA IF EXISTS swap_test CASCADE; CREATE SCHEMA swap_test")
try:
    with psycopg.connect(URL) as conn:
        for t in ("a", "b"):
            conn.execute(f"CREATE TABLE swap_test.{t} (x int); INSERT INTO swap_test.{t} VALUES (1)")
            conn.execute(f"CREATE TABLE swap_test.{t}_new (x int); INSERT INTO swap_test.{t}_new VALUES (2), (3)")
        conn.execute("CREATE VIEW swap_test.v AS SELECT x FROM swap_test.a")  # wh.fact_commute 같은 뷰
    with psycopg.connect(URL) as conn:
        swap(conn, ["swap_test.a", "swap_test.b"])
    with psycopg.connect(URL) as conn:
        assert conn.execute("SELECT array_agg(x ORDER BY x) FROM swap_test.v").fetchone()[0] == [2, 3]
        assert conn.execute("SELECT array_agg(x ORDER BY x) FROM swap_test.b").fetchone()[0] == [2, 3]
        assert conn.execute("SELECT to_regclass('swap_test.a_new'), to_regclass('swap_test.b_new')").fetchone() == (None, None)
        conn.execute("CREATE TABLE swap_test.a_new (x int); CREATE TABLE swap_test.b_new (x int); INSERT INTO swap_test.b_new VALUES (9)")
    try:
        with psycopg.connect(URL) as conn:
            swap(conn, ["swap_test.a", "swap_test.b"])
        raise AssertionError("0행인데 교체했다")
    except SystemExit:
        pass
    with psycopg.connect(URL) as conn:
        assert conn.execute("SELECT array_agg(x ORDER BY x) FROM swap_test.v").fetchone()[0] == [2, 3]
        assert conn.execute("SELECT array_agg(x ORDER BY x) FROM swap_test.b").fetchone()[0] == [2, 3]
finally:
    with psycopg.connect(URL, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS swap_test CASCADE")
print("tests.test_runner_swap: 모든 시험 통과")
