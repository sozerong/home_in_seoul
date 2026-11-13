"""같은 Spark SQL 파일 묶음을 DuckDB로 돌린다 (design_remaining_work.md 3.9, 3.10, 결정 87, 94, 95).

실행 순서는 transform/spark_sql_runner.py와 같다: codes -> 파일마다 임시 뷰 -> --show 뷰를 먼저 계산(Spark CACHE TABLE 대신
임시 표) -> test_*.sql -> 마지막 뷰 계산. PostgreSQL에는 쓰지 않는다(운영 표는 Spark 실행기만 쓴다). 결과는 DuckDB 임시 표에
두고 --compare면 stg.living_commute와 두 방향 EXCEPT ALL로 비교한다.
Iceberg: DuckDB iceberg 확장의 ATTACH는 REST 카탈로그만 받고 JDBC 카탈로그(lh)는 못 읽는다. 그래서 JDBC 카탈로그 표(iceberg DB의
iceberg_tables)에서 지금 metadata.json 경로를 읽어 iceberg_scan으로 연다. SQL의 lh.living.migration_rows는 메모리 DB lh에 같은 이름의
뷰를 만들어 그대로 둔다.
방언 차이로 고친 곳은 DIALECT에 모은다(고친 글자, 이유). 파일은 고치지 않고 실행 직전에 바꾼다.
지표: 문장마다 DuckDB 프로파일(JSON)을 /tmp/duckdb-profiles/{run_id}/에 남기고 ops.engine_run(engine='duckdb')에 한 행씩 쓴다.
  stage_id = 문장 순번, stage_ms = 지연, peak_exec_mem_bytes = 버퍼 메모리 최대, disk_spill_bytes = 임시 폴더 최대 크기.
  Spark 전용 열(태스크, 셔플, 메모리 흘림)은 0, 프로세스 최대 RSS는 jvm_heap_peak_bytes 자리에 넣는다(마지막 행).
실행(Airflow 컨테이너, duckdb는 /tmp/duckdb_pkg에 임시 설치):
  PYTHONPATH=/tmp/duckdb_pkg /opt/airflow/spark-venv/bin/python -m measure.duckdb_sql spark_sql/living --show batch_status --compare
  --memory-limit, --threads: 엔진 분석(3.10)에서 바꾼다. 기본은 DuckDB 기본값(지정하지 않음)
"""
import argparse
import datetime
import json
import os
import re
import resource
import time
import uuid

import duckdb
import psycopg

from common import ROOT, read_env
from transform.living_migration import load_codes
from transform.spark_sql_runner import record, sql_files

PROFILE_DIR = "/tmp/duckdb-profiles"
TEMP_DIR = "/tmp/duckdb-tmp"  # 메모리 DB의 기본 임시 폴더는 작업 폴더 아래 .tmp라 레포(Windows 바인드 마운트)에 쓰게 된다
DIALECT = [  # (정규식, 바꿀 글자, 이유). 실행하며 실제로 난 오류만 더한다
    (r"([\w.]+) RLIKE ('[^']*')", r"regexp_matches(\1, \2)",
     "DuckDB 파서에 RLIKE가 없다(Parser Error). regexp_matches는 Spark RLIKE처럼 부분 일치이고 패턴이 ^...$라 같은 뜻"),
    (r"\bcollect_list\(", "list(", "DuckDB에 collect_list가 없다(Catalog Error). 같은 일을 하는 집계는 list다. 순서는 둘 다 보장하지 않는다"),
]


def translate(sql):
    for pattern, repl, _ in DIALECT:
        sql = re.sub(pattern, repl, sql)
    return sql


def metadata_location():
    with psycopg.connect(read_env("DATABASE_URL").rsplit("/", 1)[0] + "/iceberg") as conn:
        loc = conn.execute("SELECT metadata_location FROM iceberg_tables WHERE catalog_name = 'lh' "
                           "AND table_namespace = 'living' AND table_name = 'migration_rows'").fetchone()[0]
    return loc.removeprefix("file:")


class Runner:
    def __init__(self, con, run_id):
        self.con, self.dir, self.rows = con, f"{PROFILE_DIR}/{run_id}", []
        os.makedirs(self.dir, exist_ok=True)

    def execute(self, job, sql):
        """문장 하나를 프로파일을 켜고 돌린다. 프로파일 JSON에서 지표 한 행을 만든다."""
        path = f"{self.dir}/{len(self.rows):02d}_{job.replace(' ', '_')}.json"
        self.con.execute(f"PRAGMA profiling_output = '{path}'")
        t0 = time.perf_counter()
        try:
            return self.con.execute(sql).fetchall()
        finally:
            ms = int((time.perf_counter() - t0) * 1000)
            self.con.execute(f"PRAGMA profiling_output = '{PROFILE_DIR}/setup.json'")  # 다음 문장(출력 등)이 이 파일을 덮어쓰지 않게
            p = (json.load(open(path)) if os.path.exists(path) else {}).get("system", {})  # DuckDB 2.0 기본 지표
            self.rows.append({"stage_id": len(self.rows), "attempt": 0, "job": job, "tasks": 0, "failed_tasks": 0,
                              "stage_ms": ms, "task_ms_max": None, "task_ms_median": None,
                              "shuffle_read_bytes": 0, "shuffle_write_bytes": 0, "memory_spill_bytes": 0,
                              "disk_spill_bytes": int(p.get("peak_temp_dir_size") or 0),
                              "peak_exec_mem_bytes": p.get("peak_buffer_memory"), "jvm_heap_peak_bytes": None})


def run(r, args, views, tests):
    con = r.con
    con.execute("CREATE TEMP TABLE codes (code VARCHAR)")
    con.executemany("INSERT INTO codes VALUES (?)", [(c,) for c in sorted(load_codes())])
    con.execute("ATTACH ':memory:' AS lh; CREATE SCHEMA lh.living")
    con.execute(f"CREATE VIEW lh.living.migration_rows AS SELECT * FROM iceberg_scan('{metadata_location()}')")
    for name, sql in views:
        if name in args.show:
            r.execute(f"show {name}", f"CREATE TEMP TABLE {name} AS {translate(sql)}")
            print(con.sql(f"SELECT * FROM {name}"))
        else:
            con.execute(f"CREATE TEMP VIEW {name} AS {translate(sql)}")
    failed = []
    for name, sql in tests:
        bad = r.execute(f"test {name}", f"SELECT * FROM ({translate(sql)}) LIMIT 20")
        print(f"{name}: {'통과' if not bad else f'위반 {len(bad)}행 이상'}")
        failed += [name] if bad else []
    if failed:
        raise SystemExit(f"테스트 실패: {failed}")
    r.execute("write result", f"CREATE TEMP TABLE result AS SELECT * FROM {views[-1][0]}")
    print("결과 행 수:", con.sql("SELECT count(*) FROM result").fetchone()[0])
    if args.compare:
        con.execute("INSTALL postgres; LOAD postgres")
        con.execute(f"ATTACH '{read_env('DATABASE_URL')}' AS pg (TYPE postgres, READ_ONLY)")
        cols = ", ".join(c for c, in con.execute("SELECT column_name FROM duckdb_columns() WHERE database_name = 'pg' "
                                                 "AND schema_name = 'stg' AND table_name = 'living_commute' ORDER BY column_index").fetchall())
        diff = con.execute(f"SELECT count(*) FROM ((SELECT {cols} FROM result EXCEPT ALL SELECT {cols} FROM pg.stg.living_commute) "
                           f"UNION ALL (SELECT {cols} FROM pg.stg.living_commute EXCEPT ALL SELECT {cols} FROM result))").fetchone()[0]
        print(f"stg.living_commute(Spark 결과)와 두 방향 차이: {diff}행")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("folder")
    parser.add_argument("--show", nargs="*", default=[])
    parser.add_argument("--memory-limit")
    parser.add_argument("--threads")
    parser.add_argument("--compare", action="store_true")
    args = parser.parse_args()
    views, tests = sql_files(ROOT / args.folder)
    run_id = f"duckdb-{uuid.uuid4().hex[:12]}"
    os.makedirs(PROFILE_DIR, exist_ok=True)
    started, t0 = datetime.datetime.now(datetime.timezone.utc), time.perf_counter()
    con = duckdb.connect()
    con.execute(f"SET temp_directory = '{TEMP_DIR}'")
    settings = {"duckdb": duckdb.__version__}
    for key, value in (("memory_limit", args.memory_limit), ("threads", args.threads)):
        if value:
            con.execute(f"SET {key} = '{value}'")
    for key in ("memory_limit", "threads", "preserve_insertion_order"):
        settings[key] = con.execute(f"SELECT current_setting('{key}')").fetchone()[0]
    con.execute("LOAD iceberg")
    con.execute("PRAGMA enable_profiling = 'json'")
    con.execute(f"PRAGMA profiling_output = '{PROFILE_DIR}/setup.json'")  # 준비 문장의 프로파일이 화면에 찍히지 않게
    r, ok, error = Runner(con, run_id), False, None
    try:
        run(r, args, views, tests)
        ok = True
    except BaseException as e:
        error = f"{type(e).__name__}: {e}"[:2000]
        raise
    finally:
        run_ms = int((time.perf_counter() - t0) * 1000)
        if r.rows:
            r.rows[-1]["jvm_heap_peak_bytes"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024  # 프로세스 최대 RSS
        record(run_id, started, settings, ok, run_ms, error, r.rows, engine="duckdb")
        print(f"ops.engine_run: {run_id} 문장 {len(r.rows)}개, {run_ms / 1000:.1f}초, ok={ok}, 설정 {settings}, "
              f"최대 RSS {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 / 1024:.2f}GiB")


if __name__ == "__main__":
    main()
