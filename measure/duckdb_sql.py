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
실행(계산 컨테이너 duckdb-job, 결정 109. duckdb 2.0.0과 iceberg·postgres 확장이 이미지에 있다):
  docker compose run --rm duckdb-job python -m measure.duckdb_sql spark_sql/living --show batch_status --compare
  --memory-limit, --threads: 엔진 분석(3.10)에서 바꾼다. 기본은 DuckDB 기본값(지정하지 않음)
  --memwatch 초: 옆 연결에서 그 간격으로 duckdb_memory()를 읽어 태그별 메모리·임시 사용을 logs/duckdb_memwatch/{run_id}.csv에
      줄마다 flush해 쓴다(결정 110 D1). 메모리 오류로 끝나도 마지막 줄까지 남는다.
  --per-batch: 배치(대상연월, 수집일)마다 원천 뷰를 그 배치로 걸러 checked -> clean, quarantine을 계산해 누적 표에 더하고,
      다 끝난 뒤 batch_status(배치를 넘는 lag)와 마지막 뷰를 누적 표로 계산한다(결정 110 D2). SQL 파일은 그대로다(결정 94).
      clean은 배치 키(target_ym, collected_date)로 묶고 quarantine은 그 키로 조인하므로 배치별 결과의 합이 전체 계산과 같다.
      어느 배치의 clean이 메모리 오류로 끝나면 그 배치만 선택지 B(checked를 임시 표로 먼저)로 다시 계산한다(지시서 4절 넘침 규칙).
      --batch-checked: 모든 배치에서 처음부터 B로 한다(결정 110 선택지 B).
  --compare: 결과를 Spark 운영 표와 두 방향 EXCEPT ALL로 비교한다. --per-batch면 batch_status, quarantine도 비교한다.
  --per-batch-proc: 배치마다 새 프로세스(결정 111, F-30). 부모가 heavy 잠금을 잡고 자식(--child 단계)을 차례로 부른다.
      자식은 잠금을 잡지 않는다(잡으면 /scratch를 비워 앞 배치 Parquet가 지워진다). 단계와 넘김:
        list -> batch(배치마다: checked -> clean, quarantine을 b/<배치>/{clean,quarantine}.parquet)
        -> status(전체 Parquet로 batch_status와 테스트, batch_status.parquet) -> commute(배치마다, b/<배치>/commute.parquet)
        -> compare(--compare면 Spark 표와 두 방향 EXCEPT ALL). 폴더: /scratch/duckdb/batches/<run_id>/
      자식이 메모리 오류(DuckDB 오류 rc 75, 커널 OOM -9)로 끝나면 그 단계만 threads 4로 한 번 더. 그 밖의 실패는 바로 실패.
      Iceberg 스냅샷(metadata.json)은 부모가 한 번 읽어 모든 자식에 넘긴다(모든 배치가 같은 스냅샷을 본다).
      요약: logs/duckdb_batches/<run_id>.json (자식마다 시간, 종료 코드, 최대 RSS(wait4), duckdb_memory 최대, 임시 최대)
  --only-batch YM:YYYY-MM-DD ...: 그 배치만 돈다. 비교도 Spark 쪽을 그 배치로 거른다(D3 작은 시험, 실패한 배치 다시 돌리기).
      ponytail: 잠금이 시작·끝에 /scratch를 비우므로 앞 실행의 Parquet는 남지 않는다. 다시 돌리면 고른 배치를 처음부터 계산하고,
      batch_status의 lag는 고른 배치 안에서만 본다(첫 배치부터 연달아 고르면 전체와 같다). 실행을 넘는 이어 돌리기가 필요해지면
      배치 Parquet를 /scratch 밖(잠금이 비우지 않는 곳)에 둔다.
"""
import argparse
import datetime
import json
import os
import re
import resource
import subprocess
import sys
import threading
import time
import uuid

import duckdb
import psycopg

from common import ROOT, read_env
from transform.living_iceberg import load_codes
from common import SCRATCH, heavy_lock, temp_budget
from transform.spark_sql_runner import record, sql_files

PROFILE_DIR = "/tmp/duckdb-profiles"
TEMP_DIR = f"{SCRATCH}/duckdb"  # 작업 공간 전용 볼륨(결정 107 S4). 메모리 DB의 기본 임시 폴더는 작업 폴더 아래 .tmp라 레포(Windows 바인드 마운트)에 쓰게 된다
DIALECT = [  # (정규식, 바꿀 글자, 이유). 실행하며 실제로 난 오류만 더한다
    (r"([\w.]+) RLIKE ('[^']*')", r"regexp_matches(\1, \2)",
     "DuckDB 파서에 RLIKE가 없다(Parser Error). regexp_matches는 Spark RLIKE처럼 부분 일치이고 패턴이 ^...$라 같은 뜻"),
    (r"\bcollect_list\(", "list(", "DuckDB에 collect_list가 없다(Catalog Error). 같은 일을 하는 집계는 list다. 순서는 둘 다 보장하지 않는다"),
    (r"\bxxhash64\(", "hash(", "DuckDB에 xxhash64가 없다(Catalog Error). hash도 여러 인자를 받는 64비트 해시다. 값은 달라도 "
     "20_clean은 한 키 안에서 해시가 모두 같은지만 보므로 판정은 같다(F-9 처방 A)"),
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


class MemWatch:
    """옆 연결(같은 DB 인스턴스의 두 번째 커서)에서 duckdb_memory()를 주기적으로 읽는다(결정 110 D1).
    한 줄 = 시각 x 태그. 읽기가 실패하면(메모리 바닥 등) 그 사실을 한 줄로 남기고 계속한다."""

    def __init__(self, con, seconds, run_id):
        self.cur, self.seconds = con.cursor(), seconds
        os.makedirs(ROOT / "logs" / "duckdb_memwatch", exist_ok=True)
        self.path = ROOT / "logs" / "duckdb_memwatch" / f"{run_id}.csv"
        self.f = open(self.path, "w", encoding="utf-8")
        self.f.write("ts,tag,memory_usage_bytes,temporary_storage_bytes\n")
        self.f.flush()
        self.stop = threading.Event()
        self.reset()
        self.thread = threading.Thread(target=self.loop, daemon=True)
        self.thread.start()

    def reset(self):
        """배치마다 최대값을 새로 잰다."""
        self.peak_mem, self.peak_temp, self.peak_tags = 0, 0, {}

    def loop(self):
        while not self.stop.wait(self.seconds):
            ts = datetime.datetime.now().isoformat(timespec="seconds")
            try:
                rows = self.cur.execute("SELECT tag, memory_usage_bytes, temporary_storage_bytes FROM duckdb_memory()").fetchall()
            except Exception as e:  # noqa: BLE001  재는 쪽이 실패해도 본 실행은 그대로 둔다
                self.f.write(f"{ts},ERROR,{type(e).__name__},\n")
                self.f.flush()
                continue
            for tag, mem, temp in rows:
                self.f.write(f"{ts},{tag},{mem},{temp}\n")
                self.peak_tags[tag] = max(self.peak_tags.get(tag, 0), mem)
            self.peak_mem = max(self.peak_mem, sum(r[1] for r in rows))
            self.peak_temp = max(self.peak_temp, sum(r[2] for r in rows))
            self.f.flush()

    def close(self):
        self.stop.set()
        self.thread.join(timeout=self.seconds + 5)
        self.f.close()


def setup(con):
    """코드표와 원천 뷰(lh.living.migration_rows)를 만들고 Iceberg 현재 metadata.json 경로를 돌려준다."""
    con.execute("CREATE TEMP TABLE codes (code VARCHAR)")
    con.executemany("INSERT INTO codes VALUES (?)", [(c,) for c in sorted(load_codes())])
    con.execute("ATTACH ':memory:' AS lh; CREATE SCHEMA lh.living")
    location = metadata_location()
    con.execute(f"CREATE VIEW lh.living.migration_rows AS SELECT * FROM iceberg_scan('{location}')")
    return location


def show_or_view(r, args, views):
    for name, sql in views:
        if name in args.show:
            r.execute(f"show {name}", f"CREATE TEMP TABLE {name} AS {translate(sql)}")
            print(r.con.sql(f"SELECT * FROM {name}"))
        else:
            r.con.execute(f"CREATE TEMP VIEW {name} AS {translate(sql)}")


def tests_and_result(r, views, tests):
    """테스트를 돌리고 마지막 뷰를 result 표로 계산한다."""
    run_tests(r, tests)
    r.execute("write result", f"CREATE TEMP TABLE result AS SELECT * FROM {views[-1][0]}")
    print("결과 행 수:", r.con.sql("SELECT count(*) FROM result").fetchone()[0])


def run(r, args, views, tests, watch=None):
    setup(r.con)
    show_or_view(r, args, views)
    tests_and_result(r, views, tests)


BATCH_VIEWS = ["checked", "clean", "quarantine"]  # 배치 키로 닫힌 뷰. 나머지(batch_status, commute)는 배치를 넘는다


def gib(b):
    return f"{b / 2 ** 30:.2f}GiB"


def run_per_batch(r, args, views, tests, watch):
    """결정 110 D2. 배치마다 원천을 걸러 clean, quarantine을 누적하고, 끝난 뒤 배치를 넘는 뷰를 계산한다."""
    con = r.con
    location = setup(con)
    assert [n for n, _ in views][:3] == BATCH_VIEWS, [n for n, _ in views]
    sql = dict(views)
    batches = r.execute("batch list", f"SELECT DISTINCT target_ym, collected_date FROM iceberg_scan('{location}') ORDER BY 1, 2")
    print(f"배치 {len(batches)}개: {[(ym, str(cd)) for ym, cd in batches]}", flush=True)
    stats = []
    for i, (ym, cd) in enumerate(batches):
        if watch:
            watch.reset()
        t0 = time.perf_counter()
        con.execute(f"CREATE OR REPLACE VIEW lh.living.migration_rows AS SELECT * FROM iceberg_scan('{location}') "
                    f"WHERE target_ym = '{ym}' AND collected_date = DATE '{cd}'")
        con.execute("DROP VIEW IF EXISTS quarantine; DROP TABLE IF EXISTS clean; DROP VIEW IF EXISTS checked; DROP TABLE IF EXISTS checked")
        used_b = bool(args.batch_checked)
        if used_b:  # 선택지 B: 배치 원천을 한 번만 읽고 checked를 표로 둔다
            r.execute(f"batch {ym} {cd} checked", f"CREATE TEMP TABLE checked AS {translate(sql['checked'])}")
        else:
            con.execute(f"CREATE TEMP VIEW checked AS {translate(sql['checked'])}")
        try:
            r.execute(f"batch {ym} {cd} clean", f"CREATE TEMP TABLE clean AS {translate(sql['clean'])}")
        except duckdb.OutOfMemoryException:
            if used_b:
                raise
            # 지시서 4절 넘침 규칙: 넘친 배치 안에서만 B로 다시. 누적 표에 넣기 전이라 다시 해도 중복이 없다
            print(f"배치 {ym} {cd}: clean 메모리 오류 → 이 배치만 선택지 B(checked를 임시 표로)로 다시", flush=True)
            used_b = True
            con.execute("DROP TABLE IF EXISTS clean; DROP VIEW IF EXISTS checked")
            r.execute(f"batch {ym} {cd} checked (B)", f"CREATE TEMP TABLE checked AS {translate(sql['checked'])}")
            r.execute(f"batch {ym} {cd} clean (B)", f"CREATE TEMP TABLE clean AS {translate(sql['clean'])}")
        con.execute(f"CREATE TEMP VIEW quarantine AS {translate(sql['quarantine'])}")
        if i == 0:
            con.execute("CREATE TEMP TABLE clean_all AS SELECT * FROM clean LIMIT 0")
            con.execute("CREATE TEMP TABLE quarantine_all AS SELECT * FROM quarantine LIMIT 0")
        con.execute("INSERT INTO clean_all SELECT * FROM clean")
        r.execute(f"batch {ym} {cd} quarantine", "INSERT INTO quarantine_all SELECT * FROM quarantine")
        sec = time.perf_counter() - t0
        row = {"target_ym": ym, "collected_date": str(cd), "sec": round(sec, 1), "b": used_b}
        if watch:
            row |= {"mem_peak": watch.peak_mem, "temp_peak": watch.peak_temp,
                    "top_tags": sorted(watch.peak_tags.items(), key=lambda kv: -kv[1])[:3]}
        stats.append(row)
        print(f"배치 {ym} {cd}: {sec:.1f}초{' (B)' if used_b else ''}" + (f", duckdb_memory 합 최대 {gib(watch.peak_mem)}, 임시 최대 {gib(watch.peak_temp)}, "
                                               f"태그 최대 {[(t, gib(b)) for t, b in row['top_tags']]}" if watch else ""), flush=True)
    con.execute("DROP VIEW quarantine; DROP TABLE clean; DROP VIEW IF EXISTS checked; DROP TABLE IF EXISTS checked")
    con.execute("CREATE TEMP VIEW clean AS SELECT * FROM clean_all; CREATE TEMP VIEW quarantine AS SELECT * FROM quarantine_all")
    con.execute(f"CREATE OR REPLACE VIEW lh.living.migration_rows AS SELECT * FROM iceberg_scan('{location}')")
    if watch:
        watch.reset()
    show_or_view(r, args, views[3:])
    tests_and_result(r, views, tests)
    if watch:
        print(f"배치를 넘는 계산(batch_status, 테스트, 마지막 뷰): duckdb_memory 합 최대 {gib(watch.peak_mem)}, 임시 최대 {gib(watch.peak_temp)}")
    print("배치별:", json.dumps(stats, ensure_ascii=False))


def compare(con, pairs, pg_where=""):
    """(DuckDB 표, stg 표) 쌍마다 DuckDB 쪽 열(운영 표에만 있는 checked_at 등은 뺀다)로 두 방향 EXCEPT ALL 행 수를 센다.
    pg_where: Spark 쪽을 거르는 조건(--only-batch). 돌려주는 값: 쌍마다 차이 행 수."""
    diffs = {}
    con.execute("INSTALL postgres; LOAD postgres")
    con.execute(f"ATTACH '{read_env('DATABASE_URL')}' AS pg (TYPE postgres, READ_ONLY)")
    for mine, theirs in pairs:
        pg_cols = {c for c, in con.execute("SELECT column_name FROM duckdb_columns() WHERE database_name = 'pg' "
                                          f"AND schema_name = 'stg' AND table_name = '{theirs}'").fetchall()}
        cols = ", ".join(c for c in con.sql(f"SELECT * FROM {mine} LIMIT 0").columns if c in pg_cols)  # 2.0: execute()는 연결을 돌려주고 columns가 없다
        spark = f"(SELECT {cols} FROM pg.stg.{theirs} {pg_where})"
        diff = con.execute(f"SELECT count(*) FROM ((SELECT {cols} FROM {mine} EXCEPT ALL SELECT * FROM {spark}) "
                           f"UNION ALL (SELECT * FROM {spark} EXCEPT ALL SELECT {cols} FROM {mine}))").fetchone()[0]
        n_mine, n_spark = (con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in (mine, spark))
        print(f"stg.{theirs}(Spark 결과, {n_spark:,}행)와 {mine}({n_mine:,}행)의 두 방향 차이: {diff}행 (비교 열: {cols})", flush=True)
        diffs[theirs] = diff
    return diffs


def engine_settings(con, args):
    """메모리·스레드·임시 상한과 그 밖의 설정을 걸고, 프로파일을 켠다. 실제로 걸린 값을 돌려준다."""
    con.execute(f"SET temp_directory = '{TEMP_DIR}'")
    settings = {"duckdb": duckdb.__version__}
    budget = temp_budget()
    if not args.max_temp and budget <= 0:
        raise SystemExit(f"임시 공간 여유가 없습니다(호스트 C: 여유에서 남길 몫을 뺀 값 {budget / 2 ** 30:.1f}GiB). 실행하지 않습니다")
    max_temp = args.max_temp or f"{budget // 2 ** 30}GiB"  # 기본값(보이는 디스크의 90%)은 가상 디스크의 거짓 용량을 본다(F-16)
    for key, value in (("memory_limit", args.memory_limit), ("threads", args.threads), ("max_temp_directory_size", max_temp),
                       *(kv.split("=", 1) for kv in args.set)):
        if value:
            con.execute(f"SET {key} = '{value}'")
    for key in ("memory_limit", "threads", "preserve_insertion_order", "max_temp_directory_size"):
        settings[key] = con.execute(f"SELECT current_setting('{key}')").fetchone()[0]
    con.execute("LOAD iceberg")
    con.execute("PRAGMA enable_profiling = 'json'")
    con.execute(f"PRAGMA profiling_output = '{PROFILE_DIR}/setup.json'")  # 준비 문장의 프로파일이 화면에 찍히지 않게
    return settings


# ---- 결정 111: 배치마다 새 프로세스 ----
OOM_RC = 75  # 자식이 DuckDB 메모리 오류로 끝날 때의 종료 코드. 커널 OOM은 -9(SIGKILL)


def batch_dir(run_dir, batch):
    return f"{run_dir}/b/{batch[0]}_{batch[1]}"


def run_tests(r, tests):
    failed = []
    for name, sql in tests:
        bad = r.execute(f"test {name}", f"SELECT * FROM ({translate(sql)}) LIMIT 20")
        print(f"{name}: {'통과' if not bad else f'위반 {len(bad)}행 이상'}")
        failed += [name] if bad and name.startswith("test_") else []  # warn_은 출력만(결정 100)
    if failed:
        raise SystemExit(f"테스트 실패: {failed}")


def copy_to(r, job, query, path):
    """결과를 임시 파일에 쓰고 이름을 바꾼다. 같은 단계를 다시 돌려도 반쯤 쓴 파일이 남지 않는다. 쓴 행 수를 돌려준다."""
    tmp = f"{path}.tmp"
    r.execute(job, f"COPY ({query}) TO '{tmp}' (FORMAT parquet)")
    os.replace(tmp, path)
    return r.con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]


def child_stage(r, args, views, tests):
    """자식 한 번 = 단계 하나(배치 단계면 배치 하나). 결과는 Parquet로만 넘긴다. 잠금은 부모가 갖고 있다."""
    con, d, sql = r.con, args.run_dir, dict(views)
    assert [n for n, _ in views] == ["checked", "clean", "quarantine", "batch_status", "commute"], [n for n, _ in views]
    stage = args.child
    if stage == "list":
        rows = r.execute("batch list", f"SELECT DISTINCT target_ym, collected_date FROM iceberg_scan('{args.location}') ORDER BY 1, 2")
        json.dump([[ym, str(cd)] for ym, cd in rows], open(f"{d}/batches.json", "w"))
        return
    if stage == "batch":
        (ym, cd), = args.only_batch
        out = batch_dir(d, (ym, cd))
        os.makedirs(out, exist_ok=True)
        con.execute("CREATE TEMP TABLE codes (code VARCHAR)")
        con.executemany("INSERT INTO codes VALUES (?)", [(c,) for c in sorted(load_codes())])
        con.execute("ATTACH ':memory:' AS lh; CREATE SCHEMA lh.living")
        con.execute(f"CREATE VIEW lh.living.migration_rows AS SELECT * FROM iceberg_scan('{args.location}') "
                    f"WHERE target_ym = '{ym}' AND collected_date = DATE '{cd}'")
        con.execute(f"CREATE TEMP VIEW checked AS {translate(sql['checked'])}")
        r.execute("clean", f"CREATE TEMP TABLE clean AS {translate(sql['clean'])}")
        con.execute(f"CREATE TEMP VIEW quarantine AS {translate(sql['quarantine'])}")
        n = copy_to(r, "write clean", "SELECT * FROM clean", f"{out}/clean.parquet")
        m = copy_to(r, "write quarantine", "SELECT * FROM quarantine", f"{out}/quarantine.parquet")
        print(f"배치 {ym} {cd}: clean {n:,}행, quarantine {m:,}행", flush=True)
        return
    con.execute(f"CREATE VIEW quarantine AS SELECT * FROM read_parquet('{d}/b/*/quarantine.parquet')")
    if stage == "status":
        con.execute(f"CREATE VIEW clean AS SELECT * FROM read_parquet('{d}/b/*/clean.parquet')")
        r.execute("batch_status", f"CREATE TEMP TABLE batch_status AS {translate(sql['batch_status'])}")
        print(con.sql("SELECT * FROM batch_status ORDER BY target_ym, collected_date"))
        run_tests(r, tests)
        copy_to(r, "write batch_status", "SELECT * FROM batch_status", f"{d}/batch_status.parquet")
        return
    con.execute(f"CREATE VIEW batch_status AS SELECT * FROM read_parquet('{d}/batch_status.parquet')")
    if stage == "commute":
        (ym, cd), = args.only_batch
        out = batch_dir(d, (ym, cd))
        con.execute(f"CREATE VIEW clean AS SELECT * FROM read_parquet('{out}/clean.parquet')")
        n = copy_to(r, "commute", translate(sql["commute"]), f"{out}/commute.parquet")
        print(f"배치 {ym} {cd}: commute {n:,}행", flush=True)
        return
    if stage == "compare":
        con.execute(f"CREATE VIEW result AS SELECT * FROM read_parquet('{d}/b/*/commute.parquet')")
        where = ("WHERE (target_ym, collected_date) IN (" + ", ".join(f"('{ym}', DATE '{cd}')" for ym, cd in args.only_batch) + ")"
                 if args.only_batch else "")
        diffs = compare(con, [("result", "living_commute"), ("batch_status", "living_batch_status"),
                              ("quarantine", "living_quarantine")], where)
        json.dump(diffs, open(f"{d}/compare.json", "w"))
        return
    raise SystemExit(f"모르는 단계: {stage}")


def child_main(args, views, tests):
    if not args.lock_held_by_parent:  # 자식이 잠금을 잡으면 /scratch를 비워 앞 배치 결과가 지워진다
        raise SystemExit("--child는 부모(--per-batch-proc)가 잠금을 잡고 부를 때만 돈다(--lock-held-by-parent)")
    run_id, started, t0 = args.child_run_id, datetime.datetime.now(datetime.timezone.utc), time.perf_counter()
    con = duckdb.connect()
    settings = engine_settings(con, args) | {"mode": f"per_batch_proc:{args.child}"}
    r, ok, error = Runner(con, run_id), False, None
    watch = MemWatch(con, args.memwatch, run_id) if args.memwatch else None
    try:
        child_stage(r, args, views, tests)
        ok = True
    except duckdb.OutOfMemoryException as e:
        error = f"{type(e).__name__}: {e}"[:2000]
        print(error, flush=True)
        raise SystemExit(OOM_RC)
    except BaseException as e:
        error = f"{type(e).__name__}: {e}"[:2000]
        raise
    finally:
        if watch:
            watch.close()
        run_ms = int((time.perf_counter() - t0) * 1000)
        if r.rows:
            r.rows[-1]["jvm_heap_peak_bytes"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
        record(run_id, started, settings, ok, run_ms, error, r.rows, engine="duckdb")
        stats = {"ok": ok, "settings": settings}
        if watch:
            stats |= {"mem_peak": watch.peak_mem, "temp_peak": watch.peak_temp,
                      "top_tags": sorted(watch.peak_tags.items(), key=lambda kv: -kv[1])[:3]}
        os.makedirs(f"{args.run_dir}/stats", exist_ok=True)
        json.dump(stats, open(f"{args.run_dir}/stats/{run_id}.json", "w"))


def parent_main(args):
    """부모: 잠금, 스냅샷 한 번, 자식을 차례로. 자식이 메모리 오류면 그 단계만 threads 4로 한 번 더(결정 111 넘침 규칙)."""
    run_id = f"duckdb-{uuid.uuid4().hex[:12]}"
    lock = heavy_lock()  # noqa: F841  /scratch를 비우고 잡는다. 자식은 잡지 않는다
    d = f"{SCRATCH}/duckdb/batches/{run_id}"
    os.makedirs(f"{d}/b", exist_ok=True)  # /scratch/duckdb를 DuckDB보다 먼저 만든다(DuckDB가 만든 임시 폴더는 닫을 때 지울 수 있다)
    location = metadata_location()
    base = [sys.executable, "-u", "-m", "measure.duckdb_sql", args.folder, "--lock-held-by-parent", "--run-dir", d,
            "--location", location]
    for flag, value in (("--memory-limit", args.memory_limit), ("--max-temp", args.max_temp), ("--memwatch", args.memwatch)):
        if value:
            base += [flag, str(value)]
    base += ["--set", *args.set] if args.set else []
    log, t0 = [], time.perf_counter()
    print(f"per_batch_proc {run_id}: 스냅샷 {location}", flush=True)

    def summary():
        out = ROOT / "logs" / "duckdb_batches" / f"{run_id}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        parquet = sum(os.path.getsize(os.path.join(dp, f)) for dp, _, fs in os.walk(d) for f in fs if f.endswith(".parquet"))
        json.dump({"run_id": run_id, "sec": round(time.perf_counter() - t0, 1), "location": location, "only_batch": args.only_batch,
                   "children": log, "parquet_bytes": parquet,
                   "compare": json.load(open(f"{d}/compare.json")) if os.path.exists(f"{d}/compare.json") else None},
                  open(out, "w"), ensure_ascii=False, indent=1)
        return out

    def child(stage, batches=(), tag=None):
        tag = tag or stage
        for attempt, threads in enumerate([args.threads, "4"]):
            cid = f"{run_id}-{tag}-{attempt}"
            cmd = base + ["--child", stage, "--child-run-id", cid] + (["--threads", threads] if threads else [])
            cmd += ["--only-batch", *[f"{ym}:{cd}" for ym, cd in batches]] if batches else []
            t = time.perf_counter()
            p = subprocess.Popen(cmd)
            _, status, ru = os.wait4(p.pid, 0)  # 자식 하나의 최대 RSS를 rusage로 받는다
            rc = p.returncode = os.waitstatus_to_exitcode(status)
            row = {"stage": stage, "batch": list(batches[0]) if len(batches) == 1 else None, "attempt": attempt, "threads": threads,
                   "rc": rc, "sec": round(time.perf_counter() - t, 1), "max_rss": ru.ru_maxrss * 1024}
            sp = f"{d}/stats/{cid}.json"
            row |= {k: v for k, v in json.load(open(sp)).items() if k != "settings"} if os.path.exists(sp) else {}
            log.append(row)
            print(f"[{tag}] 시도 {attempt} threads={threads}: rc {rc}, {row['sec']}초, 최대 RSS {gib(row['max_rss'])}"
                  + (f", duckdb_memory 최대 {gib(row['mem_peak'])}, 임시 최대 {gib(row['temp_peak'])}" if "mem_peak" in row else ""),
                  flush=True)
            if rc == 0:
                return
            if rc not in (OOM_RC, -9) or attempt == 1:
                raise SystemExit(f"[{tag}] 실패(rc {rc}). 요약 {summary()}")
            print(f"[{tag}] 메모리 오류 → 이 단계만 threads 4로 한 번 더", flush=True)

    child("list")
    batches = [tuple(b) for b in json.load(open(f"{d}/batches.json"))]
    if args.only_batch:
        missing = set(args.only_batch) - set(batches)
        if missing:
            raise SystemExit(f"없는 배치: {missing}")
        batches = [b for b in batches if b in set(args.only_batch)]
    print(f"배치 {len(batches)}개: {batches}", flush=True)
    for i, b in enumerate(batches):
        child("batch", [b], f"b{i:02d}")
    child("status")
    for i, b in enumerate(batches):
        child("commute", [b], f"c{i:02d}")
    if args.compare:
        child("compare", args.only_batch)
    out = summary()
    diffs = json.load(open(f"{d}/compare.json")) if args.compare else {}
    print(f"per_batch_proc {run_id}: {time.perf_counter() - t0:.1f}초, 요약 {out}, 비교 {diffs}", flush=True)
    if any(diffs.values()):
        raise SystemExit(f"Spark 결과와 다릅니다: {diffs}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("folder")
    parser.add_argument("--show", nargs="*", default=[])
    parser.add_argument("--memory-limit")
    parser.add_argument("--threads")
    parser.add_argument("--max-temp", help="임시 폴더 상한(예: 100GiB). 기본: common.temp_budget() (L3)")
    parser.add_argument("--compare", action="store_true")
    parser.add_argument("--memwatch", type=float, metavar="SEC", help="duckdb_memory()를 이 간격으로 logs/duckdb_memwatch/에 기록(D1)")
    parser.add_argument("--per-batch", action="store_true", help="배치(대상연월, 수집일) 단위로 계산(결정 110 D2)")
    parser.add_argument("--batch-checked", action="store_true", help="--per-batch에서 checked를 배치마다 임시 표로(결정 110 B)")
    parser.add_argument("--per-batch-proc", action="store_true", help="배치마다 새 프로세스, Parquet로 넘김(결정 111)")
    parser.add_argument("--only-batch", nargs="*", default=[], metavar="YM:DATE", help="이 배치만(--per-batch-proc)")
    parser.add_argument("--child", help=argparse.SUPPRESS)
    parser.add_argument("--child-run-id", help=argparse.SUPPRESS)
    parser.add_argument("--run-dir", help=argparse.SUPPRESS)
    parser.add_argument("--location", help=argparse.SUPPRESS)
    parser.add_argument("--lock-held-by-parent", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--set", nargs="*", default=[], metavar="KEY=VALUE", help="그 밖의 DuckDB 설정(F-18 가설 시험)")
    args = parser.parse_args()
    args.only_batch = [tuple(b.split(":", 1)) for b in args.only_batch]
    views, tests = sql_files(ROOT / args.folder)
    os.makedirs(PROFILE_DIR, exist_ok=True)
    if args.child:
        return child_main(args, views, tests)
    if args.per_batch_proc:
        return parent_main(args)
    run_id = f"duckdb-{uuid.uuid4().hex[:12]}"
    started, t0 = datetime.datetime.now(datetime.timezone.utc), time.perf_counter()
    con = duckdb.connect()
    lock = heavy_lock()  # noqa: F841  프로세스가 끝날 때까지 잡는다(L5)
    settings = engine_settings(con, args)
    settings["mode"] = ("per_batch_checked" if args.batch_checked else "per_batch") if args.per_batch else "whole"
    r, ok, error = Runner(con, run_id), False, None
    watch = MemWatch(con, args.memwatch, run_id) if args.memwatch else None
    if watch:
        print(f"memwatch: {watch.path} ({args.memwatch}초 간격)", flush=True)
    try:
        (run_per_batch if args.per_batch else run)(r, args, views, tests, watch)
        if args.compare:
            pairs = [("result", "living_commute")]
            if args.per_batch:
                pairs += [("batch_status", "living_batch_status"), ("quarantine_all", "living_quarantine")]
            compare(con, pairs)
        ok = True
    except BaseException as e:
        error = f"{type(e).__name__}: {e}"[:2000]
        raise
    finally:
        if watch:
            watch.close()
        run_ms = int((time.perf_counter() - t0) * 1000)
        if r.rows:
            r.rows[-1]["jvm_heap_peak_bytes"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024  # 프로세스 최대 RSS
        record(run_id, started, settings, ok, run_ms, error, r.rows, engine="duckdb")
        print(f"ops.engine_run: {run_id} 문장 {len(r.rows)}개, {run_ms / 1000:.1f}초, ok={ok}, 설정 {settings}, "
              f"최대 RSS {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 / 1024:.2f}GiB")


if __name__ == "__main__":
    main()
