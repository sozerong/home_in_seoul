"""Spark SQL 파일 묶음 실행기 (design_remaining_work.md 3.3, 결정 88, 90, 94-1).

폴더 안 *.sql을 이름순으로 임시 뷰로 만든다. 파일 하나 = 뷰 하나, 뷰 이름 = 파일 이름에서 번호를 뺀 것(10_checked.sql -> checked).
test_*.sql은 위반 행을 돌려주는 SQL이다. 하나라도 행이 있으면 아무것도 쓰지 않고 종료 코드 1(dbt 테스트와 같은 모양).
마지막 뷰를 JDBC로 {target}_new에 쓰고 PostgreSQL 트랜잭션 하나에서 {target}과 바꾼다(결정 90). 매번 전체 재계산.
Spark 이벤트 로그를 켜고, 끝나면(실패해도) 단계별 지표를 ops.engine_run에 남긴다. 한 행 = 실행 x 단계.
실행(Airflow 컨테이너):
  /opt/airflow/spark-venv/bin/python -m transform.spark_sql_runner spark_sql/living stg.living_commute --show batch_status
  --show 뷰: 먼저 계산해 캐시하고 출력한다(작은 뷰. 테스트가 여러 번 읽어도 한 번만 계산)
  --driver-memory, --shuffle-partitions, --aqe: 엔진 분석(3.10)에서 바꾼다. --no-write: 쓰지 않고 지표만 남긴다
  --explain: 계산하지 않고 실행 계획(EXPLAIN FORMATTED)만 출력한다
"""
import argparse
import datetime
import glob
import json
import os
import statistics
import sys
import time

import psycopg

from common import ROOT, read_env
from transform.iceberg import spark_session
from transform.living_migration import load_codes

EVENT_DIR = "/tmp/spark-events"


def sql_files(folder):
    files = sorted(folder.glob("*.sql"))
    views = [(p.stem.split("_", 1)[1], p.read_text(encoding="utf-8")) for p in files if not p.stem.startswith("test_")]
    tests = [(p.stem, p.read_text(encoding="utf-8")) for p in files if p.stem.startswith("test_")]
    return views, tests


def stage_rows(path):
    """이벤트 로그(JSON 줄)에서 (단계, 시도)마다 지표 dict를 만든다. 끝나지 못한 단계도 태스크가 있으면 남긴다."""
    jobs, stages, tasks, heap = {}, {}, {}, {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            try:
                e = json.loads(line)
            except json.JSONDecodeError:  # 실행 중이거나 드라이버가 죽은 로그(.inprogress)는 마지막 줄이 잘려 있다
                continue
            kind = e["Event"]
            if kind == "SparkListenerJobStart":
                desc = (e.get("Properties") or {}).get("spark.job.description")
                for sid in e["Stage IDs"]:
                    jobs[sid] = desc
            elif kind == "SparkListenerTaskEnd":
                m, info = e.get("Task Metrics") or {}, e["Task Info"]
                read, write = m.get("Shuffle Read Metrics", {}), m.get("Shuffle Write Metrics", {})
                tasks.setdefault((e["Stage ID"], e["Stage Attempt ID"]), []).append((
                    info["Finish Time"] - info["Launch Time"], info.get("Failed", False),
                    read.get("Remote Bytes Read", 0) + read.get("Local Bytes Read", 0), write.get("Shuffle Bytes Written", 0),
                    m.get("Memory Bytes Spilled", 0), m.get("Disk Bytes Spilled", 0), m.get("Peak Execution Memory", 0)))
            elif kind == "SparkListenerStageCompleted":
                s = e["Stage Info"]
                stages[(s["Stage ID"], s["Stage Attempt ID"])] = s
            elif kind == "SparkListenerStageExecutorMetrics":
                key = (e["Stage ID"], e["Stage Attempt ID"])
                heap[key] = max(heap.get(key, 0), e["Executor Metrics"].get("JVMHeapMemory", 0))
    rows = []
    for key in sorted(set(stages) | set(tasks)):
        t, s = tasks.get(key, []), stages.get(key, {})
        ms = [x[0] for x in t]
        done = s.get("Submission Time") and s.get("Completion Time")
        rows.append({"stage_id": key[0], "attempt": key[1], "job": jobs.get(key[0]), "tasks": len(t),
                     "failed_tasks": sum(x[1] for x in t), "stage_ms": s["Completion Time"] - s["Submission Time"] if done else None,
                     "task_ms_max": max(ms) if ms else None, "task_ms_median": int(statistics.median(ms)) if ms else None,
                     "shuffle_read_bytes": sum(x[2] for x in t), "shuffle_write_bytes": sum(x[3] for x in t),
                     "memory_spill_bytes": sum(x[4] for x in t), "disk_spill_bytes": sum(x[5] for x in t),
                     "peak_exec_mem_bytes": max((x[6] for x in t), default=None), "jvm_heap_peak_bytes": heap.get(key)})
    return rows


def record(run_id, started, settings, ok, run_ms, error, rows, engine="spark"):
    cols = list(rows[0]) if rows else []
    with psycopg.connect(read_env("DATABASE_URL")) as conn:
        conn.execute("DELETE FROM ops.engine_run WHERE run_id = %s", [run_id])
        for r in rows:
            conn.execute(f"INSERT INTO ops.engine_run (run_id, engine, started_at, settings, ok, run_ms, error, {', '.join(cols)}) "
                         f"VALUES (%s, %s, %s, %s, %s, %s, %s, {', '.join(['%s'] * len(cols))})",
                         [run_id, engine, started, json.dumps(settings), ok, run_ms, error, *r.values()])


def run(spark, args, views, tests):
    sc = spark.sparkContext
    spark.createDataFrame([(c,) for c in sorted(load_codes())], "code string").createOrReplaceTempView("codes")
    for name, sql in views:
        spark.sql(sql).createOrReplaceTempView(name)
    if args.explain:  # 계산하지 않고 계획만 (3.10). --show 뷰와 마지막 뷰
        for name in args.show + [views[-1][0]]:
            print(f"===== EXPLAIN FORMATTED {name}")
            print(spark.sql(f"EXPLAIN FORMATTED SELECT * FROM {name}").collect()[0][0])
        return
    for name in args.show:
        sc.setJobDescription(f"show {name}")
        spark.sql(f"CACHE TABLE {name}")
        spark.table(name).show(100, truncate=False)
    failed = []
    for name, sql in tests:
        sc.setJobDescription(f"test {name}")
        bad = spark.sql(sql).limit(20).collect()
        print(f"{name}: {'통과' if not bad else f'위반 {len(bad)}행 이상'}")
        for r in bad:
            print("  ", r)
        failed += [name] if bad else []
    if failed:
        raise SystemExit(f"테스트 실패: {failed}. 아무것도 쓰지 않았습니다")
    if args.no_write:
        return
    schema, table = args.target.split(".")
    new = f"{args.target}_new"
    with psycopg.connect(read_env("DATABASE_URL")) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {new}; CREATE TABLE {new} (LIKE {args.target} INCLUDING ALL)")
    sc.setJobDescription(f"write {new}")
    (spark.table(views[-1][0]).write.mode("append")
     .jdbc("jdbc:postgresql://db:5432/seoul?reWriteBatchedInserts=true", new,
           properties={"user": "seoul", "password": read_env("POSTGRES_PASSWORD"), "driver": "org.postgresql.Driver",
                       "batchsize": "10000"}))
    with psycopg.connect(read_env("DATABASE_URL")) as conn:  # 블록이 정상으로 끝나면 커밋: 교체가 한 트랜잭션
        n = conn.execute(f"SELECT count(*) FROM {new}").fetchone()[0]
        if n == 0:
            raise SystemExit(f"{new}가 0행입니다. 교체하지 않습니다")
        conn.execute(f"DROP TABLE {args.target}")
        conn.execute(f"ALTER TABLE {new} RENAME TO {table}")
    print(f"{args.target} 교체: {n:,}행")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("folder")
    parser.add_argument("target")
    parser.add_argument("--show", nargs="*", default=[])
    parser.add_argument("--driver-memory", default="8g")
    parser.add_argument("--shuffle-partitions", default="200")
    parser.add_argument("--aqe", default="true")
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument("--explain", action="store_true")
    args = parser.parse_args()
    views, tests = sql_files(ROOT / args.folder)
    settings = {"spark.driver.memory": args.driver_memory, "spark.sql.shuffle.partitions": args.shuffle_partitions,
                "spark.sql.adaptive.enabled": args.aqe}
    os.makedirs(EVENT_DIR, exist_ok=True)
    started, t0 = datetime.datetime.now(datetime.timezone.utc), time.perf_counter()
    spark = spark_session(f"spark_sql_runner {args.folder}", {**settings, "spark.eventLog.enabled": "true",
                                                             "spark.eventLog.dir": f"file:{EVENT_DIR}",
                                                             "spark.eventLog.logStageExecutorMetrics": "true"})
    run_id = spark.sparkContext.applicationId
    ok, error = False, None
    try:
        run(spark, args, views, tests)
        ok = True
    except BaseException as e:  # SystemExit(테스트 실패)도 기록한 뒤 다시 던진다
        error = f"{type(e).__name__}: {e}"[:2000]
        raise
    finally:
        run_ms = int((time.perf_counter() - t0) * 1000)
        try:
            spark.stop()
        except Exception as e:  # 드라이버가 OOM으로 죽었으면 stop도 실패한다. 이벤트 로그는 남은 만큼 읽는다
            print("spark.stop 실패:", e, file=sys.stderr)
        logs = glob.glob(f"{EVENT_DIR}/{run_id}*")
        rows = stage_rows(logs[0]) if logs else []
        record(run_id, started, settings, ok, run_ms, error, rows)
        print(f"ops.engine_run: {run_id} 단계 {len(rows)}개, {run_ms / 1000:.1f}초, ok={ok}")


if __name__ == "__main__":
    main()
