"""Spark SQL 파일 묶음 실행기 (design_remaining_work.md 3.3, 결정 88, 90, 94-1).

폴더 안 *.sql을 이름순으로 임시 뷰로 만든다. 파일 하나 = 뷰 하나, 뷰 이름 = 파일 이름에서 번호를 뺀 것(10_checked.sql -> checked).
test_*.sql은 위반 행을 돌려주는 SQL이다. warn_*.sql도 같지만 위반을 출력만 한다(결정 100). test_ 하나라도 행이 있으면 경계 표(마지막 뷰)는 바꾸지 않고 종료 코드 1(dbt 테스트와 같은 모양).
--save 뷰는 테스트가 실패해도 쓴다: 거절된 배치가 운영 카드·알림에 보여야 하므로(판단 64).
마지막 뷰를 JDBC로 {target}_new에 쓰고 PostgreSQL 트랜잭션 하나에서 {target}으로 옮긴다(결정 90). 매번 전체 재계산.
옮기기 = 비우고 복사(F-13 처방 A). 표를 지우고 이름을 바꾸면 그 표를 읽는 dbt 뷰(wh.fact_commute) 때문에 거절된다.
--save 뷰=표: 마지막 뷰 말고도 PostgreSQL에 남길 뷰(관문 판정, 격리 행. 판단 64). 같은 트랜잭션에서 옮긴다.
Spark 이벤트 로그를 켜고, 끝나면(실패해도) 단계별 지표를 ops.engine_run에 남긴다. 한 행 = 실행 x 단계.
실행(Airflow 컨테이너):
  /opt/airflow/spark-venv/bin/python -m transform.spark_sql_runner spark_sql/living stg.living_commute --show batch_status
      --save batch_status=stg.living_batch_status quarantine=stg.living_quarantine
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

from common import ROOT, heavy_lock, read_env, temp_budget
from transform.iceberg import spark_session
from transform.living_iceberg import load_codes

EVENT_DIR = "/tmp/spark-events"


def sql_files(folder):
    files = sorted(folder.glob("*.sql"))
    checks = ("test_", "warn_")  # warn_: 위반을 출력만 하고 실패시키지 않는다(결정 100)
    views = [(p.stem.split("_", 1)[1], p.read_text(encoding="utf-8")) for p in files if not p.stem.startswith(checks)]
    tests = [(p.stem, p.read_text(encoding="utf-8")) for p in files if p.stem.startswith(checks)]
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


def swap(conn, tables):
    """{표}_new를 {표}로 옮긴다. 부르는 쪽이 커밋하므로 전부 한 트랜잭션이다. 첫 표가 0행이면 아무것도 옮기지 않는다."""
    for i, table in enumerate(tables):
        n = conn.execute(f"SELECT count(*) FROM {table}_new").fetchone()[0]
        if i == 0 and n == 0:
            raise SystemExit(f"{table}_new가 0행입니다. 옮기지 않습니다")
        # ponytail: 한 번 더 복사한다. 측정 38: 경계 표 1,378만 행 72초, 격리 3,194만 행 258초, 트랜잭션 334초 동안 읽기가 잠긴다.
        # 잠금이 문제가 되면 F-13 선택지 C(이름 바꾸고 뷰 다시 만들기)
        t = time.time()
        conn.execute(f"TRUNCATE {table}")
        conn.execute(f"INSERT INTO {table} SELECT * FROM {table}_new")
        conn.execute(f"DROP TABLE {table}_new")
        print(f"{table} 교체: {n:,}행, {time.time() - t:.1f}초(커밋 전)")


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
    saves = [(views[-1][0], args.target)] + [tuple(s.split("=")) for s in args.save]
    for name in [v for v, _ in saves[1:] if v not in args.show] + args.show:  # 남길 뷰도 캐시: 테스트·쓰기가 다시 계산하지 않게
        sc.setJobDescription(f"cache {name}")
        spark.sql(f"CACHE TABLE {name}")
        if name in args.show:
            spark.table(name).show(100, truncate=False)
    failed = []
    for name, sql in tests:
        sc.setJobDescription(f"test {name}")
        bad = spark.sql(sql).limit(20).collect()
        print(f"{name}: {'통과' if not bad else f'위반 {len(bad)}행 이상'}")
        for r in bad:
            print("  ", r)
        failed += [name] if bad and name.startswith("test_") else []
    if args.no_write:
        if failed:
            raise SystemExit(f"테스트 실패: {failed}")
        return
    if failed:  # 관문 판정·격리 행은 남겨 운영 카드·알림에 거절이 보이게 한다(판단 64). 경계 표는 이전 실행 그대로
        write(spark, saves[1:])
        raise SystemExit(f"테스트 실패: {failed}. 경계 표 {args.target}은 바꾸지 않았습니다")
    write(spark, saves)


def write(spark, saves):
    """(뷰, 표)마다 JDBC로 {표}_new에 쓰고, 한 트랜잭션에서 모두 옮긴다."""
    if not saves:
        return
    for view, table in saves:
        new = f"{table}_new"
        with psycopg.connect(read_env("DATABASE_URL")) as conn:
            conn.execute(f"DROP TABLE IF EXISTS {new}; CREATE TABLE {new} (LIKE {table} INCLUDING ALL)")
        spark.sparkContext.setJobDescription(f"write {new}")
        (spark.table(view).write.mode("append")
         .jdbc("jdbc:postgresql://db:5432/seoul?reWriteBatchedInserts=true", new,
               properties={"user": "seoul", "password": read_env("POSTGRES_PASSWORD"), "driver": "org.postgresql.Driver",
                           "batchsize": "10000"}))
    t = time.time()
    with psycopg.connect(read_env("DATABASE_URL")) as conn:  # 블록이 정상으로 끝나면 커밋: 모든 표 교체가 한 트랜잭션
        swap(conn, [table for _, table in saves])
    print(f"교체 트랜잭션(커밋 포함): {time.time() - t:.1f}초")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("folder")
    parser.add_argument("target")
    parser.add_argument("--show", nargs="*", default=[])
    parser.add_argument("--save", nargs="*", default=[], metavar="VIEW=TABLE")
    parser.add_argument("--driver-memory", default="8g")
    parser.add_argument("--shuffle-partitions", default="200")
    parser.add_argument("--aqe", default="true")
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument("--explain", action="store_true")
    # 문턱 = 같은 작업의 실측 최대 C: 사용 x 1.2 (J-7, 결정 107 S6). temp_budget()이 남길 몫 50GiB를 이미 빼므로 여기에는 더하지 않는다.
    # 생활이동 F-9 처방 전: 측정 38 C: 여유 641.24 → 313.00GB = 328.24GB x 1.2 = 393.9 → 394. 호스트 C: 여유로는 약 447.7GB가 있어야 시작한다.
    # ponytail: 측정 1회 x 1.2(측정 37 메모리 예산과 같은 방식). F-9 처방 뒤 다시 잰 최대로 바꾼다.
    parser.add_argument("--min-temp-gb", type=int, default=394,
                        help="임시 공간이 이보다 적으면 시작하지 않는다(Spark에는 임시 폴더 상한이 없다, L3·L4)")
    args = parser.parse_args()
    lock = heavy_lock()  # noqa: F841  프로세스가 끝날 때까지 잡는다(L5)
    budget = temp_budget()
    if not args.explain and budget < args.min_temp_gb * 1e9:
        raise SystemExit(f"임시 공간 {budget / 1e9:.0f}GB < 필요 {args.min_temp_gb}GB(호스트 C: 여유에서 남길 몫을 뺀 값). 시작하지 않습니다")
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
