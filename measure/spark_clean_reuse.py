"""Spark `clean` 재사용 전후 측정 (측정 67, 측정 64의 후보 1, 결정 122). 운영 표와 ops에 쓰지 않는다.

base : 운영 실행기(transform/spark_sql_runner.run)와 같은 순서로 계산한다. 격리(quarantine) 캐시 → 관문(batch_status) 캐시·출력 → 시험.
       운영은 그다음 경계 표를 JDBC로 쓰는데, 여기서는 같은 계산을 캐시(cache commute)로 대신한다. JDBC 쓰기 시간은 두 변형 모두 빠진다.
reuse: 위와 같되 맨 앞에서 20_clean(키 9개 집계)을 한 번 캐시한다. 격리의 V8 조인, 관문, 경계 표가 원본부터 다시 집계하지 않고 캐시를 읽는다.
bc   : base와 같되 격리(21_quarantine)의 V8 조인만 바꾼다(측정 64 후보 2, 측정 68). 충돌 키(clean WHERE conflict)만 먼저 구해
       broadcast하고, 통과 행은 셔플 없이 그 작은 표와 조인한다. 충돌 키가 아주 많으면 broadcast가 커지므로 실험용이다.
측정 구간이 끝나면 세 결과를 운영 표(stg.living_commute, stg.living_batch_status, stg.living_quarantine)와 두 방향 EXCEPT ALL로 비교한다.
운영 표는 JDBC로 읽기만 한다. 비교 작업은 job 설명이 "compare ..."라 단계 표에서 측정 구간과 나뉜다.

  docker compose run --rm --no-deps spark-job python -u -m measure.spark_clean_reuse base|reuse|bc
이벤트 로그는 logs/spark-events/에 남는다(컨테이너에서 logs/는 그 폴더만 쓰기). 단계 표: python measure/spark_stages.py <로그>
--selftest: 작은 표로 "clean을 캐시하면 하류 뷰가 캐시를 읽는다"(실행 계획에 Scan In-memory table clean)를 확인한다. 잠금·Iceberg 없이 돈다.
"""
import argparse
import datetime
import json
import sys
import time

from common import ROOT, heavy_lock, read_env, temp_budget
from transform.spark_sql_runner import EVENT_DIR, sql_files

KEY_COLS = ["target_ym", "collected_date", "weekday", "arrive_hour_raw", "origin_cd", "dest_cd", "sex", "age", "move_type"]
# bc 변형의 격리: 21_quarantine.sql과 같은 결과, V8 조인만 충돌 키 broadcast로
QUARANTINE_BC = f"""
SELECT target_ym, collected_date, source_file, row_no, reasons
FROM checked
WHERE reasons <> ''
UNION ALL
SELECT /*+ BROADCAST(k) */ c.target_ym, c.collected_date, c.source_file, c.row_no, 'V8'
FROM checked c
JOIN (SELECT {', '.join(KEY_COLS)} FROM clean WHERE conflict) k
  ON c.reasons = '' AND {' AND '.join(f'k.{k} = c.{k}' for k in KEY_COLS)}
"""
PAIRS = [("commute", "stg.living_commute"), ("batch_status", "stg.living_batch_status"), ("quarantine", "stg.living_quarantine")]
MIN_TEMP_GB = 394  # 운영 실행기 --min-temp-gb 기본값과 같다


def storage(sc):
    """캐시된 표마다 메모리·디스크 크기(GB)와 캐시된 파티션 수."""
    return [{"name": r.name(), "mem_gb": round(r.memSize() / 1e9, 2), "disk_gb": round(r.diskSize() / 1e9, 2),
             "cached_parts": r.numCachedPartitions(), "parts": r.numPartitions()} for r in sc._jsc.sc().getRDDStorageInfo()]


def compute(spark, variant, views, tests):
    """측정 구간. 돌려주는 값: 단계별 경과(초), 캐시 크기, 실패한 시험."""
    from transform.living_iceberg import load_codes
    sc = spark.sparkContext
    spark.createDataFrame([(c,) for c in sorted(load_codes())], "code string").createOrReplaceTempView("codes")
    for name, sql in views:
        spark.sql(QUARANTINE_BC if variant == "bc" and name == "quarantine" else sql).createOrReplaceTempView(name)
    if variant == "bc":
        plan = spark.sql("EXPLAIN SELECT * FROM quarantine").collect()[0][0]
        print(f"  quarantine 계획에 BroadcastHashJoin: {'있음' if 'BroadcastHashJoin' in plan else '없음'}, "
              f"SortMergeJoin: {'있음' if 'SortMergeJoin' in plan else '없음'}", flush=True)
    sec, sizes = {}, {}
    for name in (["clean"] if variant == "reuse" else []) + ["quarantine", "batch_status"]:
        sc.setJobDescription(f"cache {name}")
        t = time.perf_counter()
        spark.sql(f"CACHE TABLE {name}")
        sec[f"cache {name}"] = round(time.perf_counter() - t, 1)
        if name == "batch_status":
            spark.table(name).show(100, truncate=False)
        sizes[name] = storage(sc)
        print(f"cache {name}: {sec[f'cache {name}']}초, 저장 {json.dumps(sizes[name], ensure_ascii=False)}", flush=True)
        if name == "clean":  # 하류 뷰가 실제로 캐시를 읽는지 계획으로 확인(계산하지 않음)
            for down in ("quarantine", "batch_status", "commute"):
                plan = spark.sql(f"EXPLAIN SELECT * FROM {down}").collect()[0][0]
                print(f"  {down} 계획에 캐시된 clean: {'있음' if 'In-memory table clean' in plan else '없음'}", flush=True)
    failed = []
    for name, sql in tests:
        sc.setJobDescription(f"test {name}")
        t = time.perf_counter()
        bad = spark.sql(sql).limit(20).collect()
        sec[f"test {name}"] = round(time.perf_counter() - t, 1)
        print(f"{name}: {'통과' if not bad else f'위반 {len(bad)}행 이상'}", flush=True)
        failed += [name] if bad and name.startswith("test_") else []
    sc.setJobDescription("cache commute")  # 운영의 "write stg.living_commute_new"와 같은 계산(쓰기 대신 캐시)
    t = time.perf_counter()
    spark.sql("CACHE TABLE commute")
    sec["cache commute"] = round(time.perf_counter() - t, 1)
    sizes["commute"] = storage(sc)
    print(f"cache commute: {sec['cache commute']}초", flush=True)
    return sec, sizes, failed


def compare(spark):
    """세 결과마다 운영 표와 두 방향 EXCEPT ALL 행 수. 운영 표는 대상연월별 조건으로 나눠 JDBC로 읽고 내 쪽 열 형으로 바꾼다."""
    from pyspark.sql import functions as F
    sc = spark.sparkContext
    yms = sorted(r[0] for r in spark.table("batch_status").select("target_ym").distinct().collect())
    preds = [f"target_ym = '{ym}'" for ym in yms] + [f"target_ym NOT IN ({', '.join(repr(y) for y in yms)})"]
    props = {"user": "seoul", "password": read_env("POSTGRES_PASSWORD"), "driver": "org.postgresql.Driver", "fetchsize": "10000"}
    out = {}
    for view, table in PAIRS:
        sc.setJobDescription(f"compare {table}")
        mine = spark.table(view)
        theirs = (spark.read.jdbc("jdbc:postgresql://db:5432/seoul", table, predicates=preds, properties=props)
                  .select([F.col(f.name).cast(f.dataType).alias(f.name) for f in mine.schema.fields]))
        n_mine, n_theirs = mine.count(), theirs.count()
        a, b = mine.exceptAll(theirs).count(), theirs.exceptAll(mine).count()
        out[table] = {"mine": n_mine, "ops": n_theirs, "mine_minus_ops": a, "ops_minus_mine": b}
        print(f"{table}: 내 결과 {n_mine:,}행, 운영 {n_theirs:,}행, 내것-운영 {a}행, 운영-내것 {b}행", flush=True)
    return out


def selftest():
    """작은 집계 뷰를 캐시하면 그것을 읽는 하류 뷰의 실행 계획에 InMemoryTableScan이 생기고, 원본 집계(HashAggregate)를 다시 하지 않는다."""
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").getOrCreate()
    spark.range(1000).selectExpr("id % 7 AS k", "id AS v").createOrReplaceTempView("src")
    spark.sql("SELECT k, count(*) AS n FROM src GROUP BY k").createOrReplaceTempView("clean")
    spark.sql("SELECT sum(n) AS total FROM clean").createOrReplaceTempView("down")  # 캐시 전에 만든 뷰(실행기와 같은 순서)
    before = spark.sql("EXPLAIN SELECT * FROM down").collect()[0][0]
    spark.sql("CACHE TABLE clean")
    after = spark.sql("EXPLAIN SELECT * FROM down").collect()[0][0]
    mark = "Scan In-memory table clean"  # EXPLAIN 기본 모양에서 InMemoryTableScan이 찍히는 이름
    assert mark not in before and mark in after, (before, after)
    assert spark.table("down").first()[0] == 1000
    print("selftest 통과: 캐시 전에 만든 하류 뷰도 캐시된 clean을 읽는다")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("variant", nargs="?", choices=["base", "reuse", "bc"])
    p.add_argument("--selftest", action="store_true")
    a = p.parse_args()
    if a.selftest:
        return selftest()
    if not a.variant:
        p.error("variant가 필요합니다")
    lock = heavy_lock()  # noqa: F841  프로세스가 끝날 때 풀리고 /scratch를 비운다
    budget = temp_budget()
    if budget < MIN_TEMP_GB * 1e9:
        raise SystemExit(f"임시 공간 {budget / 1e9:.0f}GB < 필요 {MIN_TEMP_GB}GB. 시작하지 않습니다")
    from transform.iceberg import spark_session
    views, tests = sql_files(ROOT / "spark_sql" / "living")
    settings = {"spark.driver.memory": "8g", "spark.sql.shuffle.partitions": "200", "spark.sql.adaptive.enabled": "true"}
    spark = spark_session(f"spark_clean_reuse {a.variant}", {**settings, "spark.eventLog.enabled": "true",
                                                            "spark.eventLog.dir": f"file:{EVENT_DIR}",
                                                            "spark.eventLog.logStageExecutorMetrics": "true"})
    app = spark.sparkContext.applicationId
    started = datetime.datetime.now().isoformat(timespec="seconds")
    print(f"app {app}, variant {a.variant}, 시작 {started}, 임시 여유 {budget / 1e9:.0f}GB", flush=True)
    t0 = time.perf_counter()
    try:
        sec, sizes, failed = compute(spark, a.variant, views, tests)
        measured = round(time.perf_counter() - t0, 1)
        print(f"측정 구간 {measured}초", flush=True)
        t1 = time.perf_counter()
        diffs = compare(spark)
        summary = {"app": app, "variant": a.variant, "started": started, "measured_sec": measured,
                   "compare_sec": round(time.perf_counter() - t1, 1), "sec": sec, "storage": sizes, "failed_tests": failed,
                   "diffs": diffs}
        print("SUMMARY " + json.dumps(summary, ensure_ascii=False), flush=True)
    finally:
        spark.stop()
    bad = failed or any(d["mine_minus_ops"] or d["ops_minus_mine"] for d in diffs.values())
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
