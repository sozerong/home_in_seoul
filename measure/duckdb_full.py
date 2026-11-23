"""DuckDB 실험 C (측정 63, 처음 handoff_night_2026-11-23.md N2): 새 프로세스 없이 한 프로세스·한 연결로 생활이동 10배치 전체(측정 51과 같은 SQL·단계), Spark와 두 방향 비교.
실험 B 변형 2(배치마다 Parquet로 내보내고 DROP)를 전체로. 202601처럼 threads 12에서 넘치면 같은 연결에서 threads 4로 그 배치만 다시(F-31).
운영 표(ops 포함)에 쓰지 않는다. Spark 쪽은 READ_ONLY로 붙여 읽기만 한다.
  docker compose run --rm --no-deps duckdb-job python -m common lock -- python -u -m measure.duckdb_full
"""
import json
import os
import sys
import time

sys.path.insert(0, "/opt/project")
import duckdb  # noqa: E402

from common import SCRATCH  # noqa: E402
from measure import duckdb_mem as E  # noqa: E402
from measure.duckdb_sql import compare, translate  # noqa: E402
from transform.spark_sql_runner import sql_files  # noqa: E402

_, TESTS = sql_files(E.ROOT / "spark_sql" / "living")


def main():
    class A:  # duckdb_mem.connect가 보는 설정
        memory_limit, threads, pio, max_temp, set = "16GiB", "12", None, "60GiB", []
    E.ARGS = A
    run = f"C-full-{time.strftime('%H%M%S')}"
    work = f"{SCRATCH}/duckdb/full/{run}"
    os.makedirs(f"{work}/b", exist_ok=True)
    prof = E.OUT / "profiles" / run
    prof.mkdir(parents=True, exist_ok=True)
    import subprocess
    vm = subprocess.Popen([sys.executable, "-u", "-m", "measure.vm_watch", f"duckdb_full_{run}"], cwd="/opt/project")
    sampler = E.Sampler(E.OUT / "runs" / f"{run}.jsonl")
    location = E.metadata_location()
    t0 = time.perf_counter()
    out = {"run": run, "location": location, "batches": [], "stages": []}
    con = E.connect(A)
    sampler.attach(con)
    con.execute("PRAGMA enable_profiling = 'json'")
    out["settings"] = E.settings(con)
    try:
        batches = con.execute(f"SELECT DISTINCT target_ym, collected_date FROM iceberg_scan('{location}') ORDER BY 1, 2").fetchall()
        for ym, cd in batches:
            cd = str(cd)
            d = f"{work}/b/{ym}_{cd}"
            os.makedirs(d, exist_ok=True)
            E.batch_views(con, location, ym, cd)
            row = {"batch": f"{ym}:{cd}", "tries": []}
            for threads in (12, 4):
                con.execute(f"SET threads = {threads}")
                con.execute("DROP TABLE IF EXISTS clean_b")
                r = E.timed(con, sampler, f"{ym} clean t{threads}", f"CREATE TEMP TABLE clean_b AS {translate(E.SQL['clean'])}", prof)
                row["tries"].append(r)
                if r["ok"]:
                    break
                if "Out of Memory" not in (r["err"] or ""):
                    raise SystemExit(f"{ym}: 메모리 밖 오류 {r['err']}")
            else:
                raise SystemExit(f"{ym}: threads 4로도 실패")
            con.execute("CREATE OR REPLACE TEMP VIEW quarantine_b AS " + translate(E.SQL["quarantine"]).replace("JOIN clean k", "JOIN clean_b k"))
            row["export"] = E.timed(con, sampler, f"{ym} export",
                                    f"COPY clean_b TO '{d}/clean.parquet' (FORMAT parquet); "
                                    f"COPY (SELECT * FROM quarantine_b) TO '{d}/quarantine.parquet' (FORMAT parquet)", prof)
            if not row["export"]["ok"]:
                raise SystemExit(f"{ym}: 내보내기 실패 {row['export']['err']}")
            con.execute("DROP VIEW quarantine_b; DROP TABLE clean_b")
            con.execute("SET threads = 12")
            row["after"] = E.snapshot(sampler, f"{ym} after batch")
            out["batches"].append(row)
        # 배치를 넘는 단계: 측정 51의 status, commute, compare와 같다(Parquet로 읽음)
        for name in ("checked", "clean", "quarantine", "batch_status"):
            con.execute(f"DROP VIEW IF EXISTS {name}")
        con.execute(f"CREATE VIEW quarantine AS SELECT * FROM read_parquet('{work}/b/*/quarantine.parquet')")
        con.execute(f"CREATE VIEW clean AS SELECT * FROM read_parquet('{work}/b/*/clean.parquet')")
        out["stages"].append(E.timed(con, sampler, "batch_status", f"CREATE TEMP TABLE batch_status AS {translate(E.SQL['batch_status'])}", prof))
        for name, sql in TESTS:
            bad = con.execute(f"SELECT * FROM ({translate(sql)}) LIMIT 20").fetchall()
            out.setdefault("tests", {})[name] = len(bad)
            if bad and name.startswith("test_"):
                raise SystemExit(f"테스트 실패 {name}: {bad}")
        out["stages"].append(E.timed(con, sampler, "commute", f"CREATE TEMP TABLE result AS {translate(E.SQL['commute'])}", prof))
        sampler.set_phase("compare")
        tc = time.perf_counter()
        out["compare"] = compare(con, [("result", "living_commute"), ("batch_status", "living_batch_status"), ("quarantine", "living_quarantine")])
        out["compare_sec"] = round(time.perf_counter() - tc, 1)
        out["parquet_bytes"] = sum(os.path.getsize(os.path.join(dp, f)) for dp, _, fs in os.walk(work) for f in fs if f.endswith(".parquet"))
    finally:
        out["sec"] = round(time.perf_counter() - t0, 1)
        out["peaks"] = {k: {"rss_gib": round(v["rss"] / E.GIB, 2), "mem_gib": round(v["mem"] / E.GIB, 2), "temp_gib": round(v["temp"] / E.GIB, 2),
                            "cgroup_gib": round(v["cgroup"] / E.GIB, 2)} for k, v in sampler.peaks.items()}
        import resource
        out["ru_maxrss_gib"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024 / E.GIB, 2)
        sampler.close()
        vm.terminate()
        json.dump(out, open(E.OUT / "runs" / f"{run}.summary.json", "w"), ensure_ascii=False, indent=1, default=str)
        print(f"끝 {run}: {out['sec']}초, 최대 RSS {out['ru_maxrss_gib']}GiB, 비교 {out.get('compare')}", flush=True)
    if any(out["compare"].values()):
        raise SystemExit(3)


if __name__ == "__main__":
    main()
