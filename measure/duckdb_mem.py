"""DuckDB 메모리 실험 A, B (측정 60~62, 처음 handoff_night_2026-11-23.md N2). 운영 표(ops 포함)에 쓰지 않는다.

계산 컨테이너에서 잠금을 잡고 돈다:
  docker compose run --rm --no-deps duckdb-job python -m common lock -- python -u -m measure.duckdb_mem A ...
A: 배치 하나로 문장을 나눠(checked / clean / quarantine / status) 문장마다 duckdb_memory() 태그별 최대와 RSS(VmRSS, VmHWM)를 1초 간격으로 잰다.
B: 여러 배치를 변형 1~5로. 배치가 끝날 때마다 태그와 RSS를 남긴다.
기록: logs/duckdb_mem/runs/<run>.jsonl(1초 표본), <run>.summary.json(문장·배치별 최대), profiles/<run>/(문장별 프로파일 JSON).
"""
import argparse
import datetime
import json
import os
import resource
import subprocess
import sys
import threading
import time
import uuid

sys.path.insert(0, "/opt/project")
import duckdb  # noqa: E402

from common import ROOT, SCRATCH  # noqa: E402
from measure.duckdb_sql import TEMP_DIR, metadata_location, translate  # noqa: E402
from transform.living_iceberg import load_codes  # noqa: E402
from transform.spark_sql_runner import sql_files  # noqa: E402

OUT = ROOT / "logs" / "duckdb_mem"  # 측정 60~63의 원자료는 logs/night_20261123/(옛 위치)
VIEWS, _ = sql_files(ROOT / "spark_sql" / "living")
SQL = dict(VIEWS)
GIB = 2 ** 30


def proc_mem():
    d = {}
    with open("/proc/self/status") as f:
        for line in f:
            k, v = line.split(":", 1)
            if k in ("VmRSS", "VmHWM"):
                d[k] = int(v.split()[0]) * 1024
    for p in ("/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory/memory.usage_in_bytes"):
        if os.path.exists(p):
            d["cgroup"] = int(open(p).read())
            break
    return d


def reset_hwm():
    """VmHWM(프로세스 최대 RSS)을 지금 RSS로 되돌린다. 문장마다 최대를 따로 재려고."""
    try:
        with open("/proc/self/clear_refs", "w") as f:
            f.write("5")
    except OSError:
        pass


class Sampler:
    """1초마다 duckdb_memory()와 RSS를 jsonl 한 줄로. phase마다 최대를 모은다."""

    def __init__(self, path, seconds=1.0):
        self.f, self.seconds, self.lock = open(path, "w", encoding="utf-8"), seconds, threading.Lock()
        self.cur, self.phase, self.t0 = None, "idle", time.perf_counter()
        self.peaks = {}
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.loop, daemon=True)
        self.thread.start()

    def attach(self, con):
        with self.lock:
            self.cur = con.cursor() if con is not None else None

    def set_phase(self, phase):
        with self.lock:
            self.phase = phase
            self.peaks.setdefault(phase, {"tags": {}, "temp": 0, "mem": 0, "rss": 0, "cgroup": 0, "n": 0})

    def sample(self):
        with self.lock:
            row = {"t": round(time.perf_counter() - self.t0, 1), "ts": datetime.datetime.now().isoformat(timespec="seconds"),
                   "phase": self.phase, **proc_mem()}
            if self.cur is not None:
                try:
                    tags = self.cur.execute("SELECT tag, memory_usage_bytes, temporary_storage_bytes FROM duckdb_memory()").fetchall()
                    row["mem"] = {t: m for t, m, _ in tags if m}
                    row["temp"] = sum(x for _, _, x in tags)
                except Exception as e:  # noqa: BLE001  재는 쪽 실패는 기록만
                    row["err"] = f"{type(e).__name__}: {e}"[:200]
            p = self.peaks.setdefault(self.phase, {"tags": {}, "temp": 0, "mem": 0, "rss": 0, "cgroup": 0, "n": 0})
            p["n"] += 1
            p["rss"] = max(p["rss"], row.get("VmRSS", 0))
            p["cgroup"] = max(p["cgroup"], row.get("cgroup", 0))
            p["temp"] = max(p["temp"], row.get("temp", 0))
            p["mem"] = max(p["mem"], sum(row.get("mem", {}).values()))
            for t, m in row.get("mem", {}).items():
                p["tags"][t] = max(p["tags"].get(t, 0), m)
            self.f.write(json.dumps(row) + "\n")
            self.f.flush()
            return row

    def loop(self):
        while not self.stop.wait(self.seconds):
            self.sample()

    def close(self):
        self.stop.set()
        self.thread.join(timeout=5)
        self.f.close()


def connect(args):
    con = duckdb.connect()
    con.execute(f"SET temp_directory = '{TEMP_DIR}'")
    con.execute(f"SET max_temp_directory_size = '{args.max_temp}'")
    if args.memory_limit:
        con.execute(f"SET memory_limit = '{args.memory_limit}'")
    if args.threads:
        con.execute(f"SET threads = {int(args.threads)}")
    if args.pio:
        con.execute(f"SET preserve_insertion_order = {args.pio}")
    for kv in args.set:
        k, v = kv.split("=", 1)
        con.execute(f"SET {k} = {v}")
    con.execute("LOAD iceberg")
    con.execute("CREATE TEMP TABLE codes (code VARCHAR)")
    con.executemany("INSERT INTO codes VALUES (?)", [(c,) for c in sorted(load_codes())])
    con.execute("ATTACH ':memory:' AS lh; CREATE SCHEMA lh.living")
    return con


def settings(con):
    keys = ["memory_limit", "threads", "preserve_insertion_order", "max_temp_directory_size", "temp_directory"]
    keys += [k.split("=", 1)[0] for k in ARGS.set]
    return {k: con.execute(f"SELECT current_setting('{k}')").fetchone()[0] for k in keys} | {"duckdb": duckdb.__version__}


def batch_views(con, location, ym, cd):
    """ym에 쉼표로 여러 대상연월을 주면(A 범위 실험) 그 달 전부(수집일 무관)."""
    where = (f"target_ym IN ({', '.join(repr(y) for y in ym.split(','))})" if "," in ym
             else f"target_ym = '{ym}' AND collected_date = DATE '{cd}'")
    con.execute(f"CREATE OR REPLACE VIEW lh.living.migration_rows AS SELECT * FROM iceberg_scan('{location}') WHERE {where}")
    for name in ("checked", "clean", "quarantine", "batch_status"):
        con.execute(f"CREATE OR REPLACE TEMP VIEW {name} AS {translate(SQL[name])}")


STMTS = {
    "checked": "SELECT count(*), count_if(reasons = '') FROM checked",
    # 모든 열을 써서 집계 상태가 빠지지 않게(열 가지치기 방지)
    "clean": "SELECT count(*), sum(rows), count_if(conflict), sum(pop_inexact), min(avg_minutes), max(population) FROM clean",
    "quarantine": "SELECT count(*), count_if(reasons = 'V8') FROM quarantine",
    # 같은 묶기를 고정 길이 키로(문자열 키 9개 → 정수·날짜·해시). 그룹 수가 같은지는 count(*)로 clean과 견준다.
    # 예측(n2_docs.md 추가 절): VARCHAR가 없으면 TupleData 빠른 경로를 타서 ALLOCATOR가 흘림에 비례해 늘지 않는다
    "clean_fixed": "SELECT count(*), sum(rows), count_if(conflict), sum(pop_inexact), min(avg_minutes), max(population) FROM ("
                   "SELECT CAST(target_ym AS INT) AS ym, collected_date, hash(weekday) AS wd, CAST(arrive_hour_raw AS INT) AS h, "
                   "CAST(origin_cd AS BIGINT) AS o, CAST(dest_cd AS BIGINT) AS d, hash(sex) AS s, CAST(age AS INT) AS a, hash(move_type) AS mt, "
                   "min(avg_minutes) AS avg_minutes, min(population) AS population, count(*) AS rows, "
                   "min(hash(avg_minutes_raw, population_raw)) <> max(hash(avg_minutes_raw, population_raw)) AS conflict, "
                   "count_if(population IS NOT NULL AND CAST(population AS DOUBLE) <> pop_double) AS pop_inexact "
                   "FROM checked WHERE reasons = '' GROUP BY ALL)",
    "status": "SELECT * FROM batch_status",  # 측정 49의 모양: clean이 p와 V8 조인에서 두 번 계산된다
}


def timed(con, sampler, label, sql, prof_dir):
    reset_hwm()
    sampler.set_phase(label)
    con.execute(f"PRAGMA profiling_output = '{prof_dir}/{label.replace(' ', '_').replace(':', '-')}.json'")
    t0 = time.perf_counter()
    err = None
    try:
        res = con.execute(sql).fetchall()
    except Exception as e:  # noqa: BLE001  실패도 결과다
        res, err = None, f"{type(e).__name__}: {e}"[:300]
    sec = round(time.perf_counter() - t0, 1)
    con.execute(f"PRAGMA profiling_output = '{prof_dir}/_setup.json'")
    hwm = proc_mem().get("VmHWM", 0)
    sampler.set_phase(f"{label} after")
    time.sleep(2)
    after = sampler.sample()
    p = sampler.peaks[label]
    row = {"label": label, "sec": sec, "ok": err is None, "err": err, "result": res if res and len(res) <= 12 else None,
           "peak_tags_gib": {t: round(b / GIB, 2) for t, b in sorted(p["tags"].items(), key=lambda kv: -kv[1])},
           "peak_mem_gib": round(p["mem"] / GIB, 2), "peak_temp_gib": round(p["temp"] / GIB, 2),
           "peak_rss_gib": round(max(p["rss"], hwm) / GIB, 2), "peak_cgroup_gib": round(p["cgroup"] / GIB, 2),
           "after_rss_gib": round(after.get("VmRSS", 0) / GIB, 2),
           "after_tags_gib": {t: round(b / GIB, 2) for t, b in after.get("mem", {}).items()}}
    print(json.dumps(row, ensure_ascii=False, default=str), flush=True)
    return row


def snapshot(sampler, label, con_alive=True):
    sampler.set_phase(label)
    time.sleep(2)
    s2 = sampler.sample()
    time.sleep(6)  # jemalloc 감쇠 5초(n2_docs.md)를 넘겨 한 번 더
    s = sampler.sample()
    row = {"label": label, "rss2s_gib": round(s2.get("VmRSS", 0) / GIB, 2),
           "rss_gib": round(s.get("VmRSS", 0) / GIB, 2), "cgroup_gib": round(s.get("cgroup", 0) / GIB, 2),
           "tags_gib": {t: round(b / GIB, 2) for t, b in s.get("mem", {}).items()}, "temp_gib": round(s.get("temp", 0) / GIB, 2)}
    print(json.dumps(row, ensure_ascii=False, default=str), flush=True)
    return row


def run_a(args, sampler, prof_dir, location):
    ym, cd = args.batch.split(":")
    con = connect(args)
    sampler.attach(con)
    con.execute("PRAGMA enable_profiling = 'json'")
    batch_views(con, location, ym, cd)
    out = {"settings": settings(con), "batch": args.batch, "stmts": []}
    for name in args.stmts:
        out["stmts"].append(timed(con, sampler, f"{name}", STMTS[name], prof_dir))
        if not out["stmts"][-1]["ok"] and args.stop_on_fail:
            break
    return out


def run_b(args, sampler, prof_dir, location):
    v = args.variant
    batches = [b.split(":") for b in args.batches]
    work = f"{SCRATCH}/duckdb/mem/{RUN}"
    os.makedirs(work, exist_ok=True)
    out = {"variant": v, "batches": args.batches, "rows": []}
    if v == 5:  # 배치마다 새 프로세스(측정 51 방식). 자식은 v2와 같은 일을 배치 하나로
        for ym, cd in batches:
            cmd = [sys.executable, "-u", __file__, "B", "--variant", "2", "--batches", f"{ym}:{cd}", "--child-of", RUN,
                   "--max-temp", args.max_temp] + (["--memory-limit", args.memory_limit] if args.memory_limit else []) \
                + (["--threads", args.threads] if args.threads else []) + (["--set", *args.set] if args.set else [])
            t = time.perf_counter()
            p = subprocess.Popen(cmd)
            _, status, ru = os.wait4(p.pid, 0)
            rc = os.waitstatus_to_exitcode(status)
            row = {"batch": f"{ym}:{cd}", "rc": rc, "sec": round(time.perf_counter() - t, 1), "child_max_rss_gib": round(ru.ru_maxrss * 1024 / GIB, 2)}
            row["parent_after"] = snapshot(sampler, f"{ym} parent after")
            out["rows"].append(row)
            print(json.dumps(row, ensure_ascii=False, default=str), flush=True)
            if rc != 0:
                break
        return out
    con = connect(args)
    sampler.attach(con)
    con.execute("PRAGMA enable_profiling = 'json'")
    out["settings"] = settings(con)
    out["start"] = snapshot(sampler, "start")
    for i, (ym, cd) in enumerate(batches):
        if con is None:  # v3·v4: 앞 배치 뒤 닫았으면 새 인스턴스
            con = connect(args)
            sampler.attach(con)
            con.execute("PRAGMA enable_profiling = 'json'")
            out["rows"][-1]["after_reopen"] = snapshot(sampler, f"{ym} reopen")
        batch_views(con, location, ym, cd)
        con.execute("DROP TABLE IF EXISTS clean_b")
        r = timed(con, sampler, f"{ym} clean", f"CREATE TEMP TABLE clean_b AS {translate(SQL['clean'])}", prof_dir)
        con.execute("CREATE OR REPLACE TEMP VIEW quarantine_b AS " + translate(SQL["quarantine"]).replace("JOIN clean k", "JOIN clean_b k"))
        row = {"batch": f"{ym}:{cd}", "clean": r}
        if r["ok"]:
            if v == 1:  # 측정 50: 누적 임시 표
                if i == 0:
                    con.execute("CREATE TEMP TABLE clean_all AS SELECT * FROM clean_b LIMIT 0")
                    con.execute("CREATE TEMP TABLE quarantine_all AS SELECT * FROM quarantine_b LIMIT 0")
                row["acc"] = timed(con, sampler, f"{ym} accumulate",
                                   "INSERT INTO clean_all SELECT * FROM clean_b; INSERT INTO quarantine_all SELECT * FROM quarantine_b", prof_dir)
            else:  # v2~v4: Parquet로 내보내고 DROP
                d = f"{work}/{ym}_{cd}"
                os.makedirs(d, exist_ok=True)
                row["acc"] = timed(con, sampler, f"{ym} export",
                                   f"COPY clean_b TO '{d}/clean.parquet' (FORMAT parquet); "
                                   f"COPY (SELECT * FROM quarantine_b) TO '{d}/quarantine.parquet' (FORMAT parquet)", prof_dir)
                con.execute("DROP VIEW quarantine_b; DROP TABLE clean_b")
            row["after_batch"] = snapshot(sampler, f"{ym} after batch")
            if v in (3, 4):
                if v == 4:
                    for stmt in args.flush:  # 문서에서 찾은 할당기 반환 문장
                        try:
                            con.execute(stmt)
                            row.setdefault("flush", []).append([stmt, "ok"])
                        except Exception as e:  # noqa: BLE001
                            row.setdefault("flush", []).append([stmt, f"{type(e).__name__}: {e}"[:200]])
                    row["after_flush"] = snapshot(sampler, f"{ym} after flush")
                sampler.attach(None)
                con.close()
                con = None
                import gc
                gc.collect()
                row["after_close"] = snapshot(sampler, f"{ym} after close")
        out["rows"].append(row)
        if not r["ok"] or (row.get("acc") and not row["acc"]["ok"]):
            break
    if v == 1 and con is not None and out["rows"][-1]["clean"]["ok"]:
        out["acc_rows"] = con.execute("SELECT (SELECT count(*) FROM clean_all), (SELECT count(*) FROM quarantine_all)").fetchall()
    return out


def main():
    global ARGS, RUN
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["A", "B"])
    ap.add_argument("--batch")
    ap.add_argument("--stmts", nargs="*", default=list(STMTS))
    ap.add_argument("--stop-on-fail", action="store_true")
    ap.add_argument("--batches", nargs="*", default=[])
    ap.add_argument("--variant", type=int, default=1)
    ap.add_argument("--memory-limit")
    ap.add_argument("--threads")
    ap.add_argument("--pio", choices=["true", "false"])
    ap.add_argument("--max-temp", default="60GiB")  # 배치 하나의 흘림은 10GiB 안(측정 51). 호스트 C:를 지킨다(F-20)
    ap.add_argument("--set", nargs="*", default=[], metavar="KEY=VALUE")
    ap.add_argument("--flush", nargs="*", default=[], help="v4: 배치 뒤 돌릴 할당기 반환 문장")
    ap.add_argument("--tag", default="")
    ap.add_argument("--child-of")
    args = ARGS = ap.parse_args()
    RUN = f"{args.child_of + '-' if args.child_of else ''}{args.mode}{args.variant if args.mode == 'B' else ''}-{args.tag or uuid.uuid4().hex[:6]}-{datetime.datetime.now():%H%M%S}"
    (OUT / "runs").mkdir(parents=True, exist_ok=True)
    prof_dir = OUT / "profiles" / RUN
    prof_dir.mkdir(parents=True, exist_ok=True)
    os.makedirs(TEMP_DIR, exist_ok=True)
    vm = None if args.child_of else subprocess.Popen([sys.executable, "-u", "-m", "measure.vm_watch", f"duckdb_mem_{RUN}"], cwd="/opt/project")
    sampler = Sampler(OUT / "runs" / f"{RUN}.jsonl")
    location = metadata_location()
    t0 = time.perf_counter()
    print(f"run {RUN}: {vars(args)}", flush=True)
    try:
        out = (run_a if args.mode == "A" else run_b)(args, sampler, prof_dir, location)
    finally:
        sampler.close()
        if vm:
            vm.terminate()
    out |= {"run": RUN, "args": vars(args), "sec": round(time.perf_counter() - t0, 1), "location": location,
            "ru_maxrss_gib": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024 / GIB, 2)}
    json.dump(out, open(OUT / "runs" / f"{RUN}.summary.json", "w"), ensure_ascii=False, indent=1, default=str)
    print(f"끝 {RUN}: {out['sec']}초, 최대 RSS {out['ru_maxrss_gib']}GiB", flush=True)
    failed = [s for s in out.get("stmts", []) if not s["ok"]] or [r for r in out.get("rows", []) if not r.get("clean", {"ok": True})["ok"] or r.get("rc")]
    raise SystemExit(3 if failed else 0)


if __name__ == "__main__":
    main()
