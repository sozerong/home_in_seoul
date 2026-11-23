"""Spark 이벤트 로그에서 단계별 시간, 태스크, 셔플, 흘림, GC, CPU, 쏠림을 뽑는다(측정 64, 처음 handoff_night_2026-11-23.md N3).
사용: python measure/spark_stages.py <이벤트로그> [...]  → 표를 출력하고 같은 이름 .json을 logs/spark_stages/에 쓴다.
측정 64의 출력은 logs/night_20261123/n3/(옛 위치)에 있다.
태스크 시간 나눔(태스크 시간 합 기준): GC, CPU(GC 제외 근사), 셔플 쓰기 대기, 셔플 읽기 대기, 나머지(디스크 읽기·쓰기, 흘림, 역직렬화 등).
"""
import json
import pathlib
import statistics
import sys

OUT = pathlib.Path(__file__).resolve().parents[1] / "logs" / "spark_stages"
OUT.mkdir(parents=True, exist_ok=True)
G = 1e9


def parse(path):
    jobs, stages, tasks, sql = {}, {}, {}, {}
    app = {}
    for line in open(path, encoding="utf-8"):
        try:
            e = json.loads(line)
        except json.JSONDecodeError:
            continue
        k = e["Event"]
        if k == "SparkListenerApplicationStart":
            app["start"] = e["Timestamp"]
        elif k == "SparkListenerApplicationEnd":
            app["end"] = e["Timestamp"]
        elif k == "SparkListenerEnvironmentUpdate":
            sp = dict(e.get("Spark Properties", {}))
            app["conf"] = {c: sp.get(c) for c in ("spark.master", "spark.driver.memory", "spark.sql.shuffle.partitions",
                                                  "spark.sql.adaptive.enabled", "spark.memory.fraction", "spark.local.dir",
                                                  "spark.sql.files.maxPartitionBytes", "spark.serializer")}
        elif k == "SparkListenerJobStart":
            d = (e.get("Properties") or {}).get("spark.job.description")
            for sid in e["Stage IDs"]:
                jobs[sid] = (e["Job ID"], d)
        elif k == "SparkListenerTaskEnd":
            m, i = e.get("Task Metrics") or {}, e["Task Info"]
            r, w, inp = m.get("Shuffle Read Metrics", {}), m.get("Shuffle Write Metrics", {}), m.get("Input Metrics", {})
            tasks.setdefault(e["Stage ID"], []).append({
                "dur": i["Finish Time"] - i["Launch Time"], "run": m.get("Executor Run Time", 0),
                "cpu": m.get("Executor CPU Time", 0) / 1e6, "gc": m.get("JVM GC Time", 0),
                "deser": m.get("Executor Deserialize Time", 0),
                "fetch_wait": r.get("Fetch Wait Time", 0), "sw_time": w.get("Shuffle Write Time", 0) / 1e6,
                "sr_bytes": r.get("Remote Bytes Read", 0) + r.get("Local Bytes Read", 0), "sr_recs": r.get("Total Records Read", 0),
                "sw_bytes": w.get("Shuffle Bytes Written", 0), "sw_recs": w.get("Shuffle Records Written", 0),
                "in_bytes": inp.get("Bytes Read", 0), "in_recs": inp.get("Records Read", 0),
                "mem_spill": m.get("Memory Bytes Spilled", 0), "disk_spill": m.get("Disk Bytes Spilled", 0),
                "peak": m.get("Peak Execution Memory", 0), "failed": i.get("Failed", False),
                "launch": i["Launch Time"], "finish": i["Finish Time"]})
        elif k == "SparkListenerStageCompleted":
            s = e["Stage Info"]
            stages[s["Stage ID"]] = {"name": s.get("Stage Name"), "sub": s.get("Submission Time"), "done": s.get("Completion Time"),
                                     "ntasks": s.get("Number of Tasks"),
                                     "rdds": [r.get("Scope", "") and json.loads(r["Scope"]).get("name") for r in s.get("RDD Info", []) if r.get("Scope")]}
    return app, jobs, stages, tasks


def summarize(path):
    app, jobs, stages, tasks = parse(path)
    rows = []
    for sid in sorted(set(stages) | set(tasks)):
        t, s = tasks.get(sid, []), stages.get(sid, {})
        ok = [x for x in t if not x["failed"]]
        durs = sorted(x["dur"] for x in ok) or [0]
        med = statistics.median(durs)
        longest = max(ok, key=lambda x: x["dur"]) if ok else {}
        tsum = sum(x["dur"] for x in ok)
        S = lambda key: sum(x[key] for x in ok)  # noqa: E731
        cpu_no_gc = max(S("cpu") - 0, 0)
        row = {"stage": sid, "job": jobs.get(sid, (None, None))[0], "desc": jobs.get(sid, (None, None))[1],
               "ops": sorted({o for o in s.get("rdds", []) if o}),
               "wall_s": round((s["done"] - s["sub"]) / 1000, 1) if s.get("done") and s.get("sub") else None,
               "start_s": round((s["sub"] - app["start"]) / 1000, 1) if s.get("sub") else None,
               "tasks": len(ok), "task_sum_s": round(tsum / 1000, 1),
               "task_med_s": round(med / 1000, 2), "task_max_s": round(durs[-1] / 1000, 2),
               "skew": round(durs[-1] / med, 2) if med else None,
               "longest_in_recs": longest.get("in_recs"), "longest_sr_recs": longest.get("sr_recs"),
               "med_in_recs": statistics.median([x["in_recs"] for x in ok]) if ok else None,
               "med_sr_recs": statistics.median([x["sr_recs"] for x in ok]) if ok else None,
               "in_gb": round(S("in_bytes") / G, 2), "in_recs": S("in_recs"),
               "sr_gb": round(S("sr_bytes") / G, 2), "sw_gb": round(S("sw_bytes") / G, 2),
               "mem_spill_gb": round(S("mem_spill") / G, 1), "disk_spill_gb": round(S("disk_spill") / G, 1),
               "gc_s": round(S("gc") / 1000, 1), "gc_pct": round(100 * S("gc") / tsum, 1) if tsum else None,
               "cpu_s": round(cpu_no_gc / 1000, 1), "cpu_pct": round(100 * cpu_no_gc / tsum, 1) if tsum else None,
               "sw_time_s": round(S("sw_time") / 1000, 1), "fetch_wait_s": round(S("fetch_wait") / 1000, 1),
               "peak_exec_gb": round(max((x["peak"] for x in ok), default=0) / G, 2)}
        rows.append(row)
    out = {"path": str(path), "app_s": round((app.get("end", 0) - app.get("start", 0)) / 1000, 1), "conf": app.get("conf"), "stages": rows}
    json.dump(out, open(OUT / (pathlib.Path(path).name + ".json"), "w"), ensure_ascii=False, indent=1)
    return out


if __name__ == "__main__":
    for p in sys.argv[1:]:
        o = summarize(p)
        print(o["path"], "app", o["app_s"], "s", o["conf"])
        keys = ["stage", "job", "desc", "start_s", "wall_s", "tasks", "task_sum_s", "task_med_s", "task_max_s", "skew", "in_gb", "sr_gb",
                "sw_gb", "mem_spill_gb", "disk_spill_gb", "gc_pct", "cpu_pct", "sw_time_s", "fetch_wait_s", "longest_in_recs",
                "med_in_recs", "longest_sr_recs", "med_sr_recs", "ops"]
        print("\t".join(keys))
        for r in o["stages"]:
            print("\t".join(str(r[k]) for k in keys))
