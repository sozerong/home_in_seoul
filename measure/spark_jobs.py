"""Spark 이벤트 로그에서 작업(job 설명)별 실제 경과 시간을 뽑는다(측정 42). ops.engine_run의 단계 시간 합은 겹친 단계를 두 번 세므로 벽시계 시간이 아니다.
사용: python measure/spark_jobs.py 이벤트로그 [이벤트로그 ...] (컨테이너 /tmp/spark-events/, 복사본 logs/airflow_tmp_20261114/spark-events/)"""
import json, sys, collections
for path in sys.argv[1:]:
    start, end, desc, app = {}, {}, {}, None
    for line in open(path, encoding="utf-8"):
        e = json.loads(line)
        ev = e["Event"]
        if ev == "SparkListenerApplicationStart": app = e["Timestamp"]
        if ev == "SparkListenerApplicationEnd": app_end = e["Timestamp"]
        if ev == "SparkListenerJobStart":
            start[e["Job ID"]] = e["Submission Time"]; desc[e["Job ID"]] = (e.get("Properties") or {}).get("spark.job.description", "")
        if ev == "SparkListenerJobEnd": end[e["Job ID"]] = e["Completion Time"]
    by = collections.OrderedDict()
    for j in sorted(start):
        d = desc[j]; s, t = start[j], end.get(j, start[j])
        a, b = by.get(d, (s, t)); by[d] = (min(a, s), max(b, t))
    print(path.split("/")[-1], "app", (app_end - app) / 1000)
    for d, (s, t) in by.items(): print(f"  {d:36s} start+{(s-app)/1000:7.0f}s  wall {(t-s)/1000:7.0f}s")
