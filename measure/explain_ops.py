"""EXPLAIN FORMATTED 출력(spark_sql_runner --explain)에서 뷰마다 계획 나무의 연산자 수를 센다(측정 41). 사용: python measure/explain_ops.py 파일
Exchange는 셔플만 센다(BroadcastExchange는 따로 이름이 달라 빠진다)."""
import re, sys
OPS = ["BatchScan", "SortAggregate", "HashAggregate", "SortMergeJoin", "BroadcastHashJoin", "Exchange"]
view, tree, counts = None, False, {}
for line in open(sys.argv[1], encoding="utf-8"):
    m = re.match(r"===== EXPLAIN FORMATTED (\w+)", line)
    if m:
        view, tree = m.group(1), False; counts[view] = dict.fromkeys(OPS, 0); continue
    if line.startswith("== Physical Plan =="):
        tree = True; continue
    if tree and not line.strip():
        tree = False; continue
    if tree and view:
        m = re.match(r"^[\s+\-:|]*(\w+)", line)
        if m and m.group(1) in OPS:
            counts[view][m.group(1)] += 1
for v, c in counts.items():
    print(v, " / ".join(f"{k} {n}" for k, n in c.items()))
