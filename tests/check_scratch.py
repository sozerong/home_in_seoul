"""작업 공간 전용 볼륨 /scratch 확인 (결정 107 S4·S5, handoff_judgments.md J-5·J-6). DB는 쓰지 않지만 컨테이너가 있어야 한다(CI 밖).

1. 비우기(S5, J-6): 죽은 실행의 찌꺼기를 일부러 남기고
   - 잠금이 잡혀 있으면 다른 프로세스는 잠금을 못 잡고, 찌꺼기를 비우지 않는다.
   - 잠금을 잡은 프로세스는 시작할 때 찌꺼기를 비우고, 끝날 때(예외로 끝나도) 자기 파일까지 비운다.
2. 위치(S4, J-5): 잠금을 잡고 Spark 셔플과 DuckDB 흘림을 일으켜, 파일이 /scratch 아래에 생기는지 본다.
무거운 작업이 돌고 있으면 바로 멈춘다(작업 공간을 비우므로).
실행(Airflow 컨테이너): PYTHONPATH=/tmp/duckdb_pkg /opt/airflow/spark-venv/bin/python -m tests.check_scratch
"""
import os
import subprocess
import sys
import threading
import time

from common import ROOT, SCRATCH, heavy_lock


def files_under(folder):
    return [os.path.join(d, f) for d, _, fs in os.walk(folder) for f in fs]


def python(code):
    return subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True)


# 1. 비우기
holder = subprocess.Popen(["flock", "-n", "/tmp/heavy.lock", "sleep", "5"])
time.sleep(0.5)
if holder.poll() is not None:
    raise SystemExit("다른 무거운 작업이 잠금을 잡고 있습니다. 끝난 뒤 다시 돌리세요")
junk = [f"{SCRATCH}/spark/blockmgr-dead/00/shuffle_0_0_0.data", f"{SCRATCH}/duckdb/duckdb_temp_dead.tmp", f"{SCRATCH}/unzip/living_dead/07.csv"]
for j in junk:
    os.makedirs(os.path.dirname(j), exist_ok=True)
    open(j, "w").write("x")
r = python("import common; common.heavy_lock()")
assert r.returncode != 0 and "잡고 있습니다" in r.stderr, r
assert all(os.path.exists(j) for j in junk), "잠금을 못 잡은 프로세스가 작업 공간을 지웠습니다"
holder.wait()
print("잠금이 잡혀 있을 때: 다른 프로세스는 실패하고 찌꺼기 3개를 지우지 않음")

r = python("import common, os; common.heavy_lock(); print(sorted(os.listdir(common.SCRATCH))); "
           "open(common.SCRATCH + '/mine.tmp', 'w').write('x')")
assert r.returncode == 0, r
assert r.stdout.strip() == "[]", f"시작할 때 비우지 않았습니다: {r.stdout}"
assert os.listdir(SCRATCH) == [], f"끝날 때 비우지 않았습니다: {os.listdir(SCRATCH)}"
print("잠금을 잡은 프로세스: 시작할 때 찌꺼기를 비우고, 정상으로 끝날 때 자기 파일도 비움")

r = python("import common; common.heavy_lock(); open(common.SCRATCH + '/mine.tmp', 'w').write('x'); raise RuntimeError('일부러')")
assert r.returncode != 0 and "일부러" in r.stderr, r
assert os.listdir(SCRATCH) == [], f"예외로 끝날 때 비우지 않았습니다: {os.listdir(SCRATCH)}"
print("예외로 끝나도 비움")

# 2. 위치
lock = heavy_lock()  # noqa: F841  아래 엔진들이 작업 공간을 쓴다
from transform.iceberg import SPARK_LOCAL, spark_session  # noqa: E402

spark = spark_session("check-scratch", {"spark.sql.shuffle.partitions": "4"})
spark.range(2_000_000).selectExpr("id % 1000 AS k").groupBy("k").count().collect()  # 셔플 한 번
shuffle = [f for f in files_under(SPARK_LOCAL) if ".data" in f or ".index" in f]
spark.stop()
assert SPARK_LOCAL.startswith(f"{SCRATCH}/"), SPARK_LOCAL
assert shuffle, f"{SPARK_LOCAL} 아래에 셔플 파일이 없습니다"
print(f"Spark 셔플 파일 {len(shuffle)}개가 {SPARK_LOCAL} 아래에 생김 (예: {shuffle[0]})")

try:
    import duckdb
except ImportError:
    print("DuckDB 없음: 건너뜀 (PYTHONPATH=/tmp/duckdb_pkg)")
else:
    from measure.duckdb_sql import TEMP_DIR
    con = duckdb.connect()
    con.execute(f"SET temp_directory = '{TEMP_DIR}'")
    con.execute("SET memory_limit = '100MB'")
    con.execute("SET threads = 2")
    seen = []
    done = threading.Event()

    def watch():
        while not done.is_set():
            seen.extend(files_under(TEMP_DIR))
            time.sleep(0.05)

    t = threading.Thread(target=watch)
    t.start()
    con.execute("SELECT count(*) FROM (SELECT md5(i::varchar) s FROM range(20000000) t(i) ORDER BY s)").fetchall()
    done.set()
    t.join()
    con.close()
    assert TEMP_DIR.startswith(f"{SCRATCH}/"), TEMP_DIR
    assert seen, f"{TEMP_DIR} 아래에 DuckDB 임시 파일이 생기지 않았습니다"
    print(f"DuckDB 임시 파일이 {TEMP_DIR} 아래에 생김 (예: {seen[0]})")
print("tests.check_scratch: 통과")
