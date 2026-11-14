"""자원 상한 유도 시험 (resource_guard.md 4절 5번, guarantees.md 2.7, 결정 109 H6). 일부러 넘치게 해서 "넘친 작업 하나만
드러나는 오류로 끝나고, 지휘(Airflow)는 그대로인지"를 본다.

실행(호스트, 레포 루트): python -X utf8 -u -m measure.induce_guard disk|mem [반복 수]
  disk: 계산 컨테이너(duckdb-job)에서 DuckDB(memory_limit 1GB, max_temp_directory_size 1GiB)로 디스크에 흘려야 하는 정렬을 돌린다.
        기대: DuckDB가 몇 초 안에 max_temp_directory_size 오류로 끝나고, 임시 폴더가 남지 않는다.
  mem:  가벼운 DAG(seoul_api)를 수동 실행한 뒤, 계산 이미지(seoul-duckdb-job)로 작은 상한(--memory 2g, 스왑 금지) 컨테이너를 띄워
        DuckDB memory_limit를 상한보다 크게(12GB) 잡고 메모리를 많이 쓰는 집계를 돌린다(F-17과 같은 질의).
        그동안 1초마다 메타데이터 DB에서 스케줄러 하트비트를 읽고(PostgreSQL 탐침을 겸함), 끝난 뒤 15초 더 읽는다.
        기대: 그 컨테이너만 OOMKilled=true(종료 코드 137). 하트비트 간격은 평소(5초 안팎)와 같고, DAG는 성공하고, 다른 서비스는 그대로.
        시험 컨테이너는 --rm 없이 띄워 docker inspect로 본 뒤 지우지 않는다(지시서 3절: 목록만 남긴다).
결정 109 전에는 Airflow 컨테이너 안에서 넘치면 스케줄러가 함께 죽었으므로(결정 101 A) 같은 이미지의 일회용 컨테이너로 대신 봤다.
지금은 계산이 원래 작업 컨테이너에서 돌므로 이 시험이 실제 구조 그대로다.
"""
import datetime
import shutil
import subprocess
import sys
import time

DB, IMAGE, SCHED = "new_proj-db-1", "seoul-duckdb-job:2.0.0", "new_proj-airflow-scheduler-1"
SERVICES = ["db", "airflow-apiserver", "airflow-scheduler", "airflow-dag-processor", "airflow-triggerer", "socket-proxy",
            "metabase", "pgadmin", "ui"]
HEARTBEAT = ("SELECT latest_heartbeat FROM job WHERE job_type = 'SchedulerJob' AND state = 'running' "
             "ORDER BY latest_heartbeat DESC LIMIT 1")

DISK = """
import duckdb, os, time
d = "/tmp/duckdb-induce"
c = duckdb.connect()
for k, v in (("memory_limit", "1GB"), ("threads", "4"), ("temp_directory", d), ("max_temp_directory_size", "1GiB")):
    c.execute(f"SET {k} = '{v}'")
t = time.time()
try:
    c.execute("CREATE TEMP TABLE x AS SELECT i, hash(i) AS h, repeat('x', 40) || i AS s FROM range(200000000) t(i) ORDER BY h")
    raise SystemExit("기대와 다름: 오류 없이 끝남")
except duckdb.OutOfMemoryException as e:
    left = sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(d) for f in fs) if os.path.exists(d) else 0
    print(f"오류로 끝남 {time.time() - t:.1f}초, 남은 임시 파일 {left}B: {str(e).splitlines()[0]}")
    assert "max_temp_directory_size" in str(e) and left == 0
"""

MEM = """
import duckdb
c = duckdb.connect()
print("cgroup 상한", open("/sys/fs/cgroup/memory/memory.limit_in_bytes").read().strip(), flush=True)
c.execute("SET memory_limit = '12GB'"); c.execute("SET threads = 4"); c.execute("SET preserve_insertion_order = false")
c.execute("SELECT count(*) FROM (SELECT i % 300000000 AS k, list(i) AS l FROM range(600000000) t(i) GROUP BY k)").fetchone()
print("기대와 다름: 끝까지 돌았다", flush=True)
"""


def sh(*args, **kw):
    return subprocess.run(args, capture_output=True, text=True, encoding="utf-8", errors="replace", **kw)


def psql(db, sql, timeout=10):
    return sh("docker", "exec", DB, "psql", "-U", "seoul", "-d", db, "-Atc", sql, timeout=timeout).stdout.strip()


def disk():
    r = sh("docker", "compose", "run", "--rm", "--no-deps", "duckdb-job", "python", "-u", "-c", DISK)
    print(r.stdout.strip(), r.stderr.strip()[-500:])
    return r.returncode


def trigger_light_dag():
    out = sh("docker", "exec", SCHED, "airflow", "dags", "trigger", "seoul_api", "-o", "plain").stdout
    return next(w for w in out.split() if w.startswith("manual__"))


def mem_once(rep):
    name = f"h6-oom-{rep}-{datetime.datetime.now():%H%M%S}"
    run_id = trigger_light_dag()
    sh("docker", "run", "-d", "--name", name, "--memory", "2g", "--memory-swap", "2g", "--cpus", "4", "--network", "none",
       IMAGE, "python", "-u", "-c", MEM, check=True)
    beats, fails, worst, t0 = [], 0, 0.0, time.time()

    def sample():
        nonlocal fails, worst
        s = time.time()
        hb = psql("airflow", HEARTBEAT)
        worst = max(worst, time.time() - s)
        if not hb:
            fails += 1
        elif not beats or beats[-1] != hb:
            beats.append(hb)

    while sh("docker", "inspect", "-f", "{{.State.Running}}", name).stdout.strip() == "true":
        sample()
        time.sleep(1)
    ended = time.time()
    while time.time() - ended < 15:  # 넘친 순간을 사이에 둔 하트비트 간격을 보려고 조금 더 읽는다
        sample()
        time.sleep(1)
    state = sh("docker", "inspect", "-f", "{{.State.OOMKilled}} {{.State.ExitCode}} {{.State.FinishedAt}}", name).stdout.split()
    times = [datetime.datetime.fromisoformat(b) for b in beats]
    gaps = [(b - a).total_seconds() for a, b in zip(times, times[1:])]
    while (st := psql("airflow", f"SELECT state FROM dag_run WHERE dag_id = 'seoul_api' AND run_id = '{run_id}'")) not in ("success", "failed"):
        time.sleep(2)
    ps = sh("docker", "compose", "ps", "--format", "{{.Service}} {{.State}}").stdout.split("\n")
    up = {line.split()[0] for line in ps if line.endswith(" running")}
    print(sh("docker", "logs", name).stdout.strip())
    print(f"[{rep}] {name}: {ended - t0:.0f}초 뒤 OOMKilled={state[0]} 종료 코드={state[1]} 끝난 시각={state[2]}")
    print(f"[{rep}] 스케줄러 하트비트 {len(beats)}개, 간격 최대 {max(gaps):.1f}초 평균 {sum(gaps) / len(gaps):.1f}초, "
          f"탐침 실패 {fails}, 탐침 최대 {worst * 1000:.0f}ms(docker exec 포함). seoul_api {run_id} = {st}. "
          f"멈춘 서비스: {sorted(set(SERVICES) - up) or '없음'}")
    return state[:2] == ["true", "137"] and fails == 0 and st == "success" and set(SERVICES) <= up


def mem(reps=1):
    results = [mem_once(i + 1) for i in range(reps)]
    return 0 if all(results) else 1


if __name__ == "__main__":
    if shutil.which("docker") is None:
        raise SystemExit("호스트에서 돌린다(docker 명령 필요)")
    mode = sys.argv[1]
    code = disk() if mode == "disk" else mem(int(sys.argv[2]) if len(sys.argv) > 2 else 1)
    print("통과" if code == 0 else "기대와 다름")
    sys.exit(code)
