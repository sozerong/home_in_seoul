"""자원 상한 유도 시험 (resource_guard.md 4절 5번, guarantees.md 2.7). 일부러 넘치게 해서 "기계 전체가 멈추지 않고 넘친 작업 하나가
드러나는 오류로 끝나는지"를 본다. F-16(VM 전체 멈춤)의 처방(L2 컨테이너 상한, L3 DuckDB 임시 폴더 상한)을 확인한다.

실행(호스트, 레포 루트): .venv\\Scripts\\python -u -m measure.induce_guard disk|mem
  disk: Airflow 컨테이너 안에서 DuckDB(memory_limit 1GB, max_temp_directory_size 1GiB)로 디스크에 흘려야 하는 정렬을 돌린다.
        기대: DuckDB가 몇 초 안에 max_temp_directory_size 오류로 끝나고, 임시 폴더가 남지 않는다.
  mem:  같은 이미지로 일회용 컨테이너(--memory 2g, 스왑 금지)를 띄우고, DuckDB memory_limit를 일부러 상한보다 크게(12GB) 잡아
        메모리를 많이 쓰는 집계를 돌린다. 끝날 때까지 1초마다 PostgreSQL에 SELECT 1.
        기대: 그 컨테이너만 OOMKilled=true(종료 코드 137)로 끝나고, PostgreSQL 탐침 실패 0, 다른 컨테이너는 그대로.
  Airflow 컨테이너 자체를 넘치게 하지 않는 이유: 스케줄러가 함께 죽는다(결정 101 A의 알려진 단점). 같은 이미지·같은 cgroup 방식으로 대신 본다.
조건 메모: DuckDB는 Airflow 컨테이너의 /tmp/duckdb_pkg에 깐 2.0.0을 쓴다(judgment_log 71). mem은 그것을 logs/duckdb_pkg로 복사해 붙인다.
"""
import shutil
import subprocess
import sys
import time

from common import ROOT

AIRFLOW, DB, IMAGE, TWIN = "new_proj-airflow-1", "new_proj-db-1", "seoul-airflow:3.3.2", "induce-guard-mem"
PY = "/opt/airflow/spark-venv/bin/python"

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


def disk():
    r = sh("docker", "exec", "-e", "PYTHONPATH=/tmp/duckdb_pkg", AIRFLOW, PY, "-u", "-c", DISK)
    print(r.stdout.strip(), r.stderr.strip()[-500:])
    return r.returncode


def mem():
    pkg = ROOT / "logs" / "duckdb_pkg"
    if not pkg.exists():
        sh("docker", "cp", f"{AIRFLOW}:/tmp/duckdb_pkg", str(pkg), check=True)
    sh("docker", "rm", "-f", TWIN)
    sh("docker", "run", "-d", "--name", TWIN, "--memory", "2g", "--memory-swap", "2g", "--cpus", "4", "--network", "none",
       "-v", f"{pkg}:/duckdb_pkg:ro", "-e", "PYTHONPATH=/duckdb_pkg", "--entrypoint", PY, IMAGE, "-u", "-c", MEM, check=True)
    probes, fails, worst, t0 = 0, 0, 0.0, time.time()
    while sh("docker", "inspect", "-f", "{{.State.Running}}", TWIN).stdout.strip() == "true":
        s = time.time()
        r = sh("docker", "exec", DB, "psql", "-U", "seoul", "-d", "seoul", "-At", "-c", "select 1", timeout=10)
        probes, fails, worst = probes + 1, fails + (r.stdout.strip() != "1"), max(worst, time.time() - s)
        time.sleep(1)
    state = sh("docker", "inspect", "-f", "{{.State.OOMKilled}} {{.State.ExitCode}}", TWIN).stdout.split()
    others = sh("docker", "ps", "--filter", "name=new_proj-", "--format", "{{.Names}} {{.Status}}").stdout.strip()
    print(sh("docker", "logs", TWIN).stdout.strip())
    print(f"{time.time() - t0:.0f}초 뒤 OOMKilled={state[0]} 종료 코드={state[1]}, PostgreSQL 탐침 {probes}회 실패 {fails} "
          f"최대 {worst * 1000:.0f}ms(docker exec 포함)\n{others}")
    sh("docker", "rm", TWIN)
    ok = state == ["true", "137"] and fails == 0 and others.count("Up") == 4
    return 0 if ok else 1


if __name__ == "__main__":
    if shutil.which("docker") is None:
        raise SystemExit("호스트에서 돌린다(docker 명령 필요)")
    code = {"disk": disk, "mem": mem}[sys.argv[1]]()
    print("통과" if code == 0 else "기대와 다름")
    sys.exit(code)
