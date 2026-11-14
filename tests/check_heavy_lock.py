"""heavy 잠금이 컨테이너를 넘어 막는지 시험 (결정 109 H3, resource_guard.md L5). 실제 무거운 작업은 돌리지 않는다.

잠금은 PostgreSQL advisory lock(common.heavy_lock)이라 스케줄러와 계산 컨테이너가 같은 잠금을 본다.
1. 스케줄러 컨테이너에서 잠금을 잡은 채(python -m common lock -- sleep) duckdb-job 컨테이너가 잡으려 하면 바로 실패한다.
2. 첫 번째가 끝나면 풀려서 duckdb-job이 잡는다.
3. 잡은 프로세스가 kill -9로 죽어도(OOM과 같은 강제 종료) 잠금이 풀린다.
무거운 작업이 돌고 있으면 1에서 스케줄러 쪽이 잡지 못해 멈춘다. 작업 공간(/scratch)을 비우므로 무거운 작업이 없을 때 돌린다.
실행(호스트, 레포 폴더, docker CLI만 쓴다): python -X utf8 tests/check_heavy_lock.py
"""
import subprocess
import time

SCHED = "new_proj-airflow-scheduler-1"
HELD = "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted AND objid = 20261114"


def sh(*args, **kw):
    return subprocess.run(args, capture_output=True, text=True, encoding="utf-8", **kw)


def held():
    return int(sh("docker", "exec", "new_proj-db-1", "psql", "-U", "seoul", "-d", "seoul", "-Atc", HELD).stdout.strip())


def wait(cond, what, limit=60):
    t0 = time.time()
    while not cond():
        if time.time() - t0 > limit:
            raise SystemExit(f"{limit}초 안에 {what}")
        time.sleep(0.5)


def kill(prefix, sig):
    """스케줄러 컨테이너에서 명령줄이 prefix로 시작하는 프로세스에 신호를 보낸다. 이미지에 pkill·ps가 없어 /proc을 읽는다."""
    code = ("import os; [os.kill(int(p), %d) for p in os.listdir('/proc') if p.isdigit() and int(p) != os.getpid() "
            "and open(f'/proc/{p}/cmdline', 'rb').read().replace(bytes(1), b' ').startswith(%r)]" % (sig, prefix.encode()))
    r = sh("docker", "exec", SCHED, "python", "-c", code)
    assert r.returncode == 0, r.stderr


def job_lock():
    t0 = time.time()
    r = sh("docker", "compose", "run", "--rm", "--no-deps", "duckdb-job", "python", "-m", "common", "lock", "--", "true")
    return r, time.time() - t0


assert held() == 0, "다른 무거운 작업이 잠금을 잡고 있습니다. 끝난 뒤 다시 돌리세요"

# 1. 스케줄러가 잡고 있을 때
holder = subprocess.Popen(["docker", "exec", "-w", "/opt/project", SCHED, "python", "-m", "common", "lock", "--", "sleep", "60"])
wait(lambda: held() == 1, "스케줄러 쪽이 잠금을 잡지 못했습니다")
r, sec = job_lock()
assert r.returncode != 0 and "잠금을 잡고 있습니다" in r.stderr, (r.returncode, r.stderr[-500:])
assert "'heavy " in r.stderr, r.stderr  # 메시지에 잡은 쪽(application_name 'heavy <호스트> <pid> <명령>')이 들어 있다
print(f"1. 스케줄러가 잡은 동안 duckdb-job은 바로 실패({sec:.1f}초, 컨테이너 시작 포함). 메시지: {r.stderr.strip().splitlines()[-1][:160]}")

# 2. 첫 번째가 끝나면 풀린다
kill("sleep 60", 15)  # 명령(sleep)이 끝나면 래퍼도 그 종료 코드를 돌려주고 끝난다
holder.wait(timeout=30)
wait(lambda: held() == 0, "첫 번째가 끝났는데 잠금이 남아 있습니다")
r, sec = job_lock()
assert r.returncode == 0, (r.returncode, r.stderr[-500:])
print(f"2. 첫 번째가 끝난 뒤 duckdb-job이 잡고 정상 종료({sec:.1f}초)")

# 3. kill -9로 죽어도 풀린다
holder = subprocess.Popen(["docker", "exec", "-w", "/opt/project", SCHED, "python", "-m", "common", "lock", "--", "sleep", "60"])
wait(lambda: held() == 1, "스케줄러 쪽이 잠금을 잡지 못했습니다")
kill("python -m common lock -- sleep 60", 9)
holder.wait(timeout=30)
t0 = time.time()
wait(lambda: held() == 0, "kill -9 뒤에도 잠금이 남아 있습니다", limit=30)
print(f"3. 잡은 프로세스를 kill -9 하자 {time.time() - t0:.1f}초 안에 잠금이 풀림")
kill("sleep 60", 15)  # 고아가 된 sleep 정리
print("tests.check_heavy_lock: 통과")
