"""여러 스크립트가 함께 쓰는 설정 읽기.

스크립트는 프로젝트 루트에서 `python -m 폴더.파일이름` 으로 실행한다.
예: python -m ingestion.molit_rent offi 11680 202609
"""
import atexit
import os
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SCRATCH = "/scratch"  # 작업 공간 전용 볼륨(결정 107 S4). 안에 spark/, duckdb/, unzip/이 생긴다
RESERVE_BYTES = 50 * 1024 ** 3  # ponytail: 호스트 C:에 남길 몫(제안값). PostgreSQL 증가, Windows, 상품 출력. 근거 생기면 바꾼다



def read_env(name):
    """환경 변수에 이름이 있으면 그 값을, 없으면 프로젝트 루트의 .env에서 `이름=값` 줄을 찾아 돌려준다.

    Airflow 컨테이너처럼 실행 위치마다 다른 값(DB 주소 등)은 환경 변수로 덮어쓴다.
    """
    if name in os.environ:
        return os.environ[name]
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        key, sep, value = line.partition("=")
        if sep and key.strip() == name:
            return value.strip()
    raise SystemExit(f".env에 {name}가 없습니다")


def temp_budget(tmp=SCRATCH):
    """무거운 작업이 임시 파일(spill, 셔플)에 써도 되는 바이트 (resource_guard.md L3, L4).

    작업 공간 /scratch(이름 볼륨, 결정 107)는 호스트 C:에서 늘어나는 가상 디스크라, VM이 보는 여유(가상 상한 1TB)가 아니라 호스트 C: 여유가 진짜 한계다(F-16).
    레포 바인드 마운트(ROOT)는 호스트 C: 여유를 그대로 보고한다(2026-11-13 확인, 측정 36). 둘 중 작은 쪽에서 남길 몫을 뺀다.
    """
    return min(shutil.disk_usage(tmp).free, shutil.disk_usage(ROOT).free) - RESERVE_BYTES


HEAVY_LOCK_KEY = 20261114  # PostgreSQL advisory lock 키 하나(결정 109 H3). 값 자체는 뜻이 없고 모든 무거운 작업이 같은 값을 쓴다
_held = []  # 잡은 연결을 프로세스가 끝날 때까지 들고 있는다(지역 변수로만 두면 버려질 때 연결이 닫혀 잠금이 풀린다)
WHO_HOLDS = ("SELECT a.pid, a.application_name, a.client_addr, a.backend_start FROM pg_locks l JOIN pg_stat_activity a USING (pid) "
             f"WHERE l.locktype = 'advisory' AND l.granted AND l.objid = {HEAVY_LOCK_KEY}")


def heavy_lock():
    """무거운 작업(Spark 실행기, DuckDB, dbt 전체 빌드, 리니지 추출)은 한 번에 하나 (resource_guard.md L5).

    Airflow는 pool heavy(크기 1)로 줄 세우고, 손으로 돌린 측정도 같은 잠금을 잡는다. 기다리지 않고 바로 실패한다(겹침이 드러나게).
    잠금은 PostgreSQL advisory lock이라 컨테이너를 넘어 막는다(결정 109: 스케줄러, spark-job, duckdb-job이 서로 다른 컨테이너).
    잡은 연결이 닫히면 풀린다: 프로세스가 끝나거나 죽으면(kill -9, OOM) 커널이 소켓을 닫고, PostgreSQL이 그 세션을 끝내며 놓는다.
    잠금을 잡으면 작업 공간을 비우고 시작하고, 프로세스가 끝날 때(예외로 끝나도) 다시 비운다(결정 107 S5, J-6).
    잠금이 한 번에 하나를 보장하므로 다른 실행의 파일을 지울 위험이 없다. 잠금을 못 잡으면 비우지 않는다(위에서 실패).
    강제 종료되면 끝 비우기는 돌지 않고, 다음 잠금을 잡은 실행이 시작할 때 비운다.
    """
    import socket
    import sys

    import psycopg
    who = f"heavy {socket.gethostname()} {os.getpid()} {' '.join(sys.argv)}"[:63]  # pg_stat_activity에서 잡은 쪽을 알아보게
    conn = psycopg.connect(read_env("DATABASE_URL"), autocommit=True, application_name=who)
    if not conn.execute("SELECT pg_try_advisory_lock(%s)", [HEAVY_LOCK_KEY]).fetchone()[0]:
        holder = conn.execute(WHO_HOLDS).fetchall()
        conn.close()
        raise SystemExit(f"다른 무거운 작업이 잠금을 잡고 있습니다(advisory lock {HEAVY_LOCK_KEY}, 잡은 쪽 {holder}). "
                         f"끝난 뒤 다시 돌리세요. 확인: {WHO_HOLDS}")
    _held.append(conn)
    clear_scratch()
    atexit.register(clear_scratch)
    return conn


def clear_scratch():
    """작업 공간 안을 모두 지운다. heavy_lock을 잡은 프로세스만 부른다. 지우다 실패하면 예외로 드러난다(설계 원칙 4)."""
    if not os.path.isdir(SCRATCH):  # Windows에서 돌리는 시험 등 볼륨이 없는 곳
        return
    for name in os.listdir(SCRATCH):
        path = os.path.join(SCRATCH, name)
        try:
            shutil.rmtree(path) if os.path.isdir(path) and not os.path.islink(path) else os.remove(path)
        except FileNotFoundError:  # 엔진이 끝나며 같은 폴더를 함께 지운 경우
            pass


def stop_orphan_jobs(docker_url="tcp://socket-proxy:2375"):
    """스케줄러가 시작할 때 부른다(S2): 아직 도는 계산 컨테이너(라벨 seoul.job=1)를 멈춘다. 지우지는 않고 목록을 찍는다.
    LocalExecutor라 태스크 프로세스는 스케줄러 컨테이너 안에 있다. 그래서 스케줄러가 새로 뜨는 순간 도는 계산 컨테이너는
    주인(태스크 프로세스)이 없다. 그대로 두면 heavy 잠금을 쥔 채 끝까지 돌고, 약 5분 뒤 Airflow가 고아로 재설정한 새 시도가 같은
    작업을 다시 돈다(F-32 첫 시험: 300초 작업이 두 번 돎. 작업이 5분보다 길면 새 시도는 잠금에 막혀 실패할 것으로 추론).
    먼저 멈추면 잠금이 풀려 새 시도 하나만 처음부터 돈다(계산은 멱등, F-32 처방 뒤 시험). 멈춘 컨테이너는 원인 조사용으로 남는다.
    ponytail: LocalExecutor 전제. Celery·k8s로 가면 태스크 상태와 라벨을 대조하는 정리로 바꾼다."""
    import docker
    cli = docker.DockerClient(base_url=docker_url)
    stopped = []
    for c in cli.containers.list(filters={"label": "seoul.job=1", "status": "running"}):
        c.stop(timeout=10)
        stopped.append((c.short_id, c.labels.get("seoul.dag_id"), c.labels.get("seoul.run_id"), c.labels.get("seoul.task_id"),
                        c.labels.get("seoul.try")))
    print(f"주인 없는 계산 컨테이너 {len(stopped)}개 멈춤: {stopped}")
    return stopped


if __name__ == "__main__":
    # 셸 명령을 잠금 안에서 돌린다: python -m common lock -- <명령...>. DAG의 dbt 전체 빌드와 리니지 추출이 쓴다(옛 flock -n 자리).
    # 명령의 종료 코드를 그대로 돌려준다(설계 원칙 4). 잠금을 못 잡으면 명령을 돌리지 않고 실패한다.
    # python -m common stop-orphan-jobs: 스케줄러 시작 때 주인 없는 계산 컨테이너를 멈춘다(S2, docker-compose.yml airflow-scheduler)
    import subprocess
    import sys
    if sys.argv[1:2] == ["stop-orphan-jobs"]:
        stop_orphan_jobs()
        raise SystemExit(0)
    if sys.argv[1:3] != ["lock", "--"] or len(sys.argv) < 4:
        raise SystemExit("사용법: python -m common lock -- <명령...> | python -m common stop-orphan-jobs")
    heavy_lock()
    raise SystemExit(subprocess.run(sys.argv[3:]).returncode)  # 끝 비우기는 heavy_lock이 건 atexit이 한다
