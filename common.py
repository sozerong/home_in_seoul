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


def heavy_lock():
    """무거운 작업(Spark 실행기, DuckDB, dbt 전체 빌드, 리니지 추출)은 한 번에 하나 (resource_guard.md L5).

    Airflow는 pool heavy(크기 1)로 줄 세우고, 손으로 돌린 측정도 같은 잠금 파일을 잡는다. 기다리지 않고 바로 실패한다(겹침이 드러나게).
    돌려준 파일을 닫거나 프로세스가 끝나면 풀린다. 잠금 파일은 컨테이너 안(/tmp)이라 같은 컨테이너의 프로세스끼리만 막는다.
    잠금을 잡으면 작업 공간을 비우고 시작하고, 프로세스가 끝날 때(예외로 끝나도) 다시 비운다(결정 107 S5, J-6).
    잠금이 한 번에 하나를 보장하므로 다른 실행의 파일을 지울 위험이 없다. 잠금을 못 잡으면 비우지 않는다(위에서 실패).
    강제 종료(kill -9, OOM)되면 끝 비우기는 돌지 않고, 다음 잠금을 잡은 실행이 시작할 때 비운다.
    """
    import fcntl
    f = open("/tmp/heavy.lock", "w")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit("다른 무거운 작업이 /tmp/heavy.lock을 잡고 있습니다. 끝난 뒤 다시 돌리세요") from None
    clear_scratch()
    atexit.register(clear_scratch)
    return f


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
