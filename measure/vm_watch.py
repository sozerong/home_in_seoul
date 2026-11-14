"""L0 관측 (resource_guard.md 1절): 무거운 실행 동안 VM 메모리·스왑·디스크와 이 컨테이너의 cgroup 메모리를 5초마다 한 줄씩 남긴다.

F-16 때는 VM이 멈추며 커널 로그와 출력이 함께 사라져 원인(메모리·스왑 대 디스크)을 판정하지 못했다. 그래서 바인드 마운트
(/opt/project/logs/vm_watch/, 호스트 레포의 logs/, git 제외)에 줄마다 바로 쓴다. VM이 멈추면 마지막 줄이 멈춘 시각이다.
/proc/meminfo, /proc/vmstat은 컨테이너 안에서도 VM 전체 값이다. cgroup v1 memory는 이 컨테이너 값이다(L2 상한을 걸면 limit이 보인다).

실행(Airflow 컨테이너, 백그라운드): /opt/airflow/spark-venv/bin/python -u -m measure.vm_watch 이름 &
끝내기: kill <pid>. --once: 한 줄만 찍고 끝(점검용).
열: ts, VM 메모리 전체·사용 가능(kB), 스왑 전체·남음(kB), 누적 스왑 in/out(페이지), cgroup 사용·상한·rss·failcnt·oom_kill,
    /tmp가 놓인 파일 시스템의 사용·여유(byte. 폴더 크기가 아니다: 지금은 VM 데이터 디스크 전체, 이미지·볼륨 포함, F-21),
    이 컨테이너에서 RSS가 가장 큰 프로세스(kB, 이름).
"""
import datetime
import os
import shutil
import sys
import time

from common import ROOT

CG = "/sys/fs/cgroup/memory"
COLUMNS = ["ts", "mem_total_kb", "mem_avail_kb", "swap_total_kb", "swap_free_kb", "pswpin", "pswpout",
           "cg_usage_b", "cg_limit_b", "cg_rss_b", "cg_failcnt", "cg_oom_kill", "tmp_used_b", "tmp_free_b",
           "top_rss_kb", "top_comm"]


def keyed(path, sep=None):
    with open(path) as f:
        return {k.rstrip(":"): v for k, v, *_ in (line.split(sep) for line in f if line.strip())}


def read(path):
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return ""


def top_process():
    best = (0, "")
    for pid in filter(str.isdigit, os.listdir("/proc")):
        try:
            status = keyed(f"/proc/{pid}/status", ":")
        except OSError:  # 그 사이 끝난 프로세스
            continue
        rss = int(status.get("VmRSS", "0 kB").split()[0])
        best = max(best, (rss, status.get("Name", "").strip()))
    return best


def sample():
    mem, vm = keyed("/proc/meminfo"), keyed("/proc/vmstat")
    stat = keyed(f"{CG}/memory.stat") if os.path.exists(f"{CG}/memory.stat") else {}
    oom = keyed(f"{CG}/memory.oom_control") if os.path.exists(f"{CG}/memory.oom_control") else {}
    disk = shutil.disk_usage("/tmp")
    rss, comm = top_process()
    return [datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
            mem["MemTotal"], mem["MemAvailable"], mem["SwapTotal"], mem["SwapFree"], vm.get("pswpin", ""), vm.get("pswpout", ""),
            read(f"{CG}/memory.usage_in_bytes"), read(f"{CG}/memory.limit_in_bytes"), stat.get("total_rss", ""),
            read(f"{CG}/memory.failcnt"), oom.get("oom_kill", ""), disk.used, disk.free, rss, comm]


def main():
    name = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("--") else "watch"
    if "--once" in sys.argv:
        print("\t".join(f"{c}={v}" for c, v in zip(COLUMNS, sample())))
        return
    out = ROOT / "logs" / "vm_watch" / f"{datetime.datetime.now():%Y%m%d-%H%M%S}_{name}.tsv"
    out.parent.mkdir(parents=True, exist_ok=True)
    print(f"vm_watch: {out} (pid {os.getpid()})")
    with open(out, "a", buffering=1) as f:  # 줄 버퍼: 줄마다 파일에 닿는다
        f.write("\t".join(COLUMNS) + "\n")
        while True:
            f.write("\t".join(map(str, sample())) + "\n")
            os.fsync(f.fileno())
            time.sleep(5)


if __name__ == "__main__":
    main()
