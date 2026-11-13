"""컨테이너별 메모리 측정 (resource_guard.md 2절 1번). 호스트에서 `docker stats --no-stream`을 주기마다 읽어 TSV로 남기고, 끝나면 컨테이너별 최대를 찍는다.

실행(호스트): .venv\\Scripts\\python -u -m measure.docker_mem 이름 [--seconds 600] [--interval 10] [-- 명령...]
  -- 뒤에 명령을 주면 그 명령이 끝날 때까지 잰다(--seconds 무시). 명령의 종료 코드를 그대로 돌려준다(실패를 삼키지 않는다).
출력: logs/docker_mem/<시각>_<이름>.tsv (ts, name, mem_bytes, cpu_pct). 마지막에 컨테이너별 최대 메모리.
한계: docker stats 한 번에 약 2초가 걸려 주기가 그보다 짧아지지 않고, 주기 사이의 순간 최대는 놓친다. 순간 최대는 vm_watch(5초)와 함께 본다.
"""
import datetime
import subprocess
import sys
import time

from common import ROOT

UNITS = {"B": 1, "KiB": 1024, "MiB": 1024 ** 2, "GiB": 1024 ** 3, "TiB": 1024 ** 4, "kB": 1000, "MB": 1000 ** 2, "GB": 1000 ** 3}


def to_bytes(text):
    """'1.633GiB' -> 1753441009"""
    num = text.rstrip("BKMGTi")
    return int(float(num) * UNITS[text[len(num):]])


def sample():
    out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}"],
                         capture_output=True, text=True, check=True).stdout
    for line in out.splitlines():
        name, mem, cpu = line.split("\t")
        if name.startswith("new_proj-"):
            yield name, to_bytes(mem.split(" / ")[0]), float(cpu.rstrip("%"))


def main():
    args = sys.argv[1:]
    cmd = args[args.index("--") + 1:] if "--" in args else None
    args = args[:args.index("--")] if "--" in args else args
    name = args[0]
    seconds = int(args[args.index("--seconds") + 1]) if "--seconds" in args else 600
    interval = int(args[args.index("--interval") + 1]) if "--interval" in args else 10
    out = ROOT / "logs" / "docker_mem" / f"{datetime.datetime.now():%Y%m%d-%H%M%S}_{name}.tsv"
    out.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen(cmd) if cmd else None
    started, peak = time.time(), {}
    with open(out, "w", buffering=1, encoding="utf-8") as f:
        f.write("ts\tname\tmem_bytes\tcpu_pct\n")
        while (proc.poll() is None) if proc else (time.time() - started < seconds):
            ts = datetime.datetime.now().astimezone().isoformat(timespec="seconds")
            for cname, mem, cpu in sample():
                f.write(f"{ts}\t{cname}\t{mem}\t{cpu}\n")
                peak[cname] = max(peak.get(cname, 0), mem)
            time.sleep(interval)
    print(f"{out} ({time.time() - started:.0f}초)")
    for cname, mem in sorted(peak.items()):
        print(f"  최대 {cname}: {mem / 1024 ** 3:.3f} GiB")
    if proc:
        print("명령 종료 코드:", proc.returncode)
        sys.exit(proc.returncode)


if __name__ == "__main__":
    assert to_bytes("1.633GiB") == int(1.633 * 1024 ** 3) and to_bytes("266MiB") == 266 * 1024 ** 2 and to_bytes("0B") == 0
    main()
