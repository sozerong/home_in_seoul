"""계산 컨테이너(seoul-spark-job, seoul-duckdb-job, seoul-agent-job) 메모리 관측 (결정 109 H4·D2). vm_watch는 Airflow 구성 요소 안에서 돌고,
계산 컨테이너는 코드·logs가 읽기 전용이라 그 안에서 기록할 수 없어 호스트에서 docker stats로 5초마다 한 줄씩 남긴다.

실행(호스트, 레포 루트, 백그라운드): python -u -m measure.job_stats 이름 [이미지 접두어=seoul-]
  이름이 붙은 TSV를 logs/job_stats/에 쓴다. 계산 컨테이너가 하나도 없으면 다음 줄을 기다린다. 끝내기: 프로세스 종료.
열: ts, 컨테이너 이름, 이미지, 메모리 사용(B, docker stats MemUsage 앞쪽, 페이지 캐시 일부 제외), 상한(B), CPU%
"""
import datetime
import re
import subprocess
import sys
import time

from common import ROOT

UNITS = {"B": 1, "KiB": 2 ** 10, "MiB": 2 ** 20, "GiB": 2 ** 30, "TiB": 2 ** 40, "kB": 1e3, "MB": 1e6, "GB": 1e9}


def to_bytes(text):
    m = re.fullmatch(r"([\d.]+)\s*([A-Za-z]+)", text.strip())
    return int(float(m.group(1)) * UNITS[m.group(2)])


def main():
    name, prefix = sys.argv[1], (sys.argv[2] if len(sys.argv) > 2 else "seoul-")
    out = ROOT / "logs" / "job_stats" / f"{datetime.datetime.now():%Y%m%d-%H%M%S}_{name}.tsv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        f.write("ts\tcontainer\timage\tmem_b\tlimit_b\tcpu_pct\n")
        while True:
            ids = subprocess.run(["docker", "ps", "-q", "--filter", f"ancestor={prefix}spark-job:3.5.9",
                                  "--filter", f"ancestor={prefix}duckdb-job:2.0.0",
                                  "--filter", f"ancestor={prefix}agent-job:0.2.182"], capture_output=True, text=True).stdout.split()
            if ids:
                r = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}", *ids],
                                   capture_output=True, text=True)
                ts = datetime.datetime.now().isoformat(timespec="seconds")
                for line in r.stdout.splitlines():
                    cname, usage, cpu = line.split("\t")
                    used, limit = usage.split(" / ")
                    image = subprocess.run(["docker", "inspect", "-f", "{{.Config.Image}}", cname], capture_output=True, text=True).stdout.strip()
                    f.write(f"{ts}\t{cname}\t{image}\t{to_bytes(used)}\t{to_bytes(limit)}\t{cpu.rstrip('%')}\n")
                f.flush()
            time.sleep(5)


if __name__ == "__main__":
    main()
