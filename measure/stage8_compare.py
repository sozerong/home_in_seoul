"""단계 8: 생활이동 행정동 한 달치에 같은 집계를 Python 표준 라이브러리, DuckDB, Spark로 돌려 시간과 메모리를 잰다.

집계: (도착 행정동, 도착시간, 이동유형)마다 행 수, 가려지지 않은 이동인구 합, 이동인구 가중 평균 이동 시간.
입력: 레이크 zip을 임시 폴더에 한 번 풀어 둔 CSV 24개(압축 해제 시간은 세지 않는다). 세 도구 모두 같은 파일을 읽는다.
인코딩: 집계에 쓰는 열(코드, 숫자, 이동유형)은 모두 ASCII다. Python과 Spark는 latin-1로 읽는다(바이트를 그대로 읽어 깨지지 않음).
DuckDB 2.0.0은 latin-1 지정 시 병렬 CSV 읽기가 실패해서("state machine reached an invalid state") 인코딩을 지정하지 않고
strict_mode=false, ignore_errors=true로 읽는다. 빠진 행이 없는지는 세 도구의 행 수가 같은지로 확인한다.
실행: python -m measure.stage8_compare 압축해제폴더 [반복 횟수, 기본 3]
       압축 해제: python -m measure.stage8_compare --extract 압축해제폴더
"""
import csv
import glob
import json
import os
import platform
import subprocess
import sys
import threading
import time
import zipfile
from pathlib import Path

import psutil

from common import ROOT

ZIP = ROOT / "lake/seoul_living_migration/collected_date=2026-10-21/생활이동_행정동_202607.zip"
# 열 순서: 대상연월, 요일, 도착시간, 출발, 도착, 성별, 나이, 이동유형, 평균 이동 시간, 이동인구


def run_python(folder):
    acc = {}
    for path in sorted(glob.glob(os.path.join(folder, "*.csv"))):
        with open(path, encoding="latin-1", newline="") as f:
            rows = csv.reader(f)
            next(rows)
            for r in rows:
                key = (r[4], r[2], r[7])
                a = acc.get(key)
                if a is None:
                    a = acc[key] = [0, 0.0, 0.0]
                a[0] += 1
                if r[9] != "*":
                    p = float(r[9])
                    a[1] += p
                    a[2] += p * int(r[8])
    return len(acc), sum(a[0] for a in acc.values())


def run_duckdb(folder):
    import duckdb
    con = duckdb.connect()
    con.execute(f"SET threads = {os.cpu_count()}")
    q = f"""
        SELECT count(*) AS groups, sum(n) AS rows FROM (
          SELECT column4 AS dest, column2 AS hour, column7 AS move, count(*) AS n,
                 sum(TRY_CAST(column9 AS DOUBLE)) AS pop,
                 sum(TRY_CAST(column9 AS DOUBLE) * CAST(column8 AS INTEGER)) AS weighted
          FROM read_csv('{folder.replace(os.sep, '/')}/*.csv', auto_detect = false, header = true, delim = ',',
                        quote = '"', strict_mode = false, ignore_errors = true,
                        columns = {{'c0': 'VARCHAR', 'c1': 'VARCHAR', 'column2': 'VARCHAR', 'c3': 'VARCHAR',
                                    'column4': 'VARCHAR', 'c5': 'VARCHAR', 'c6': 'VARCHAR', 'column7': 'VARCHAR',
                                    'column8': 'VARCHAR', 'column9': 'VARCHAR'}})
          GROUP BY 1, 2, 3)"""
    groups, rows = con.execute(q).fetchone()
    return groups, rows


def run_spark(folder):
    from pyspark.sql import SparkSession, functions as F
    spark = (SparkSession.builder.master("local[*]").appName("stage8")
             .config("spark.driver.memory", "8g").config("spark.ui.enabled", "false").config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    df = (spark.read.option("header", True).option("encoding", "ISO-8859-1")
          .csv(sorted(glob.glob(os.path.join(folder, "*.csv"))))  # 파일 목록을 직접 넘긴다. "*.csv" 경로는 Spark가 Windows에서 hadoop.dll 없이 펼치지 못하고 JVM이 죽는다
          .toDF("ym", "dow", "hour", "origin", "dest", "sex", "age", "move", "minutes", "pop"))
    pop = F.col("pop").cast("double")
    agg = df.groupBy("dest", "hour", "move").agg(F.count(F.lit(1)).alias("n"), F.sum(pop).alias("p"),
                                                 F.sum(pop * F.col("minutes").cast("int")).alias("w"))
    out = agg.agg(F.count(F.lit(1)), F.sum("n")).collect()[0]
    spark.stop()
    return out[0], out[1]


def peak_rss_of_tree(proc, stop, box):
    """자식 프로세스(Spark의 java 포함)까지 합친 메모리 사용량의 최댓값을 0.2초마다 잰다."""
    while not stop.is_set():
        try:
            total = proc.memory_info().rss + sum(c.memory_info().rss for c in proc.children(recursive=True))
            box[0] = max(box[0], total)
        except psutil.Error:
            pass
        time.sleep(0.2)


def measure(tool, folder):
    """도구 하나를 새 Python 프로세스로 한 번 돌려 (경과 초, 최대 메모리 바이트, 결과)를 잰다."""
    started = time.perf_counter()
    proc = psutil.Popen([sys.executable, "-X", "utf8", "-m", "measure.stage8_compare", "--run", tool, folder],
                        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, errors="replace")
    stop, box = threading.Event(), [0]
    watcher = threading.Thread(target=peak_rss_of_tree, args=(proc, stop, box))
    watcher.start()
    try:
        out, err = proc.communicate(timeout=3600)  # 멈추면 조용히 기다리지 않고 실패시킨다
    except subprocess.TimeoutExpired:
        proc.kill()
        out, err = proc.communicate()
    stop.set()
    watcher.join()
    if proc.returncode != 0:
        raise SystemExit(f"{tool} 실패 (종료 코드 {proc.returncode})\n" + "\n".join(err.splitlines()[-20:]))
    return time.perf_counter() - started, box[0], json.loads([l for l in out.splitlines() if l.startswith("RESULT ")][-1][7:])  # Spark 종료 뒤 taskkill 안내문이 stdout에 붙는다


def main():
    if sys.argv[1:2] == ["--run"]:  # 자식 프로세스: 도구 하나만 돌리고 결과를 JSON으로 출력
        print("RESULT", json.dumps({"python": run_python, "duckdb": run_duckdb, "spark": run_spark}[sys.argv[2]](sys.argv[3])))
        return
    if sys.argv[1:2] == ["--extract"]:
        folder = Path(sys.argv[2])
        folder.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(ZIP) as zf:
            zf.extractall(folder)
        print(f"압축 해제: {len(list(folder.glob('*.csv')))}개 파일, {sum(p.stat().st_size for p in folder.glob('*.csv')):,} B")
        return
    folder, repeat = sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 3
    import duckdb
    import pyspark
    files = sorted(glob.glob(os.path.join(folder, "*.csv")))
    print(f"입력: {ZIP.name}을 푼 CSV {len(files)}개, {sum(os.path.getsize(f) for f in files):,} B")
    print(f"환경: {platform.processor()}, 논리 코어 {os.cpu_count()}, RAM {psutil.virtual_memory().total / 2**30:.1f}GiB, "
          f"{platform.platform()}, Python {platform.python_version()}, duckdb {duckdb.__version__}, pyspark {pyspark.__version__}")
    for tool in os.environ.get("STAGE8_TOOLS", "python,duckdb,spark").split(","):  # 일부만 다시 잴 때 STAGE8_TOOLS=duckdb,spark
        for i in range(1, repeat + 1):
            seconds, peak, result = measure(tool, folder)
            print(f"{tool} {i}회: {seconds:.1f}초, 최대 메모리 {peak / 2**30:.2f}GiB, 결과 (그룹 수, 행 수) = {result}", flush=True)


if __name__ == "__main__":
    main()
