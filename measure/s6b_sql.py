"""S6b 재측정 (생활이동 Spark SQL 이전 뒤, 측정 18과 같은 질문). design_remaining_work.md 3.8, 결정 96.

"저녁 귀가(18~20시)도 보고 싶다"를 dbt 프로젝트 사본에서 *더하고*(출근을 바꾸지 않음) 전체를 빌드해 고친 곳과 번지는 범위를 잰다.
고치는 곳: 새 마트 모델 commute_od_evening 하나(commute_od를 복사해 범위 조건 한 줄만 WH, 18_20으로). Spark SQL, Python, 경계 표는 그대로.
이동유형 WH = 주간상주지 -> 야간상주지(측정 18). 비교 대상은 지금 실제 mart다. *_s6b 빌드 스키마는 끝나면 지운다.
실행(Airflow 컨테이너 안): python -m measure.s6b_sql
"""
import json
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import psycopg

from common import read_env
from measure.s6a_sql import DBT, PRODUCTS, columns

PROJECT = Path("/opt/project/warehouse/dbt")
OLD = "where day_type = '평일' and hour_group = '07_09' and move_type = 'HW'"
NEW = "where day_type = '평일' and hour_group = '18_20' and move_type = 'WH'"
SCHEMAS = ("stg_dbt_s6b", "wh_s6b", "mart_s6b")


def main():
    with tempfile.TemporaryDirectory() as tmp, psycopg.connect(read_env("DATABASE_URL"), autocommit=True) as conn:
        copy, base = Path(tmp) / "dbt", Path(tmp) / "base" / "dbt"
        for d in (copy, base):  # F-14: 두 쪽 범위를 같게(target/, logs/ 뺌). 원본 쪽에만 남은 컴파일 결과와 새 파일이 짝지어지지 않게
            shutil.copytree(PROJECT, d, ignore=shutil.ignore_patterns("target", "logs"))
        text = (copy / "models/mart/commute_od.sql").read_text(encoding="utf-8")
        assert text.count(OLD) == 1
        (copy / "models/mart/commute_od_evening.sql").write_text(
            "-- 저녁 귀가(평일 18~20시 도착, 주간상주지 -> 야간상주지 WH). commute_od와 같은 계산 (S6b)\n" + text.replace(OLD, NEW),
            encoding="utf-8")
        changed = subprocess.run(["git", "diff", "--no-index", "--numstat", "--ignore-cr-at-eol", str(base), str(copy)],
                                 capture_output=True, text=True).stdout
        print("고친 파일(더한 줄, 지운 줄):")
        for line in changed.splitlines():
            added, removed, name = line.split("\t")
            print(f"  +{added} -{removed}  {name.split('/dbt/')[-1].rstrip('}')}")
        for s in SCHEMAS:
            conn.execute(f"DROP SCHEMA IF EXISTS {s} CASCADE")
        started = time.perf_counter()
        out = subprocess.run([DBT, "build", "--vars", json.dumps({"build_suffix": "_s6b"}), "--target-path", "target/s6b",
                              "--log-path", "logs/s6b", "--project-dir", str(copy)], cwd=copy, capture_output=True, text=True)
        seconds = time.perf_counter() - started
        print(f"dbt build 종료코드 {out.returncode}, {seconds:.1f}초")
        for line in out.stdout.splitlines():
            if any(w in line for w in ("FAIL", "ERROR", "WARN", "Done.")):
                print("   ", line.split("  ", 1)[-1][:200])
        try:
            if out.returncode == 0:
                for table in PRODUCTS:
                    cols = ", ".join(columns(conn, "mart", table))
                    d = conn.execute(f"SELECT count(*) FROM ((SELECT {cols} FROM mart.{table} EXCEPT ALL SELECT {cols} FROM mart_s6b.{table}) "
                                     f"UNION ALL (SELECT {cols} FROM mart_s6b.{table} EXCEPT ALL SELECT {cols} FROM mart.{table})) x").fetchone()[0]
                    print(f"{table}: 지금 mart와 두 방향 차이 {d}")
                r = conn.execute("SELECT count(*), round(avg(avg_minutes), 1), count(*) FILTER (WHERE basis = 'age20_34'), "
                                 "string_agg(DISTINCT target_months, ' / ') FROM mart_s6b.commute_od_evening").fetchone()
                m = conn.execute("SELECT count(*), round(avg(avg_minutes), 1) FROM mart.commute_od").fetchone()
                print(f"commute_od_evening: {r[0]}행, 평균 이동 시간 평균 {r[1]}분, 20~34세 기준 {r[2]}행, 대상연월 {r[3]}")
                print(f"commute_od(출근):    {m[0]}행, 평균 이동 시간 평균 {m[1]}분")
        finally:
            for s in SCHEMAS:
                conn.execute(f"DROP SCHEMA IF EXISTS {s} CASCADE")


if __name__ == "__main__":
    main()
