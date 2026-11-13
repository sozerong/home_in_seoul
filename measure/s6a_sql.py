"""S6a 재측정 (상가정보 SQL 이전 뒤, 측정 17과 같은 질문). design_remaining_work.md 2.6, 결정 78, 79.

"생활 편의에 헬스장(R10307)을 더한다"를 dbt 프로젝트 사본에서 고치고 전체를 빌드해, 고친 곳(파일, 줄)과 상품에 번지는 범위를 잰다.
고치는 곳: seed amenity_category 한 줄, mart/dong_amenity 한 줄, mart/dong_profile 두 줄. 원본 다시 적재와 Python은 없다.
비교 대상은 지금 실제 mart(같은 원본으로 만든 것)다. 실제 스키마는 읽기만 하고 *_s6a 빌드 스키마는 끝나면 지운다.
실행(Airflow 컨테이너 안): python -m measure.s6a_sql
"""
import json
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import psycopg

from common import read_env

DBT = "/opt/airflow/dbt-venv/bin/dbt"
PROJECT = Path("/opt/project/warehouse/dbt")
CODE, NAME = "R10307", "헬스장"  # 측정 17과 같은 업종
EDITS = [  # (파일, 찾을 글자, 바꿀 글자)
    ("seeds/amenity_category.csv", "S20902,셀프 빨래방\n", f"S20902,셀프 빨래방\n{CODE},{NAME}\n"),
    ("models/mart/dong_amenity.sql",
     "       count(s.sub_cd) filter (where s.sub_cd in ('S20901', 'S20902')) as laundry\n",
     "       count(s.sub_cd) filter (where s.sub_cd in ('S20901', 'S20902')) as laundry,\n"
     f"       count(s.sub_cd) filter (where s.sub_cd = '{CODE}') as gym\n"),
    ("models/mart/dong_profile.sql", "a.internal_pediatric_clinic, a.laundry,", "a.internal_pediatric_clinic, a.laundry, a.gym,"),
    ("models/mart/dong_profile.sql", "       round(a.laundry / d.area_km2, 1) as laundry_per_km2\n",
     "       round(a.laundry / d.area_km2, 1) as laundry_per_km2,\n       round(a.gym / d.area_km2, 1) as gym_per_km2\n"),
]
SCHEMAS = ("stg_dbt_s6a", "wh_s6a", "mart_s6a")
PRODUCTS = ("dong_profile", "commute_od", "rent_listing", "dong_mois_map")


def columns(conn, schema, table):
    return [r[0] for r in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = %s "
                                       "AND table_name = %s ORDER BY ordinal_position", [schema, table])]


def diff(conn, table, cols):
    pick = ", ".join(cols)
    return conn.execute(f"SELECT count(*) FROM ((SELECT {pick} FROM mart.{table} EXCEPT ALL SELECT {pick} FROM mart_s6a.{table}) "
                        f"UNION ALL (SELECT {pick} FROM mart_s6a.{table} EXCEPT ALL SELECT {pick} FROM mart.{table})) d").fetchone()[0]


def main():
    with tempfile.TemporaryDirectory() as tmp, psycopg.connect(read_env("DATABASE_URL"), autocommit=True) as conn:
        copy = Path(tmp) / "dbt"
        shutil.copytree(PROJECT, copy, ignore=shutil.ignore_patterns("target", "logs"))
        files = set()
        for path, a, b in EDITS:
            f = copy / path
            text = f.read_text(encoding="utf-8")
            assert text.count(a) == 1, (path, a)
            f.write_text(text.replace(a, b), encoding="utf-8")
            files.add(path)
        changed = subprocess.run(["git", "diff", "--no-index", "--numstat", "--ignore-cr-at-eol", str(PROJECT), str(copy)],
                                 capture_output=True, text=True).stdout
        print("고친 파일(더한 줄, 지운 줄):")
        for line in changed.splitlines():
            added, removed, name = line.split("\t")
            if "/target/" not in name and "/logs/" not in name:
                print(f"  +{added} -{removed}  {name.split('/dbt/')[-1].rstrip('}')}")
        for s in SCHEMAS:
            conn.execute(f"DROP SCHEMA IF EXISTS {s} CASCADE")
        started = time.perf_counter()
        out = subprocess.run([DBT, "build", "--vars", json.dumps({"build_suffix": "_s6a"}), "--target-path", "target/s6a",
                              "--log-path", "logs/s6a", "--project-dir", str(copy)], cwd=copy, capture_output=True, text=True)
        seconds = time.perf_counter() - started
        print(f"dbt build 종료코드 {out.returncode}, {seconds:.1f}초")
        for line in out.stdout.splitlines():
            if any(w in line for w in ("FAIL", "ERROR", "WARN", "Done.")):
                print("   ", line.split("  ", 1)[-1][:200])
        try:
            if out.returncode == 0:
                for table in PRODUCTS:
                    old, new = columns(conn, "mart", table), columns(conn, "mart_s6a", table)
                    rows = [conn.execute(f"SELECT count(*) FROM {s}.{table}").fetchone()[0] for s in ("mart", "mart_s6a")]
                    print(f"{table}: 행 {rows[0]} -> {rows[1]}, 더한 컬럼 {[c for c in new if c not in old]}, "
                          f"뺀 컬럼 {[c for c in old if c not in new]}, 있던 컬럼 기준 두 방향 차이 {diff(conn, table, old)}")
                gym = conn.execute("SELECT sum(gym), count(*) FILTER (WHERE gym > 0) FROM mart_s6a.dong_profile").fetchone()
                print(f"헬스장 합계 {gym[0]}, 헬스장이 있는 동 {gym[1]}")
        finally:
            for s in SCHEMAS:
                conn.execute(f"DROP SCHEMA IF EXISTS {s} CASCADE")


if __name__ == "__main__":
    main()
