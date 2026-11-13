"""S2·S3 재측정 (전월세 SQL 이전 뒤, 측정 12·14와 같은 질문). 결정 72.

S2 무해한 필드 추가: 측정 12와 같은 처리 단위(offi, 수집일 2026-11-03, 11680, 202610)의 원본 item마다 newField를 더한다.
    ① 원본 그대로  ② item마다 newField  -> ②도 통과하고(경고만) 정제 결과가 ①과 같아야 한다.
    원본 적재(transform/rent_raw.py)는 필드를 검사하지 않고 자식 태그 전부를 jsonb에 담으므로, 새 필드를 단 jsonb를 바로 넣는다.
S3 규칙 정정: 측정 14와 같은 C6 정정(산 지번). 옛 규칙 = 산 지번은 V9로 격리, 대지구분은 늘 1.
    raw.rent 전체로 옛 규칙(사본 프로젝트의 두 줄)과 지금 규칙을 각각 빌드하고, 결과가 달라진 처리 단위를 쿼리 한 번으로 센다.
실제 raw, stg_dbt, wh는 건드리지 않는다. raw_s2 스키마와 stg_dbt_s2/s3old/s3new 빌드 스키마만 쓰고 끝나면 지운다.
실행(Airflow 컨테이너 안): python -m measure.s2_s3_sql
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
UNIT = ("offi", "2026-11-03", "11680", "202610")
OLD_RULE = [  # 옛 transform/rent.py(커밋 b824215)의 C6과 같게 되돌리는 두 줄
    ("models/stg_dbt/rent_rows.sql", "regexp_match(v.jibun, '^(산)?([0-9]+)(?:-([0-9]+))?$') as jibun_m",
     "case when v.jibun like '산%' then null else regexp_match(v.jibun, '^(산)?([0-9]+)(?:-([0-9]+))?$') end as jibun_m"),
    ("models/stg_dbt/rent_rows.sql", "case when c.jibun_m[1] is null then '1' else '2' end", "'1'"),
]
SELECT = ["--select", "rent_rows", "rent", "rent_quarantine", "source:raw.rent+", "--exclude", "wh", "mart"]


def build(project, raw_schema, suffix, unit_tests=True):
    args = [DBT, "build", *SELECT, *([] if unit_tests else ["test_type:unit"]),
            "--vars", json.dumps({"raw_schema": raw_schema, "build_suffix": suffix}),
            "--target-path", f"target/{suffix.strip('_')}", "--log-path", "logs/mut", "--project-dir", str(project)]
    started = time.perf_counter()
    out = subprocess.run(args, cwd=project, capture_output=True, text=True)
    lines = [l.split("  ", 1)[-1][:200] for l in out.stdout.splitlines()
             if any(w in l for w in ("FAIL", "ERROR", "WARN", "Done."))]
    return out.returncode, time.perf_counter() - started, lines


def diff(conn, a, b, where="true"):
    return conn.execute(f"SELECT count(*) FROM ((SELECT * FROM {a} WHERE {where} EXCEPT ALL SELECT * FROM {b} WHERE {where}) "
                        f"UNION ALL (SELECT * FROM {b} WHERE {where} EXCEPT ALL SELECT * FROM {a} WHERE {where})) d").fetchone()[0]


def s2(conn):
    where = ("source_type = '{}' AND collected_date = '{}' AND sgg_cd = '{}' AND deal_ymd = '{}'".format(*UNIT))
    results = []
    for case, extra in (("①", "'{}'::jsonb"), ("②", """'{"newField": "x"}'::jsonb""")):
        conn.execute("DROP SCHEMA IF EXISTS raw_s2 CASCADE; CREATE SCHEMA raw_s2; "
                     "CREATE TABLE raw_s2.rent (LIKE raw.rent INCLUDING ALL)")
        conn.execute(f"INSERT INTO raw_s2.rent SELECT source_type, collected_date, sgg_cd, deal_ymd, source_file, item_no, "
                     f"raw || {extra}, loaded_at FROM raw.rent WHERE {where}")
        conn.execute("DROP SCHEMA IF EXISTS stg_dbt_s2 CASCADE")
        rc, seconds, lines = build(PROJECT, "raw_s2", "_s2")
        rows = quarantined = same = None
        if rc == 0:
            rows = conn.execute("SELECT count(*) FROM stg_dbt_s2.rent").fetchone()[0]
            quarantined = conn.execute("SELECT count(*) FROM stg_dbt_s2.rent_quarantine").fetchone()[0]
            same = diff(conn, "stg_dbt_s2.rent", "stg_dbt.rent", where)
        results.append((case, rc, rows, quarantined, same))
        print(f"S2 {case} 종료코드 {rc}, {seconds:.1f}초, 통과 {rows}, 격리 {quarantined}, 실제 stg_dbt.rent 그 단위와 차이 {same}")
        for line in lines:
            print("   ", line)
    return results[0][1] == 0 and results[1][1] == 0 and results[1][2] == results[0][2] and results[1][4] == 0


def s3(conn, tmp):
    old = Path(tmp) / "dbt"
    shutil.copytree(PROJECT, old, ignore=shutil.ignore_patterns("target", "logs"))
    for path, a, b in OLD_RULE:
        f = old / path
        text = f.read_text(encoding="utf-8")
        assert text.count(a) == 1, (path, a)
        f.write_text(text.replace(a, b), encoding="utf-8")
    for schema in ("stg_dbt_s3old", "stg_dbt_s3new"):
        conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    # 옛 규칙은 지금 unit test(산 지번 통과)를 어기므로 unit test를 빼고 빌드한다
    for name, project, suffix, unit in (("옛 규칙", old, "_s3old", False), ("지금 규칙", PROJECT, "_s3new", True)):
        rc, seconds, lines = build(project, "raw", suffix, unit)
        print(f"S3 {name} 빌드: 종료코드 {rc}, {seconds:.1f}초", *lines[-1:])
        if rc != 0:
            return False
    started = time.perf_counter()
    key = "source_type, collected_date, sgg_cd, deal_ymd"
    # 재처리 대상 찾기 = 쿼리 한 번: 두 빌드에서 (정제 결과 + 격리 사유)가 달라진 처리 단위
    units = conn.execute(f"""
        WITH a AS (SELECT {key}, source_file, item_no, pnu AS v FROM stg_dbt_s3old.rent
                   UNION ALL SELECT {key}, source_file, item_no, array_to_string(reasons, ',') FROM stg_dbt_s3old.rent_quarantine),
             b AS (SELECT {key}, source_file, item_no, pnu FROM stg_dbt_s3new.rent
                   UNION ALL SELECT {key}, source_file, item_no, array_to_string(reasons, ',') FROM stg_dbt_s3new.rent_quarantine),
             d AS ((SELECT * FROM a EXCEPT SELECT * FROM b) UNION (SELECT * FROM b EXCEPT SELECT * FROM a))
        SELECT {key}, count(DISTINCT (source_file, item_no)) FROM d GROUP BY {key} ORDER BY {key}""").fetchall()
    seconds = time.perf_counter() - started
    total = conn.execute(f"SELECT count(DISTINCT ({key})) FROM raw.rent").fetchone()[0]
    by_day = {}
    for u in units:
        by_day[str(u[1])] = by_day.get(str(u[1]), 0) + 1
    print(f"S3 달라진 처리 단위 {len(units)}개 / 전체 {total}개, item {sum(u[4] for u in units)}개, 출처 "
          f"{sorted({u[0] for u in units})}, 쿼리 {seconds:.2f}초")
    print("   수집일별 단위:", by_day)
    rule_only = conn.execute(f"SELECT count(DISTINCT ({key})) FROM raw.rent WHERE btrim(raw ->> 'jibun') LIKE '산%'").fetchone()[0]
    print(f"   규칙 전용 고르기(원본 지번이 산으로 시작하는 단위) {rule_only}개")
    return len(units) == rule_only


def main():
    with psycopg.connect(read_env("DATABASE_URL"), autocommit=True) as conn, tempfile.TemporaryDirectory() as tmp:
        try:
            ok2 = s2(conn)
            ok3 = s3(conn, tmp)
        finally:
            for s in ("raw_s2", "stg_dbt_s2", "stg_dbt_s3old", "stg_dbt_s3new"):
                conn.execute(f"DROP SCHEMA IF EXISTS {s} CASCADE")
    print("기대대로: S2", ok2, "S3", ok3)
    if not (ok2 and ok3):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
