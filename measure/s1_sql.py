"""S1 재측정 (혼잡도 SQL 이전 뒤, 측정 13과 같은 질문). 원천이 쓰는 필드 이름을 바꾸면(DPTRE_STTN -> STTN_NM)?

실제 raw, stg_dbt, wh는 건드리지 않는다. raw_s1 스키마에 옛 수집일(10-21 원본)과 새 수집일(11-03, 키 개명)을 넣고
dbt를 --vars '{raw_schema: raw_s1, build_suffix: _s1}'로 stg_dbt 모델과 원본 테스트만 빌드한다(stg_dbt_s1).
① 대조군: 두 수집일 모두 원본          지금 코드
② 11-03 개명                         지금 코드          -> 멈춰야 한다
③ ②와 같음                           고친 코드(사본)    -> 두 수집일 모두 처리돼야 한다
고친 코드는 dbt 프로젝트를 임시 폴더에 복사해 두 줄(매크로 별칭, congestion_rows의 coalesce)만 바꾼 것이다.
실행(Airflow 컨테이너 안): python -m measure.s1_sql
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
OLD, NEW = "2026-10-21", "2026-11-03"
FIX = [("macros/congestion_keys.sql", "['DPTRE_STTN']", "['DPTRE_STTN', 'STTN_NM']"),
       ("models/stg_dbt/congestion_rows.sql", "coalesce(r.raw ->> 'DPTRE_STTN', '')",
        "coalesce(r.raw ->> 'STTN_NM', r.raw ->> 'DPTRE_STTN', '')")]


def load(conn, rename):
    conn.execute("DROP SCHEMA IF EXISTS raw_s1 CASCADE; CREATE SCHEMA raw_s1; "
                 "CREATE TABLE raw_s1.congestion (LIKE raw.congestion INCLUDING ALL)")
    conn.execute("INSERT INTO raw_s1.congestion SELECT * FROM raw.congestion WHERE collected_date = ANY(%s)",
                 [[OLD, NEW]])
    if rename:
        conn.execute("UPDATE raw_s1.congestion SET raw = raw - 'DPTRE_STTN' || jsonb_build_object('STTN_NM', raw -> 'DPTRE_STTN') "
                     "WHERE collected_date = %s", [NEW])


def build(project):
    conn_vars = json.dumps({"raw_schema": "raw_s1", "build_suffix": "_s1"})
    started = time.perf_counter()
    out = subprocess.run([DBT, "build", "--select", "stg_dbt", "source:raw", "--vars", conn_vars, "--target-path",
                          "target/s1", "--log-path", "logs/mut", "--project-dir", str(project)],
                         cwd=project, capture_output=True, text=True)
    lines = [l for l in out.stdout.splitlines() if any(w in l for w in ("FAIL", "ERROR", "WARN", "Done."))]
    return out.returncode, time.perf_counter() - started, lines


def main():
    url = read_env("DATABASE_URL")
    results = []
    with psycopg.connect(url, autocommit=True) as conn, tempfile.TemporaryDirectory() as tmp:
        fixed = Path(tmp) / "dbt"
        shutil.copytree(PROJECT, fixed, ignore=shutil.ignore_patterns("target", "logs"))
        for path, old, new in FIX:
            f = fixed / path
            text = f.read_text(encoding="utf-8")
            assert text.count(old) == 1, (path, old)
            f.write_text(text.replace(old, new), encoding="utf-8")
        try:
            for case, rename, project in (("①", False, PROJECT), ("②", True, PROJECT), ("③", True, fixed)):
                load(conn, rename)
                conn.execute("DROP SCHEMA IF EXISTS stg_dbt_s1 CASCADE")
                rc, seconds, lines = build(project)
                rows = conn.execute("SELECT collected_date::text, count(*), count(*) FILTER (WHERE sttn_nm = '') "
                                    "FROM stg_dbt_s1.congestion GROUP BY 1 ORDER BY 1").fetchall() if rc == 0 else []
                same = None
                if rc == 0:
                    same = conn.execute("SELECT count(*) FROM ((SELECT * FROM stg_dbt_s1.congestion EXCEPT ALL "
                                        "SELECT * FROM stg_dbt.congestion WHERE collected_date = ANY(%s)) UNION ALL "
                                        "(SELECT * FROM stg_dbt.congestion WHERE collected_date = ANY(%s) EXCEPT ALL "
                                        "SELECT * FROM stg_dbt_s1.congestion)) d", [[OLD, NEW], [OLD, NEW]]).fetchone()[0]
                results.append((case, rc, seconds, rows, same, lines))
                print(f"{case} 종료코드 {rc}, {seconds:.1f}초, 수집일별 (행, 역이름 빈 행) {rows}, 실제 stg_dbt와 차이 {same}")
                for line in lines:
                    print("   ", line.split("  ", 1)[-1][:200])
        finally:
            for s in ("raw_s1", "stg_dbt_s1"):
                conn.execute(f"DROP SCHEMA IF EXISTS {s} CASCADE")
    ok = results[0][1] == 0 and results[1][1] != 0 and results[2][1] == 0 and results[2][4] == 0
    print("기대대로:", ok)
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
