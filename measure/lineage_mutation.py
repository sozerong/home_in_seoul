"""변이 시험 (P4-6, 결정 49, 59). 리니지 지도가 말하는 영향을 실제 실행으로 확인한다.

변이 하나마다:
1. stg_mut 스키마에 stg 표마다 뷰를 만든다. 변이할 컬럼만 식으로 바꾼다(4천만 행 생활이동을 복사하지 않으려고 뷰로 둔다).
2. dbt seed + run을 --vars '{source_schema: stg_mut, build_suffix: <접미사>}'로 돌려 wh·mart를 다른 스키마에 만든다.
   테스트(dbt build)는 돌리지 않는다. 변이가 테스트를 깨면 하류 모델이 만들어지지 않기 때문이다.
   컴파일 결과는 target/mut에 써서 lineage가 읽는 target/을 건드리지 않는다.
3. 상품 표 4개(export_product.TABLES)를 대조군(M0: 변이 없음, _mut0)과 컬럼마다 비교한다. 키가 있는 표는 (키, 컬럼),
   rent_listing은 컬럼 값의 다중집합을 두 방향 EXCEPT ALL로 본다.
4. 바뀐 상품 컬럼(실행) ⊆ lineage.downstream이 말한 상품 컬럼(지도)인지 본다. 지도에 없는 변화가 있으면 지도가 놓친 것이다.
M0은 실제 mart와도 비교해 var 기본값과 뷰 경유가 결과를 바꾸지 않음을 확인한다(결정 59의 검증).
실제 stg, wh, mart는 읽기만 한다. 끝나면 stg_mut, wh_mut*, mart_mut*를 지운다.

실행(Airflow 컨테이너 안, dbt가 거기 있다): python -m measure.lineage_mutation [M1 M2 ...]
주의: M1, M2는 혼잡도를 dbt로 옮기기 전(2026-11-13, 측정 22) stg.congestion에 건 변이다. 옮긴 뒤에는 혼잡도가
raw.congestion에서 오므로 stg 변이가 닿지 않는다. 다시 재려면 raw 변이(jsonb 키 값 바꾸기)로 고친다.
"""
import json
import subprocess
import sys
import time

import psycopg

from common import read_env
from lineage.product import product_tables

DBT = ["/opt/airflow/dbt-venv/bin/dbt"]
DBT_DIR = "/opt/project/warehouse/dbt"
SOURCES = ["sgis_dong", "sbiz_store", "parcel", "station_master", "congestion", "rent", "living_migration"]
KEYS = {"dong_profile": ["dong_id"], "commute_od": ["origin_dong_id", "dest_dong_id"],
        "dong_mois_map": ["mois_dong_cd"], "rent_listing": []}  # rent_listing은 거래 고유 키가 없다

# 변이: {stg 표: {컬럼: 바꿀 SQL 식}}, 설계서가 예상한 상품 컬럼
MUTATIONS = {
    "M0": ({}, "없음 (대조군)"),
    "M1": ({"congestion": {"dow": "CASE WHEN dow = '평일' THEN '주중' ELSE dow END"}},
           "dong_profile.am_peak_congestion"),
    "M2": ({"congestion": {"congestion": "congestion + 1"}}, "dong_profile.am_peak_congestion"),
    "M3": ({"station_master": {"lat": "CASE WHEN bldn_id = %(station)s THEN lat + 0.05 ELSE lat END",
                               "lot": "CASE WHEN bldn_id = %(station)s THEN lot + 0.05 ELSE lot END"}},
           "dong_profile.nearest_station_*"),
    "M4": ({"rent": {"monthly_rent": "CASE WHEN monthly_rent > 0 AND item_no % 10 = 0 THEN 0 ELSE monthly_rent END"}},
           "dong_profile.monthly_*, jeonse_*"),
    "M5": ({"living_migration": {"age": "CASE WHEN age = '25' THEN '26' ELSE age END"}},
           "commute_od.avg_minutes_age20_34, population_age20_34, basis, avg_minutes"),
    "M6": ({"sbiz_store": {"sub_cd": "CASE WHEN sub_cd = 'G20405' AND row_no % 2 = 0 THEN 'Z99999' ELSE sub_cd END"}},
           "dong_profile.convenience_store*"),
}


def columns(conn, schema, table):
    return [r[0] for r in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = %s "
                                       "AND table_name = %s ORDER BY ordinal_position", [schema, table])]


def make_stg_mut(conn, changes, params):
    conn.execute("DROP SCHEMA IF EXISTS stg_mut CASCADE; CREATE SCHEMA stg_mut")
    for table in SOURCES:
        exprs = changes.get(table, {})
        cols = columns(conn, "stg", table)
        assert set(exprs) <= set(cols), (table, exprs)
        select = ", ".join(f"({exprs[c]})::{typ(conn, table, c)} AS {c}" if c in exprs else c for c in cols)
        conn.execute(f"CREATE VIEW stg_mut.{table} AS SELECT {select} FROM stg.{table}".replace("%(station)s",
                                                                                             f"'{params['station']}'"))


def typ(conn, table, column):
    return conn.execute("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = %s::regclass "
                        "AND attname = %s", [f"stg.{table}", column]).fetchone()[0]


def dbt(suffix):
    vars_ = json.dumps({"source_schema": "stg_mut", "build_suffix": suffix})
    for cmd in (["seed", "--full-refresh"], ["run"]):
        out = subprocess.run(DBT + cmd + ["--vars", vars_, "--target-path", "target/mut", "--log-path", "logs/mut"],
                             cwd=DBT_DIR, capture_output=True, text=True)
        if out.returncode != 0:
            raise SystemExit(f"dbt {cmd[0]} 실패 ({suffix}):\n{out.stdout[-3000:]}")


def diff(conn, left, right):
    """상품 표마다 (행 수 차이, 값이 바뀐 컬럼)."""
    result = {}
    for table in product_tables():
        key = KEYS[table]
        rows = [conn.execute(f"SELECT count(*) FROM {s}.{table}").fetchone()[0] for s in (left, right)]
        changed = []
        for c in columns(conn, right, table):
            if c in key:
                continue
            pick = ", ".join(key + [c])
            n = conn.execute(f"SELECT count(*) FROM ((SELECT {pick} FROM {left}.{table} EXCEPT ALL "
                             f"SELECT {pick} FROM {right}.{table}) UNION ALL (SELECT {pick} FROM {right}.{table} "
                             f"EXCEPT ALL SELECT {pick} FROM {left}.{table})) d").fetchone()[0]
            if n:
                changed.append(c)
        result[table] = {"rows": rows, "changed": changed}
    return result


def predicted(conn, changes):
    """지도: 바꾼 stg 컬럼에서 닿는 상품 컬럼."""
    pairs = [(f"stg.{t}", c) for t, cols in changes.items() for c in cols]
    found = set()
    for t, c in pairs:
        found |= {(r[0].removeprefix("product."), r[1]) for r in conn.execute(
            "SELECT to_table, to_column FROM lineage.downstream WHERE from_table = %s AND from_column = %s "
            "AND is_product", [t, c])}
    return found


def drop_all(conn):
    for s in ("stg_mut", "wh_mut", "mart_mut", "wh_mut0", "mart_mut0"):
        conn.execute(f"DROP SCHEMA IF EXISTS {s} CASCADE")


def main():
    names = sys.argv[1:] or [m for m in MUTATIONS if m != "M0"]
    with psycopg.connect(read_env("DATABASE_URL"), autocommit=True) as conn:
        run = conn.execute("SELECT run_id, git_commit FROM lineage.run WHERE status = 'success' "
                           "ORDER BY run_id DESC LIMIT 1").fetchone()
        print(f"지도: lineage 회차 {run[0]} (git {str(run[1])[:7]})")
        # M3: 동 하나(가장 작은 dong_id)의 가장 가까운 역을 옮긴다. 그 동의 결과는 반드시 바뀐다
        station = conn.execute("SELECT s.bldn_id FROM mart.dong_profile p JOIN stg.station_master s "
                               "ON s.bldn_nm = p.nearest_station_nm ORDER BY p.dong_id, s.bldn_id LIMIT 1").fetchone()[0]
        params = {"station": station}
        try:
            started = time.perf_counter()
            make_stg_mut(conn, {}, params)
            dbt("_mut0")
            base = diff(conn, "mart", "mart_mut0")
            print(f"M0 대조군 vs 실제 mart: {base}  ({time.perf_counter() - started:.0f}초)")
            if any(v["changed"] or v["rows"][0] != v["rows"][1] for v in base.values()):
                raise SystemExit("대조군이 실제 mart와 다릅니다. stg가 그 사이 바뀌었거나 var 기본값이 결과를 바꿨습니다")
            report = []
            for name in names:
                changes, expected = MUTATIONS[name]
                started = time.perf_counter()
                make_stg_mut(conn, changes, params)
                conn.execute("DROP SCHEMA IF EXISTS wh_mut CASCADE; DROP SCHEMA IF EXISTS mart_mut CASCADE")
                dbt("_mut")
                d = diff(conn, "mart_mut0", "mart_mut")
                observed = {(t, c) for t, v in d.items() for c in v["changed"]}
                rows = {t: v["rows"] for t, v in d.items() if v["rows"][0] != v["rows"][1]}
                pred = predicted(conn, changes)
                missed = observed - pred
                report.append((name, changes, expected, observed, rows, pred, missed))
                print(f"\n{name} {changes} ({time.perf_counter() - started:.0f}초)")
                print(f"  설계서 예상: {expected}")
                print(f"  실행으로 바뀐 상품 컬럼 {len(observed)}개: {sorted(observed)}")
                print(f"  행 수가 바뀐 상품 표: {rows or '없음'}")
                print(f"  지도가 말한 상품 컬럼 {len(pred)}개: {sorted(pred)}")
                print(f"  지도가 놓친 것(실행 - 지도): {sorted(missed) or '없음'}")
                print(f"  지도에만 있는 것(이번 변이로는 안 바뀜): {sorted(pred - observed) or '없음'}")
        finally:
            drop_all(conn)
    print(f"\nM3 옮긴 역: {station}")
    bad = [r[0] for r in report if r[6]]
    print("지도가 놓친 변이:", bad or "없음")
    if bad:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
