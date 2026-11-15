# M5는 8절 은퇴 뒤 재현 불가(옛 표 stg.living_migration, 결정 105로 지움)라 뺐다. 측정 31의 M5는 커밋 3efb02f에서 재현한다.
# 뷰 만들기와 비교는 agent/sandbox.py와 함께 쓴다(결정 108 D4).
"""변이 시험 (P4-6, 결정 49, 59). 리니지 지도가 말하는 영향을 실제 실행으로 확인한다.

변이 하나마다:
1. stg_mut, raw_mut 스키마에 원천 표마다 뷰를 만든다. 변이할 컬럼만 식으로 바꾼다(4천만 행 생활이동을 복사하지 않으려고 뷰로 둔다).
   정제를 dbt로 옮긴 출처(혼잡도, 전월세, 상가정보)는 raw 표의 jsonb 키 값을 바꾼다(결정 84). 나머지는 stg 컬럼을 바꾼다.
2. dbt seed + run을 --vars '{source_schema: stg_mut, raw_schema: raw_mut, build_suffix: <접미사>}'로 돌려
   stg_dbt·wh·mart를 다른 스키마에 만든다.
   테스트(dbt build)는 돌리지 않는다. 변이가 테스트를 깨면 하류 모델이 만들어지지 않기 때문이다.
   컴파일 결과는 target/mut에 써서 lineage가 읽는 target/을 건드리지 않는다.
3. 상품 표 4개(export_product.TABLES)를 대조군(M0: 변이 없음, _mut0)과 컬럼마다 비교한다. 키가 있는 표는 (키, 컬럼),
   rent_listing은 컬럼 값의 다중집합을 두 방향 EXCEPT ALL로 본다.
4. 바뀐 상품 컬럼(실행) ⊆ lineage.downstream이 말한 상품 컬럼(지도)인지 본다. 지도에 없는 변화가 있으면 지도가 놓친 것이다.
M0은 실제 mart와도 비교해 var 기본값과 뷰 경유가 결과를 바꾸지 않음을 확인한다(결정 59의 검증).
실제 stg, wh, mart는 읽기만 한다. 끝나면 stg_mut, wh_mut*, mart_mut*를 지운다.

실행(Airflow 컨테이너 안, dbt가 거기 있다): python -m measure.lineage_mutation [M1 M2 ...]
M1, M2, M4, M6은 처음(측정 22)에는 stg 표에 건 변이였다. 정제를 dbt로 옮긴 뒤(결정 72, 78) 그 값은 raw jsonb에서 오므로
raw 키 값 변이로 다시 썼다(결정 84). M7은 결정 80의 contract_type 변이다.
"""
import json
import subprocess
import sys
import time

import psycopg

from common import read_env
from agent.sandbox import diff, make_views, mutation_params, predicted

DBT = ["/opt/airflow/dbt-venv/bin/dbt"]
DBT_DIR = "/opt/project/warehouse/dbt"
def set_key(key, value, when):
    """raw jsonb의 키 하나를 조건에 맞는 행만 바꾸는 식. 원천 값은 모두 문자열이라 문자열로 넣는다."""
    return f"CASE WHEN {when} THEN jsonb_set(raw, ARRAY['{key}'], to_jsonb(({value})::text)) ELSE raw END"


# 혼잡도 시간 칸(TIME0530 ~ TIME0030) 숫자에 모두 1을 더한다
TIME_PLUS_1 = ("(SELECT jsonb_object_agg(k, CASE WHEN k LIKE 'TIME%' AND v ~ '^[0-9]+([.][0-9]+)?$' "
               "THEN to_jsonb((v::numeric + 1)::text) ELSE to_jsonb(v) END) FROM jsonb_each_text(raw) t(k, v))")

# 변이: ({"스키마.표": {컬럼: 바꿀 SQL 식}}, 설계서가 예상한 상품 컬럼, 지도에서 찾을 원천 (표, 컬럼)).
# raw 변이의 지도 출처는 키('raw.DOW_SE')다. 혼잡도 시간 칸은 jsonb_each_text(raw)로 풀어 컬럼 'raw' 전체에서 온다.
MUTATIONS = {
    "M0": ({}, "없음 (대조군)", []),
    "M1": ({"raw.congestion": {"raw": set_key("DOW_SE", "'주중'", "raw ->> 'DOW_SE' = '평일'")}},
           "dong_profile.am_peak_congestion", [("raw.congestion", "raw.DOW_SE")]),
    "M2": ({"raw.congestion": {"raw": TIME_PLUS_1}}, "dong_profile.am_peak_congestion", [("raw.congestion", "raw")]),
    "M3": ({"stg.station_master": {"lat": "CASE WHEN bldn_id = %(station)s THEN lat + 0.05 ELSE lat END",
                                   "lot": "CASE WHEN bldn_id = %(station)s THEN lot + 0.05 ELSE lot END"}},
           "dong_profile.nearest_station_*", [("stg.station_master", "lat"), ("stg.station_master", "lot")]),
    "M4": ({"raw.rent": {"raw": set_key("monthlyRent", "'0'",
                                        "btrim(raw ->> 'monthlyRent') NOT IN ('0', '') AND item_no % 10 = 0")}},
           "dong_profile.monthly_*, jeonse_*", [("raw.rent", "raw.monthlyRent")]),
    "M6": ({"raw.sbiz_store": {"raw": set_key("상권업종소분류코드", "'Z99999'",
                                              "raw ->> '상권업종소분류코드' = 'G20405' AND row_no % 2 = 0")}},
           "dong_profile.convenience_store*", [("raw.sbiz_store", "raw.상권업종소분류코드")]),
    "M7": ({"raw.rent": {"raw": set_key("contractType", "'신규계약'", "raw ->> 'contractType' = '신규' AND item_no % 10 = 0")}},
           "dong_profile.monthly_deals, jeonse_deals 등, rent_listing 행 수", [("raw.rent", "raw.contractType")]),
}


def make_mut(conn, changes, params):
    make_views(conn, "_mut", changes, params)


def dbt(suffix):
    vars_ = json.dumps({"source_schema": "stg_mut", "raw_schema": "raw_mut", "build_suffix": suffix})
    for cmd in (["seed", "--full-refresh"], ["run"]):
        out = subprocess.run(DBT + cmd + ["--vars", vars_, "--target-path", "target/mut", "--log-path", "logs/mut"],
                             cwd=DBT_DIR, capture_output=True, text=True)
        if out.returncode != 0:
            raise SystemExit(f"dbt {cmd[0]} 실패 ({suffix}):\n{out.stdout[-3000:]}")


def drop_all(conn):
    for s in ("stg_mut", "raw_mut", "stg_dbt_mut", "wh_mut", "mart_mut", "stg_dbt_mut0", "wh_mut0", "mart_mut0"):
        conn.execute(f"DROP SCHEMA IF EXISTS {s} CASCADE")


def main():
    names = sys.argv[1:] or [m for m in MUTATIONS if m != "M0"]
    with psycopg.connect(read_env("DATABASE_URL"), autocommit=True) as conn:
        run = conn.execute("SELECT run_id, git_commit FROM lineage.run WHERE status = 'success' "
                           "ORDER BY run_id DESC LIMIT 1").fetchone()
        print(f"지도: lineage 회차 {run[0]} (git {str(run[1])[:7]})")
        params = mutation_params(conn)  # M3: 동 하나의 가장 가까운 역
        station = params["station"]
        try:
            started = time.perf_counter()
            make_mut(conn, {}, params)
            dbt("_mut0")
            base = diff(conn, "mart", "mart_mut0")
            print(f"M0 대조군 vs 실제 mart: {base}  ({time.perf_counter() - started:.0f}초)")
            if any(v["changed"] or v["rows"][0] != v["rows"][1] for v in base.values()):
                raise SystemExit("대조군이 실제 mart와 다릅니다. stg가 그 사이 바뀌었거나 var 기본값이 결과를 바꿨습니다")
            report = []
            for name in names:
                changes, expected, sources = MUTATIONS[name]
                started = time.perf_counter()
                make_mut(conn, changes, params)
                conn.execute("DROP SCHEMA IF EXISTS stg_dbt_mut CASCADE; DROP SCHEMA IF EXISTS wh_mut CASCADE; "
                             "DROP SCHEMA IF EXISTS mart_mut CASCADE")
                dbt("_mut")
                d = diff(conn, "mart_mut0", "mart_mut")
                observed = {(t, c) for t, v in d.items() for c in v["changed"]}
                rows = {t: v["rows"] for t, v in d.items() if v["rows"][0] != v["rows"][1]}
                pred = predicted(conn, sources)
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
