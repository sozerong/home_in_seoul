"""격리 파이프라인 (design_agent.md 2절, 11절, 결정 108). "같은 코드, 다른 대상".

코드 폴더(git worktree) 하나와 데이터(실제 또는 변이)를 받아 dbt build를 <스키마>_sbx_<라벨>에 돌린다.
원천은 raw_sbx_<라벨>, stg_sbx_<라벨>의 뷰로 읽는다(복사하지 않음. 변이는 바꿀 컬럼만 식으로 바꾼 뷰).
기준선(main)과 후보(고친 브랜치)를 같은 데이터로 빌드하고 G1~G5로 판정한다. 판정은 이 코드가 하고 에이전트는 하지 않는다.
운영 스키마(raw, stg, stg_dbt, wh, mart)는 읽기만 한다. 끝나면 *_sbx_* 스키마를 지운다.

변이 시험(measure/lineage_mutation.py)도 여기의 뷰 만들기와 비교(diff, predicted)를 쓴다(D4).
실행은 agent/run.py가 한다. 이 파일만으로 사람이 고친 브랜치를 판정하려면:
    python -m agent.run --cand-branch <브랜치> [--data S1]   (agent-job 컨테이너, 결정 112)
"""
import json
import subprocess
from pathlib import Path

import yaml

from common import ROOT
from lineage.product import product_tables

DBT = "/opt/dbt-venv/bin/dbt"  # seoul-agent-job 이미지(결정 112). 버전은 Airflow 이미지의 dbt-venv와 같다
KEYS = {"dong_profile": ["dong_id"], "commute_od": ["origin_dong_id", "dest_dong_id"],
        "dong_mois_map": ["mois_dong_cd"], "rent_listing": []}  # rent_listing은 거래 고유 키가 없다
# 출처마다: 원본 표, 정제 표, 격리 표, batch_status.batch_key와 같은 식 (batch_status_*.sql, rent_batch_key 매크로)
SOURCES = {
    "congestion": ("raw.congestion", "congestion", "congestion_quarantine", "collected_date::text"),
    "rent": ("raw.rent", "rent", "rent_quarantine", "concat_ws('/', source_type, collected_date, sgg_cd, deal_ymd)"),
    "store": ("raw.sbiz_store", "store", "store_quarantine", "concat_ws('/', collected_date, source_file)"),
}
# 평가용 데이터 (docs/agent_answer_key.json). M은 measure/lineage_mutation.py MUTATIONS를 쓴다
LATEST = "(SELECT max(collected_date) FROM raw.congestion)"
EVAL_DATA = {
    "S1": {"raw.congestion": {"raw": f"CASE WHEN collected_date = {LATEST} THEN raw - 'DPTRE_STTN' || "
                                     "jsonb_build_object('STTN_NM', raw -> 'DPTRE_STTN') ELSE raw END"}},
}


def data_changes(name):
    """데이터 이름 -> {원천 표: {컬럼: 식}}. real은 변이 없음."""
    if name == "real":
        return {}
    if name in EVAL_DATA:
        return EVAL_DATA[name]
    from measure.lineage_mutation import MUTATIONS  # 함수 안 import: lineage_mutation이 이 모듈을 import한다
    if name not in MUTATIONS:
        raise SystemExit(f"모르는 데이터 {name}. real, {', '.join(EVAL_DATA)}, {', '.join(MUTATIONS)} 중 하나")
    return MUTATIONS[name][0]


def mutation_params(conn):
    """M3: 동 하나(가장 작은 dong_id)의 가장 가까운 역을 옮긴다."""
    return {"station": conn.execute("SELECT s.bldn_id FROM mart.dong_profile p JOIN stg.station_master s "
                                    "ON s.bldn_nm = p.nearest_station_nm ORDER BY p.dong_id, s.bldn_id LIMIT 1").fetchone()[0]}


def columns(conn, schema, table):
    return [r[0] for r in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = %s "
                                       "AND table_name = %s ORDER BY ordinal_position", [schema, table])]


def typ(conn, table, column):
    return conn.execute("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = %s::regclass "
                        "AND attname = %s", [table, column]).fetchone()[0]


def source_tables():
    """dbt sources.yml의 {스키마: [표]}. 뷰를 만들 원천 목록을 dbt와 같은 파일에서 읽는다."""
    src = yaml.safe_load((ROOT / "warehouse" / "dbt" / "models" / "sources.yml").read_text(encoding="utf-8"))
    return {s["name"]: [t["name"] for t in s["tables"]] for s in src["sources"]}


def make_views(conn, suffix, changes, params=None):
    """raw{suffix}, stg{suffix}에 원천 표마다 뷰. changes의 컬럼만 식으로 바꾼다."""
    tables = source_tables()
    unknown = set(changes) - {f"{s}.{t}" for s, ts in tables.items() for t in ts}
    if unknown:
        raise SystemExit(f"dbt 원천에 없는 표를 바꾸려 합니다: {sorted(unknown)}")
    for schema, names in tables.items():
        conn.execute(f"DROP SCHEMA IF EXISTS {schema}{suffix} CASCADE; CREATE SCHEMA {schema}{suffix}")
        for table in names:
            name = f"{schema}.{table}"
            exprs = changes.get(name, {})
            cols = columns(conn, schema, table)
            assert set(exprs) <= set(cols), (name, exprs)
            select = ", ".join(f"({exprs[c]})::{typ(conn, name, c)} AS {c}" if c in exprs else c for c in cols)
            sql = f"CREATE VIEW {schema}{suffix}.{table} AS SELECT {select} FROM {name}"
            for k, v in (params or {}).items():
                sql = sql.replace(f"%({k})s", f"'{v}'")
            conn.execute(sql)


def diff(conn, left, right):
    """상품 표마다 (행 수 [왼쪽, 오른쪽], 값이 바뀐 컬럼). 키가 있으면 (키, 컬럼), 없으면 컬럼 값 다중집합을 두 방향 EXCEPT ALL."""
    result = {}
    for table in product_tables():
        key = KEYS[table]
        # 기준선 빌드에서 상류 테스트가 실패하면 dbt가 상품 모델을 skip해 표가 없다(M7, F-34).
        # 그때는 고객이 지금 받는 운영 상품(mart)과 비교한다. ponytail: 운영 mart가 마지막 정상본이라는 가정
        left_t = left if conn.execute("SELECT to_regclass(%s)", [f"{left}.{table}"]).fetchone()[0] else "mart"
        rows = [conn.execute(f"SELECT count(*) FROM {s}.{table}").fetchone()[0] for s in (left_t, right)]
        changed = []
        for c in columns(conn, right, table):
            if c in key:
                continue
            pick = ", ".join(key + [c])
            n = conn.execute(f"SELECT count(*) FROM ((SELECT {pick} FROM {left_t}.{table} EXCEPT ALL "
                             f"SELECT {pick} FROM {right}.{table}) UNION ALL (SELECT {pick} FROM {right}.{table} "
                             f"EXCEPT ALL SELECT {pick} FROM {left_t}.{table})) d").fetchone()[0]
            if n:
                changed.append(c)
        result[table] = {"rows": rows, "changed": changed} | ({"left": left_t} if left_t != left else {})
    return result


def predicted(conn, pairs):
    """지도: 원천 (표, 컬럼)에서 닿는 상품 컬럼. 컬럼이 None이면 그 표의 모든 컬럼에서."""
    found = set()
    for t, c in pairs:
        found |= {(r[0].removeprefix("product."), r[1]) for r in conn.execute(
            "SELECT to_table, to_column FROM lineage.downstream WHERE from_table = %s "
            "AND (%s::text IS NULL OR from_column = %s) AND is_product", [t, c, c])}
    return found


def drop(conn, label=None):
    """label의 *_sbx_<label> 스키마를 지운다. label이 없으면 모든 *_sbx_* (시작 비우기, J-6)."""
    pattern = r"%\_sbx\_%" if label is None else rf"%\_sbx\_{label}"
    for (s,) in conn.execute("SELECT nspname FROM pg_namespace WHERE nspname LIKE %s", [pattern]).fetchall():
        conn.execute(f'DROP SCHEMA "{s}" CASCADE')


def build(conn, code, label, changes, params=None):
    """code(레포 폴더)의 dbt를 *_sbx_<label>에 build. 실패한 노드가 있어도 예외가 아니다(기준선은 깨진 데이터로 실패하는 게 정상).
    dbt가 run_results를 남기지 못한 경우(컴파일 오류 등)만 예외."""
    s = f"_sbx_{label}"
    drop(conn, label)
    make_views(conn, s, changes, params)
    dbt_dir = Path(code) / "warehouse" / "dbt"
    vars_ = json.dumps({"source_schema": f"stg{s}", "raw_schema": f"raw{s}", "build_suffix": s})
    out = subprocess.run([DBT, "build", "--vars", vars_, "--target-path", f"target/sbx_{label}",
                          "--log-path", f"logs/sbx_{label}"], cwd=dbt_dir, capture_output=True, text=True)
    results = dbt_dir / "target" / f"sbx_{label}" / "run_results.json"
    if not results.exists():
        raise SystemExit(f"dbt build({label})가 결과를 남기지 못했습니다 rc={out.returncode}:\n{out.stdout[-3000:]}")
    failed = [{"node": r["unique_id"], "status": r["status"], "message": (r.get("message") or "")[:300]}
              for r in json.loads(results.read_text(encoding="utf-8"))["results"]
              if r["status"] in ("error", "fail", "skipped")]
    return {"rc": out.returncode, "failed": failed}


def signal_from(conn, label, built):
    """기준선 빌드에서 신호를 뽑는다: 출처별 거절 배치와 실패한 dbt 노드. 없으면 None(대조군과 같음).
    생활이동(Spark 경로)은 1단계 범위 밖이라 보지 않는다(design_agent.md 2.2). 경고(warn)는 신호가 아니다(3절)."""
    rejected = {}
    for src in SOURCES:
        rows = conn.execute(f"SELECT batch_key, failed_checks FROM stg_dbt_sbx_{label}.batch_status "
                            "WHERE source = %s AND NOT approved ORDER BY batch_key", [src]).fetchall()
        if rows:
            checks = sorted({c for _, cs in rows for c in cs})
            rejected[src] = {"count": len(rows), "batches": [r[0] for r in rows], "checks": checks}
    failed = [f for f in built["failed"] if f["status"] != "skipped"]
    if not rejected and not failed:
        return None
    sources = set(rejected) | {s for s in SOURCES for f in failed if f"{s}_" in f["node"] or f".{s}" in f["node"]}
    return {"sources": sorted(sources), "rejected": rejected, "failed_nodes": failed}


# 빌드할 때마다 now()가 들어가는 컬럼. 비교에서 빼지 않으면 같은 코드도 모든 행이 다르다(S1 첫 판정에서 G2 거짓 실패)
VOLATILE = {"checked_at", "quarantined_at", "loaded_at"}


def except_all(conn, a, b, cols):
    """두 관계의 cols(text로)를 두 방향 EXCEPT ALL. 차이 행 수."""
    pick = "ROW(" + ", ".join(f"t.{c}" for c in cols if c not in VOLATILE) + ")::text"
    return conn.execute(f"SELECT count(*) FROM ((SELECT {pick} FROM {a} t EXCEPT ALL SELECT {pick} FROM {b} t) "
                        f"UNION ALL (SELECT {pick} FROM {b} t EXCEPT ALL SELECT {pick} FROM {a} t)) d").fetchone()[0]


def judge(conn, base, cand, signal, cand_built, contract):
    """G1~G5 (design_agent.md 5절, 11.3). 결과: pass | needs_decision | fail."""
    srcs = signal["sources"]
    g = {}
    # G1 실패가 풀렸나: 후보 빌드에 실패 노드 0, 신호 출처의 배치가 모두 승인
    if cand_built["failed"]:  # 실패 노드가 있으면 하류 표가 skip돼 없을 수 있다(S3, F-34). G1만으로 fail이다
        return "fail", {"G1": {"ok": False, "failed_nodes": cand_built["failed"][:20], "rejected_batches": None}}
    left = conn.execute(f"SELECT count(*) FROM stg_dbt_sbx_{cand}.batch_status WHERE source = ANY(%s) AND NOT approved",
                        [srcs]).fetchone()[0]
    g["G1"] = {"ok": left == 0, "failed_nodes": [], "rejected_batches": left}
    # G2 다른 곳은 그대로인가: 신호 출처의 지도 하류가 아닌 표는 기준선과 같아야 한다
    down = {r[0] for r in conn.execute("SELECT DISTINCT to_table FROM lineage.downstream WHERE from_table = ANY(%s)",
                                       [[SOURCES[s][0] for s in srcs]])}
    differ = []
    for layer in ("stg_dbt", "wh", "mart"):
        for (t,) in conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s "
                                 "AND table_type = 'BASE TABLE' ORDER BY 1", [f"{layer}_sbx_{base}"]).fetchall():
            if f"{layer}.{t}" in down:
                continue
            if not conn.execute("SELECT to_regclass(%s)", [f"{layer}_sbx_{cand}.{t}"]).fetchone()[0]:
                differ.append([f"{layer}.{t}", "후보에 없음"])
            elif n := except_all(conn, f"{layer}_sbx_{base}.{t}", f"{layer}_sbx_{cand}.{t}",
                                 columns(conn, f"{layer}_sbx_{base}", t)):
                differ.append([f"{layer}.{t}", n])
    g["G2"] = {"ok": not differ, "differ": differ}
    # G3 상품 변화가 지도 예측 안인가. ponytail: 지도는 운영(main) 회차를 쓴다. 후보 브랜치로 다시 뽑는 것은 리니지 추출이 무거워 뺐다
    d = diff(conn, f"mart_sbx_{base}", f"mart_sbx_{cand}")
    observed = {(t, c) for t, v in d.items() for c in v["changed"]}
    pred = predicted(conn, [(SOURCES[s][0], None) for s in srcs])
    g["G3"] = {"ok": observed <= pred, "changed": sorted(observed), "outside_map": sorted(observed - pred),
               "rows": {t: v["rows"] for t, v in d.items() if v["rows"][0] != v["rows"][1]}}
    # G4 계약 그대로인가
    from warehouse.export_product import actual_schema  # pyarrow까지 따라오므로 쓸 때만
    g["G4"] = {"ok": actual_schema(conn, f"mart_sbx_{cand}") == contract}
    # G5 해석이 바뀌었나: 기준선에서도 승인돼 있던 배치의 정제 행이 같아야 한다
    changed = {}
    for s in srcs:
        _, clean, _, key = SOURCES[s]
        approved = f"(SELECT batch_key FROM stg_dbt_sbx_{base}.batch_status WHERE source = '{s}' AND approved)"
        n = except_all(conn, f"(SELECT * FROM stg_dbt_sbx_{base}.{clean} WHERE {key} IN {approved})",
                       f"(SELECT * FROM stg_dbt_sbx_{cand}.{clean} WHERE {key} IN {approved})",
                       columns(conn, f"stg_dbt_sbx_{base}", clean))
        if n:
            changed[s] = n
    g["G5"] = {"ok": not changed, "changed_rows": changed}
    if not all(g[k]["ok"] for k in ("G1", "G2", "G3", "G4")):
        verdict = "fail"
    else:
        verdict = "pass" if g["G5"]["ok"] else "needs_decision"
    return verdict, g
