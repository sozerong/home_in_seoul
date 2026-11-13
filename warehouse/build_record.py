"""wh_load 빌드 기록 (P6 규칙 버전, 결정 81). dbt_build 뒤, swap_schemas 직전에 돈다.

ops.build_run 한 행: git 커밋, 작업 트리 깨끗 여부, 모델·seed별 해시와 그 묶음 해시(sql_hash), 빌드 표 행 수, 빌드 시간.
ops.build_diff: 정제 표를 처리 단위별로 새 빌드(*_build)와 지금 스키마를 두 방향 EXCEPT ALL로 비교해 바뀐 단위만.
해시는 lineage.extract도 같은 함수로 남긴다. MCP는 둘을 비교해 stale을 판정한다.
실행: python -m warehouse.build_record   (run_results.json은 dbt 명령마다 덮어써지므로 dbt build 바로 뒤에 돌아야 한다)
"""
import hashlib
import json
import subprocess
import time

import psycopg
from psycopg.types.json import Jsonb

from common import ROOT, read_env

DBT = ROOT / "warehouse" / "dbt"
UNITS = {  # 정제 표: 처리 단위 키
    "stg_dbt.rent": ["source_type", "collected_date", "sgg_cd", "deal_ymd"],
    "stg_dbt.congestion": ["collected_date"],
    "stg_dbt.store": ["collected_date", "source_file"],
}  # ponytail: stg.living_commute(_new와 지금 표)는 3절 실행기가 생기면 더한다


def git_state():
    """(커밋, 작업 트리 깨끗 여부). 컨테이너의 바인드 마운트는 소유자가 달라 이 호출에서만 safe.directory를 연다."""
    def git(*args):
        out = subprocess.run(["git", "-c", f"safe.directory={ROOT.as_posix()}", "-C", str(ROOT), *args],
                             capture_output=True, text=True)
        return out.stdout if out.returncode == 0 else None
    commit, status = git("rev-parse", "HEAD"), git("status", "--porcelain")
    return (commit.strip() if commit else None), (None if status is None else status.strip() == "")


def sql_hashes(manifest):
    """({모델·seed 이름: sha256}, 묶음 sha256). 모델은 target/compiled의 컴파일 SQL, seed는 csv 파일 내용."""
    hashes = {}
    for node in manifest["nodes"].values():
        if node["resource_type"] == "model":
            path = DBT / "target" / "compiled" / node["package_name"] / node["original_file_path"]
        elif node["resource_type"] == "seed":
            path = DBT / node["original_file_path"]
        else:
            continue
        hashes[node["name"]] = hashlib.sha256(path.read_bytes()).hexdigest()
    joined = "\n".join(f"{k}:{v}" for k, v in sorted(hashes.items()))
    return hashes, hashlib.sha256(joined.encode()).hexdigest()


def diff(conn, table):
    """[(단위, 더한 행, 뺀 행)]. 지금 표가 없으면(첫 빌드) 빈 목록."""
    schema, name = table.split(".")
    if conn.execute("SELECT to_regclass(%s)", [table]).fetchone()[0] is None:
        return []
    cols = [r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s "
        "AND column_name IN (SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s) "
        "ORDER BY ordinal_position", [schema, name, f"{schema}_build", name])]
    pick, unit = ", ".join(cols), f"concat_ws('/', {', '.join(UNITS[table])})"
    new, old = f"{schema}_build.{name}", table
    return conn.execute(f"""
        SELECT unit, sum(a), sum(r) FROM (
            SELECT {unit} unit, 1 a, 0 r FROM (SELECT {pick} FROM {new} EXCEPT ALL SELECT {pick} FROM {old}) x
            UNION ALL
            SELECT {unit}, 0, 1 FROM (SELECT {pick} FROM {old} EXCEPT ALL SELECT {pick} FROM {new}) y
        ) d GROUP BY unit ORDER BY unit""").fetchall()


def main():
    manifest = json.loads((DBT / "target" / "manifest.json").read_text(encoding="utf-8"))
    run_results = json.loads((DBT / "target" / "run_results.json").read_text(encoding="utf-8"))
    hashes, sql_hash = sql_hashes(manifest)
    commit, clean = git_state()
    tables = sorted(f"{n['schema']}.{n['alias']}" for n in manifest["nodes"].values()
                    if n["resource_type"] in ("model", "seed") and n["config"]["materialized"] != "view")
    with psycopg.connect(read_env("DATABASE_URL")) as conn:  # 한 트랜잭션: 기록과 차이가 같이 들어가거나 같이 빠진다
        counts = {t: conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in tables}
        started = time.perf_counter()
        diffs = {t: diff(conn, t) for t in UNITS}
        diff_seconds = time.perf_counter() - started
        warnings = [{"node": r["unique_id"], "message": r["message"]} for r in run_results["results"] if r["status"] == "warn"]
        build_id = conn.execute(
            "INSERT INTO ops.build_run (git_commit, clean, sql_hash, model_hashes, row_counts, build_seconds, diff_seconds, "
            "warnings) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING build_id",
            [commit, clean, sql_hash, Jsonb(hashes), Jsonb(counts), round(run_results["elapsed_time"], 1),
             round(diff_seconds, 1), Jsonb(warnings)]).fetchone()[0]
        for t, rows in diffs.items():
            for unit, added, removed in rows:
                conn.execute("INSERT INTO ops.build_diff VALUES (%s, %s, %s, %s, %s)", [build_id, t, unit, added, removed])
    changed = {t: len(rows) for t, rows in diffs.items()}
    print(f"빌드 기록 {build_id}: 커밋 {commit and commit[:7]}, 깨끗 {clean}, sql_hash {sql_hash[:12]}, "
          f"모델·seed {len(hashes)}개, 표 {len(counts)}개, 바뀐 단위 {changed}, 비교 {diff_seconds:.1f}초")


if __name__ == "__main__":
    main()
