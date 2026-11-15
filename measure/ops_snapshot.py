"""운영 스냅숏: 에이전트가 운영을 바꾸지 않는지 보는 전후 비교 (결정 112, guarantees.md 2.8, 측정 45의 방법을 파일로).

python measure/ops_snapshot.py --save logs/agent/ops_before.json     실행 전
python measure/ops_snapshot.py --compare logs/agent/ops_before.json  실행 후: 다르면 종료 코드 1
담는 것: raw, stg, stg_dbt, wh, mart 스키마의 모든 표 행 수(정확한 count), 최신 상품 버전 데이터 파일 sha256, 남은 *_sbx_* 스키마 수.
ops 스키마는 뺀다(에이전트가 ops.agent_run, ops.agent_signal에 쓰는 것은 의도한 기록이다).
"""
import argparse
import hashlib
import json
import sys
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import ROOT, read_env  # noqa: E402

SCHEMAS = ("raw", "stg", "stg_dbt", "wh", "mart")


def snapshot():
    with psycopg.connect(read_env("DATABASE_URL")) as conn:
        tables = conn.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = ANY(%s) "
                              "AND table_type = 'BASE TABLE' ORDER BY 1, 2", [list(SCHEMAS)]).fetchall()
        rows = {f"{s}.{t}": conn.execute(f'SELECT count(*) FROM "{s}"."{t}"').fetchone()[0] for s, t in tables}
        sbx = conn.execute(r"SELECT count(*) FROM pg_namespace WHERE nspname LIKE '%\_sbx\_%'").fetchone()[0]
    prod = sorted(Path(ROOT, "product").glob("v*"))[-1]
    files = {f.name: hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted(prod.iterdir()) if f.suffix in (".csv", ".parquet")}
    return {"rows": rows, "product": {"version": prod.name, "files": files}, "sbx_schemas": sbx}


def main():
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--save")
    g.add_argument("--compare")
    a = p.parse_args()
    now = snapshot()
    if a.save:
        Path(a.save).write_text(json.dumps(now, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"표 {len(now['rows'])}개, 상품 {now['product']['version']} 파일 {len(now['product']['files'])}개, sbx {now['sbx_schemas']}")
        return
    before = json.loads(Path(a.compare).read_text(encoding="utf-8"))
    diff = {k: (before["rows"].get(k), now["rows"].get(k)) for k in set(before["rows"]) | set(now["rows"])
            if before["rows"].get(k) != now["rows"].get(k)}
    same_prod = before["product"]["files"] == now["product"]["files"]
    print(f"표 {len(now['rows'])}개 중 행 수 다름 {len(diff)}: {diff}")
    print(f"상품 데이터 파일 같음: {same_prod} ({before['product']['version']} -> {now['product']['version']}), 남은 sbx 스키마 {now['sbx_schemas']}")
    raise SystemExit(0 if not diff and same_prod and now["sbx_schemas"] == 0 else 1)


if __name__ == "__main__":
    main()
