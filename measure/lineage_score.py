"""컬럼 리니지 정답 대조 (P4-6, 결정 49, 66). DB 없이 돈다.

정답(tests/lineage_truth/<모델>.csv)과 추출기(lineage.edges.extract_edges) 결과를 (출력 컬럼, 출처 표, 출처 컬럼, kind)로 맞춘다.
subkind는 맞은 키 안에서 정답 subkind 집합과 추출 subkind 집합이 같은지로 따로 센다(README 원칙 3).
입력은 warehouse/dbt/target(manifest, compiled)이다. dbt compile 또는 build가 먼저 돌아 있어야 한다.
사용법: python -m measure.lineage_score [--detail]
"""
import collections
import csv
import sys

from common import ROOT
from lineage import schema as S
from lineage.edges import extract_edges
from lineage.spark_sql import FOLDERS, combined_sql

TRUTH = ROOT / "tests" / "lineage_truth"


def normalize(table):
    """seoul.wh_build.dim_dong -> wh.dim_dong (설계서 1.7의 lineage.edge와 같은 정규화)"""
    if not table.startswith("seoul."):  # Iceberg(lh.*), 레이크 코드표(lake.*)는 그대로 (lineage.norm과 같음)
        return table
    db, name = table.split(".")[-2:]
    return f"{db.removesuffix('_build')}.{name}"


def keyed(rows):
    out = collections.defaultdict(set)  # (출력, 출처 표, 출처 컬럼, kind) -> {subkind}
    for r in rows:
        out[(r["target_column"], r["source_table"], r["source_column"], r["kind"])].add(r["subkind"])
    return out


def main():
    detail = "--detail" in sys.argv
    schema, models = S.build(S.load_manifest())
    by_table = {normalize(name): (name, sql, "postgres") for name, _, sql in models}
    for folder, name in FOLDERS.items():  # Spark SQL 묶음은 묶음 전체가 모델 하나다(lineage/spark_sql.py)
        by_table[normalize(name)] = (name, combined_sql(folder), "spark")
    totals = collections.Counter()
    for path in sorted(TRUTH.glob("*.csv")):
        table = (path.stem.replace("stg_", "stg.", 1) if path.stem.startswith("stg_")
                 else ("wh." if path.stem.startswith("map_") else "mart.") + path.stem)
        name, sql, dialect = by_table[table]
        got_rows = [{**r, "source_table": normalize(r["source_table"])} for r in extract_edges(sql, name, schema, dialect)
                    if r["status"] == "ok"]
        truth, got = keyed(csv.DictReader(open(path, encoding="utf-8"))), keyed(got_rows)
        print(f"## {table}")
        for kind in ("DIRECT", "INDIRECT", "ROWSET"):
            t = {k for k in truth if k[3] == kind}
            g = {k for k in got if k[3] == kind}
            hit, miss, wrong = t & g, t - g, g - t
            same_sub = sum(truth[k] == got[k] for k in hit)
            totals.update({f"{kind} 맞음": len(hit), f"{kind} 놓침": len(miss), f"{kind} 틀림": len(wrong),
                           f"{kind} subkind 일치": same_sub})
            print(f"  {kind}: 정답 {len(t)}, 맞음 {len(hit)}, 놓침 {len(miss)}, 틀림 {len(wrong)}, 맞은 것 중 subkind 일치 {same_sub}")
            if detail:
                for k in sorted(miss):
                    print(f"    놓침 {k[0]} <- {k[1]}.{k[2]} {sorted(truth[k])}")
                for k in sorted(wrong):
                    print(f"    틀림 {k[0]} <- {k[1]}.{k[2]} {sorted(got[k])}")
                for k in sorted(hit):
                    if truth[k] != got[k]:
                        print(f"    subkind {k[0]} <- {k[1]}.{k[2]} 정답 {sorted(truth[k])} 추출 {sorted(got[k])}")
    print("## 합계")
    for kind in ("DIRECT", "INDIRECT", "ROWSET"):  # ROWSET: 결정 80
        hit, miss, wrong = (totals[f"{kind} {x}"] for x in ("맞음", "놓침", "틀림"))
        print(f"  {kind}: 맞음 {hit}, 놓침 {miss}, 틀림 {wrong}, 재현율 {hit / (hit + miss):.1%}, "
              f"정밀도 {hit / (hit + wrong):.1%}, subkind 일치 {totals[f'{kind} subkind 일치']}/{hit}")


if __name__ == "__main__":
    main()
