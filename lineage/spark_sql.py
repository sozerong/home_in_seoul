"""Spark SQL 파일 묶음의 간선 (결정 91, design_remaining_work.md 3.7).

실행기(transform/spark_sql_runner.py)처럼 파일 하나 = 임시 뷰 하나다. 임시 뷰는 물리 표가 아니므로 CTE로 보고, 묶음 전체를
한 쿼리(WITH codes, checked, clean, ..., 마지막 파일)로 이어 extract_edges(spark 방언)에 넣는다. 그래서 간선은 물리 표
(Iceberg 원본, 코드표)에서 마지막 파일의 대상 표로 바로 간다. JDBC 복사는 같은 이름 열을 그대로 옮기므로 따로 간선을 만들지 않는다.
"""
import sqlglot
from sqlglot import exp

from common import ROOT
from lineage.edges import extract_edges

FOLDERS = {"spark_sql/living": "seoul.stg.living_commute"}  # 묶음 폴더: 마지막 파일의 대상 표
CODES = "lake.seoul_living_migration.code_table"  # 실행기가 만드는 임시 뷰 codes의 원천(spark_sql/iceberg_tables.sql)


def combined_sql(folder):
    files = sorted(p for p in (ROOT / folder).glob("*.sql") if not p.stem.startswith(("test_", "warn_")))
    views = [(p.stem.split("_", 1)[1], sqlglot.parse_one(p.read_text(encoding="utf-8"), dialect="spark")) for p in files]
    final = views[-1][1]
    ctes = [exp.CTE(this=sqlglot.parse_one(f"SELECT code FROM {CODES}", dialect="spark"), alias=exp.TableAlias(this=exp.to_identifier("codes")))]
    ctes += [exp.CTE(this=tree, alias=exp.TableAlias(this=exp.to_identifier(name))) for name, tree in views[:-1]]
    key = "with_" if "with_" in final.arg_types else "with"
    own = final.args.get(key)
    final.set(key, exp.With(expressions=ctes + (own.expressions if own else [])))  # 마지막 파일의 CTE(latest)는 뒤에 둔다
    return final.sql(dialect="spark")


def spark_sql_edges(schema):
    edges = []
    for folder, target in FOLDERS.items():
        edges += [{**e, "sql_file": folder, "file_kind": "spark_sql"}
                  for e in extract_edges(combined_sql(folder), target, schema, dialect="spark")]
    return edges
