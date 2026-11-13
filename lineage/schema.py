"""1단계: 스키마 추론 (결정 53). DB에 묻지 않고 레포 안의 것으로 표마다 컬럼 목록을 만든다.

- 소스: warehouse/sql의 stg DDL(01~06)과 원본 표 DDL(09·10_raw_*)의 CREATE TABLE
- seed: warehouse/dbt/seeds/*.csv 첫 줄
- 모델: manifest의 depends_on 순서대로 컴파일된 SQL을 qualify해 얻은 출력 컬럼
앞 모델의 출력이 뒤 모델의 입력이라 순서대로만 돈다(결정 56). 키는 컴파일된 SQL 그대로(seoul.wh_build.dim_dong).
"""
import csv
import json
from graphlib import TopologicalSorter

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.qualify import qualify

from common import ROOT

DBT = ROOT / "warehouse" / "dbt"
CATALOG = "seoul"  # profiles.yml의 dbname
STG_DDL = sorted([*(ROOT / "warehouse" / "sql").glob("0[1-6]_stg_*.sql"), *(ROOT / "warehouse" / "sql").glob("[0-9][0-9]_raw_*.sql")])


def stg_tables():
    """{(db, 표): {컬럼: 타입}}"""
    tables = {}
    for path in STG_DDL:
        for statement in sqlglot.parse(path.read_text(encoding="utf-8"), dialect="postgres"):
            if isinstance(statement, exp.Create) and statement.kind == "TABLE":
                table = statement.this.this
                tables[(table.db, table.name)] = {c.name: c.args["kind"].sql("postgres")
                                                  for c in statement.this.expressions if isinstance(c, exp.ColumnDef)}
    return tables


def load_manifest():
    return json.loads((DBT / "target" / "manifest.json").read_text(encoding="utf-8"))


def models_in_order(manifest):
    """[(unique_id, node)] 모델만, 의존 순서대로."""
    nodes = {k: n for k, n in manifest["nodes"].items() if n["resource_type"] == "model"}
    order = TopologicalSorter({k: [d for d in n["depends_on"]["nodes"] if d in nodes] for k, n in nodes.items()})
    return [(k, nodes[k]) for k in order.static_order()]


def compiled_path(node):
    return DBT / "target" / "compiled" / node["package_name"] / node["original_file_path"]


def table_name(node):
    return f"{CATALOG}.{node['schema']}.{node['alias']}"


def build(manifest):
    """sqlglot이 받는 중첩 스키마 {catalog: {db: {표: {컬럼: 타입}}}}와 모델 목록 [(표 원문 이름, sql 파일, sql)]"""
    schema = {CATALOG: {}}

    def put(db, name, columns):
        schema[CATALOG].setdefault(db, {})[name] = columns

    for (db, name), columns in stg_tables().items():
        put(db, name, columns)
    for node in manifest["nodes"].values():
        if node["resource_type"] == "seed":
            with open(DBT / node["original_file_path"], encoding="utf-8", newline="") as f:
                put(node["schema"], node["alias"], {c: "text" for c in next(csv.reader(f))})
    models = []
    for _, node in models_in_order(manifest):
        sql = compiled_path(node).read_text(encoding="utf-8")
        tree = qualify(sqlglot.parse_one(sql, dialect="postgres"), schema=schema, dialect="postgres",
                       validate_qualify_columns=False)
        put(node["schema"], node["alias"], {c: "text" for c in tree.named_selects})  # ponytail: 타입은 리니지에 쓰지 않아 text로 둔다
        models.append((table_name(node), node["original_file_path"], sql))
    return schema, models
