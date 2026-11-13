"""3단계: 상품 층 간선 (결정 55). 내보내기는 SELECT * FROM mart.{name}이라 상품 파일 컬럼 = 마트 컬럼(1:1)이다.

warehouse/export_product.py의 TABLES 키(상품 파일 이름)를 ast로 읽는다. import하면 DB 접속 모듈까지 따라오기 때문이다.
"""
import ast

from common import ROOT
from lineage.schema import CATALOG

EXPORT = ROOT / "warehouse" / "export_product.py"


def product_tables():
    tree = ast.parse(EXPORT.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "TABLES" for t in node.targets):
            return list(ast.literal_eval(node.value))
    raise SystemExit(f"{EXPORT}에서 TABLES를 찾지 못했습니다. 내보내기 코드가 바뀌었으면 lineage/product.py도 맞춰야 합니다")


def product_edges(schema, mart_db="mart_build"):
    """상품 파일마다 product.<파일>.<컬럼> <- seoul.mart_build.<표>.<컬럼> IDENTITY."""
    edges = []
    for name in product_tables():
        columns = schema[CATALOG][mart_db].get(name)
        if columns is None:
            raise SystemExit(f"상품 파일 {name}의 마트 표가 리니지 스키마에 없습니다")
        edges += [{"target_table": f"product.{name}", "target_column": c, "source_table": f"{CATALOG}.{mart_db}.{name}",
                   "source_column": c, "kind": "DIRECT", "subkind": "IDENTITY", "status": "ok", "detail": None}
                  for c in columns]
    return edges
