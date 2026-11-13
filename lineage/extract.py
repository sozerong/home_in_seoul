"""컬럼 리니지 추출 진입점 (P4, 결정 44, 47, 56). 1~4단계를 묶는다.

1 드라이버: 스키마 추론(순서대로만 됨)        lineage/schema.py
2 엔진:     SQL 파일마다 extract_edges        lineage/edges.py  ← 엔진(python/spark)은 여기서만 고른다
3 드라이버: 상품 층 간선                      lineage/product.py
           Spark SQL 묶음 간선                lineage/spark_sql.py
4 저장:     lineage.run 한 행 + lineage.edge_raw (python은 psycopg COPY, spark는 JDBC)
입력은 warehouse/dbt/target(manifest, compiled). dbt build/compile이 먼저 돌아 있어야 한다(compile은 DB가 필요하다).
사용법: python -m lineage.extract --engine python|spark --trigger manual|asset
        spark는 Airflow 컨테이너 spark-venv에서: /opt/airflow/spark-venv/bin/python -m lineage.extract --engine spark ...
"""
import argparse
import itertools
import time
import urllib.parse

import psycopg
import sqlglot

from common import ROOT, read_env
from lineage import schema as S
from lineage.edges import extract_edges
from lineage.product import product_edges
from lineage.spark_sql import spark_sql_edges
from warehouse.build_record import git_state, sql_hashes

COLUMNS = ["run_id", "sql_file", "file_kind", "target_table", "target_column", "source_table", "source_column",
           "kind", "subkind", "status", "detail"]


def model_edges(models, schema):
    """[(표 이름, sql 파일, sql)] -> 간선 dict 목록. 파일 하나가 일 하나다."""
    return [{**edge, "sql_file": path, "file_kind": "model"} for name, path, sql in models
            for edge in extract_edges(sql, name, schema)]


def run_python(models, schema):
    return list(itertools.chain.from_iterable(map(lambda m: model_edges([m], schema), models)))


def run_spark(models, schema):
    from transform.iceberg import spark_session  # Iceberg 설정이 함께 오지만 이 작업은 쓰지 않는다. PostgreSQL JDBC jar가 필요해서 같은 세션을 쓴다
    spark = spark_session("lineage")
    shared = spark.sparkContext.broadcast(schema)
    rdd = spark.sparkContext.parallelize(models, len(models)).flatMap(lambda m: model_edges([m], shared.value))
    return spark, rdd


def jdbc(url):
    u = urllib.parse.urlparse(url)
    return f"jdbc:postgresql://{u.hostname}:{u.port or 5432}{u.path}", {"user": u.username, "password": u.password,
                                                                       "driver": "org.postgresql.Driver"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=["python", "spark"], required=True)
    parser.add_argument("--trigger", choices=["manual", "asset"], required=True)
    args = parser.parse_args()

    started = time.perf_counter()
    manifest = S.load_manifest()
    schema, models = S.build(manifest)
    url = read_env("DATABASE_URL")
    commit, clean = git_state()
    with psycopg.connect(url, autocommit=True) as conn:
        run_id = conn.execute(
            "INSERT INTO lineage.run (status, trigger, git_commit, sqlglot_version, engine, manifest_generated_at, sql_files,"
            " sql_hash, clean) VALUES ('running', %s, %s, %s, %s, %s, %s, %s, %s) RETURNING run_id",
            [args.trigger, commit, sqlglot.__version__, args.engine, manifest["metadata"]["generated_at"],
             len(models), sql_hashes(manifest)[1], clean]).fetchone()[0]
        try:
            product = [{**e, "sql_file": "export_product.py", "file_kind": "product"} for e in product_edges(schema)]
            product += spark_sql_edges(schema)  # Spark SQL 묶음(결정 91). 묶음 하나가 일 하나라 드라이버에서 만든다
            if args.engine == "python":
                edges = run_python(models, schema) + product
                with conn.cursor().copy(f"COPY lineage.edge_raw ({', '.join(COLUMNS)}) FROM STDIN") as copy:
                    for e in edges:
                        copy.write_row([run_id] + [e[c] for c in COLUMNS[1:]])
                count = len(edges)
            else:
                spark, rdd = run_spark(models, schema)
                rows = rdd.collect() + product  # ponytail: 간선 수천 개라 드라이버로 모아 상품 층을 더한다. 수백만이 되면 union으로
                df = spark.createDataFrame([[run_id] + [e[c] for c in COLUMNS[1:]] for e in rows],
                                           "run_id long, " + ", ".join(f"{c} string" for c in COLUMNS[1:]))
                target, props = jdbc(url)
                df.write.jdbc(target, "lineage.edge_raw", mode="append", properties=props)
                count = len(rows)
                spark.stop()
            bad = conn.execute("SELECT count(*) FROM lineage.edge_raw WHERE run_id = %s AND status <> 'ok'",
                               [run_id]).fetchone()[0]
            conn.execute("UPDATE lineage.run SET status = 'success', finished_at = now(), edges = %s WHERE run_id = %s",
                         [count, run_id])
        except BaseException:
            conn.execute("UPDATE lineage.run SET status = 'failed', finished_at = now() WHERE run_id = %s", [run_id])
            raise
    print(f"lineage 회차 {run_id}: 엔진 {args.engine}, SQL 파일 {len(models)}개, 간선 {count}개"
          f"(ok 아님 {bad}), {time.perf_counter() - started:.2f}초")
    if bad:  # 회차는 남기되 사람이 보게 실패로 끝낸다 (CLAUDE.md 4절 4번)
        raise SystemExit(f"파싱 실패 또는 풀지 못한 간선 {bad}개. lineage.edge_raw run_id={run_id}의 detail을 보세요")


if __name__ == "__main__":
    main()
