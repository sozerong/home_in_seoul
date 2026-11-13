"""컬럼 리니지 MCP 서버 (P4-5, 결정 51, 58). stdio로 돈다. Claude Code가 레포 루트 .mcp.json으로 띄운다.

lineage 스키마의 뷰를 읽기만 하는 도구 5개. 자유 SQL 도구는 없다. DB에는 lineage_reader 역할(lineage 스키마 SELECT만)로 붙는다.
응답마다 맨 위에 지도 회차(run_id, 끝난 시각, git 커밋, 깨끗 여부)와 마지막 빌드를 붙여, 지도가 지금 표와 어긋나는지(stale) 판단하게 한다(결정 81).

처음 한 번(관리자 계정으로 로그인 비밀번호 넣기): python -m lineage_mcp.server --setup-role
    .env의 LINEAGE_READER_PASSWORD를 쓴다. 역할 자체와 권한은 warehouse/sql/08_lineage.sql이 만든다.
"""
import sys
import urllib.parse

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from common import read_env

ROLE = "lineage_reader"
LIMIT = 500  # 응답 한 번의 최대 행. 넘으면 truncated로 알린다


def reader_url():
    u = urllib.parse.urlparse(read_env("DATABASE_URL"))
    password = urllib.parse.quote(read_env("LINEAGE_READER_PASSWORD"), safe="")
    return u._replace(netloc=f"{ROLE}:{password}@{u.hostname}:{u.port or 5432}").geturl()


def setup_role():
    with psycopg.connect(read_env("DATABASE_URL"), autocommit=True) as conn:
        conn.execute(sql.SQL("ALTER ROLE {} LOGIN PASSWORD {}").format(
            sql.Identifier(ROLE), sql.Literal(read_env("LINEAGE_READER_PASSWORD"))))
    print(f"{ROLE} 로그인 설정 완료")


def query(text, params=()):
    """읽기 전용 트랜잭션에서 뷰 하나를 읽고 회차 정보를 붙인다."""
    with psycopg.connect(reader_url(), row_factory=dict_row) as conn:
        conn.read_only = True
        run = conn.execute("SELECT run_id, finished_at::text, git_commit, engine, trigger, clean, sql_hash FROM lineage.run "
                           "WHERE status = 'success' ORDER BY run_id DESC LIMIT 1").fetchone()
        built = conn.execute("SELECT build_id, built_at::text, sql_hash FROM lineage.latest_build").fetchone()
        rows = conn.execute(text, params).fetchall()
    if run:  # 결정 81: 커밋되지 않은 코드로 만든 지도이거나, 마지막 빌드와 컴파일 SQL 해시가 다르면 stale
        run["stale"] = (None if run["clean"] is None or not (built and run["sql_hash"])
                        else not run["clean"] or run["sql_hash"] != built["sql_hash"])
    return {"run": run, "latest_build": built, "count": len(rows), "truncated": len(rows) > LIMIT, "rows": rows[:LIMIT]}


def build():
    from mcp.server.mcpserver import MCPServer  # MCP SDK 2.x (1.x의 FastMCP)

    server = MCPServer("lineage", instructions=(
        "서울 동네 찾기 파이프라인의 컬럼 리니지 지도. 표 이름은 stg.x, wh.x, mart.x, product.x 형식이다. "
        "dbt 모델이나 정제 코드를 고치기 전에 downstream으로 영향받는 컬럼(특히 is_product)을 먼저 본다. "
        "value_path는 값이 그대로 흐르는 경로, row_path는 필터·조인 등 행 관계로 영향을 주는 컬럼 단위 경로, "
        "rowset_path는 표의 행 집합(*)을 거쳐 표 단위로 넓게 잡힌 경로다(결정 80). rowset_path만 true면 행 수가 바뀔 수 있다는 뜻이다. "
        "run.stale이 true면 지도가 커밋되지 않은 코드로 만들어졌거나 마지막 빌드와 컴파일 SQL이 달라, 지금 표와 어긋날 수 있다. "
        "지도는 SQL에 적힌 의존만 본다. Python 정제 안의 범위나 행 집합 변화는 보이지 않는다(측정 20)."))

    @server.tool(description="컬럼 하나가 바뀌면 영향받는 하류 컬럼. include_indirect=False면 값 경로만. "
                             "컬럼 단위 영향(value_path, row_path)과 행 집합 경로 영향(rowset_path)을 나눠 보인다.")
    def downstream(table: str, column: str, include_indirect: bool = True) -> dict:
        return query("SELECT to_table, to_column, depth, value_path, row_path, rowset_path, is_product FROM lineage.downstream "
                     "WHERE from_table = %s AND from_column = %s AND (%s OR value_path) "
                     "ORDER BY is_product DESC, to_table, to_column", (table, column, include_indirect))

    @server.tool(description="컬럼 하나의 상류 출처 컬럼(스테이징까지).")
    def upstream(table: str, column: str) -> dict:
        return query("SELECT source_table, source_column, depth, value_path, row_path, rowset_path FROM lineage.upstream "
                     "WHERE table_name = %s AND column_name = %s ORDER BY depth, source_table, source_column",
                     (table, column))

    @server.tool(description="표 단위 간선. table을 주면 그 표가 출발이나 도착인 것만.")
    def table_edges(table: str | None = None) -> dict:
        return query("SELECT source_table, target_table, kinds, column_edges FROM lineage.table_edge "
                     "WHERE %s::text IS NULL OR %s IN (source_table, target_table) ORDER BY 1, 2", (table, table))

    @server.tool(description="최신 성공 회차와 그 직전 성공 회차의 간선 차이(added, removed).")
    def run_diff() -> dict:
        return query("SELECT * FROM lineage.run_diff ORDER BY change, target_table, target_column, source_table")

    @server.tool(description="이름 일부로 지도에 있는 (표, 컬럼)을 찾는다. 정확한 이름을 모를 때.")
    def find_columns(name_part: str) -> dict:
        return query("SELECT DISTINCT t, c FROM (SELECT source_table, source_column FROM lineage.edge "
                     "UNION SELECT target_table, target_column FROM lineage.edge) e(t, c) "
                     "WHERE t || '.' || c ILIKE %s ORDER BY t, c", (f"%{name_part}%",))

    return server


if __name__ == "__main__":
    if "--setup-role" in sys.argv:
        setup_role()
    else:
        build().run("stdio")
