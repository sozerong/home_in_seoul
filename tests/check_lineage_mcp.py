"""lineage MCP 서버를 stdio로 띄워 도구 5개를 부르고, downstream 결과가 뷰를 직접 읽은 결과와 같은지 본다(결정 51의 검증).

DB와 lineage 회차가 있어야 한다(CI 밖). 실행: python -m tests.check_lineage_mcp
"""
import asyncio
import json
import sys

import psycopg
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

from common import ROOT, read_env

QUESTION = {"table": "stg_dbt.congestion", "column": "dow"}  # S4-1 (측정 15, 20). 혼잡도 이전 뒤 이름 (측정 23)


def payload(result):
    if result.structured_content:
        data = result.structured_content
        return data.get("result", data)
    return json.loads(result.content[0].text)


async def main():
    params = StdioServerParameters(command=sys.executable, args=["-m", "lineage_mcp.server"], cwd=str(ROOT))
    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        tools = sorted(t.name for t in (await session.list_tools()).tools)
        assert tools == ["downstream", "find_columns", "run_diff", "table_edges", "upstream"], tools
        down = payload(await session.call_tool("downstream", QUESTION))
        up = payload(await session.call_tool("upstream", {"table": "product.dong_profile",
                                                          "column": "am_peak_congestion"}))
        found = payload(await session.call_tool("find_columns", {"name_part": "congestion.dow"}))
        diff = payload(await session.call_tool("run_diff", {}))
        tables = payload(await session.call_tool("table_edges", {"table": "mart.dong_profile"}))
    with psycopg.connect(read_env("DATABASE_URL")) as conn:
        direct = conn.execute("SELECT to_table, to_column FROM lineage.downstream WHERE from_table = %s AND "
                              "from_column = %s", (QUESTION["table"], QUESTION["column"])).fetchall()
    got = {(r["to_table"], r["to_column"]) for r in down["rows"]}
    assert got == set(direct), (got ^ set(direct))
    assert ("stg_dbt.congestion", "dow") in {(r["source_table"], r["source_column"]) for r in up["rows"]}
    assert {"t": "stg_dbt.congestion", "c": "dow"} in found["rows"]
    print("회차:", down["run"], "HEAD:", down["repo_head"])
    print(f"downstream {QUESTION}: {down['count']}행, 뷰 직접 조회와 같음. 상품:",
          [(r["to_table"], r["to_column"], r["depth"], r["value_path"], r["row_path"]) for r in down["rows"]
           if r["is_product"]])
    print(f"upstream {up['count']}행, find_columns {found['count']}행, run_diff {diff['count']}행, "
          f"table_edges(mart.dong_profile) {tables['count']}행")
    print("lineage MCP 확인 통과")


if __name__ == "__main__":
    asyncio.run(main())
