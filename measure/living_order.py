"""생활이동 대상연월별 원본 파일 배치·키 순서 비교 (F-31, F-42 "202601만 threads 12에서 넘치는 이유", 측정 69).
운영 표에 쓰지 않는다. Iceberg 지금 스냅샷의 데이터 파일을 Parquet 메타데이터와 앞부분 행으로 본다.

  docker compose run --rm --no-deps duckdb-job python -u -m measure.living_order [대상연월 ...]
기본 대상연월: 202512 202601 202602 202605(성공한 달 셋과 202601).
보는 것:
  1. 파일·행 그룹: 파일 수, 파일당 행, 행 그룹 수와 행 그룹당 행, 압축 크기
  2. 키 순서: 행 그룹마다 키 열의 min/max 통계. min = max인 행 그룹 비율이 높으면 그 열로 정렬돼 있다
  3. 앞부분 반복: 파일마다 앞 N행(파일 안 순서 = row_no)에서 키 9개 그룹 수 / 행 수, 출발 코드 수, 연속 행이 같은 출발 코드인 비율
"""
import json
import statistics
import sys

import duckdb

from measure.duckdb_sql import metadata_location

KEYS = ["target_ym", "collected_date", "weekday", "arrive_hour_raw", "origin_cd", "dest_cd", "sex", "age", "move_type"]
HEAD = 2_000_000


def main():
    yms = sys.argv[1:] or ["202512", "202601", "202602", "202605"]
    con = duckdb.connect()
    con.execute("LOAD iceberg")
    loc = metadata_location()
    files = con.execute(f"SELECT file_path, record_count FROM iceberg_metadata('{loc}') "
                        "WHERE status <> 'DELETED' AND content = 'DATA'").fetchall()
    out = {"metadata": loc, "months": {}}
    for ym in yms:
        mine = sorted((p.removeprefix("file:"), n) for p, n in files if f"target_ym={ym}/" in p)
        paths = [p for p, _ in mine]
        rg = con.execute(f"SELECT file_name, row_group_id, any_value(row_group_num_rows), sum(total_compressed_size) "
                         f"FROM parquet_metadata({paths!r}) GROUP BY 1, 2").fetchall()
        stats = con.execute(f"SELECT path_in_schema, count(*), count_if(stats_min_value = stats_max_value) "
                            f"FROM parquet_metadata({paths!r}) WHERE path_in_schema IN ({', '.join(repr(k) for k in KEYS)}) "
                            "GROUP BY 1 ORDER BY 1").fetchall()
        rows_per_rg = [r[2] for r in rg]
        heads = []
        for p in paths:
            h = con.execute(f"""
                WITH t AS (SELECT {', '.join(KEYS)}, row_no FROM read_parquet('{p}') ORDER BY row_no LIMIT {HEAD}),
                w AS (SELECT *, (origin_cd = lag(origin_cd) OVER (ORDER BY row_no))::int AS same FROM t)
                SELECT count(*), count(DISTINCT ({', '.join(KEYS)})), count(DISTINCT origin_cd), avg(same)
                FROM w""").fetchone()
            heads.append({"file": p.rsplit("/", 1)[-1], "rows": h[0], "groups": h[1], "group_ratio": round(h[1] / h[0], 4),
                          "origins": h[2], "same_origin_as_prev": round(h[3], 4)})
        m = {"files": len(mine), "rows": sum(n for _, n in mine), "rows_per_file_max": max(n for _, n in mine),
             "row_groups": len(rg), "rows_per_rg_min_median_max": [min(rows_per_rg), int(statistics.median(rows_per_rg)), max(rows_per_rg)],
             "compressed_gb": round(sum(r[3] for r in rg) / 1e9, 2),
             "rg_const_ratio": {k: round(c / n, 3) for k, n, c in stats},
             "head_group_ratio_min_median_max": [min(h["group_ratio"] for h in heads),
                                                 statistics.median(h["group_ratio"] for h in heads),
                                                 max(h["group_ratio"] for h in heads)],
             "head_same_origin_median": statistics.median(h["same_origin_as_prev"] for h in heads),
             "heads": heads}
        out["months"][ym] = m
        print(ym, json.dumps({k: v for k, v in m.items() if k != "heads"}, ensure_ascii=False), flush=True)
    print("JSON " + json.dumps(out, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
