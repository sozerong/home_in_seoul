-- 파이프라인 UI가 DB에서 읽는 쿼리 전부(docs/handoff_ui.md 4절). "-- name: x" 한 줄이 쿼리 하나의 시작이다.
-- 이 이름이 화면 아래 "이 숫자는 어디서 왔나"와 API 응답의 sources[].name에 그대로 나온다.

-- name: lineage_status
-- 지도 최신 회차, 간선 수, stale. stale 계산은 lineage_mcp/server.py query()와 같다(결정 81):
-- 커밋되지 않은 코드로 만든 지도이거나 마지막 빌드와 컴파일 SQL 해시가 다르면 true, 판단할 값이 없으면 null.
WITH run AS (
    SELECT run_id, finished_at, git_commit, clean, sql_hash
    FROM lineage.run WHERE status = 'success' ORDER BY run_id DESC LIMIT 1
)
SELECT run.run_id, run.finished_at, run.git_commit, b.build_id, b.built_at,
       (SELECT count(*) FROM lineage.edge) AS edges,
       CASE WHEN run.clean IS NULL OR b.sql_hash IS NULL OR run.sql_hash IS NULL THEN NULL
            ELSE NOT run.clean OR run.sql_hash <> b.sql_hash END AS stale
FROM run LEFT JOIN lineage.latest_build b ON true;

-- name: batch_rejected
-- 출처별 관문 판정 수와 거절 수(stg_dbt.batch_status, 결정 75·100).
SELECT source, count(*) AS batches, count(*) FILTER (WHERE NOT approved) AS rejected, max(checked_at) AS last_checked
FROM stg_dbt.batch_status GROUP BY source ORDER BY source;

-- name: batch_detail
-- 상세: 출처의 관문 판정을 수집일마다 묶는다. 거절 사유는 거절된 배치의 failed_checks만(경고 전용 검사는 승인 배치에도 남을 수 있다).
SELECT source, collected_date, count(*) AS batches, count(*) FILTER (WHERE NOT approved) AS rejected,
       min(period) AS first_period, max(period) AS last_period, max(checked_at) AS checked_at,
       string_agg(DISTINCT array_to_string(failed_checks, ', '), ' / ') FILTER (WHERE NOT approved) AS reasons
FROM stg_dbt.batch_status WHERE source = ANY(%(src)s)
GROUP BY source, collected_date ORDER BY collected_date DESC, source;

-- name: quality
-- 상세: 통과 행과 격리 행(ops.quality, 07_ops.sql).
SELECT * FROM ops.quality WHERE source = ANY(%(src)s);

-- name: freshness
-- 상세: 마지막 수집과 늦음 판정(ops.freshness, 07_ops.sql).
SELECT * FROM ops.freshness WHERE source = ANY(%(src)s);

-- name: build_runs
-- 상세(wh_load): 빌드 기록과 바뀐 단위 수(ops.build_run, ops.build_diff, 결정 81).
SELECT r.build_id, r.built_at, left(r.git_commit, 7) AS git_commit, r.clean, r.build_seconds, r.diff_seconds,
       jsonb_array_length(coalesce(r.warnings, '[]')) AS warnings,
       count(d.unit) AS diff_units, coalesce(sum(d.added), 0) AS added, coalesce(sum(d.removed), 0) AS removed
FROM ops.build_run r LEFT JOIN ops.build_diff d USING (build_id)
GROUP BY r.build_id ORDER BY r.build_id DESC LIMIT 20;

-- name: engine_runs
-- 상세(생활이동): 엔진 실행마다 한 줄. 원본은 단계(stage)마다 한 행이다(ops.engine_run).
SELECT run_id, engine, min(started_at) AS started_at, bool_and(ok) AS ok, max(run_ms) AS run_ms, count(*) AS stages,
       sum(disk_spill_bytes) AS disk_spill_bytes, max(jvm_heap_peak_bytes) AS jvm_heap_peak_bytes, left(max(error), 200) AS error
FROM ops.engine_run GROUP BY run_id, engine ORDER BY min(started_at) DESC LIMIT 20;

-- name: lineage_runs
-- 상세(lineage): 리니지 회차(lineage.run). 엔진 지표는 ops.engine_run에 남지 않는다.
SELECT run_id, started_at, status, trigger, engine, left(git_commit, 7) AS git_commit, clean, sql_files, edges,
       round(extract(epoch FROM finished_at - started_at)) AS seconds
FROM lineage.run ORDER BY run_id DESC LIMIT 20;

-- name: lineage_edges
-- 데이터 지도: 최신 성공 회차의 컬럼 간선(lineage.edge 뷰가 회차와 시험 파일을 거른다). 같은 간선이 여러 SQL 파일에서 나오면 하나로 묶는다.
SELECT DISTINCT source_table, source_column, target_table, target_column, kind FROM lineage.edge;

-- name: impact_down
-- 데이터 지도: 고른 컬럼(또는 표 전체)이 닿는 하류 컬럼. 리니지 MCP downstream과 같은 뷰다(lineage_mcp/server.py).
SELECT to_table AS tbl, to_column AS col, min(depth) AS depth,
       bool_or(value_path) AS value_path, bool_or(row_path) AS row_path, bool_or(rowset_path) AS rowset_path
FROM lineage.downstream WHERE from_table = %(t)s AND (%(c)s::text IS NULL OR from_column = %(c)s)
GROUP BY 1, 2 ORDER BY 1, 2;

-- name: impact_up
-- 데이터 지도: 고른 컬럼(또는 표 전체)이 오는 상류 컬럼. 리니지 MCP upstream과 같은 뷰다.
SELECT source_table AS tbl, source_column AS col, min(depth) AS depth,
       bool_or(value_path) AS value_path, bool_or(row_path) AS row_path, bool_or(rowset_path) AS rowset_path
FROM lineage.upstream WHERE table_name = %(t)s AND (%(c)s::text IS NULL OR column_name = %(c)s)
GROUP BY 1, 2 ORDER BY 1, 2;
