-- 컬럼 리니지 저장 (P4, 결정 47, 48). 원본 표 2개 + 목적별 뷰 5개(공통 정규화 edge + 4개).
-- 원본 표에는 추출기가 낸 값을 그대로 둔다(빌드 스키마 이름 wh_build, 파싱 실패 행 포함). 거르고 이름을 바꾸는 일은 뷰가 한다.
-- 회차 하나: run 행을 running으로 넣고 → 간선을 쓰고 → success로 바꾼다. 실패하면 failed로 남고 뷰는 그 회차를 읽지 않는다.
-- 다시 실행해도 된다(IF NOT EXISTS, CREATE OR REPLACE). 실행: python -m warehouse.run_sql warehouse/sql/08_lineage.sql

CREATE SCHEMA IF NOT EXISTS lineage;

CREATE TABLE IF NOT EXISTS lineage.run (
    run_id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at            timestamptz NOT NULL DEFAULT now(),
    finished_at           timestamptz,
    status                text NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    trigger               text NOT NULL CHECK (trigger IN ('asset', 'manual')),  -- asset: wh_load 뒤 자동, manual: 코드 작업 뒤 손으로
    git_commit            text,
    sqlglot_version       text NOT NULL,
    engine                text NOT NULL CHECK (engine IN ('spark', 'python')),
    manifest_generated_at timestamptz,          -- 입력(dbt 컴파일)이 언제 만들어졌나
    sql_files             integer,
    edges                 integer
);

CREATE TABLE IF NOT EXISTS lineage.edge_raw (
    run_id        bigint NOT NULL REFERENCES lineage.run,
    sql_file      text NOT NULL,               -- target/compiled 기준 경로. 상품 층은 'export_product.py'
    file_kind     text NOT NULL CHECK (file_kind IN ('model', 'test', 'product')),
    target_table  text,                        -- 원문 그대로 (seoul.wh_build.dim_dong, product.dong_profile)
    target_column text,
    source_table  text,
    source_column text,                        -- '*'는 표의 행, 'raw.X'는 jsonb 키
    kind          text CHECK (kind IN ('DIRECT', 'INDIRECT')),
    subkind       text,
    status        text NOT NULL CHECK (status IN ('ok', 'unresolved', 'parse_error')),
    detail        text                         -- 오류 메시지
);
CREATE INDEX IF NOT EXISTS edge_raw_run ON lineage.edge_raw (run_id);

-- 이름 정규화: seoul. 접두어를 빼고 빌드 스키마(wh_build, mart_build, stg_dbt_build)를 실제 이름으로
CREATE OR REPLACE FUNCTION lineage.norm(name text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$ SELECT regexp_replace(regexp_replace(name, '^seoul\.', ''), '^(\w+)_build\.', '\1.') $$;

-- 공통 바탕: 가장 최근 성공 회차의 정상 간선, 테스트 SQL 제외, 이름 정규화
CREATE OR REPLACE VIEW lineage.edge AS
SELECT e.run_id, e.sql_file, e.file_kind,
       lineage.norm(e.target_table) AS target_table, e.target_column,
       lineage.norm(e.source_table) AS source_table, e.source_column, e.kind, e.subkind
FROM lineage.edge_raw e
WHERE e.run_id = (SELECT max(run_id) FROM lineage.run WHERE status = 'success')
  AND e.file_kind <> 'test' AND e.status = 'ok';

-- 그림·대시보드용: 표 단위로 묶은 간선
CREATE OR REPLACE VIEW lineage.table_edge AS
SELECT source_table, target_table, array_agg(DISTINCT kind ORDER BY kind) AS kinds, count(*) AS column_edges
FROM lineage.edge
GROUP BY source_table, target_table;

-- 영향 범위(S1, S4, S6, MCP): 컬럼 하나에서 재귀로 닫은 하류 쌍.
-- 상태를 (출발, 도착, 깊이, 행 관계를 거쳤나)로만 두고 UNION으로 중복을 없앤다. 경로 배열을 들고 다니면 경로 수만큼 행이
-- 늘어나기 때문이다. 컬럼 그래프는 dbt 모델 순서를 따르는 DAG라 순환이 없지만, 깊이 20에서 끊어 만일을 막는다.
CREATE OR REPLACE VIEW lineage.downstream AS
WITH RECURSIVE e AS (
    SELECT DISTINCT source_table, source_column, target_table, target_column, kind = 'INDIRECT' AS indirect FROM lineage.edge
),
walk (from_table, from_column, to_table, to_column, depth, indirect) AS (
    SELECT source_table, source_column, target_table, target_column, 1, indirect FROM e
    UNION
    SELECT w.from_table, w.from_column, e.target_table, e.target_column, w.depth + 1, w.indirect OR e.indirect
    FROM walk w JOIN e ON e.source_table = w.to_table AND e.source_column = w.to_column
    WHERE w.depth < 20
)
SELECT from_table, from_column, to_table, to_column,
       min(depth) AS depth,
       bool_or(NOT indirect) AS value_path,  -- 값이 그대로 흘러가는 경로(모두 DIRECT)가 있다
       bool_or(indirect) AS row_path,        -- 행 관계(필터·조인 등)를 한 번이라도 거치는 경로가 있다: 행이 빠지거나 바뀌는 영향
       to_table LIKE 'product.%' AS is_product
FROM walk
GROUP BY from_table, from_column, to_table, to_column;

-- 출처 추적(P5, MCP): downstream을 뒤집은 것. 상품 컬럼에서 stg(혼잡도 이전 뒤에는 raw)까지
CREATE OR REPLACE VIEW lineage.upstream AS
SELECT to_table AS table_name, to_column AS column_name, from_table AS source_table, from_column AS source_column,
       depth, value_path, row_path
FROM lineage.downstream;

-- 회차 비교(혼잡도 이전 전후, 결정 45): 최근 성공 회차와 그 직전 성공 회차의 간선 차이
CREATE OR REPLACE VIEW lineage.run_diff AS
WITH runs AS (
    SELECT run_id, row_number() OVER (ORDER BY run_id DESC) AS n FROM lineage.run WHERE status = 'success'
),
pick AS (
    SELECT DISTINCT r.n, lineage.norm(e.target_table) AS target_table, e.target_column,
           lineage.norm(e.source_table) AS source_table, e.source_column, e.kind, e.subkind
    FROM lineage.edge_raw e JOIN runs r USING (run_id)
    WHERE r.n <= 2 AND e.status = 'ok' AND e.file_kind <> 'test'
)
SELECT 'added' AS change, (SELECT run_id FROM runs WHERE n = 2) AS from_run, (SELECT run_id FROM runs WHERE n = 1) AS to_run, d.*
FROM (SELECT target_table, target_column, source_table, source_column, kind, subkind FROM pick WHERE n = 1
      EXCEPT SELECT target_table, target_column, source_table, source_column, kind, subkind FROM pick WHERE n = 2) d
UNION ALL
SELECT 'removed', (SELECT run_id FROM runs WHERE n = 2), (SELECT run_id FROM runs WHERE n = 1), d.*
FROM (SELECT target_table, target_column, source_table, source_column, kind, subkind FROM pick WHERE n = 2
      EXCEPT SELECT target_table, target_column, source_table, source_column, kind, subkind FROM pick WHERE n = 1) d;

-- MCP 서버가 붙는 읽기 전용 역할(결정 58). 로그인 비밀번호는 .env의 LINEAGE_READER_PASSWORD로 lineage_mcp가 넣는다.
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'lineage_reader') THEN
        CREATE ROLE lineage_reader NOLOGIN;
    END IF;
END $$;
GRANT USAGE ON SCHEMA lineage TO lineage_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA lineage TO lineage_reader;
GRANT EXECUTE ON FUNCTION lineage.norm(text) TO lineage_reader;
