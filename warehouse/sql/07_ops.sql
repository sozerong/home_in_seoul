-- 운영 대시보드가 읽는 곳 (결정 38). 데이터 내용이 아니라 도착 시점과 정합성을 보여 준다.
CREATE SCHEMA IF NOT EXISTS ops;

-- 상품 버전마다 파일 하나 = 한 행. warehouse/export_product.py가 manifest와 같은 내용을 쓴다.
CREATE TABLE IF NOT EXISTS ops.product_file (
    version    text NOT NULL,
    file       text NOT NULL,
    rows       bigint NOT NULL,
    sha256     char(64) NOT NULL,
    PRIMARY KEY (version, file)
);

-- 작업 실패 한 번 = 한 행. dags/alerting.py가 logs/alerts.log와 같은 내용을 쓴다. 다시 쓰여도 한 행만 남는다.
CREATE TABLE IF NOT EXISTS ops.failure_log (
    at          timestamptz NOT NULL,
    dag_id      text NOT NULL,
    task_id     text NOT NULL,
    map_index   integer NOT NULL,
    run_id      text NOT NULL,
    try_number  integer NOT NULL,
    error       text NOT NULL,
    PRIMARY KEY (dag_id, task_id, map_index, run_id, try_number)
);

-- wh_load 빌드 한 번 = 한 행 (P6, 결정 81). warehouse/build_record.py가 dbt_build 뒤, 교체 직전에 쓴다.
-- sql_hash = 모델별 컴파일 SQL sha256(model_hashes)을 이름순으로 이은 것의 sha256. 매크로만 고쳐도 바뀐다.
CREATE TABLE IF NOT EXISTS ops.build_run (
    build_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    built_at      timestamptz NOT NULL DEFAULT now(),
    git_commit    text,
    clean         boolean,          -- git status --porcelain이 비었나. git이 없으면 null
    sql_hash      char(64) NOT NULL,
    model_hashes  jsonb NOT NULL,   -- {모델 이름: 컴파일 SQL sha256}
    row_counts    jsonb NOT NULL,   -- {스키마.표: 행 수}, 빌드 스키마의 표(뷰 제외)
    build_seconds numeric,          -- dbt run_results.json elapsed_time
    diff_seconds  numeric,          -- 아래 build_diff 계산 시간
    warnings      jsonb             -- [{node, message}] dbt warn. run_results.json은 다음 dbt 명령이 덮어써서 여기 남긴다(결정 86)
);

-- 교체 직전 정제 표의 처리 단위마다 바뀐 행 수 (P6, 결정 81). 바뀐 단위만 남긴다. 처음 빌드(지금 스키마 없음)는 비교하지 않는다.
CREATE TABLE IF NOT EXISTS ops.build_diff (
    build_id  bigint NOT NULL REFERENCES ops.build_run,
    tbl       text NOT NULL,        -- stg_dbt.rent 등
    unit      text NOT NULL,        -- 처리 단위 키를 '/'로 이은 것
    added     bigint NOT NULL,      -- 새 빌드에만 있는 행
    removed   bigint NOT NULL,      -- 지금 표에만 있는 행
    PRIMARY KEY (build_id, tbl, unit)
);

-- 표 행 수를 이름으로 센다. dbt가 빌드마다 스키마를 통째로 바꾸는 표(stg_dbt.*)는 뷰가 직접 가리키면 교체 때
-- drop schema ... cascade로 뷰까지 지워지므로 이름으로만 부른다(결정 61). 표가 없으면 null.
CREATE OR REPLACE FUNCTION ops.row_count(table_name text) RETURNS bigint
LANGUAGE plpgsql STABLE AS $$
DECLARE n bigint;
BEGIN
    IF to_regclass(table_name) IS NULL THEN RETURN NULL; END IF;
    EXECUTE format('SELECT count(*) FROM %s', to_regclass(table_name)) INTO n;
    RETURN n;
END $$;

-- 관문을 통과한 마지막 배치(결정 75). batch_status도 교체되는 표라 이름으로 부른다. 표가 없으면 null.
CREATE OR REPLACE FUNCTION ops.last_approved(src text, OUT collected date, OUT period text)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    IF to_regclass('stg_dbt.batch_status') IS NULL THEN RETURN; END IF;
    EXECUTE 'SELECT max(collected_date), max(period) FROM stg_dbt.batch_status WHERE source = $1 AND approved'
        INTO collected, period USING src;
END $$;

-- 출처 하나 = 한 행. 전월세와 생활이동은 수집일만으로는 멈춘 것을 모른다(다시 받아도 수집일은 새로워진다).
-- 그래서 최신 기간도 함께 본다. 생활이동은 다음 달 20일 공개(매뉴얼)에 센서 7일을 더해, 그 다음 달 자료가 올 때가 지나면 늦음이다.
CREATE OR REPLACE VIEW ops.freshness AS
WITH src (source, last_collected, last_period, expected_days, period_late, rule) AS (
    SELECT '전월세', (ops.last_approved('rent')).collected, (ops.last_approved('rent')).period::char(6), 7, false, '7일 안에 수집'  -- 승인된 배치(결정 75)
    UNION ALL SELECT '역사마스터', (SELECT max(collected_date) FROM stg.station_master), NULL, 7, false, '7일 안에 수집'
    UNION ALL SELECT '혼잡도', (ops.last_approved('congestion')).collected, NULL, 7, false, '7일 안에 수집'  -- 승인된 배치(결정 75)
    UNION ALL SELECT '생활이동', l.collected, l.period, 38,  -- 승인된 배치(판단 64. 관문은 Spark SQL, stg.living_batch_status)
                     current_date > to_date(l.period || '01', 'YYYYMMDD') + interval '2 months 27 days',
                     '다음 달 자료가 공개(20일) 후 7일 안에 도착'
              FROM ops.last_approved('living') l
    UNION ALL SELECT '상가정보', (ops.last_approved('store')).collected, (ops.last_approved('store')).period::char(6), 100, false, '100일 안에 수집'  -- 승인된 배치(결정 75, 78)
    UNION ALL SELECT '행정동 경계', (SELECT max(collected_date) FROM stg.sgis_dong), NULL, 380, false, '380일 안에 수집'
    UNION ALL SELECT '연속지적도', (SELECT max(collected_date) FROM stg.parcel), NULL, NULL, false, '수시'
)
SELECT source, last_collected, last_period, expected_days,
       current_date - last_collected AS age_days,
       period_late OR current_date - last_collected > expected_days AS is_late,
       rule
FROM src;

-- 출처 하나 = 한 행. 지금까지 넣은 전체에서 격리 비율. 기준 5%는 생활이동·전월세 등의 F2와 같다(rules.md).
CREATE OR REPLACE VIEW ops.quality AS
WITH src (source, good, quarantined) AS (
    SELECT '전월세', ops.row_count('stg_dbt.rent'), ops.row_count('stg_dbt.rent_quarantine')  -- dbt 정제(결정 72)
    UNION ALL SELECT '역사마스터', (SELECT count(*) FROM stg.station_master), (SELECT count(*) FROM stg.station_master_quarantine)
    UNION ALL SELECT '혼잡도', ops.row_count('stg_dbt.congestion'), ops.row_count('stg_dbt.congestion_quarantine')  -- dbt 정제(결정 42)
    UNION ALL SELECT '생활이동', (SELECT sum(passed)::bigint FROM stg.living_batch_status), (SELECT count(*) FROM stg.living_quarantine)  -- 관문과 같은 분모(판단 64). 통과 = C4 뒤 원본 행
    UNION ALL SELECT '상가정보', ops.row_count('stg_dbt.store'), ops.row_count('stg_dbt.store_quarantine')  -- dbt 정제, 서울 전체 업종(결정 78)
    UNION ALL SELECT '행정동 경계', (SELECT count(*) FROM stg.sgis_dong), (SELECT count(*) FROM stg.sgis_dong_quarantine)
    UNION ALL SELECT '연속지적도', (SELECT count(*) FROM stg.parcel), (SELECT count(*) FROM stg.parcel_quarantine)
)
SELECT source, good, quarantined, round(100.0 * quarantined / nullif(good + quarantined, 0), 3) AS quarantined_pct FROM src;

-- 생활이동 최신 대상연월과 달별 행 수를 표 전체를 훑지 않고 얻는다 (ops.freshness가 12.5초 걸렸다).
CREATE INDEX IF NOT EXISTS living_migration_target_ym_idx ON stg.living_migration (target_ym);

-- 엔진 실행 한 번 x 단계(stage) 하나 = 한 행 (결정 94-1, design_remaining_work.md 3.3). transform/spark_sql_runner.py가
-- 실행이 끝나면(실패해도) Spark 이벤트 로그에서 읽어 쓴다. 3.10 엔진 분석의 지표. 시간은 ms, 크기는 byte.
CREATE TABLE IF NOT EXISTS ops.engine_run (
    run_id              text NOT NULL,         -- Spark application id
    engine              text NOT NULL,         -- spark
    started_at          timestamptz NOT NULL,
    settings            jsonb NOT NULL,        -- 바꾼 설정(메모리, 셔플 파티션, AQE)
    ok                  boolean NOT NULL,      -- 실행 전체 성공
    run_ms              bigint NOT NULL,
    error               text,
    stage_id            integer NOT NULL,
    attempt             integer NOT NULL,
    job                 text,                  -- 이 단계를 부른 작업 설명(show, test, write)
    tasks               integer NOT NULL,      -- 끝난 태스크 수
    failed_tasks        integer NOT NULL,
    stage_ms            bigint,                -- 제출 ~ 완료. 끝나지 못한 단계는 NULL
    task_ms_max         bigint,
    task_ms_median      bigint,
    shuffle_read_bytes  bigint NOT NULL,
    shuffle_write_bytes bigint NOT NULL,
    memory_spill_bytes  bigint NOT NULL,
    disk_spill_bytes    bigint NOT NULL,
    peak_exec_mem_bytes bigint,                -- 태스크 최대 실행 메모리(Peak Execution Memory)의 최대
    jvm_heap_peak_bytes bigint,                -- 단계 동안 JVM 힙 최대 (StageExecutorMetrics)
    PRIMARY KEY (run_id, stage_id, attempt)
);
