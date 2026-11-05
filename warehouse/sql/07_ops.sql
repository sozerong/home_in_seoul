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

-- 출처 하나 = 한 행. 전월세와 생활이동은 수집일만으로는 멈춘 것을 모른다(다시 받아도 수집일은 새로워진다).
-- 그래서 최신 기간도 함께 본다. 생활이동은 다음 달 20일 공개(매뉴얼)에 센서 7일을 더해, 그 다음 달 자료가 올 때가 지나면 늦음이다.
CREATE OR REPLACE VIEW ops.freshness AS
WITH src (source, last_collected, last_period, expected_days, period_late, rule) AS (
    SELECT '전월세', (SELECT max(collected_date) FROM stg.rent), (SELECT max(deal_ymd) FROM stg.rent), 7, false, '7일 안에 수집'
    UNION ALL SELECT '역사마스터', (SELECT max(collected_date) FROM stg.station_master), NULL, 7, false, '7일 안에 수집'
    UNION ALL SELECT '혼잡도', (SELECT max(collected_date) FROM stg.congestion), NULL, 7, false, '7일 안에 수집'
    UNION ALL SELECT '생활이동', (SELECT max(collected_date) FROM stg.living_migration), m.ym, 38,
                     current_date > to_date(m.ym || '01', 'YYYYMMDD') + interval '2 months 27 days',
                     '다음 달 자료가 공개(20일) 후 7일 안에 도착'
              FROM (SELECT max(target_ym) AS ym FROM stg.living_migration) m
    UNION ALL SELECT '상가정보', (SELECT max(collected_date) FROM stg.sbiz_store), (SELECT max(base_ym) FROM stg.sbiz_store), 100, false, '100일 안에 수집'
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
    SELECT '전월세', (SELECT count(*) FROM stg.rent), (SELECT count(*) FROM stg.rent_quarantine)
    UNION ALL SELECT '역사마스터', (SELECT count(*) FROM stg.station_master), (SELECT count(*) FROM stg.station_master_quarantine)
    UNION ALL SELECT '혼잡도', (SELECT count(*) FROM stg.congestion), (SELECT count(*) FROM stg.congestion_quarantine)
    UNION ALL SELECT '생활이동', (SELECT count(*) FROM stg.living_migration), (SELECT count(*) FROM stg.living_migration_quarantine)
    UNION ALL SELECT '상가정보', (SELECT count(*) FROM stg.sbiz_store), (SELECT count(*) FROM stg.sbiz_store_quarantine)
    UNION ALL SELECT '행정동 경계', (SELECT count(*) FROM stg.sgis_dong), (SELECT count(*) FROM stg.sgis_dong_quarantine)
    UNION ALL SELECT '연속지적도', (SELECT count(*) FROM stg.parcel), (SELECT count(*) FROM stg.parcel_quarantine)
)
SELECT source, good, quarantined, round(100.0 * quarantined / nullif(good + quarantined, 0), 3) AS quarantined_pct FROM src;

-- 생활이동 최신 대상연월과 달별 행 수를 표 전체를 훑지 않고 얻는다 (ops.freshness가 12.5초 걸렸다).
CREATE INDEX IF NOT EXISTS living_migration_target_ym_idx ON stg.living_migration (target_ym);
