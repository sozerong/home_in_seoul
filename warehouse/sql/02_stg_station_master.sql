-- 단계 4 스테이징: 서울시 역사마스터. 규칙은 docs/rules.md 2절. 수집일마다 스냅샷을 모두 남긴다.
CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.station_master (
    collected_date date          NOT NULL,  -- 레이크 수집일 (처리 단위)
    source_file    text          NOT NULL,
    row_no         integer       NOT NULL,  -- 파일 안에서 몇 번째 row인지 (1부터)
    bldn_id        char(4)       NOT NULL,  -- 역사_ID, 앞자리 0 보존 (SC2)
    bldn_nm        text          NOT NULL,  -- 역사명, 그대로
    route          text          NOT NULL,  -- 호선, 그대로
    lat            numeric(9, 6) NOT NULL,  -- 위도 (SC3)
    lot            numeric(9, 6) NOT NULL,  -- 경도 (SC3)
    PRIMARY KEY (collected_date, bldn_id)   -- SF2의 안전망
);

CREATE TABLE IF NOT EXISTS stg.station_master_quarantine (
    collected_date date        NOT NULL,
    source_file    text        NOT NULL,
    row_no         integer     NOT NULL,
    reasons        text[]      NOT NULL,  -- 예: {SV3}
    raw            jsonb       NOT NULL,  -- 원본 row의 필드 이름과 값
    quarantined_at timestamptz NOT NULL DEFAULT now()
);