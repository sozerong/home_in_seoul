-- 전월세 실거래가 원본 표 (결정 42, 60, 72). Python은 레이크 XML을 파싱만 해서 item 하나를 jsonb 한 칸에 원천 모양 그대로 담는다.
-- 정제, 검증, 격리는 dbt(warehouse/dbt/models/stg_dbt/rent_*)가 SQL로 한다. 새 필드가 와도 jsonb 안에 남아 적재가 멈추지 않는다.
-- 처리 단위는 (출처, 수집일, 구, 계약년월): 그 단위 행을 지우고 다시 넣는다(transform/rent_raw.py).
-- 다시 실행해도 된다. 실행: python -m warehouse.run_sql warehouse/sql/10_raw_rent.sql
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.rent (
    source_type    text        NOT NULL,   -- offi / rh
    collected_date date        NOT NULL,
    sgg_cd         char(5)     NOT NULL,   -- 요청한 구 코드 (LAWD_CD)
    deal_ymd       char(6)     NOT NULL,   -- 요청한 계약년월
    source_file    text        NOT NULL,   -- 레이크 파일 이름 (구, 계약년월, 페이지가 들어 있다)
    item_no        integer     NOT NULL,   -- 파일 안에서 몇 번째 item (1부터)
    raw            jsonb       NOT NULL,   -- item의 자식 태그 전부, 값은 문자열 그대로(공백 포함)
    loaded_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_type, collected_date, source_file, item_no)
);
CREATE INDEX IF NOT EXISTS rent_unit_idx ON raw.rent (source_type, collected_date, sgg_cd, deal_ymd);
