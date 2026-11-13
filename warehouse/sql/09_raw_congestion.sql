-- 혼잡도 원본 표 (결정 42, 60). Python은 레이크 XML을 파싱만 해서 row 하나를 jsonb 한 칸에 원천 모양 그대로 담는다.
-- 이름 대응, 정제, 검증, 격리는 dbt(warehouse/dbt/models/stg_dbt)가 SQL로 한다. 새 필드가 와도 jsonb 안에 남아 적재가 멈추지 않는다.
-- 처리 단위는 수집일 하나: 그 수집일 행을 지우고 다시 넣는다(transform/congestion_raw.py).
-- 다시 실행해도 된다. 실행: python -m warehouse.run_sql warehouse/sql/09_raw_congestion.sql
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.congestion (
    collected_date date        NOT NULL,
    source_file    text        NOT NULL,
    row_no         integer     NOT NULL,   -- 파일 안에서 몇 번째 row
    raw            jsonb       NOT NULL,   -- row의 자식 태그 전부, 값은 문자열 그대로(공백 포함)
    loaded_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (collected_date, source_file, row_no)
);
