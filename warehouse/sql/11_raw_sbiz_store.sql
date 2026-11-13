-- 상가(상권)정보 원본 표 (결정 42, 60, 78). Python은 레이크 zip 안 서울 CSV를 파싱만 해서 업소 한 행을 jsonb 한 칸에 담는다.
-- 서울 전체 업소, 모든 컬럼(헤더 이름이 키, 값은 문자열 그대로, 공백 포함). 업종 고르기, 정제, 검증은 dbt가 SQL로 한다(2.2).
-- 배치는 (수집일, 원천 파일): 그 배치 행을 지우고 다시 넣는다(transform/sbiz_store_raw.py).
-- 다시 실행해도 된다. 실행: python -m warehouse.run_sql warehouse/sql/11_raw_sbiz_store.sql
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.sbiz_store (
    collected_date date        NOT NULL,
    source_file    text        NOT NULL,   -- zip 안 서울 CSV 이름 (기준월이 들어 있다: ..._서울_202606.csv)
    row_no         integer     NOT NULL,   -- CSV 안에서 몇 번째 데이터 행 (1부터). 배치 = 파일 하나라 배치 안 번호와 같다
    raw            jsonb       NOT NULL,   -- 행의 모든 열, 값은 문자열 그대로
    loaded_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (collected_date, source_file, row_no)
);
