-- 단계 4 스테이징: 소상공인시장진흥공단 상가(상권)정보, 서울 파일. 규칙은 docs/rules_sbiz_store.md.
-- 범위 A: 생활 편의 필수 5종 소분류 6코드(G20405, G20404, G21501, Q10201, S20901, S20902)만.
-- 처리 단위: collected_date(zip 하나). 이 단위로 두 테이블의 행을 지우고 다시 넣는다. 수집일마다 스냅샷을 남긴다.
CREATE SCHEMA IF NOT EXISTS stg;

-- 한 행 = 수집일 x 상가업소 하나.
CREATE TABLE IF NOT EXISTS stg.sbiz_store (
    collected_date date             NOT NULL,  -- 레이크 수집일 (처리 단위, MC4)
    base_ym        char(6)          NOT NULL,  -- 파일 이름의 기준월, 예: 202606 (MC4)
    source_file    text             NOT NULL,  -- zip 안의 서울 CSV 이름 (MC4)
    row_no         integer          NOT NULL,  -- CSV 안에서 몇 번째 데이터 행인지 (헤더 제외, 1부터)
    store_id       char(20)         NOT NULL,  -- 상가업소번호
    store_name     text,                       -- 상호명, 앞뒤 공백 제거 (MC2)
    sub_cd         char(6)          NOT NULL,  -- 상권업종소분류코드. 대·중분류는 앞자리로 안다
    sub_nm         text             NOT NULL,  -- 상권업종소분류명
    sido_cd        char(2)          NOT NULL,
    sgg_cd         char(5)          NOT NULL,  -- 행정안전부식 (강남 11680)
    adm_cd         char(8)          NOT NULL,  -- 행정동코드, 행정안전부식. 단계 5 공간 조인 결과 대조용
    bjd_cd         char(10)         NOT NULL,  -- 법정동코드
    lon            double precision NOT NULL,  -- 경도, 도 단위 (MC3). 변환과 geometry는 단계 5
    lat            double precision NOT NULL,  -- 위도, 도 단위 (MC3)
    PRIMARY KEY (collected_date, store_id)     -- MV2의 안전망
);

-- 검증에 어긋난 행. 원본 39열을 문자열 그대로 남긴다.
CREATE TABLE IF NOT EXISTS stg.sbiz_store_quarantine (
    collected_date date        NOT NULL,
    source_file    text        NOT NULL,
    row_no         integer     NOT NULL,
    reasons        text[]      NOT NULL,  -- 예: {MV1,MV3}
    raw            jsonb       NOT NULL,  -- 원본 행의 열 이름과 값
    quarantined_at timestamptz NOT NULL DEFAULT now()
);
