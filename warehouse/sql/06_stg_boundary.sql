-- 단계 5 스테이징: SGIS 행정동 경계(rules.md 6절)와 연속지적도(rules.md 7절).
-- 처리 단위는 수집일 하나(zip 하나). 수집일마다 스냅샷을 남긴다.
CREATE SCHEMA IF NOT EXISTS stg;

-- 한 행 = 수집일 x 서울 행정동 하나 (2025 SGIS 경계, 통계청 코드)
CREATE TABLE IF NOT EXISTS stg.sgis_dong (
    collected_date date        NOT NULL,
    source_file    text        NOT NULL,
    row_no         integer     NOT NULL,  -- shp 안에서 몇 번째 도형인지 (1부터)
    base_date      date        NOT NULL,  -- SGIS BASE_DATE
    adm_cd         char(8)     NOT NULL,  -- 통계청 행정동 코드 8자리
    adm_nm         text        NOT NULL,
    made_valid     boolean     NOT NULL,  -- BV2: ST_MakeValid로 고쳤으면 true
    geom           geometry(MultiPolygon, 5179) NOT NULL,  -- BC2
    PRIMARY KEY (collected_date, adm_cd)  -- BF3의 안전망
);

CREATE TABLE IF NOT EXISTS stg.sgis_dong_quarantine (
    collected_date date        NOT NULL,
    source_file    text        NOT NULL,
    row_no         integer     NOT NULL,
    reasons        text[]      NOT NULL,
    raw            jsonb       NOT NULL,  -- 속성 값만 남긴다. 도형은 레이크 원본에서 row_no로 찾는다
    quarantined_at timestamptz NOT NULL DEFAULT now()
);

-- 한 행 = 수집일 x 서울 필지 하나 (원래 좌표계 EPSG:5186 그대로, PC1)
CREATE TABLE IF NOT EXISTS stg.parcel (
    collected_date date        NOT NULL,
    source_file    text        NOT NULL,
    row_no         integer     NOT NULL,
    base_ym        char(6)     NOT NULL,  -- 파일 이름의 기준월 (예: LSMD_CONT_LDREG_11_202610 -> 202610)
    pnu            char(19)    NOT NULL,  -- 필지 고유번호
    jibun          text,                  -- 예: "301-7 대" (지번 + 지목)
    sgg_cd         char(5),               -- COL_ADM_SE, 행정안전부식 시군구 코드
    made_valid     boolean     NOT NULL,  -- PV2
    geom           geometry(MultiPolygon, 5186) NOT NULL,
    PRIMARY KEY (collected_date, pnu)     -- PF3의 안전망
);

CREATE TABLE IF NOT EXISTS stg.parcel_quarantine (
    collected_date date        NOT NULL,
    source_file    text        NOT NULL,
    row_no         integer     NOT NULL,
    reasons        text[]      NOT NULL,
    raw            jsonb       NOT NULL,
    quarantined_at timestamptz NOT NULL DEFAULT now()
);
