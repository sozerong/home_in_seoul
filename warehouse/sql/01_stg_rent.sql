-- 단계 4 스테이징: 전월세 실거래가 (오피스텔 + 연립다세대). 규칙은 docs/rules.md 1절.
CREATE SCHEMA IF NOT EXISTS stg;

-- 정제·검증을 통과한 행. 동·호가 없어 완전히 같은 행도 모두 남기므로 기본 키가 없다.
CREATE TABLE IF NOT EXISTS stg.rent (
    source_type      text         NOT NULL,  -- offi / rh
    collected_date   date         NOT NULL,  -- 레이크 수집일
    source_file      text         NOT NULL,  -- 레이크 파일 이름
    item_no          integer      NOT NULL,  -- 그 파일 안에서 몇 번째 item인지 (1부터)
    deal_ymd         char(6)      NOT NULL,  -- 요청한 계약년월
    sgg_cd           char(5)      NOT NULL,
    umd_nm           text         NOT NULL,
    bjd_cd           char(10)     NOT NULL,  -- 법정동 코드 (C6)
    jibun            text         NOT NULL,
    pnu              char(19)     NOT NULL,  -- 필지 고유번호 (C6)
    building_name    text,                   -- offiNm / mhouseNm (C1)
    house_type       text,                   -- 연립다세대만
    build_year       integer,
    floor            integer,
    excl_area        numeric(7, 2) NOT NULL, -- 전용면적 ㎡
    deal_date        date         NOT NULL,  -- (C4)
    deposit          integer      NOT NULL,  -- 만원 (C3)
    monthly_rent     integer      NOT NULL,  -- 만원, 0이면 전세
    contract_type    text,                   -- 신규 / 갱신 / NULL
    contract_term    text,
    pre_deposit      integer,
    pre_monthly_rent integer,
    use_rr_right     text
);

-- 검증에 어긋난 행. 원본 값을 문자열 그대로 남겨 나중에 되살릴 수 있게 한다.
CREATE TABLE IF NOT EXISTS stg.rent_quarantine (
    source_type    text        NOT NULL,
    collected_date date        NOT NULL,
    source_file    text        NOT NULL,
    item_no        integer     NOT NULL,
    deal_ymd       char(6)     NOT NULL,
    sgg_cd         char(5)     NOT NULL,
    reasons        text[]      NOT NULL,  -- 어긋난 규칙 번호들, 예: {V1,V8}
    raw            jsonb       NOT NULL,  -- 원본 item의 필드 이름과 값
    quarantined_at timestamptz NOT NULL DEFAULT now()
);