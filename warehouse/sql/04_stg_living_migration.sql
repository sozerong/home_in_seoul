-- 단계 4 스테이징: 서울 생활이동 (행정동). 규칙은 docs/rules_living_migration.md.
-- 범위 C: 이동유형 HW, 월~금, 도착 07·08·09시, 출발·도착 모두 서울 행정동(11로 시작하는 7자리, 2021 코드).
-- 처리 단위: (collected_date, target_ym). 이 단위로 두 테이블의 행을 지우고 다시 넣는다.
CREATE SCHEMA IF NOT EXISTS stg;

-- 정제·검증을 통과한 행. 이동유형은 범위상 HW뿐이라 컬럼을 두지 않는다.
CREATE TABLE IF NOT EXISTS stg.living_migration (
    collected_date date     NOT NULL,  -- 레이크 수집일
    target_ym      char(6)  NOT NULL,  -- 대상연월
    dest_cd        char(7)  NOT NULL,  -- 도착(직장) 행정동, 2021 통계청 코드
    origin_cd      char(7)  NOT NULL,  -- 출발(집) 행정동
    weekday        char(1)  NOT NULL,  -- 월 화 수 목 금
    arrive_hour    smallint NOT NULL,  -- 7, 8, 9 (7 = 07:00~07:59 도착)
    sex            char(1)  NOT NULL,  -- M / F
    age            char(2)  NOT NULL,  -- 나이 구간 시작값 '00', '10', '15', ..., '80'
    avg_minutes    smallint NOT NULL,  -- 평균 이동 시간(분), 모든 이동 수단 포함
    population     numeric,            -- 그 달 같은 요일들의 합. NULL = 원본 "*" (C2)
    source_file    text     NOT NULL,  -- zip 안 CSV 이름
    row_no         integer  NOT NULL,  -- 그 CSV의 줄 번호 (헤더 = 1)
    -- C4(완전 중복 하나만)와 V8(키 충돌 격리)을 거친 뒤라 키가 겹치지 않는다. 겹치면 적재가 실패한다.
    -- 마트는 직장(dest_cd)으로 찾으므로 dest_cd를 앞에 둔다.
    PRIMARY KEY (collected_date, target_ym, dest_cd, origin_cd, weekday, arrive_hour, sex, age)
);

-- 검증에 어긋난 행. 원본 열 10개를 문자열 그대로 남긴다.
CREATE TABLE IF NOT EXISTS stg.living_migration_quarantine (
    collected_date date        NOT NULL,
    target_ym      char(6)     NOT NULL,
    source_file    text        NOT NULL,
    row_no         integer     NOT NULL,
    reasons        text[]      NOT NULL,  -- 어긋난 규칙 번호들, 예: {V6}
    raw            jsonb       NOT NULL,  -- 원본 헤더 이름과 값
    quarantined_at timestamptz NOT NULL DEFAULT now()
);
