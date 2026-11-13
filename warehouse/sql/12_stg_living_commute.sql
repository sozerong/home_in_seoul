-- 생활이동 경계 표 (design_remaining_work.md 3.3~3.4, 결정 88~90, 96). Spark SQL 실행기(transform/spark_sql_runner.py)가
-- spark_sql/living/30_commute.sql의 결과를 stg.living_commute_new에 쓰고 트랜잭션 하나에서 이 표로 옮긴다(F-13 처방 A: 비우고 복사). 매번 전체 재계산.
-- 한 행 = 대상연월 x 출발 x 도착(2021 통계청 코드) x 평일/주말 x 도착시간 묶음 x 이동유형 x 나이 구간.
-- 무엇이 이 표에 들어오고 무엇이 Spark 쪽에 남는지는 30_commute.sql 머리 주석에 적는다.
CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.living_commute (
    target_ym          char(6) NOT NULL,
    collected_date     date    NOT NULL,  -- 대상연월마다 승인된 가장 최근 수집일 (결정 90)
    origin_cd          char(7) NOT NULL,  -- 출발 행정동 (2021 코드, 서울)
    dest_cd            char(7) NOT NULL,  -- 도착 행정동
    day_type           text    NOT NULL,  -- 평일(월~금) / 주말(토, 일)
    hour_group         text    NOT NULL,  -- 07_09 / 18_20 / 기타 (도착시간)
    move_type          char(2) NOT NULL,  -- HW / WH
    age_group          text    NOT NULL,  -- 20_34(나이 20, 25, 30) / 기타
    population         numeric,           -- 가려지지 않은 이동인구 합. 묶인 행이 모두 "*"면 NULL
    population_minutes numeric,           -- 가려지지 않은 행의 이동인구 x 평균 이동 시간 합. 평균 = population_minutes / population
    masked_rows        bigint  NOT NULL,  -- 이동인구 "*"(가려짐) 행 수
    rows               bigint  NOT NULL,  -- 묶인 정제 행 수 (C4 완전 중복을 하나로 센 뒤)
    PRIMARY KEY (target_ym, origin_cd, dest_cd, day_type, hour_group, move_type, age_group)
);

-- 생활이동 배치 관문 판정 (결정 90, 76, 판단 64). spark_sql/living/22_batch_status.sql의 결과. 한 행 = 배치(대상연월, 수집일).
-- 실행기가 경계 표와 같은 트랜잭션에서 바꾼다(--save). dbt batch_status_living이 다른 세 출처와 같은 모양으로 읽는다.
CREATE TABLE IF NOT EXISTS stg.living_batch_status (
    target_ym        char(6) NOT NULL,
    collected_date   date    NOT NULL,
    raw_rows         bigint  NOT NULL,  -- 원본 행 (Iceberg)
    passed           bigint  NOT NULL,  -- 통과 행 (C4 완전 중복을 하나로 센 뒤, 키 충돌 제외)
    quarantined      bigint  NOT NULL,  -- 격리 행 (stg.living_quarantine의 이 배치 행 수)
    c4_dropped       bigint  NOT NULL,  -- C4 완전 중복으로 빠진 행
    pop_inexact      bigint  NOT NULL,  -- 이동인구를 DECIMAL(12, 2)로 바꾸며 값이 달라진 통과 행
    prev_raw_rows    bigint,            -- 직전 배치의 원본 행 (living_raw_shrink 기준)
    quarantine_ratio double precision,  -- quarantined / (quarantined + passed)
    failed_checks    text    NOT NULL,  -- 걸린 검사 이름을 쉼표로 이음. 빈 글자 = 통과
    approved         boolean NOT NULL,
    checked_at       timestamptz NOT NULL DEFAULT now(),  -- 실행기가 쓴 시각 (= 판정 시각)
    PRIMARY KEY (target_ym, collected_date)
);

-- 생활이동 격리 행 (spark_sql/living/21_quarantine.sql, 판단 64). 한 행 = 원본 한 행. 옛 stg.living_migration_quarantine과 같은 뜻이나
-- 범위 C(HW, 평일, 07~09시, 서울) 밖 행도 들어간다. reasons = 규칙 번호를 쉼표로 이은 글자(Spark 쪽 모양 그대로).
CREATE TABLE IF NOT EXISTS stg.living_quarantine (
    target_ym      char(6) NOT NULL,
    collected_date date    NOT NULL,
    source_file    text    NOT NULL,
    row_no         int     NOT NULL,
    reasons        text    NOT NULL,
    PRIMARY KEY (target_ym, collected_date, source_file, row_no)
);
