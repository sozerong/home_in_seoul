-- 생활이동 경계 표 (design_remaining_work.md 3.3~3.4, 결정 88~90, 96). Spark SQL 실행기(transform/spark_sql_runner.py)가
-- spark_sql/living/30_commute.sql의 결과를 stg.living_commute_new에 쓰고 트랜잭션 하나에서 이 표와 이름을 바꾼다. 매번 전체 재계산.
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
