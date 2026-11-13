-- 한 행 = 대상연월 x 출발 x 도착(2021 코드) x 평일/주말 x 도착시간 묶음 x 이동유형 x 나이 구간. 경계 표 stg.living_commute 그대로.
-- 정제, 대상연월마다 승인된 최신 수집일 고르기, 묶기는 Spark SQL(spark_sql/living)이 한다(결정 88~90, 96). 여기서는 고르지 않는다.
-- 값은 합뿐이다. 평균은 population_minutes / population으로 다시 계산한다. 2025 행정동으로 옮기는 일은 마트에서 map_dong_2021로 한다.
{{ config(materialized='view') }}

select target_ym, origin_cd, dest_cd, day_type, hour_group, move_type, age_group,
       population, population_minutes, masked_rows, rows, collected_date
from {{ source('stg', 'living_commute') }}
