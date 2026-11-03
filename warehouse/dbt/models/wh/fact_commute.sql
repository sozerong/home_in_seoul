-- 한 행 = 대상연월 x 출발 x 도착(2021 코드) x 요일 x 도착시간 x 성별 x 나이. 범위는 결정 9(HW, 월~금, 07~09시, 서울↔서울).
-- 2025 행정동으로 옮기는 일은 마트에서 map_dong_2021로 한다 (합계가 두 배가 되지 않게).
{{ config(materialized='view') }}

select m.target_ym, m.origin_cd, m.dest_cd, m.weekday, m.arrive_hour, m.sex, m.age, m.avg_minutes, m.population,
       m.collected_date
from {{ source('stg', 'living_migration') }} m
join (select target_ym, max(collected_date) as collected_date
      from {{ source('stg', 'living_migration') }} group by target_ym) latest
  using (target_ym, collected_date)
