-- 한 행 = 요일 x 호선 x 역(원래 역번호) x 방향 x 30분 구간, 가장 최근 수집분. 역사_ID는 map_congestion_station으로 얻는다.
{{ config(materialized='view') }}

select c.dow, c.line, c.sttn_no, c.sttn_nm, c.direction, c.slot, c.congestion, c.collected_date
from {{ source('stg', 'congestion') }} c
where c.collected_date = (select max(collected_date) from {{ source('stg', 'congestion') }})
