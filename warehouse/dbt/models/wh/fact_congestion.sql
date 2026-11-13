-- 한 행 = 요일 x 호선 x 역(원래 역번호) x 방향 x 30분 구간, 가장 최근 수집분. 역사_ID는 map_congestion_station으로 얻는다.
{{ config(materialized='view') }}

select c.dow, c.line, c.sttn_no, c.sttn_nm, c.direction, c.slot, c.congestion, c.collected_date
from {{ ref('congestion') }} c  -- stg_dbt: 원본(raw.congestion)을 SQL로 정제한 것 (결정 42, 2026-11-13 이전)
where c.collected_date = (select max(collected_date) from {{ ref('congestion') }})
