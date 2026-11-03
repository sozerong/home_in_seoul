-- 한 행 = 행정동. 행정동 안 대표점(결정 16)에서 가장 가까운 역과, 혼잡도 자료가 있는 가장 가까운 역의 평일 07:00~09:00 최대 혼잡도.
-- question.md 6절에 미뤄 둔 "전체 역 vs 1~8호선 역"은 두 가지를 모두 준다.

with c as (
    select dong_id, ST_PointOnSurface(geom) as p from {{ ref('dim_dong') }}
),

covered as (
    select distinct bldn_id from {{ ref('map_congestion_station') }}
),

peak as (
    select m.bldn_id, max(f.congestion) as am_peak_congestion
    from {{ ref('fact_congestion') }} f
    join {{ ref('map_congestion_station') }} m using (line, sttn_no)
    where f.dow = '평일' and f.slot between '07:00' and '08:30'  -- 30분 구간 4개: 07:00, 07:30, 08:00, 08:30
    group by m.bldn_id
)

select c.dong_id,
       a.bldn_id as nearest_station_id, a.bldn_nm as nearest_station_nm, a.route as nearest_station_route,
       round(ST_Distance(a.point, c.p)::numeric) as nearest_station_distance_m,
       k.bldn_id as nearest_congestion_station_id, k.bldn_nm as nearest_congestion_station_nm, k.route as nearest_congestion_station_route,
       round(ST_Distance(k.point, c.p)::numeric) as nearest_congestion_station_distance_m,
       pk.am_peak_congestion,
       (select max(collected_date) from {{ ref('fact_congestion') }}) as congestion_collected_date
from c
cross join lateral (select s.* from {{ ref('dim_station') }} s order by s.point <-> c.p limit 1) a
cross join lateral (select s.* from {{ ref('dim_station') }} s where s.bldn_id in (select bldn_id from covered)
                    order by s.point <-> c.p limit 1) k
left join peak pk on pk.bldn_id = k.bldn_id
