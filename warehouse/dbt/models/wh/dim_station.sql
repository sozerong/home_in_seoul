-- 한 행 = 역(호선별) 하나, 역사마스터의 가장 최근 스냅샷.
{{ config(indexes=[{'columns': ['point'], 'type': 'gist'}]) }}

with station as (
    select bldn_id, bldn_nm, route, collected_date,
           ST_Transform(ST_SetSRID(ST_MakePoint(lot::float8, lat::float8), 4326), 5179)::geometry(Point, 5179) as point
    from {{ source('stg', 'station_master') }}
    where collected_date = (select max(collected_date) from {{ source('stg', 'station_master') }})
)

select s.bldn_id, s.bldn_nm, s.route, s.point, d.dong_id, s.collected_date as source_collected_date
from station s
left join {{ ref('dim_dong') }} d on ST_Contains(d.geom, s.point)
