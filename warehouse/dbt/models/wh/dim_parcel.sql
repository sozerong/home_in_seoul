-- 한 행 = 서울 필지 하나(연속지적도 가장 최근 수집분). 폴리곤 대신 도형 안 대표점만 둔다 (결정 16).
{{ config(indexes=[{'columns': ['point'], 'type': 'gist'}, {'columns': ['dong_id']}]) }}

with parcel as (
    select pnu, collected_date, ST_Transform(ST_PointOnSurface(geom), 5179)::geometry(Point, 5179) as point
    from {{ source('stg', 'parcel') }}
    where collected_date = (select max(collected_date) from {{ source('stg', 'parcel') }})
)

select p.pnu, p.point, d.dong_id, p.collected_date as source_collected_date
from parcel p
left join {{ ref('dim_dong') }} d on ST_Contains(d.geom, p.point)
