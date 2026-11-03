-- 한 행 = 필수 6코드 업소 하나(상가정보 가장 최근 수집분). 서울 행정동 경계 밖이면 dong_id가 NULL.
-- mois_dong_cd는 업소 원본의 행정안전부 행정동 코드로, map_dong_mois와 dim_dong이 다수결에 쓴다 (결정 31 E1).
-- 상가정보 좌표는 도 단위 경위도라 WGS84(EPSG:4326)로 보고 5179로 바꾼다 (data_sources.md 13절, [확인 필요]).
{{ config(indexes=[{'columns': ['dong_id']}]) }}

with boundary as (
    select adm_cd, geom
    from {{ source('stg', 'sgis_dong') }}
    where collected_date = (select max(collected_date) from {{ source('stg', 'sgis_dong') }})
),

store as (
    select collected_date, store_id, sub_cd, base_ym, adm_cd as mois_dong_cd,
           ST_Transform(ST_SetSRID(ST_MakePoint(lon, lat), 4326), 5179)::geometry(Point, 5179) as point
    from {{ source('stg', 'sbiz_store') }}
    where collected_date = (select max(collected_date) from {{ source('stg', 'sbiz_store') }})
)

select s.collected_date, s.store_id, s.sub_cd, s.base_ym, s.point, b.adm_cd as dong_id, s.mois_dong_cd
from store s
left join boundary b on ST_Contains(b.geom, s.point)
