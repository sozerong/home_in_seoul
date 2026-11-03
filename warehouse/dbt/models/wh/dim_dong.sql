-- 한 행 = 2025 SGIS 기준 서울 행정동 하나. 이 웨어하우스의 중심(순응 차원). SGIS 경계의 가장 최근 수집분.
-- mois_dong_cd는 그 동 업소의 1위 행정안전부 코드 (결정 13, 25).
{{ config(indexes=[{'columns': ['geom'], 'type': 'gist'}]) }}

select d.adm_cd as dong_id,
       d.adm_nm as dong_nm,
       left(d.adm_cd, 5)::char(5) as sgg_cd,
       m.mois_dong_cd,
       d.base_date as boundary_base_date,
       d.collected_date as source_collected_date,
       round((ST_Area(d.geom) / 1e6)::numeric, 3)::numeric(8, 3) as area_km2,
       d.geom
from {{ source('stg', 'sgis_dong') }} d
left join {{ ref('map_dong_mois') }} m on m.dong_id = d.adm_cd and m.is_primary
where d.collected_date = (select max(collected_date) from {{ source('stg', 'sgis_dong') }})
