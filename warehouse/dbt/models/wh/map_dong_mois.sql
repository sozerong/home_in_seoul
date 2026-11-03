-- 한 행 = 행정안전부 행정동 코드 하나와 그 코드가 속한 2025 행정동 (결정 25). 상가정보 업소 다수결로 만든다.
-- 동마다 1위 코드는 is_primary. 어느 동에서도 1위가 아닌 코드는 그 코드 업소가 가장 많은 동에 붙인다(용신동 = 신설동 + 용두동).
-- 1위가 아닌 나머지(경계 근처 업소의 이웃 동 코드)는 넣지 않는다. 그래서 코드 하나는 동 하나에만 이어진다.

with vote as (
    select dong_id, mois_dong_cd, count(*) as stores,
           count(*)::numeric / sum(count(*)) over (partition by dong_id) as share,
           row_number() over (partition by dong_id order by count(*) desc, mois_dong_cd) as rank
    from {{ ref('fact_store') }}
    where dong_id is not null
    group by dong_id, mois_dong_cd
),

ranked as (
    select v.*, row_number() over (partition by mois_dong_cd order by stores desc, dong_id) as rank_in_code
    from vote v
)

select mois_dong_cd, dong_id, stores::integer, round(share, 3)::numeric(4, 3) as share, rank = 1 as is_primary
from ranked
where rank = 1
   or (rank_in_code = 1 and mois_dong_cd not in (select mois_dong_cd from vote where rank = 1))
