-- 한 행 = 행정동. 필수 5종 업소 수와 면적당 밀도(question.md 3절 생활 편의).

with s as (
    select dong_id, sub_cd from {{ ref('fact_store') }}
    where collected_date = (select max(collected_date) from {{ ref('fact_store') }}) and dong_id is not null
)

select d.dong_id, d.area_km2,
       count(s.sub_cd) filter (where s.sub_cd = 'G20405') as convenience_store,
       count(s.sub_cd) filter (where s.sub_cd = 'G20404') as supermarket,
       count(s.sub_cd) filter (where s.sub_cd = 'G21501') as pharmacy,
       count(s.sub_cd) filter (where s.sub_cd = 'Q10201') as internal_pediatric_clinic,
       count(s.sub_cd) filter (where s.sub_cd in ('S20901', 'S20902')) as laundry
from {{ ref('dim_dong') }} d
left join s using (dong_id)
group by d.dong_id, d.area_km2
