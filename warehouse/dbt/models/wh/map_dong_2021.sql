-- 한 행 = 2021 생활이동 코드 하나와 2025 행정동 하나의 짝. 분동된 동은 2행 (결정 13, data_sources.md 10절).
-- 바뀐 6개 동은 손 대응표(seed), 나머지는 "2021 코드 + '0' = 2025 코드" 규칙. 앞 7자리로 자르지 않아 11230510/11230511 충돌을 피한다.

with manual as (
    select dong_cd_2021, dong_id from {{ ref('map_dong_2021_manual') }}
),

code as (
    select origin_cd as code from {{ source('stg', 'living_migration') }}
    union
    select dest_cd from {{ source('stg', 'living_migration') }}
)

select dong_cd_2021, dong_id, 'manual'::text as method
from manual
union all
select c.code::char(7), d.dong_id, 'prefix'
from code c
join {{ ref('dim_dong') }} d on d.dong_id = c.code || '0'
where c.code not in (select dong_cd_2021 from manual)
