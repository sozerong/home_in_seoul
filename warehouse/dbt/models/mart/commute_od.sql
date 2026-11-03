-- 한 행 = 출발 행정동(2025) x 직장 행정동(2025). 평일 07~09시 도착, 야간상주지 -> 주간상주지(HW) 이동의 평균 이동 시간.
-- 2021 코드를 2025 동으로 옮길 때 분동된 동은 같은 값을 복사한다(결정 13). 평균만 복사하고, 이동 인구 합은 복사된 행에서 더하면 안 된다.

with by_code as (
    select origin_cd, dest_cd,
           sum(population * avg_minutes) filter (where age in ('20', '25', '30') and population is not null)
             / nullif(sum(population) filter (where age in ('20', '25', '30') and population is not null), 0) as avg_minutes_age20_34,
           coalesce(sum(population) filter (where age in ('20', '25', '30')), 0) as population_age20_34,
           sum(population * avg_minutes) filter (where population is not null)
             / nullif(sum(population) filter (where population is not null), 0) as avg_minutes_all_ages,
           coalesce(sum(population), 0) as population_all_ages,
           string_agg(distinct target_ym, ',' order by target_ym) as target_months
    from {{ ref('fact_commute') }}
    group by origin_cd, dest_cd
),

split as (
    select dong_cd_2021, count(*) > 1 as is_split from {{ ref('map_dong_2021') }} group by dong_cd_2021
)

select mo.dong_id as origin_dong_id, md.dong_id as dest_dong_id,
       round(b.avg_minutes_age20_34, 1) as avg_minutes_age20_34, round(b.population_age20_34, 2) as population_age20_34,
       round(b.avg_minutes_all_ages, 1) as avg_minutes_all_ages, round(b.population_all_ages, 2) as population_all_ages,
       -- 권장 값: 20~34세 가려지지 않은 이동인구 합이 30 이상이면 20~34세, 아니면 전체 나이 (기준 30, 결정 22. 2026-10-22 민감도 확인 후 유지)
       round(case when b.population_age20_34 >= 30 and b.avg_minutes_age20_34 is not null
                  then b.avg_minutes_age20_34 else b.avg_minutes_all_ages end, 1) as avg_minutes,
       case when b.population_age20_34 >= 30 and b.avg_minutes_age20_34 is not null then 'age20_34' else 'all_ages' end as basis,
       (so.is_split or sd.is_split) as copied_from_2021_split_dong,
       -- 결정 23: 생활이동 이동 시간은 근거리 직행 이동을 빼고 경유를 포함해 가까운 동네일수록 길게 나온다.
       --         두 행정동 안 대표점(결정 16) 사이의 직선거리를 함께 준다.
       round((ST_Distance(ST_PointOnSurface(dgo.geom), ST_PointOnSurface(dgd.geom)) / 1000)::numeric, 2) as distance_km,
       b.target_months
from by_code b
join {{ ref('map_dong_2021') }} mo on mo.dong_cd_2021 = b.origin_cd
join {{ ref('map_dong_2021') }} md on md.dong_cd_2021 = b.dest_cd
join {{ ref('dim_dong') }} dgo on dgo.dong_id = mo.dong_id
join {{ ref('dim_dong') }} dgd on dgd.dong_id = md.dong_id
join split so on so.dong_cd_2021 = b.origin_cd
join split sd on sd.dong_cd_2021 = b.dest_cd
where b.avg_minutes_all_ages is not null  -- 모든 행이 가려진(*) 경로는 평균을 낼 수 없어 뺀다
