-- 전월세 배치 관문 (결정 75~77). 한 행 = 처리 단위(출처, 수집일, 구, 계약년월). 정제(rent, rent_quarantine)는 이 모델만 읽는다.
-- 출처마다 관문을 나눈다(failure_analysis F-7 처방 A, 결정 97).
-- 열: source, batch_key, collected_date, period, approved, failed_checks, checked_at (운영 카드용 합집합 batch_status와 같음)
with rent_raw as (  -- F1(1) 쓰는 키 없음 또는 별칭 둘 이상
    select r.source_type, r.collected_date, r.sgg_cd, r.deal_ymd,
           count(*) filter (where false
           {%- for kind in ['offi', 'rh'] %}
           {%- for group in rent_used_keys(kind) %}
              or (r.source_type = '{{ kind }}' and not (r.raw ?| array['{{ group | join("', '") }}']))
              {%- if group | length > 1 %}
              or (r.source_type = '{{ kind }}' and (select count(*) from unnest(array['{{ group | join("', '") }}']) k where r.raw ? k) > 1)
              {%- endif %}
           {%- endfor %}
           {%- endfor %}) as missing_keys
    from {{ source('raw', 'rent') }} r
    group by r.source_type, r.collected_date, r.sgg_cd, r.deal_ymd
),
rent_ratio as (  -- F2: 처리 단위의 격리 비율. 옛 테스트는 키마다 가장 최근 단위만 봤다
    select source_type, collected_date, sgg_cd, deal_ymd, count(*) as items, count(*) filter (where reasons <> '') as bad
    from {{ ref('rent_rows') }}
    group by source_type, collected_date, sgg_cd, deal_ymd
),
rent as (
    select r.source_type, r.collected_date, r.sgg_cd, r.deal_ymd,
           array_remove(array[
               case when r.missing_keys > 0 then 'rent_raw_keys_present' end,
               case when q.bad::numeric / q.items > 0.05 then 'rent_quarantine_ratio' end
           ], null)::text[] as failed_checks
    from rent_raw r
    join rent_ratio q using (source_type, collected_date, sgg_cd, deal_ymd)
)
select 'rent' as source, {{ rent_batch_key() }} as batch_key, collected_date, deal_ymd as period,
       cardinality(failed_checks) = 0 as approved, failed_checks, now() as checked_at
from rent
