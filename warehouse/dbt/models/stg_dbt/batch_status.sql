-- 배치 관문 (결정 75~77). 한 행 = (출처, 원천 배치). 혼잡도 배치는 수집일, 전월세 배치는 처리 단위(출처, 수집일, 구, 계약년월),
-- 상가정보 배치는 (수집일, 원천 파일)이고 검사는 store_batch_checks에서 가져온다(순환을 피해 따로 둠).
-- 데이터 문제는 그 배치만 거절한다: 걸린 검사가 하나라도 있으면 approved = false이고, 정제 결과(congestion, rent)와
-- 격리(*_quarantine)는 승인된 배치만 읽는다. 하류는 키마다 가장 최근 수집일을 고르므로 거절되면 직전 승인 배치가 쓰인다.
-- 검사 이름은 옮기기 전 dbt 테스트 이름 그대로다(결정 76). 매 빌드 지금 상태만 계산한다(이력 없음, 결정 77).
-- 거절 사유를 보려면 stg_dbt.congestion_rows, stg_dbt.rent_rows(뷰, 거르지 않음)에서 그 배치를 찾는다.
with cong_raw as (  -- CF1(1) 쓰는 키 없음 또는 별칭 둘 이상, CF4 재료
    select r.collected_date, count(*) as raw_rows,
           count(*) filter (where false
           {%- for group in congestion_id_keys() + congestion_time_keys() %}
              or not (r.raw ?| array['{{ group | join("', '") }}'])
              {%- if group | length > 1 %}
              or (select count(*) from unnest(array['{{ group | join("', '") }}']) k where r.raw ? k) > 1
              {%- endif %}
           {%- endfor %}) as missing_keys
    from {{ source('raw', 'congestion') }} r
    group by r.collected_date
),
cong_shrink as (  -- CF4: 직전 수집일(승인 여부와 무관)보다 원본 row가 10% 넘게 줄었음. 옛 테스트는 최근 수집일만 봤다
    select collected_date, raw_rows < lag(raw_rows) over (order by collected_date) * 0.9 as shrunk
    from cong_raw
),
cong_bad as (  -- CF3: 행 단위 격리(CV1~CV4)는 39칸으로 센다(옛 transform/congestion.py와 같다)
    select collected_date, 39 * count(*) as cells from {{ ref('congestion_rows') }}
    where cardinality(reasons) > 0 group by collected_date
),
cong_cells as (  -- CF3 칸 단위(CV5)와 통과 칸, CF2 같은 칸 중복. 키(TIME0730)와 시각(07:30)은 일대일이라 키로 센다
    select collected_date,
           count(*) filter (where number between 0 and 300) as good_cells,
           count(*) filter (where number is null or number not between 0 and 300) as bad_cells
    from {{ ref('congestion_cells') }} group by collected_date
),
cong_dup as (
    select collected_date, count(*) as dup_cells from (
        select collected_date from {{ ref('congestion_cells') }}
        where number between 0 and 300
        group by collected_date, dow, line, sttn_no, direction, key
        having count(*) > 1
    ) d group by collected_date
),
cong as (
    select r.collected_date,
           array_remove(array[
               case when r.missing_keys > 0 then 'congestion_raw_keys_present' end,
               case when s.shrunk then 'congestion_raw_shrink' end,
               case when coalesce(d.dup_cells, 0) > 0 then 'congestion_cell_unique' end,
               case when (coalesce(b.cells, 0) + coalesce(c.bad_cells, 0))::numeric
                         / nullif(coalesce(b.cells, 0) + coalesce(c.bad_cells, 0) + coalesce(c.good_cells, 0), 0) > 0.05
                    then 'congestion_quarantine_ratio' end
           ], null)::text[] as failed_checks
    from cong_raw r
    join cong_shrink s using (collected_date)
    left join cong_bad b using (collected_date)
    left join cong_cells c using (collected_date)
    left join cong_dup d using (collected_date)
),
rent_raw as (  -- F1(1) 쓰는 키 없음 또는 별칭 둘 이상
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
select 'congestion' as source, collected_date::text as batch_key, collected_date, null::text as period,
       cardinality(failed_checks) = 0 as approved, failed_checks, now() as checked_at
from cong
union all
select 'rent', {{ rent_batch_key() }}, collected_date, deal_ymd,
       cardinality(failed_checks) = 0, failed_checks, now()
from rent
union all
select 'store', concat_ws('/', collected_date, source_file), collected_date, base_ym,
       cardinality(failed_checks) = 0, failed_checks, now()
from {{ ref('store_batch_checks') }}
