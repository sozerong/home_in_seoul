-- 혼잡도 배치 관문 (결정 75~77). 한 행 = 수집일. 정제(congestion, congestion_quarantine)는 이 모델만 읽는다.
-- 출처마다 관문을 나눠, 다른 출처 원본의 변화가 리니지에서 이 출처 정제에 닿지 않게 한다(failure_analysis F-7 처방 A, 결정 97).
-- 열: source, batch_key, collected_date, period, approved, failed_checks, checked_at (운영 카드용 합집합 batch_status와 같음)
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
)
select 'congestion' as source, collected_date::text as batch_key, collected_date, null::text as period,
       cardinality(failed_checks) = 0 as approved, failed_checks, now() as checked_at
from cong
