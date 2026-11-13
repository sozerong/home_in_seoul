-- 상가정보 배치(수집일, 원천 파일)의 관문 검사 (결정 75~78). batch_status가 이 뷰를 그대로 합친다.
-- 따로 둔 이유: 법정동 코드표(bjd_code)도 승인 배치만 써야 하는데, 전월세 정제가 코드표를 읽고 batch_status가 전월세 정제를
-- 읽으므로 batch_status 하나에 모으면 순환이 된다(judgment_log 47).
-- 검사: 쓰는 키 없음(옛 MF2를 쓰는 키로 줄임), 파일 이름에 기준월 없음(옛 read_seoul), 행 수 급감(MF5), 격리 비율 5% 초과(MF4).
-- 옛 MF4·MF5는 6코드 행 기준이었고 지금은 서울 전체 업소 기준이다(결정 78).
{{ config(materialized='view') }}

with raw_rows as (
    select r.collected_date, r.source_file, count(*) as raw_rows,
           count(*) filter (where false
           {%- for group in sbiz_used_keys() %}
              or not (r.raw ?| array['{{ group | join("', '") }}'])
           {%- endfor %}) as missing_keys
    from {{ source('raw', 'sbiz_store') }} r
    group by r.collected_date, r.source_file
),
ratio as (
    select collected_date, source_file, count(*) filter (where reasons <> '') as bad, count(*) as total,
           max(base_ym) as base_ym
    from {{ ref('store_rows') }}
    group by collected_date, source_file
)
select r.collected_date, r.source_file, q.base_ym,
       array_remove(array[
           case when r.missing_keys > 0 then 'store_raw_keys_present' end,
           case when q.base_ym is null then 'store_file_base_ym' end,
           case when r.raw_rows < lag(r.raw_rows) over (order by r.collected_date, r.source_file) * 0.9 then 'store_raw_shrink' end,
           case when q.bad::numeric / q.total > 0.05 then 'store_quarantine_ratio' end
       ], null)::text[] as failed_checks
from raw_rows r
join ratio q using (collected_date, source_file)
