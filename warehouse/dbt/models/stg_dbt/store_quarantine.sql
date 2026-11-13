-- 검증에 어긋난 상가 업소. 원본 행을 그대로 남긴다. 옛 stg.sbiz_store_quarantine과 같은 모양. quarantined_at은 빌드 시각이다.
select collected_date, source_file, row_no, string_to_array(reasons, ',')::text[] as reasons, raw, now() as quarantined_at
from {{ ref('store_rows') }}
where reasons <> ''
  and concat_ws('/', collected_date, source_file) in (select batch_key from {{ ref('batch_status_store') }} where source = 'store' and approved)  -- 관문(결정 75)
