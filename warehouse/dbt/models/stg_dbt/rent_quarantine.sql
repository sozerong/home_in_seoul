-- 검증에 어긋난 전월세 item. 원본 값을 문자열 그대로 남긴다. 옛 stg.rent_quarantine과 같은 모양. quarantined_at은 빌드 시각이다.
select source_type, collected_date, source_file, item_no, deal_ymd, sgg_cd,
       string_to_array(reasons, ',')::text[] as reasons, raw, now() as quarantined_at
from {{ ref('rent_rows') }}
where reasons <> ''
  and {{ rent_batch_key() }} in (select batch_key from {{ ref('batch_status') }} where source = 'rent' and approved)  -- 관문(결정 75)
