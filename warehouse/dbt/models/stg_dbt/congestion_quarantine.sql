-- 격리. 행 단위(CV1~CV4)는 원본 row 하나에 한 줄(raw 전체), 칸 단위(CV5)는 칸 하나에 한 줄(신원 필드 5개 + 그 칸, 원문 그대로).
-- 옛 stg.congestion_quarantine과 같은 모양. quarantined_at은 빌드 시각이다.
select r.collected_date, r.source_file, r.row_no, r.reasons, r.raw, now() as quarantined_at
from {{ ref('congestion_rows') }} r
where cardinality(r.reasons) > 0
  and r.collected_date in (select collected_date from {{ ref('batch_status_congestion') }} where source = 'congestion' and approved)  -- 관문(결정 75)
union all
select c.collected_date, c.source_file, c.row_no, array['CV5']::text[],
       jsonb_build_object('DOW_SE', c.raw -> 'DOW_SE', 'LINE', c.raw -> 'LINE', 'STTN_NO', c.raw -> 'STTN_NO',
                          'DPTRE_STTN', c.raw -> 'DPTRE_STTN', 'UP_DOWN_SE', c.raw -> 'UP_DOWN_SE', c.key, c.raw -> c.key),
       now()
from {{ ref('congestion_cells') }} c
where (c.number is null or c.number not between 0 and 300)
  and c.collected_date in (select collected_date from {{ ref('batch_status_congestion') }} where source = 'congestion' and approved)
