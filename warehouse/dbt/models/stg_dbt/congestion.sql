-- 한 행 = 수집일 x 요일 x 호선 x 역(원래 역번호) x 방향 x 30분 구간. 값이 0~300인 칸만(CV5).
-- 옛 stg.congestion(transform/congestion.py)과 컬럼 이름, 순서, 타입이 같다(설계서 2.3). 매번 전체를 다시 만든다(결정 62).
select c.collected_date, c.source_file, c.row_no, c.dow, c.line, c.sttn_no,
       lpad(c.sttn_no, 4, '0')::char(4) as sttn_no4,  -- CC2
       c.sttn_nm, c.direction,
       make_time(substr(c.key, 5, 2)::int, substr(c.key, 7, 2)::int, 0) as slot,  -- CC4: TIME0730 -> 07:30
       c.number::numeric(5, 1) as congestion
from {{ ref('congestion_cells') }} c
where c.number between 0 and 300
  and c.collected_date in (select collected_date from {{ ref('batch_status') }} where source = 'congestion' and approved)  -- 관문(결정 75)
