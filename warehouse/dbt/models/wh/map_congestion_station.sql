-- 한 행 = 혼잡도의 (호선, 역번호) 하나와 역사마스터 역사_ID (결정 17).
-- 0 채우기는 호선까지 같아야 인정한다(9001이 GTX 역에 붙는 것을 막는다). 특수 번호와 예외는 손 대응표(seed).

select distinct c.line, c.sttn_no, s.bldn_id, 'padded'::text as method, null::text as note
from {{ ref('fact_congestion') }} c
join {{ ref('dim_station') }} s on s.bldn_id = lpad(c.sttn_no, 4, '0') and s.route = c.line
union all
select line, sttn_no, bldn_id, 'manual', note
from {{ ref('congestion_station_manual') }}
