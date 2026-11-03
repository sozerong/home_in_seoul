-- 08 검사: 역사_ID에 잇지 못한 혼잡도 역
select distinct c.line, c.sttn_no
from {{ ref('fact_congestion') }} c
left join {{ ref('map_congestion_station') }} m using (line, sttn_no)
where m.line is null
