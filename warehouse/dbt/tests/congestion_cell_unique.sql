-- CF2: (수집일, 요일, 호선, 역번호, 방향, 시각) 중복. 옛 stg.congestion의 기본 키와 같다
select collected_date, dow, line, sttn_no, direction, slot, count(*) as n
from {{ ref('congestion') }}
group by collected_date, dow, line, sttn_no, direction, slot
having count(*) > 1
