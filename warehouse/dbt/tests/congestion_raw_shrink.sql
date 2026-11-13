-- CF4: 가장 최근 수집일의 원본 row 수가 직전 수집일보다 10% 넘게 줄었음. 최근 것만 본다.
-- 하류(wh.fact_congestion)는 가장 최근 수집일만 쓰므로, 그 뒤 정상 수집일이 오면 줄었던 날은 더 쓰이지 않는다
with days as (
    select collected_date, count(*) as n, lag(count(*)) over (order by collected_date) as previous
    from {{ source('raw', 'congestion') }}
    group by collected_date
)
select * from days
where collected_date = (select max(collected_date) from days) and n < previous * 0.9
