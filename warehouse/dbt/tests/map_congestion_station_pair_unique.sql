-- 07의 PRIMARY KEY (line, sttn_no)
select line, sttn_no from {{ ref('map_congestion_station') }} group by 1, 2 having count(*) > 1
