-- 09 검사: commute_od에 (출발, 직장) 중복이 없어야 한다
select origin_dong_id, dest_dong_id from {{ ref('commute_od') }} group by 1, 2 having count(*) > 1
