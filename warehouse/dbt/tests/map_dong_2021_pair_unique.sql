-- 07의 PRIMARY KEY (dong_cd_2021, dong_id)
select dong_cd_2021, dong_id from {{ ref('map_dong_2021') }} group by 1, 2 having count(*) > 1
