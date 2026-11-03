-- 09 검사: dong_profile 행 수가 행정동 수와 같아야 한다
select (select count(*) from {{ ref('dong_profile') }}) as profile_rows, (select count(*) from {{ ref('dim_dong') }}) as dong_rows
where (select count(*) from {{ ref('dong_profile') }}) <> (select count(*) from {{ ref('dim_dong') }})
