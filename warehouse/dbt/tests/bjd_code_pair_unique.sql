-- 법정동 코드표: 한 (수집일, 시군구, 법정동명)에 코드가 둘. 옛 stg.bjd_code는 기본 키가 막아 적재가 실패했다
select collected_date, sgg_cd, bjd_nm, count(*) as n
from {{ ref('bjd_code') }}
group by collected_date, sgg_cd, bjd_nm
having count(*) > 1
