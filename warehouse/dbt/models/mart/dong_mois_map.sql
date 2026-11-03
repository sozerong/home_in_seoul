-- 한 행 = 행정안전부 행정동 코드 하나 (결정 25). 받는 쪽이 행정안전부 코드로 이을 때 쓴다. 용신동만 코드가 둘이다.

select m.mois_dong_cd, m.dong_id, d.dong_nm, m.is_primary, m.stores, m.share
from {{ ref('map_dong_mois') }} m
join {{ ref('dim_dong') }} d using (dong_id)
