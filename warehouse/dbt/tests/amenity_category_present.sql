-- 옛 MF3: 편의 업종의 (코드, 이름)이 가장 최근 상가 업소에 하나라도 없음. 분류 체계가 바뀌었으면 seed amenity_category를 고친다.
-- 옛 코드는 이 수집일 처리 전체를 실패시켰다. 배치 문제가 아니라 요구사항과 원천 분류가 어긋난 것이라 빌드를 멈춘다(결정 76과 같은 구분)
select a.sub_cd, a.sub_nm
from {{ ref('amenity_category') }} a
where not exists (
    select 1 from {{ ref('fact_store') }} f
    join {{ ref('store') }} s on s.collected_date = f.collected_date and s.store_id = f.store_id
    where f.sub_cd = a.sub_cd and s.sub_nm = a.sub_nm
)
