-- 09 검사: 25개 구가 모두 들어온 전월세 계약월이 없으면 rent_listing이 비고 상품의 주거비가 모두 빈다
select 1 as no_rows where not exists (select 1 from {{ ref('rent_listing') }})
