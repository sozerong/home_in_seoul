-- 한 행 = 최근 6개월 신규 전월세 계약 한 건 (오피스텔, 연립다세대). 받는 쪽이 예산·면적으로 직접 거른다.
-- 부동산 플랫폼에 줄 데이터 상품의 원천 테이블(결정 12). 계산 방식은 docs/question.md 2, 3절.

-- 전월세 기간: 25개 구 x 2개 출처가 모두 들어온 계약월 중 가장 최근 6개월(question.md 2절 "최근 6개월").
-- 일부 구만 들어온 달(예: 시험으로 받은 202610)은 빼서, 구마다 기간이 달라지지 않게 한다.
with rent_months as (
    select deal_ymd from {{ ref('fact_rent') }}
    group by deal_ymd
    having count(distinct (source_type, sgg_cd)) = 50
    order by deal_ymd desc
    limit 6
)

select d.dong_id, d.mois_dong_cd, d.dong_nm, r.source_type, r.house_type,
       case when r.monthly_rent = 0 then '전세' else '월세' end as lease_type,
       r.deal_ymd, r.deal_date, r.deposit as deposit_10k_won, r.monthly_rent as monthly_rent_10k_won,
       r.excl_area as excl_area_m2, r.floor, r.build_year
from {{ ref('fact_rent') }} r
join {{ ref('dim_parcel') }} p using (pnu)
join {{ ref('dim_dong') }} d on d.dong_id = p.dong_id
where r.contract_type = '신규'                       -- question.md 2절: 신규 계약만
  and r.deal_ymd in (select deal_ymd from rent_months)
