-- 한 행 = 신고된 전월세 계약 한 건 (오피스텔, 연립다세대). 행정동은 pnu로 dim_parcel에서 얻는다.
-- 같은 처리 단위가 여러 수집일에 있으면 가장 최근 수집분만 본다. 한 번의 수집이 그 단위 전체를 담기 때문이다(결정 15).
{{ config(materialized='view') }}

select r.source_type, r.sgg_cd, r.deal_ymd, r.deal_date, r.pnu, r.umd_nm, r.jibun, r.building_name,
       r.house_type, r.build_year, r.floor, r.excl_area, r.deposit, r.monthly_rent, r.contract_type,
       r.collected_date, r.source_file, r.item_no
from {{ source('stg', 'rent') }} r
join (select source_type, sgg_cd, deal_ymd, max(collected_date) as collected_date
      from {{ source('stg', 'rent') }} group by source_type, sgg_cd, deal_ymd) latest
  using (source_type, sgg_cd, deal_ymd, collected_date)
