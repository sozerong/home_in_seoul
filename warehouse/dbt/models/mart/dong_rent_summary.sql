-- 한 행 = 행정동 x 월세/전세. 면적 조건 없이 요약한 시세. 면적을 거른 시세는 rent_listing으로 받는 쪽이 계산한다.

select dong_id, lease_type, count(*) as deals,
       percentile_cont(0.5) within group (order by deposit_10k_won) as median_deposit_10k_won,
       percentile_cont(0.5) within group (order by monthly_rent_10k_won) as median_monthly_rent_10k_won,
       percentile_cont(0.5) within group (order by excl_area_m2) as median_excl_area_m2,
       min(deal_ymd) as first_ym, max(deal_ymd) as last_ym
from {{ ref('rent_listing') }}
group by dong_id, lease_type
