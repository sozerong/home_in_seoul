-- 한 행 = 행정동. 받는 쪽이 동네 하나를 보여 줄 때 쓰는 요약(출퇴근은 직장에 따라 달라서 commute_od로 따로 준다).

select d.dong_id, d.mois_dong_cd, d.dong_nm, d.sgg_cd, d.area_km2, d.boundary_base_date,
       mr.deals as monthly_deals, mr.median_deposit_10k_won as monthly_median_deposit_10k_won,
       mr.median_monthly_rent_10k_won as monthly_median_rent_10k_won,
       jr.deals as jeonse_deals, jr.median_deposit_10k_won as jeonse_median_deposit_10k_won,
       st.nearest_station_nm, st.nearest_station_route, st.nearest_station_distance_m,
       st.nearest_congestion_station_nm, st.nearest_congestion_station_route, st.nearest_congestion_station_distance_m,
       st.am_peak_congestion,
       a.convenience_store, a.supermarket, a.pharmacy, a.internal_pediatric_clinic, a.laundry,
       round(a.convenience_store / d.area_km2, 1) as convenience_store_per_km2,
       round(a.supermarket / d.area_km2, 1) as supermarket_per_km2,
       round(a.pharmacy / d.area_km2, 1) as pharmacy_per_km2,
       round(a.internal_pediatric_clinic / d.area_km2, 1) as internal_pediatric_clinic_per_km2,
       round(a.laundry / d.area_km2, 1) as laundry_per_km2
from {{ ref('dim_dong') }} d
left join {{ ref('dong_rent_summary') }} mr on mr.dong_id = d.dong_id and mr.lease_type = '월세'
left join {{ ref('dong_rent_summary') }} jr on jr.dong_id = d.dong_id and jr.lease_type = '전세'
left join {{ ref('dong_station') }} st on st.dong_id = d.dong_id
left join {{ ref('dong_amenity') }} a on a.dong_id = d.dong_id
