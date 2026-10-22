-- 단계 9 확인용 시연(결정 12): 받는 쪽(부동산 플랫폼)이 이 데이터로 질문에 답하는 방법의 예.
-- 맨 위 params만 바꿔서 실행한다. 마트(mart.*)만 읽는다.
-- 실행: docker compose exec -T db psql -U seoul -d seoul < warehouse/sql/demo.sql
WITH params AS (
    SELECT '역삼1동'::text AS work_dong_nm,     -- 직장 행정동 이름 (이름이 겹치면 work_sgg_cd로 고른다)
           '11230'::text   AS work_sgg_cd,      -- 직장 시군구 (통계청 5자리, 강남구 = 11230)
           '월세'::text    AS lease_type,       -- 월세 / 전세
           3000            AS max_deposit,      -- 보증금 상한 (만원)
           80              AS max_monthly_rent, -- 월세 상한 (만원). 전세면 무시
           40              AS max_area_m2,      -- 전용면적 상한 (㎡)
           50              AS max_minutes,      -- 출퇴근 시간 한도 (분)
           3               AS near_km,          -- 이 직선거리 안이면 이동 시간과 관계없이 통과 (결정 23)
           3               AS min_deals         -- 예산 안 거래가 이만큼은 있어야 "구할 수 있는 동네"로 본다
),
work AS (
    SELECT d.dong_id FROM mart.dong_profile d, params p
    WHERE d.dong_nm = p.work_dong_nm AND d.sgg_cd = p.work_sgg_cd
),
fit AS (  -- 예산과 면적 조건을 만족한 최근 6개월 신규 계약
    SELECT r.dong_id, count(*) AS deals_in_budget,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY r.monthly_rent_10k_won) AS median_rent_in_budget,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY r.deposit_10k_won) AS median_deposit_in_budget
    FROM mart.rent_listing r, params p
    WHERE r.lease_type = p.lease_type
      AND r.deposit_10k_won <= p.max_deposit
      AND (p.lease_type = '전세' OR r.monthly_rent_10k_won <= p.max_monthly_rent)
      AND r.excl_area_m2 <= p.max_area_m2
    GROUP BY r.dong_id
)
SELECT d.dong_nm, d.mois_dong_cd,
       c.avg_minutes AS commute_minutes, c.distance_km, c.basis AS commute_basis,
       f.deals_in_budget, f.median_deposit_in_budget, f.median_rent_in_budget,
       d.nearest_station_nm, d.nearest_station_distance_m,
       d.nearest_congestion_station_nm, d.am_peak_congestion,
       d.convenience_store_per_km2, d.pharmacy_per_km2, d.laundry_per_km2
FROM mart.commute_od c
JOIN work w ON w.dong_id = c.dest_dong_id
JOIN mart.dong_profile d ON d.dong_id = c.origin_dong_id
JOIN fit f ON f.dong_id = c.origin_dong_id
CROSS JOIN params p
WHERE (c.avg_minutes <= p.max_minutes OR c.distance_km <= p.near_km)  -- 필터 1: 출퇴근 한도 또는 가까운 거리 (결정 23)
  AND f.deals_in_budget >= p.min_deals        -- 필터 2: 예산 안 거래가 충분한 동네
ORDER BY c.distance_km, f.median_rent_in_budget  -- 직선거리 순 (속도 같은 가정을 넣지 않는다)
LIMIT 20;
