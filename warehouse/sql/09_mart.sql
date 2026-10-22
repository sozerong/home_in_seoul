-- 단계 9 마트: 부동산 플랫폼에 줄 데이터 상품의 원천 테이블(결정 12). 계산 방식은 docs/question.md 2, 3절.
-- 한 트랜잭션으로 mart 스키마의 테이블을 모두 다시 만든다(멱등). 웨어하우스(wh)만 읽는다.
BEGIN;
CREATE SCHEMA IF NOT EXISTS mart;

-- 전월세 기간: 25개 구 x 2개 출처가 모두 들어온 계약월 중 가장 최근 6개월(question.md 2절 "최근 6개월").
-- 일부 구만 들어온 달(예: 시험으로 받은 202610)은 빼서, 구마다 기간이 달라지지 않게 한다.
CREATE TEMP TABLE rent_months ON COMMIT DROP AS
SELECT deal_ymd FROM (
    SELECT deal_ymd FROM wh.fact_rent GROUP BY deal_ymd
    HAVING count(DISTINCT (source_type, sgg_cd)) = 50
    ORDER BY deal_ymd DESC LIMIT 6) m;

-- 1. 한 행 = 최근 6개월 신규 전월세 계약 한 건 (오피스텔, 연립다세대). 받는 쪽이 예산·면적으로 직접 거른다.
DROP TABLE IF EXISTS mart.rent_listing;
CREATE TABLE mart.rent_listing AS
SELECT d.dong_id, d.mois_dong_cd, d.dong_nm, r.source_type, r.house_type,
       CASE WHEN r.monthly_rent = 0 THEN '전세' ELSE '월세' END AS lease_type,
       r.deal_ymd, r.deal_date, r.deposit AS deposit_10k_won, r.monthly_rent AS monthly_rent_10k_won,
       r.excl_area AS excl_area_m2, r.floor, r.build_year
FROM wh.fact_rent r
JOIN wh.dim_parcel p USING (pnu)
JOIN wh.dim_dong d ON d.dong_id = p.dong_id
WHERE r.contract_type = '신규'                       -- question.md 2절: 신규 계약만
  AND r.deal_ymd IN (SELECT deal_ymd FROM rent_months);

-- 2. 한 행 = 행정동 x 월세/전세. 면적 조건 없이 요약한 시세. 면적을 거른 시세는 rent_listing으로 받는 쪽이 계산한다.
DROP TABLE IF EXISTS mart.dong_rent_summary;
CREATE TABLE mart.dong_rent_summary AS
SELECT dong_id, lease_type, count(*) AS deals,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY deposit_10k_won) AS median_deposit_10k_won,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY monthly_rent_10k_won) AS median_monthly_rent_10k_won,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY excl_area_m2) AS median_excl_area_m2,
       min(deal_ymd) AS first_ym, max(deal_ymd) AS last_ym
FROM mart.rent_listing
GROUP BY dong_id, lease_type;

-- 3. 한 행 = 출발 행정동(2025) x 직장 행정동(2025). 평일 07~09시 도착, 야간상주지 -> 주간상주지(HW) 이동의 평균 이동 시간.
--    2021 코드를 2025 동으로 옮길 때 분동된 동은 같은 값을 복사한다(결정 13). 평균만 복사하고, 이동 인구 합은 복사된 행에서 더하면 안 된다.
DROP TABLE IF EXISTS mart.commute_od;
CREATE TABLE mart.commute_od AS
WITH by_code AS (
    SELECT origin_cd, dest_cd,
           sum(population * avg_minutes) FILTER (WHERE age IN ('20', '25', '30') AND population IS NOT NULL)
             / NULLIF(sum(population) FILTER (WHERE age IN ('20', '25', '30') AND population IS NOT NULL), 0) AS avg_minutes_age20_34,
           coalesce(sum(population) FILTER (WHERE age IN ('20', '25', '30')), 0) AS population_age20_34,
           sum(population * avg_minutes) FILTER (WHERE population IS NOT NULL)
             / NULLIF(sum(population) FILTER (WHERE population IS NOT NULL), 0) AS avg_minutes_all_ages,
           coalesce(sum(population), 0) AS population_all_ages,
           string_agg(DISTINCT target_ym, ',' ORDER BY target_ym) AS target_months
    FROM wh.fact_commute
    GROUP BY origin_cd, dest_cd),
split AS (SELECT dong_cd_2021, count(*) > 1 AS is_split FROM wh.map_dong_2021 GROUP BY dong_cd_2021)
SELECT mo.dong_id AS origin_dong_id, md.dong_id AS dest_dong_id,
       round(b.avg_minutes_age20_34, 1) AS avg_minutes_age20_34, round(b.population_age20_34, 2) AS population_age20_34,
       round(b.avg_minutes_all_ages, 1) AS avg_minutes_all_ages, round(b.population_all_ages, 2) AS population_all_ages,
       -- 권장 값: 20~34세 가려지지 않은 이동인구 합이 30 이상이면 20~34세, 아니면 전체 나이 (기준 30은 잠정, decisions.md 결정 22)
       round(CASE WHEN b.population_age20_34 >= 30 AND b.avg_minutes_age20_34 IS NOT NULL
                  THEN b.avg_minutes_age20_34 ELSE b.avg_minutes_all_ages END, 1) AS avg_minutes,
       CASE WHEN b.population_age20_34 >= 30 AND b.avg_minutes_age20_34 IS NOT NULL THEN 'age20_34' ELSE 'all_ages' END AS basis,
       (so.is_split OR sd.is_split) AS copied_from_2021_split_dong,
       -- 결정 23: 생활이동 이동 시간은 근거리 직행 이동을 빼고 경유를 포함해 가까운 동네일수록 길게 나온다.
       --         두 행정동 안 대표점(결정 16) 사이의 직선거리를 함께 준다.
       round((ST_Distance(ST_PointOnSurface(dgo.geom), ST_PointOnSurface(dgd.geom)) / 1000)::numeric, 2) AS distance_km,
       b.target_months
FROM by_code b
JOIN wh.map_dong_2021 mo ON mo.dong_cd_2021 = b.origin_cd
JOIN wh.map_dong_2021 md ON md.dong_cd_2021 = b.dest_cd
JOIN wh.dim_dong dgo ON dgo.dong_id = mo.dong_id
JOIN wh.dim_dong dgd ON dgd.dong_id = md.dong_id
JOIN split so ON so.dong_cd_2021 = b.origin_cd
JOIN split sd ON sd.dong_cd_2021 = b.dest_cd
WHERE b.avg_minutes_all_ages IS NOT NULL;  -- 모든 행이 가려진(*) 경로는 평균을 낼 수 없어 뺀다

-- 4. 한 행 = 행정동. 행정동 안 대표점(결정 16)에서 가장 가까운 역과, 혼잡도 자료가 있는 가장 가까운 역의 평일 07:00~09:00 최대 혼잡도.
--    question.md 6절에 미뤄 둔 "전체 역 vs 1~8호선 역"은 두 가지를 모두 준다.
DROP TABLE IF EXISTS mart.dong_station;
CREATE TABLE mart.dong_station AS
WITH c AS (SELECT dong_id, ST_PointOnSurface(geom) AS p FROM wh.dim_dong),
covered AS (SELECT DISTINCT bldn_id FROM wh.map_congestion_station),
peak AS (
    SELECT m.bldn_id, max(f.congestion) AS am_peak_congestion
    FROM wh.fact_congestion f
    JOIN wh.map_congestion_station m USING (line, sttn_no)
    WHERE f.dow = '평일' AND f.slot BETWEEN '07:00' AND '08:30'  -- 30분 구간 4개: 07:00, 07:30, 08:00, 08:30
    GROUP BY m.bldn_id)
SELECT c.dong_id,
       a.bldn_id AS nearest_station_id, a.bldn_nm AS nearest_station_nm, a.route AS nearest_station_route,
       round(ST_Distance(a.point, c.p)::numeric) AS nearest_station_distance_m,
       k.bldn_id AS nearest_congestion_station_id, k.bldn_nm AS nearest_congestion_station_nm, k.route AS nearest_congestion_station_route,
       round(ST_Distance(k.point, c.p)::numeric) AS nearest_congestion_station_distance_m,
       pk.am_peak_congestion,
       (SELECT max(collected_date) FROM wh.fact_congestion) AS congestion_collected_date
FROM c
CROSS JOIN LATERAL (SELECT s.* FROM wh.dim_station s ORDER BY s.point <-> c.p LIMIT 1) a
CROSS JOIN LATERAL (SELECT s.* FROM wh.dim_station s WHERE s.bldn_id IN (SELECT bldn_id FROM covered)
                    ORDER BY s.point <-> c.p LIMIT 1) k
LEFT JOIN peak pk ON pk.bldn_id = k.bldn_id;

-- 5. 한 행 = 행정동. 필수 5종 업소 수와 면적당 밀도(question.md 3절 생활 편의).
DROP TABLE IF EXISTS mart.dong_amenity;
CREATE TABLE mart.dong_amenity AS
WITH s AS (SELECT dong_id, sub_cd FROM wh.fact_store
           WHERE collected_date = (SELECT max(collected_date) FROM wh.fact_store) AND dong_id IS NOT NULL)
SELECT d.dong_id, d.area_km2,
       count(s.sub_cd) FILTER (WHERE s.sub_cd = 'G20405') AS convenience_store,
       count(s.sub_cd) FILTER (WHERE s.sub_cd = 'G20404') AS supermarket,
       count(s.sub_cd) FILTER (WHERE s.sub_cd = 'G21501') AS pharmacy,
       count(s.sub_cd) FILTER (WHERE s.sub_cd = 'Q10201') AS internal_pediatric_clinic,
       count(s.sub_cd) FILTER (WHERE s.sub_cd IN ('S20901', 'S20902')) AS laundry
FROM wh.dim_dong d LEFT JOIN s USING (dong_id)
GROUP BY d.dong_id, d.area_km2;

-- 6. 한 행 = 행정동. 받는 쪽이 동네 하나를 보여 줄 때 쓰는 요약(출퇴근은 직장에 따라 달라서 commute_od로 따로 준다).
DROP TABLE IF EXISTS mart.dong_profile;
CREATE TABLE mart.dong_profile AS
SELECT d.dong_id, d.mois_dong_cd, d.dong_nm, d.sgg_cd, d.area_km2, d.boundary_base_date,
       mr.deals AS monthly_deals, mr.median_deposit_10k_won AS monthly_median_deposit_10k_won,
       mr.median_monthly_rent_10k_won AS monthly_median_rent_10k_won,
       jr.deals AS jeonse_deals, jr.median_deposit_10k_won AS jeonse_median_deposit_10k_won,
       st.nearest_station_nm, st.nearest_station_route, st.nearest_station_distance_m,
       st.nearest_congestion_station_nm, st.nearest_congestion_station_route, st.nearest_congestion_station_distance_m,
       st.am_peak_congestion,
       a.convenience_store, a.supermarket, a.pharmacy, a.internal_pediatric_clinic, a.laundry,
       round(a.convenience_store / d.area_km2, 1) AS convenience_store_per_km2,
       round(a.supermarket / d.area_km2, 1) AS supermarket_per_km2,
       round(a.pharmacy / d.area_km2, 1) AS pharmacy_per_km2,
       round(a.internal_pediatric_clinic / d.area_km2, 1) AS internal_pediatric_clinic_per_km2,
       round(a.laundry / d.area_km2, 1) AS laundry_per_km2
FROM wh.dim_dong d
LEFT JOIN mart.dong_rent_summary mr ON mr.dong_id = d.dong_id AND mr.lease_type = '월세'
LEFT JOIN mart.dong_rent_summary jr ON jr.dong_id = d.dong_id AND jr.lease_type = '전세'
LEFT JOIN mart.dong_station st ON st.dong_id = d.dong_id
LEFT JOIN mart.dong_amenity a ON a.dong_id = d.dong_id;

-- ===== 구조 검사 =====
DO $$
DECLARE n bigint;
BEGIN
    SELECT count(*) INTO n FROM rent_months;
    IF n = 0 THEN RAISE EXCEPTION '25개 구가 모두 들어온 전월세 계약월이 없습니다'; END IF;
    SELECT count(*) INTO n FROM mart.dong_profile;
    IF n <> (SELECT count(*) FROM wh.dim_dong) THEN RAISE EXCEPTION 'dong_profile 행 수가 행정동 수와 다릅니다: %', n; END IF;
    SELECT count(*) INTO n FROM (SELECT origin_dong_id, dest_dong_id FROM mart.commute_od GROUP BY 1, 2 HAVING count(*) > 1) t;
    IF n > 0 THEN RAISE EXCEPTION 'commute_od에 (출발, 직장) 중복 % 개', n; END IF;
END $$;

COMMIT;
