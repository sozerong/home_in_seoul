-- 단계 5 웨어하우스 채우기. 스테이징의 가장 최근 수집분으로 wh의 테이블 전체를 다시 만든다(멱등).
-- 한 트랜잭션이라 중간에 실패하면 이전 상태로 돌아간다. 구조 검사에 어긋나면 RAISE로 멈춘다.
-- 설계: docs/erd.md, 결정 13(행정동 기준), 16(대표점), 17(혼잡도 대응표).
BEGIN;

TRUNCATE wh.fact_store, wh.map_congestion_station, wh.dim_station, wh.dim_parcel, wh.map_dong_2021, wh.dim_dong;

-- 1. 행정동 차원: SGIS 경계의 가장 최근 수집분
INSERT INTO wh.dim_dong (dong_id, dong_nm, sgg_cd, boundary_base_date, source_collected_date, area_km2, geom)
SELECT adm_cd, adm_nm, left(adm_cd, 5), base_date, collected_date, round((ST_Area(geom) / 1e6)::numeric, 3), geom
FROM stg.sgis_dong
WHERE collected_date = (SELECT max(collected_date) FROM stg.sgis_dong);

-- 2. 통계청 -> 행정안전부 코드 (결정 13): 필수 6코드 업소 좌표를 경계에 공간 조인해, 동마다 가장 많은 행정안전부 코드를 고른다.
--    상가정보 좌표는 도 단위 경위도라 WGS84(EPSG:4326)로 보고 5179로 바꾼다 (data_sources.md 13절, [확인 필요]).
CREATE TEMP TABLE store_point ON COMMIT DROP AS
SELECT s.collected_date, s.store_id, s.sub_cd, s.base_ym, s.adm_cd AS mois_dong_cd,
       ST_Transform(ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326), 5179) AS point
FROM stg.sbiz_store s
WHERE s.collected_date = (SELECT max(collected_date) FROM stg.sbiz_store);

CREATE TEMP TABLE store_in_dong ON COMMIT DROP AS
SELECT p.*, d.dong_id
FROM store_point p
LEFT JOIN wh.dim_dong d ON ST_Contains(d.geom, p.point);

CREATE TEMP TABLE dong_mois_vote ON COMMIT DROP AS
SELECT dong_id, mois_dong_cd, count(*) AS stores,
       count(*)::numeric / sum(count(*)) OVER (PARTITION BY dong_id) AS share,
       row_number() OVER (PARTITION BY dong_id ORDER BY count(*) DESC, mois_dong_cd) AS rank
FROM store_in_dong
WHERE dong_id IS NOT NULL
GROUP BY dong_id, mois_dong_cd;

UPDATE wh.dim_dong d SET mois_dong_cd = v.mois_dong_cd
FROM dong_mois_vote v
WHERE v.dong_id = d.dong_id AND v.rank = 1;

-- 3. 2021 생활이동 코드 -> 2025 행정동 (data_sources.md 10절)
--    바뀐 6개는 손으로, 나머지는 "2021 코드 + '0' = 2025 코드" 규칙. 앞 7자리로 자르지 않아 11230510/11230511 충돌을 피한다.
INSERT INTO wh.map_dong_2021 (dong_cd_2021, dong_id, method) VALUES
    ('1116064', '11160751', 'manual'),  -- 가양1동: 코드 변경
    ('1116072', '11160761', 'manual'),  -- 방화3동: 코드 변경
    ('1117068', '11170730', 'manual'),  -- 오류2동: 분동 -> 오류2동
    ('1117068', '11170740', 'manual'),  -- 오류2동: 분동 -> 항동
    ('1123074', '11230511', 'manual'),  -- 일원2동 -> 개포3동: 이름과 코드 변경
    ('1125051', '11250750', 'manual'),  -- 강일동: 코드 변경
    ('1125052', '11250760', 'manual'),  -- 상일동: 분동 -> 상일1동
    ('1125052', '11250770', 'manual');  -- 상일동: 분동 -> 상일2동

INSERT INTO wh.map_dong_2021 (dong_cd_2021, dong_id, method)
SELECT c.code, d.dong_id, 'prefix'
FROM (SELECT origin_cd AS code FROM stg.living_migration UNION SELECT dest_cd FROM stg.living_migration) c
JOIN wh.dim_dong d ON d.dong_id = c.code || '0'
WHERE c.code NOT IN (SELECT dong_cd_2021 FROM wh.map_dong_2021);

-- 4. 필지: 도형 안 대표점(결정 16)을 5179로 바꾸고 행정동에 붙인다
INSERT INTO wh.dim_parcel (pnu, point, dong_id, source_collected_date)
SELECT p.pnu, p.point, d.dong_id, p.collected_date
FROM (SELECT pnu, collected_date, ST_Transform(ST_PointOnSurface(geom), 5179) AS point
      FROM stg.parcel WHERE collected_date = (SELECT max(collected_date) FROM stg.parcel)) p
LEFT JOIN wh.dim_dong d ON ST_Contains(d.geom, p.point);

-- 5. 역: 역사마스터의 가장 최근 스냅샷
INSERT INTO wh.dim_station (bldn_id, bldn_nm, route, point, dong_id, source_collected_date)
SELECT s.bldn_id, s.bldn_nm, s.route, s.point, d.dong_id, s.collected_date
FROM (SELECT bldn_id, bldn_nm, route, collected_date,
             ST_Transform(ST_SetSRID(ST_MakePoint(lot::float8, lat::float8), 4326), 5179) AS point
      FROM stg.station_master WHERE collected_date = (SELECT max(collected_date) FROM stg.station_master)) s
LEFT JOIN wh.dim_dong d ON ST_Contains(d.geom, s.point);

-- 6. 업소 팩트
INSERT INTO wh.fact_store (collected_date, store_id, sub_cd, base_ym, point, dong_id)
SELECT collected_date, store_id, sub_cd, base_ym, point, dong_id FROM store_in_dong;

-- 7. 혼잡도 역번호 -> 역사_ID (결정 17). 0 채우기는 호선까지 같아야 인정한다(9001이 GTX 역에 붙는 것을 막는다).
INSERT INTO wh.map_congestion_station (line, sttn_no, bldn_id, method, note)
SELECT DISTINCT c.line, c.sttn_no, s.bldn_id, 'padded', NULL::text
FROM wh.fact_congestion c
JOIN wh.dim_station s ON s.bldn_id = lpad(c.sttn_no, 4, '0') AND s.route = c.line;

INSERT INTO wh.map_congestion_station (line, sttn_no, bldn_id, method, note) VALUES
    ('2호선', '9001', '0211', 'manual', '성수E -> 성수'),
    ('2호선', '9002', '0211', 'manual', '성수 -> 성수'),
    ('2호선', '9003', '0234', 'manual', '신도림 -> 신도림'),
    ('5호선', '9005', '2549', 'manual', '강동(마천) -> 강동'),
    ('6호선', '9006', '2611', 'manual', '응암S -> 응암'),
    ('2호선', '260', '2519', 'manual', '까치산(2호선) -> 까치산(5호선 역사), 같은 역사 위치');

-- ===== 구조 검사: 어긋나면 멈춘다 (트랜잭션 전체 취소) =====
DO $$
DECLARE n bigint;
BEGIN
    SELECT count(*) INTO n FROM (SELECT origin_cd AS code FROM stg.living_migration UNION SELECT dest_cd FROM stg.living_migration) c
    WHERE c.code NOT IN (SELECT dong_cd_2021 FROM wh.map_dong_2021);
    IF n > 0 THEN RAISE EXCEPTION '2021 대응표에 없는 생활이동 코드 % 개', n; END IF;

    SELECT count(*) INTO n FROM wh.dim_dong WHERE dong_id NOT IN (SELECT dong_id FROM wh.map_dong_2021);
    IF n > 0 THEN RAISE EXCEPTION '2021 코드가 하나도 가리키지 않는 2025 행정동 % 개', n; END IF;

    SELECT count(*) INTO n FROM (SELECT DISTINCT line, sttn_no FROM wh.fact_congestion) c
    WHERE (c.line, c.sttn_no) NOT IN (SELECT line, sttn_no FROM wh.map_congestion_station);
    IF n > 0 THEN RAISE EXCEPTION '역사_ID에 잇지 못한 혼잡도 역 % 개', n; END IF;

    SELECT count(*) INTO n FROM wh.dim_dong WHERE mois_dong_cd IS NULL;
    IF n > 0 THEN RAISE EXCEPTION '행정안전부 코드를 정하지 못한 행정동 % 개', n; END IF;
END $$;

COMMIT;
