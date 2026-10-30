-- 단계 5 웨어하우스. 설계는 docs/erd.md, 결정 13~17.
-- 모든 도형은 EPSG:5179. 공간 조인이 들어가는 것은 테이블, 컬럼만 고르는 것은 스테이징 위의 뷰(결정 15).
CREATE SCHEMA IF NOT EXISTS wh;

-- ===== 차원과 연결표 (테이블) =====

-- 한 행 = 2025 SGIS 기준 서울 행정동 하나. 이 웨어하우스의 중심(순응 차원).
CREATE TABLE IF NOT EXISTS wh.dim_dong (
    dong_id               char(8)  PRIMARY KEY,  -- 통계청 행정동 코드 (SGIS ADM_CD)
    dong_nm               text     NOT NULL,
    sgg_cd                char(5)  NOT NULL,     -- 통계청 시군구 코드 (dong_id 앞 5자리)
    mois_dong_cd          char(8),               -- 행정안전부 행정동 코드. 상가정보로 만든 대응표로 채운다 (결정 13)
    boundary_base_date    date     NOT NULL,     -- SGIS BASE_DATE
    source_collected_date date     NOT NULL,     -- 어느 수집일의 stg.sgis_dong에서 왔는지
    area_km2              numeric(8, 3) NOT NULL,
    geom                  geometry(MultiPolygon, 5179) NOT NULL
);
CREATE INDEX IF NOT EXISTS dim_dong_geom_idx ON wh.dim_dong USING gist (geom);

-- 한 행 = 2021 생활이동 코드 하나와 2025 행정동 하나의 짝. 분동된 동은 2행 (결정 13).
CREATE TABLE IF NOT EXISTS wh.map_dong_2021 (
    dong_cd_2021 char(7) NOT NULL,
    dong_id      char(8) NOT NULL REFERENCES wh.dim_dong,
    method       text    NOT NULL CHECK (method IN ('prefix', 'manual')),
    PRIMARY KEY (dong_cd_2021, dong_id)
);

-- 한 행 = 서울 필지 하나. 폴리곤 대신 도형 안 대표점만 둔다 (결정 16).
CREATE TABLE IF NOT EXISTS wh.dim_parcel (
    pnu                   char(19) PRIMARY KEY,
    point                 geometry(Point, 5179) NOT NULL,
    dong_id               char(8) REFERENCES wh.dim_dong,  -- 서울 행정동 경계 밖이면 NULL
    source_collected_date date     NOT NULL
);
CREATE INDEX IF NOT EXISTS dim_parcel_point_idx ON wh.dim_parcel USING gist (point);
CREATE INDEX IF NOT EXISTS dim_parcel_dong_idx ON wh.dim_parcel (dong_id);

-- 한 행 = 역(호선별) 하나, 역사마스터의 가장 최근 스냅샷.
CREATE TABLE IF NOT EXISTS wh.dim_station (
    bldn_id               char(4) PRIMARY KEY,
    bldn_nm               text    NOT NULL,
    route                 text    NOT NULL,
    point                 geometry(Point, 5179) NOT NULL,
    dong_id               char(8) REFERENCES wh.dim_dong,  -- 서울 행정동 경계 밖이면 NULL
    source_collected_date date    NOT NULL
);
CREATE INDEX IF NOT EXISTS dim_station_point_idx ON wh.dim_station USING gist (point);

-- 한 행 = 혼잡도의 (호선, 원래 역번호) 하나와 역사_ID의 짝 (결정 17).
-- 한 행 = 행정안전부 행정동 코드 하나와 그 코드가 속한 2025 행정동(결정 25). 상가정보 업소 다수결로 만든다.
-- 동마다 1위 코드는 is_primary. 어느 동에서도 1위가 아닌 코드는 그 코드 업소가 가장 많은 동에 붙인다(용신동 = 신설동 + 용두동).
CREATE TABLE IF NOT EXISTS wh.map_dong_mois (
    mois_dong_cd char(8)      PRIMARY KEY,
    dong_id      char(8)      NOT NULL REFERENCES wh.dim_dong,
    stores       integer      NOT NULL,          -- 그 동 경계 안에 있는, 이 코드를 가진 업소 수
    share        numeric(4,3) NOT NULL,          -- 그 동 업소 중 이 코드의 비율
    is_primary   boolean      NOT NULL
);

CREATE TABLE IF NOT EXISTS wh.map_congestion_station (
    line    text    NOT NULL,
    sttn_no text    NOT NULL,
    bldn_id char(4) NOT NULL REFERENCES wh.dim_station,
    method  text    NOT NULL CHECK (method IN ('padded', 'manual')),
    note    text,
    PRIMARY KEY (line, sttn_no)
);

-- 한 행 = 수집일 x 필수 6코드 업소 하나.
CREATE TABLE IF NOT EXISTS wh.fact_store (
    collected_date date    NOT NULL,
    store_id       char(20) NOT NULL,
    sub_cd         char(6)  NOT NULL,
    base_ym        char(6)  NOT NULL,
    point          geometry(Point, 5179) NOT NULL,
    dong_id        char(8) REFERENCES wh.dim_dong,  -- 서울 행정동 경계 밖이면 NULL
    PRIMARY KEY (collected_date, store_id)
);
CREATE INDEX IF NOT EXISTS fact_store_dong_idx ON wh.fact_store (dong_id);

-- ===== 팩트 (스테이징 위의 뷰) =====
-- 같은 처리 단위가 여러 수집일에 있으면 가장 최근 수집분만 본다. 한 번의 수집이 그 단위 전체를 담기 때문이다.

-- 한 행 = 신고된 전월세 계약 한 건 (오피스텔, 연립다세대). 행정동은 pnu로 wh.dim_parcel에서 얻는다.
CREATE OR REPLACE VIEW wh.fact_rent AS
SELECT r.source_type, r.sgg_cd, r.deal_ymd, r.deal_date, r.pnu, r.umd_nm, r.jibun, r.building_name,
       r.house_type, r.build_year, r.floor, r.excl_area, r.deposit, r.monthly_rent, r.contract_type,
       r.collected_date, r.source_file, r.item_no
FROM stg.rent r
JOIN (SELECT source_type, sgg_cd, deal_ymd, max(collected_date) AS collected_date
      FROM stg.rent GROUP BY source_type, sgg_cd, deal_ymd) latest
  USING (source_type, sgg_cd, deal_ymd, collected_date);

-- 한 행 = 대상연월 x 출발 x 도착(2021 코드) x 요일 x 도착시간 x 성별 x 나이. 범위는 결정 9(HW, 월~금, 07~09시, 서울↔서울).
-- 2025 행정동으로 옮기는 일은 마트에서 wh.map_dong_2021로 한다 (합계가 두 배가 되지 않게).
CREATE OR REPLACE VIEW wh.fact_commute AS
SELECT m.target_ym, m.origin_cd, m.dest_cd, m.weekday, m.arrive_hour, m.sex, m.age, m.avg_minutes, m.population,
       m.collected_date
FROM stg.living_migration m
JOIN (SELECT target_ym, max(collected_date) AS collected_date
      FROM stg.living_migration GROUP BY target_ym) latest
  USING (target_ym, collected_date);

-- 한 행 = 요일 x 호선 x 역(원래 역번호) x 방향 x 30분 구간, 가장 최근 수집분. 역사_ID는 wh.map_congestion_station으로 얻는다.
CREATE OR REPLACE VIEW wh.fact_congestion AS
SELECT c.dow, c.line, c.sttn_no, c.sttn_nm, c.direction, c.slot, c.congestion, c.collected_date
FROM stg.congestion c
WHERE c.collected_date = (SELECT max(collected_date) FROM stg.congestion);
