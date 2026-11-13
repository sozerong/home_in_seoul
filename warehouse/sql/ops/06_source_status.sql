-- title: 출처별 상태
-- db: seoul
-- display: table
-- pos: 5 0 24 8
-- status: 상태
-- desc: 늦음은 원천이 멈춘 것, 미반영은 들어왔지만 상품까지 도착하지 않은 것이다. 늦음 기준: 전월세·혼잡도·역사마스터 7일, 상가정보 100일, 행정동 경계 380일 안에 수집, 생활이동은 다음 달 자료가 공개(20일) 후 7일 안에 도착, 연속지적도는 수시.
WITH reflected (source, reflected, expected) AS (
    SELECT '전월세', (SELECT max(collected_date)::text FROM wh.fact_rent), (SELECT max(collected_date)::text FROM stg.rent)
    UNION ALL SELECT '생활이동', (SELECT max(right(target_months, 6)) FROM mart.commute_od), (SELECT max(target_ym) FROM stg.living_migration)
    UNION ALL SELECT '상가정보', (SELECT max(base_ym) FROM wh.fact_store), (SELECT max(base_ym) FROM stg.sbiz_store)
    UNION ALL SELECT '혼잡도', (SELECT max(collected_date)::text FROM wh.fact_congestion), (SELECT max(collected_date)::text FROM raw.congestion)
    UNION ALL SELECT '역사마스터', (SELECT max(source_collected_date)::text FROM wh.dim_station), (SELECT max(collected_date)::text FROM stg.station_master)
    UNION ALL SELECT '행정동 경계', (SELECT max(source_collected_date)::text FROM wh.dim_dong), (SELECT max(collected_date)::text FROM stg.sgis_dong)
    UNION ALL SELECT '연속지적도', (SELECT max(source_collected_date)::text FROM wh.dim_parcel), (SELECT max(collected_date)::text FROM stg.parcel)
),
result AS (
SELECT f.source AS "출처",
       CASE WHEN f.is_late THEN '늦음' WHEN r.reflected IS DISTINCT FROM r.expected THEN '미반영' ELSE '정상' END AS "상태",
       to_char(f.last_collected, 'YYYY-MM-DD') AS "마지막 수집일",
       coalesce(f.last_period, '-') AS "데이터 최신 기간", r.reflected AS "상품 반영",
       CASE WHEN f.is_late AND f.source = '생활이동'
                 THEN to_char(to_date(f.last_period || '01', 'YYYYMMDD') + interval '1 month', 'YYYYMM') || ' 파일을 lake/_inbox/living_migration에 넣기'
            WHEN f.is_late THEN '수집 DAG 로그 확인'
            WHEN r.reflected IS DISTINCT FROM r.expected THEN 'wh_load 실행'
            ELSE '-' END AS "할 일"
FROM ops.freshness f JOIN reflected r USING (source)
)
SELECT * FROM result ORDER BY "상태" = '정상', "출처"
