-- 경계 표 stg.living_commute (결정 89, 96). 한 행 = 대상연월 x 출발 x 도착(2021 코드) x 평일/주말 x 도착시간 묶음 x 이동유형 x 나이 구간.
-- 경계, 즉 Spark가 여기서 고르고 묶어 dbt로 넘기지 않는 것:
--   출발·도착 모두 서울(11로 시작하는 7자리), 이동유형 HW·WH만, 도착시간 묶음 07_09 / 18_20 / 기타,
--   나이 구간 20_34(20, 25, 30, 결정 22) / 기타, 성별은 합침, 요일은 평일/주말로 합침.
--   이 밖의 요구(새 시간 묶음, 다른 이동유형, 더 잘게 나눈 나이, 서울 밖)는 이 파일을 고친다. dbt만으로는 안 된다.
-- 값은 더할 수 있는 것만(결정 89 G-c). 평균은 dbt가 population_minutes / population으로 다시 계산한다.
-- 대상연월마다 승인된 배치 중 가장 최근 수집일만 읽는다(결정 90, 옛 fact_commute의 최신 수집일 고르기가 여기로 옴).
WITH latest AS (
    SELECT target_ym, max(collected_date) AS collected_date FROM batch_status WHERE approved GROUP BY target_ym
)
SELECT c.target_ym, c.collected_date, c.origin_cd, c.dest_cd,
       IF(c.weekday IN ('월', '화', '수', '목', '금'), '평일', '주말') AS day_type,
       CASE WHEN c.arrive_hour_raw BETWEEN '07' AND '09' THEN '07_09'
            WHEN c.arrive_hour_raw BETWEEN '18' AND '20' THEN '18_20' ELSE '기타' END AS hour_group,
       c.move_type,
       IF(c.age IN ('20', '25', '30'), '20_34', '기타') AS age_group,
       sum(c.population) AS population,
       sum(c.population * c.avg_minutes) AS population_minutes,
       count_if(c.population IS NULL) AS masked_rows,
       count(*) AS rows
FROM clean c
JOIN latest l ON l.target_ym = c.target_ym AND l.collected_date = c.collected_date
WHERE NOT c.conflict
  AND length(c.origin_cd) = 7 AND c.origin_cd LIKE '11%' AND length(c.dest_cd) = 7 AND c.dest_cd LIKE '11%'
  AND c.move_type IN ('HW', 'WH')
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
