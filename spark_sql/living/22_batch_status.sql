-- 생활이동 배치 관문 (결정 90, 76). 한 행 = 배치(수집일, 대상연월). 걸린 검사가 하나라도 있으면 approved = false.
-- living_raw_shrink: 직전 대상연월보다 원본 행이 10% 넘게 줄었음(혼잡도 CF4와 같은 기준). living_quarantine_ratio: 격리 비율 5% 초과(F2).
-- living_empty: 통과 행 0. 쓰는 열 없음(F1)과 시간대 파일 빠짐(F3)은 파서(transform/living_iceberg.py)가 이미 실패시킨다.
-- 격리 비율의 분모 = 통과(C4 뒤) + 격리. 옛 경로와 달리 범위 C 밖 행도 들어간다.
-- pop_inexact: 이동인구를 DECIMAL(12, 2)로 바꾸며 값이 달라진 통과 행 수(test_population_exact가 0인지 본다).
WITH raw AS (
    SELECT target_ym, collected_date, count(*) AS raw_rows,
           count_if(reasons = '' AND population IS NOT NULL AND CAST(population AS DOUBLE) <> pop_double) AS pop_inexact
    FROM checked GROUP BY target_ym, collected_date
),
q AS (SELECT target_ym, collected_date, count(*) AS quarantined FROM quarantine GROUP BY target_ym, collected_date),
p AS (SELECT target_ym, collected_date, count(*) AS passed, sum(rows) - count(*) AS c4_dropped
      FROM clean WHERE NOT conflict GROUP BY target_ym, collected_date),
s AS (
    SELECT r.target_ym, r.collected_date, r.raw_rows, coalesce(p.passed, 0) AS passed, coalesce(q.quarantined, 0) AS quarantined,
           coalesce(p.c4_dropped, 0) AS c4_dropped, r.pop_inexact,
           lag(r.raw_rows) OVER (ORDER BY r.target_ym, r.collected_date) AS prev_raw_rows
    FROM raw r
    LEFT JOIN q ON q.target_ym = r.target_ym AND q.collected_date = r.collected_date
    LEFT JOIN p ON p.target_ym = r.target_ym AND p.collected_date = r.collected_date
)
SELECT *, failed_checks = '' AS approved
FROM (SELECT *, round(quarantined / (quarantined + passed), 6) AS quarantine_ratio,
             concat_ws(',', IF(raw_rows < prev_raw_rows * 0.9, 'living_raw_shrink', NULL),
                            IF(quarantined / (quarantined + passed) > 0.05, 'living_quarantine_ratio', NULL),
                            IF(passed = 0, 'living_empty', NULL)) AS failed_checks
      FROM s) t
