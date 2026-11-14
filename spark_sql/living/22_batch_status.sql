-- 생활이동 배치 관문 (결정 90, 76). 한 행 = 배치(수집일, 대상연월). 걸린 검사가 하나라도 있으면 approved = false.
-- living_raw_shrink: 직전 대상연월보다 원본 행이 10% 넘게 줄었음(혼잡도 CF4와 같은 기준). living_quarantine_ratio: 격리 비율 5% 초과(F2).
-- living_empty: 통과 행 0. 쓰는 열 없음(F1)과 시간대 파일 빠짐(F3)은 파서(transform/living_iceberg.py)가 이미 실패시킨다.
-- 격리 비율의 분모 = 통과(C4 뒤) + 격리. 옛 경로와 달리 범위 C 밖 행도 들어간다.
-- pop_inexact: 이동인구를 DECIMAL(12, 2)로 바꾸며 값이 달라진 통과 행 수(test_population_exact가 0인지 본다).
-- 원본을 따로 읽지 않는다(F-9 처방 C, 결정 103). 원본 한 행은 격리(행 단위 위반, V8)이거나, 충돌 없는 키의 통과 행 둘 중 하나라서
--   원본 행 수 = 격리 행 수 + 충돌 없는 키의 행 수다. 격리 수는 21_quarantine을 그대로 세어 V8을 두 번 정의하지 않는다(J-10).
--   실행기는 --save 뷰(quarantine)를 관문보다 먼저 캐시하므로, 관문은 clean만 새로 계산한다(원본 읽기 1번).
WITH q AS (SELECT target_ym, collected_date, count(*) AS quarantined FROM quarantine GROUP BY target_ym, collected_date),
p AS (SELECT target_ym, collected_date, count_if(NOT conflict) AS passed, sum(IF(conflict, 0, rows)) AS passed_rows,
             sum(pop_inexact) AS pop_inexact
      FROM clean GROUP BY target_ym, collected_date),
s AS (
    SELECT target_ym, collected_date,
           coalesce(q.quarantined, 0) + coalesce(p.passed_rows, 0) AS raw_rows,
           coalesce(p.passed, 0) AS passed, coalesce(q.quarantined, 0) AS quarantined,
           coalesce(p.passed_rows - p.passed, 0) AS c4_dropped, coalesce(p.pop_inexact, 0) AS pop_inexact
    FROM q FULL JOIN p USING (target_ym, collected_date)
),
w AS (SELECT *, lag(raw_rows) OVER (ORDER BY target_ym, collected_date) AS prev_raw_rows FROM s)
SELECT *, failed_checks = '' AS approved
FROM (SELECT *, round(quarantined / (quarantined + passed), 6) AS quarantine_ratio,
             concat_ws(',', IF(raw_rows < prev_raw_rows * 0.9, 'living_raw_shrink', NULL),
                            IF(quarantined / (quarantined + passed) > 0.05, 'living_quarantine_ratio', NULL),
                            IF(passed = 0, 'living_empty', NULL)) AS failed_checks
      FROM w) t
