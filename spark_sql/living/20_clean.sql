-- 통과 행(reasons = '')을 키(대상연월, 수집일, 요일~이동유형)로 묶는다. 한 행 = 키 하나. 옛 read_rows()의 C4와 V8.
-- C4: 값까지 같은 완전 중복은 하나만 남긴다(rows - 1개가 빠짐). 값이 같으므로 옛 경로처럼 먼저 온 줄을 고를 필요가 없다.
-- V8: 키는 같은데 값(평균 이동 시간, 이동인구)이 다르면 conflict. 그 키의 모든 행이 격리된다(21_quarantine).
SELECT target_ym, collected_date, weekday, arrive_hour_raw, origin_cd, dest_cd, sex, age, move_type,
       min(avg_minutes) AS avg_minutes,
       min(population) AS population,
       count(*) AS rows,
       min(avg_minutes_raw) <> max(avg_minutes_raw) OR min(population_raw) <> max(population_raw) AS conflict
FROM checked
WHERE reasons = ''
GROUP BY target_ym, collected_date, weekday, arrive_hour_raw, origin_cd, dest_cd, sex, age, move_type
