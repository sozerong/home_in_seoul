-- 통과 행(reasons = '')을 키(대상연월, 수집일, 요일~이동유형)로 묶는다. 한 행 = 키 하나. 옛 read_rows()의 C4와 V8.
-- C4: 값까지 같은 완전 중복은 하나만 남긴다(rows - 1개가 빠짐). 값이 같으므로 옛 경로처럼 먼저 온 줄을 고를 필요가 없다.
-- V8: 키는 같은데 값(평균 이동 시간, 이동인구)의 원본 글자가 다르면 conflict. 그 키의 모든 행이 격리된다(21_quarantine).
-- conflict는 글자 쌍의 64비트 해시 min/max로 본다(F-9 처방 A, 결정 103). 글자 min/max는 집계 버퍼가 가변 길이라 Spark가
--   정렬 집계(SortAggregate)로 14억 행을 정렬했다. 해시는 고정 길이라 HashAggregate가 된다. 숫자 min/max가 아니라 해시인 것은
--   "05"와 "5", "*"와 숫자 섞임을 옛 글자 비교와 똑같이 다르다고 봐야 해서다(tests/check_living_conflict.py).
--   ponytail: 서로 다른 글자 쌍의 해시가 같으면(확률 2^-64) 충돌을 놓친다. 처방 전후 두 방향 EXCEPT ALL(측정 42)로 확인한다.
-- pop_inexact: 이동인구를 DECIMAL(12, 2)로 바꾸며 값이 달라진 행 수. 관문이 원본을 다시 읽지 않게 여기서 센다(처방 C).
SELECT target_ym, collected_date, weekday, arrive_hour_raw, origin_cd, dest_cd, sex, age, move_type,
       min(avg_minutes) AS avg_minutes,
       min(population) AS population,
       count(*) AS rows,
       min(xxhash64(avg_minutes_raw, population_raw)) <> max(xxhash64(avg_minutes_raw, population_raw)) AS conflict,
       count_if(population IS NOT NULL AND CAST(population AS DOUBLE) <> pop_double) AS pop_inexact
FROM checked
WHERE reasons = ''
GROUP BY target_ym, collected_date, weekday, arrive_hour_raw, origin_cd, dest_cd, sex, age, move_type
