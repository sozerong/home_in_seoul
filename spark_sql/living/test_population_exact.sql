-- 이동인구를 DECIMAL(12, 2)로 바꾸며 값이 달라진 배치. 원본에 소수 셋째 자리가 생기면 합이 조용히 반올림되므로 실패시킨다.
SELECT target_ym, collected_date, pop_inexact FROM batch_status WHERE pop_inexact > 0
