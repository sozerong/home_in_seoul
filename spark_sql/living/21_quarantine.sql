-- 격리 행: 행 단위 위반(10_checked) + 키 충돌(V8, 20_clean). 한 행 = 원본 한 행. 옛 stg.living_migration_quarantine과 같은 뜻.
-- 관문(22_batch_status)의 격리 비율이 이것을 세고, 실행기 --save가 stg.living_quarantine에 남긴다(판단 64).
SELECT target_ym, collected_date, source_file, row_no, reasons
FROM checked
WHERE reasons <> ''
UNION ALL
SELECT c.target_ym, c.collected_date, c.source_file, c.row_no, 'V8'
FROM checked c
JOIN clean k
  ON k.conflict AND c.reasons = ''
 AND k.target_ym = c.target_ym AND k.collected_date = c.collected_date AND k.weekday = c.weekday
 AND k.arrive_hour_raw = c.arrive_hour_raw AND k.origin_cd = c.origin_cd AND k.dest_cd = c.dest_cd
 AND k.sex = c.sex AND k.age = c.age AND k.move_type = c.move_type
