-- 승인된 배치가 하나도 없는 대상연월. Iceberg 원본은 대상연월마다 수집일 하나만 두므로(덮어쓰기) 거절되면 그 달이
-- 경계 표에서 조용히 빠진다. 결정 90의 "직전 승인 배치를 쓴다"가 지금 구조로는 안 되므로 실패로 드러낸다.
SELECT target_ym, collect_list(failed_checks) AS failed_checks FROM batch_status GROUP BY target_ym HAVING NOT bool_or(approved)
