-- 승인된 배치가 하나도 없는 대상연월. 그 달은 경계 표에서 빠지고 나머지 달은 갱신된다(결정 100).
-- Iceberg 원본은 대상연월마다 수집일 하나만 두므로(덮어쓰기) 거절되면 같은 달의 직전 배치가 없다. 상품(commute_od)은 최근 6개월만 써서
-- 새 달이 빠지면 이전 6개월 그대로다. 거절 사실은 stg.living_batch_status, 운영 카드 14, wh_load 알림에 남고, 여기서는 실행 로그에 찍기만 한다.
-- 같은 달을 다시 받았는데 거절되면 이전 승인 내용까지 빠진다: 런북은 직전 수집일 zip으로 transform.living_iceberg를 다시 돌리기(결정 100 E).
SELECT target_ym, collect_list(failed_checks) AS failed_checks FROM batch_status GROUP BY target_ym HAVING NOT bool_or(approved)
