-- title: 관문에서 거절된 배치
-- db: seoul
-- display: table
-- pos: 49 0 24 5
-- desc: 결정 75. 데이터 문제로 거절된 원천 배치. 그 출처는 직전 승인 배치로 상품을 만든다. 비어 있어야 한다. 사유를 보려면 stg_dbt.congestion_rows, stg_dbt.rent_rows에서 그 배치를 찾는다. 규칙을 고쳐서 생긴 거절이면 코드를, 원천이 틀렸으면 데이터를 본다(결정 76).
SELECT source AS "출처", batch_key AS "배치", collected_date AS "수집일",
       array_to_string(failed_checks, ', ') AS "걸린 검사", checked_at AS "판정 시각"
FROM stg_dbt.batch_status
WHERE NOT approved
ORDER BY collected_date DESC, source, batch_key
