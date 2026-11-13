-- 상가정보 배치 관문 (결정 75~77). 한 행 = (수집일, 원천 파일). 검사는 store_batch_checks(순환을 피해 따로 둠, 판단 기록 47).
-- 정제(store, store_quarantine)는 이 모델만 읽는다(failure_analysis F-7 처방 A, 결정 97).
-- 열: source, batch_key, collected_date, period, approved, failed_checks, checked_at (운영 카드용 합집합 batch_status와 같음)
select 'store' as source, concat_ws('/', collected_date, source_file) as batch_key, collected_date, base_ym as period,
       cardinality(failed_checks) = 0 as approved, failed_checks, now() as checked_at
from {{ ref('store_batch_checks') }}
