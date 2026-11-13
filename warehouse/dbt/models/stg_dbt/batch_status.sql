-- 배치 관문 (결정 75~77). 한 행 = (출처, 원천 배치). 혼잡도 배치는 수집일, 전월세 배치는 처리 단위(출처, 수집일, 구, 계약년월),
-- 상가정보 배치는 (수집일, 원천 파일)이고 검사는 store_batch_checks에서 가져온다(순환을 피해 따로 둠).
-- 데이터 문제는 그 배치만 거절한다: 걸린 검사가 하나라도 있으면 approved = false이고, 정제 결과(congestion, rent)와
-- 격리(*_quarantine)는 승인된 배치만 읽는다. 하류는 키마다 가장 최근 수집일을 고르므로 거절되면 직전 승인 배치가 쓰인다.
-- 검사 이름은 옮기기 전 dbt 테스트 이름 그대로다(결정 76). 매 빌드 지금 상태만 계산한다(이력 없음, 결정 77).
-- 거절 사유를 보려면 stg_dbt.congestion_rows, stg_dbt.rent_rows(뷰, 거르지 않음)에서 그 배치를 찾는다.
-- 2026-11-13 F-7 처방 A(결정 97): 판정은 출처마다 batch_status_congestion, batch_status_rent, batch_status_store가 하고,
-- 이 모델은 그 합집합이다. 운영 카드(ops/14)와 wh_load 요약 알림만 읽는다. 정제 모델은 이것을 읽지 않는다.
select * from {{ ref('batch_status_congestion') }}
union all
select * from {{ ref('batch_status_rent') }}
union all
select * from {{ ref('batch_status_store') }}
