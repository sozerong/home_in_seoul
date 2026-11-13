-- 관문(결정 75~77)이 출처마다 판정을 하나 이상 냈음. 판정이 없으면 원본이 비었거나 batch_status SQL이 틀린 것이라
-- 데이터 문제가 아니라 빌드 전체를 멈춘다(결정 76: 관문 검사는 batch_status로 옮기고 여기에는 최소 검사만 둔다)
select s.source
from (values ('congestion'), ('rent')) s (source)
where not exists (select 1 from {{ ref('batch_status') }} b where b.source = s.source)
