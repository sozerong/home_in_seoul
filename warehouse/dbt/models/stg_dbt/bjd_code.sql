-- 법정동 코드표: 승인된 상가정보 배치의 서울 업소 전체(업종 무관)에 나온 (시군구코드, 법정동명) -> 법정동코드. 원본 값 그대로.
-- 전월세 정제 C6(rent_rows)이 가장 최근 수집일 것을 읽는다. 옛 stg.bjd_code(transform/sbiz_store.py)를 SQL로 옮겨
-- 전월세 리니지가 원천(raw.sbiz_store)까지 이어진다(design_remaining_work.md 2.2). 한 이름에 코드가 둘이면 테스트 bjd_code_pair_unique가 멈춘다.
select distinct r.collected_date, (r.raw ->> '시군구코드')::char(5) as sgg_cd, r.raw ->> '법정동명' as bjd_nm,
       (r.raw ->> '법정동코드')::char(10) as bjd_cd
from {{ source('raw', 'sbiz_store') }} r
join {{ ref('store_batch_checks') }} c using (collected_date, source_file)
where cardinality(c.failed_checks) = 0  -- 관문(결정 75). batch_status를 읽으면 순환이라 같은 판정을 직접 본다
