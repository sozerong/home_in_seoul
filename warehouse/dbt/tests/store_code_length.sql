-- 통과 업소의 업소번호 20자리, 소분류코드 6자리. store가 char로 캐스트하면서 긴 값을 조용히 자르지 않게 원래 길이를 본다.
-- 옛 코드는 char 열에 넣다가 적재가 실패했다
select collected_date, source_file, row_no, store_id, sub_cd
from {{ ref('store_rows') }}
where reasons = '' and (length(store_id) <> 20 or length(sub_cd) <> 6)
