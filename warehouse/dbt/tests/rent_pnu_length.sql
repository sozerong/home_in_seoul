-- C6 안전망: 통과 행의 필지 고유번호는 19자리여야 한다. 본번·부번이 4자리를 넘으면 길어진다(옛 코드는 적재가 실패했다)
select source_type, collected_date, source_file, item_no, pnu
from {{ ref('rent_rows') }}
where reasons = '' and length(pnu) <> 19
