-- 한 행 = 혼잡도 원본 row 하나 (raw.congestion). 신원 필드 5개를 다듬고 행 단위 규칙(CV1~CV4)에 걸린 사유를 붙인다.
-- 규칙은 docs/rules.md 3절, 옛 transform/congestion.py의 clean()과 같다. 정제 결과(congestion)와 격리(congestion_quarantine)가 함께 읽는다.
-- 원천 키 이름이 바뀌면 coalesce(raw->>'새이름', raw->>'옛이름')으로 별칭을 더한다(결정 64).
{{ config(materialized='view') }}

with src as (
    select r.collected_date, r.source_file, r.row_no, r.raw,
           btrim(coalesce(r.raw ->> 'DOW_SE', ''), E' \t\n\r\f\v') as dow,  -- Python strip()과 같은 공백 문자
           btrim(coalesce(r.raw ->> 'LINE', ''), E' \t\n\r\f\v') as line,
           btrim(coalesce(r.raw ->> 'STTN_NO', ''), E' \t\n\r\f\v') as sttn_no,
           btrim(coalesce(r.raw ->> 'DPTRE_STTN', ''), E' \t\n\r\f\v') as sttn_nm,
           btrim(coalesce(r.raw ->> 'UP_DOWN_SE', ''), E' \t\n\r\f\v') as direction
    from {{ source('raw', 'congestion') }} r
)
select s.*,
       array_remove(array[
           case when s.dow = '' or s.line = '' or s.sttn_no = '' or s.sttn_nm = '' or s.direction = '' then 'CV1' end,
           case when (s.dow <> '' and s.dow not in ('평일', '토요일', '일요일'))
                  or (s.direction <> '' and s.direction not in ('상선', '하선', '내선', '외선')) then 'CV2' end,
           case when s.line <> '' and s.line not in ('1호선', '2호선', '3호선', '4호선', '5호선', '6호선', '7호선', '8호선')
                then 'CV3' end,
           case when s.sttn_no <> '' and s.sttn_no !~ '^[0-9]{3,4}$' then 'CV4' end
       ], null)::text[] as reasons  -- 사유는 규칙 번호 순서(정렬됨)
from src s
