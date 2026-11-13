-- 한 행 = 정제·검증을 통과한 전월세 계약 한 건. 옛 stg.rent(transform/rent.py)와 컬럼 이름, 순서, 타입이 같다.
-- 동·호가 없어 완전히 같은 행도 모두 남긴다(거래 고유 키 없음). 매번 전체를 다시 만든다(결정 62, 72).
select source_type, collected_date, source_file, item_no, deal_ymd, sgg_cd, umd_nm, bjd_cd, jibun,
       pnu::char(19) as pnu,  -- 길이는 테스트 rent_pnu_length가 본다(명시 캐스트는 긴 값을 조용히 자른다)
       building_name, house_type, build_year, floor,
       excl_area::numeric(7, 2) as excl_area,
       deal_date, deposit, monthly_rent, contract_type, contract_term, pre_deposit, pre_monthly_rent, use_rr_right
from {{ ref('rent_rows') }}
where reasons = ''
  and {{ rent_batch_key() }} in (select batch_key from {{ ref('batch_status') }} where source = 'rent' and approved)  -- 관문(결정 75)
