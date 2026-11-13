-- 한 행 = 정제·검증을 통과한 상가 업소 하나, 서울 전체 업종(결정 78). 옛 stg.sbiz_store(transform/sbiz_store.py)와 컬럼 이름,
-- 순서, 타입이 같다. 옛 표는 생활 편의 6코드만 있었다. 업종은 편의 지표(mart.dong_amenity)에서 고른다(결정 79). 매번 전체를 다시 만든다.
select collected_date, base_ym::char(6) as base_ym, source_file, row_no,
       store_id::char(20) as store_id,  -- 길이는 테스트 store_code_length가 본다(명시 캐스트는 긴 값을 조용히 자른다)
       store_name, sub_cd::char(6) as sub_cd, sub_nm, sido_cd::char(2) as sido_cd, sgg_cd::char(5) as sgg_cd,
       adm_cd::char(8) as adm_cd, bjd_cd::char(10) as bjd_cd, lon, lat
from {{ ref('store_rows') }}
where reasons = ''
  and concat_ws('/', collected_date, source_file) in (select batch_key from {{ ref('batch_status') }} where source = 'store' and approved)  -- 관문(결정 75)
