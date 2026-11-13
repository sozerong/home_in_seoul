-- 한 행 = 전월세 원본 item 하나 (raw.rent). 정제(C1~C6)한 값과 검증(V1~V9)에 걸린 사유를 붙인다.
-- 규칙은 docs/rules.md 1절, 옛 transform/rent.py의 clean()과 같다. 정제 결과(rent)와 격리(rent_quarantine)가 함께 읽는다.
-- reasons는 규칙 번호를 번호 순서로 쉼표로 이은 글자다(없으면 ''). dbt unit test가 배열 컬럼을 비교하지 못해서 글자로 두고,
-- 격리 표에서 배열로 바꾼다. 원천 키 이름이 바뀌면 rent_text('새이름')을 coalesce로 더한다(결정 64).
{{ config(materialized='view') }}

-- v, n은 materialized: 안 붙이면 PostgreSQL이 CTE를 펼쳐 같은 정규식을 사유 계산마다 다시 돌린다(전체 27초 -> 8초, 측정 24)
with v as materialized (  -- C2: 공백 제거, 빈 값은 null
    select r.source_type, r.collected_date, r.source_file, r.item_no, r.deal_ymd, r.sgg_cd, r.raw,
           {{ rent_text('umdNm') }} as umd_nm,
           {{ rent_text('jibun') }} as jibun,
           case r.source_type when 'offi' then {{ rent_text('offiNm') }} else {{ rent_text('mhouseNm') }} end as building_name,  -- C1
           {{ rent_text('houseType') }} as house_type,
           {{ rent_text('buildYear') }} as build_year_t,
           {{ rent_text('floor') }} as floor_t,
           {{ rent_text('excluUseAr') }} as excl_area_t,
           {{ rent_text('deposit') }} as deposit_t,
           {{ rent_text('monthlyRent') }} as monthly_rent_t,
           {{ rent_text('preDeposit') }} as pre_deposit_t,
           {{ rent_text('preMonthlyRent') }} as pre_monthly_rent_t,
           {{ rent_text('dealYear') }} as deal_year_t,
           {{ rent_text('dealMonth') }} as deal_month_t,
           {{ rent_text('dealDay') }} as deal_day_t,
           {{ rent_text('contractType') }} as contract_type,
           {{ rent_text('contractTerm') }} as contract_term,
           {{ rent_text('useRRRight') }} as use_rr_right
    from {{ source('raw', 'rent') }} r
),
n as materialized (  -- C3, C4, C5: 숫자로 읽기. 읽지 못하면 null
    select v.*,
           {{ rent_int('build_year_t') }} as build_year,
           {{ rent_int('floor_t') }} as floor,
           {{ rent_float('excl_area_t') }} as excl_area,
           {{ rent_int('deposit_t') }} as deposit,
           {{ rent_int('monthly_rent_t') }} as monthly_rent,
           {{ rent_int('pre_deposit_t') }} as pre_deposit,
           {{ rent_int('pre_monthly_rent_t') }} as pre_monthly_rent,
           {{ rent_int('deal_year_t') }} as y,
           {{ rent_int('deal_month_t') }} as m,
           {{ rent_int('deal_day_t') }} as d,
           regexp_match(v.jibun, '^(산)?([0-9]+)(?:-([0-9]+))?$') as jibun_m  -- V9: [산]본번 또는 [산]본번-부번
    from v
),
c as (
    select n.*,
           case when n.y between 1 and 9999 and n.m between 1 and 12 then  -- C4: 없는 날짜면 null (V3)
               case when n.d between 1 and extract(day from make_date(n.y, n.m, 1) + interval '1 month - 1 day')
                    then make_date(n.y, n.m, n.d) end
           end as deal_date,
           b.bjd_cd  -- C6: (요청한 구, 법정동명) -> 법정동코드. 가장 최근 상가정보에서 만든 코드표
    from n
    left join {{ ref('bjd_code') }} b  -- 원본 raw.sbiz_store에서 만든 SQL 모델(design_remaining_work.md 2.2)
      on b.collected_date = (select max(collected_date) from {{ ref('bjd_code') }})
     and b.sgg_cd = n.sgg_cd and b.bjd_nm = n.umd_nm
)
select c.source_type, c.collected_date, c.source_file, c.item_no, c.deal_ymd, c.sgg_cd, c.raw,
       c.umd_nm, c.bjd_cd, c.jibun,
       case when c.jibun_m is not null and c.bjd_cd is not null  -- C6: 법정동코드 + 대지구분(산이면 2) + 본번 4 + 부번 4
            then c.bjd_cd || case when c.jibun_m[1] is null then '1' else '2' end
                 || {{ zfill4('c.jibun_m[2]') }} || {{ zfill4("coalesce(c.jibun_m[3], '0')") }}
       end as pnu,
       c.building_name, c.house_type, c.build_year, c.floor, c.excl_area, c.deal_date, c.deposit, c.monthly_rent,
       c.contract_type, c.contract_term, c.pre_deposit, c.pre_monthly_rent, c.use_rr_right,
       array_to_string(array[
           case when (c.deposit_t is not null and c.deposit is null) or (c.monthly_rent_t is not null and c.monthly_rent is null)
                  or (c.pre_deposit_t is not null and c.pre_deposit is null)
                  or (c.pre_monthly_rent_t is not null and c.pre_monthly_rent is null) then 'C3' end,
           case when (c.deal_year_t is not null and c.y is null) or (c.deal_month_t is not null and c.m is null)
                  or (c.deal_day_t is not null and c.d is null) then 'C4' end,
           case when (c.build_year_t is not null and c.build_year is null) or (c.floor_t is not null and c.floor is null)
                  or (c.excl_area_t is not null and c.excl_area is null) then 'C5' end,
           case when c.deposit_t is null or c.monthly_rent_t is null or c.deal_year_t is null or c.deal_month_t is null
                  or c.deal_day_t is null or c.excl_area_t is null or c.umd_nm is null or c.jibun is null then 'V1' end,
           case when c.y is not null and c.m is not null and c.d is not null
                 and to_char(c.y, 'FM0000') || to_char(c.m, 'FM00') <> c.deal_ymd then 'V2' end,  -- 요청한 계약년월과 다름
           case when c.y is not null and c.m is not null and c.d is not null and c.deal_date is null then 'V3' end,
           case when coalesce(c.deposit, 0) < 0 or coalesce(c.monthly_rent, 0) < 0 then 'V4' end,
           case when c.excl_area is not null and not (c.excl_area > 0 and c.excl_area <= 300) then 'V5' end,
           case when c.floor is not null and c.floor not between -5 and 80 then 'V6' end,
           case when c.contract_type is not null and c.contract_type not in ('신규', '갱신') then 'V7' end,
           case when c.umd_nm is not null and c.bjd_cd is null then 'V8' end,
           case when c.jibun is not null and c.jibun_m is null then 'V9' end
       ], ',') as reasons  -- array_to_string은 null을 건너뛴다
from c
