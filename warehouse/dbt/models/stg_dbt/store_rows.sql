-- 한 행 = 상가정보 원본 행 하나 (raw.sbiz_store, 서울 전체 업소). 정제(MC2, MC3)한 값과 검증(MV1~MV4)에 걸린 사유를 붙인다.
-- 규칙은 docs/rules.md 5절, 옛 transform/sbiz_store.py의 clean()·split()과 같다. 다른 점: 업종을 고르지 않는다(결정 78, 79).
-- 그래서 MV2(업소번호 중복)는 6코드 안이 아니라 배치(수집일, 원천 파일) 전체에서 센다.
-- reasons는 규칙 번호를 정렬해 쉼표로 이은 글자다(없으면 ''). 정제 결과(store), 격리(store_quarantine), 관문이 함께 읽는다.
{{ config(materialized='view') }}

with v as materialized (  -- MC2: 공백 제거, 빈 값은 null
    select r.collected_date, r.source_file, r.row_no, r.raw,
           {{ sbiz_text('상가업소번호') }} as store_id,
           {{ sbiz_text('상호명') }} as store_name,
           {{ sbiz_text('상권업종소분류코드') }} as sub_cd,
           {{ sbiz_text('상권업종소분류명') }} as sub_nm,
           {{ sbiz_text('시도코드') }} as sido_cd,
           {{ sbiz_text('시군구코드') }} as sgg_cd,
           {{ sbiz_text('행정동코드') }} as adm_cd,
           {{ sbiz_text('법정동코드') }} as bjd_cd,
           {{ sbiz_text('경도') }} as lon_t,
           {{ sbiz_text('위도') }} as lat_t,
           substring(r.source_file from '_([0-9]{6})[.]csv$') as base_ym  -- MC4: 파일 이름의 기준월
    from {{ source('raw', 'sbiz_store') }} r
),
n as materialized (  -- MC3: 실수로. Python float()이 읽는 꼴만 숫자. nan, inf는 읽히지만 범위 밖이라 MV3
    select v.*,
           case when v.lon_t ~ '^[+-]?([0-9]+[.]?[0-9]*|[.][0-9]+)([eE][+-]?[0-9]+)?$' then v.lon_t::double precision end as lon,
           case when v.lat_t ~ '^[+-]?([0-9]+[.]?[0-9]*|[.][0-9]+)([eE][+-]?[0-9]+)?$' then v.lat_t::double precision end as lat,
           coalesce(lower(v.lon_t) ~ '^[+-]?(nan|inf|infinity)$', false) as lon_special,
           coalesce(lower(v.lat_t) ~ '^[+-]?(nan|inf|infinity)$', false) as lat_special,
           count(*) over (partition by v.collected_date, v.source_file, v.store_id) as id_count
    from v
)
select n.collected_date, n.base_ym, n.source_file, n.row_no, n.raw, n.store_id, n.store_name, n.sub_cd, n.sub_nm,
       n.sido_cd, n.sgg_cd, n.adm_cd, n.bjd_cd, n.lon, n.lat,
       array_to_string(array[  -- 정렬된 순서 MC3, MV1, MV2, MV3, MV4 (옛 sorted()와 같다)
           case when (n.lon_t is not null and n.lon is null and not n.lon_special)
                  or (n.lat_t is not null and n.lat is null and not n.lat_special) then 'MC3' end,
           case when n.store_id is null or n.sub_cd is null or n.sub_nm is null or n.sido_cd is null or n.sgg_cd is null
                  or n.adm_cd is null or n.bjd_cd is null or n.lon_t is null or n.lat_t is null then 'MV1' end,
           case when n.store_id is not null and n.id_count > 1 then 'MV2' end,
           case when n.lon_special or n.lat_special or n.lon not between 126.7 and 127.3 or n.lat not between 37.4 and 37.8
                then 'MV3' end,
           case when (n.sido_cd is not null and n.sido_cd <> '11')
                  or (n.sgg_cd is not null and (n.sgg_cd !~ '^[0-9]{5}$'
                      or (n.adm_cd is not null and not (n.adm_cd ~ '^[0-9]{8}$' and left(n.adm_cd, 5) = n.sgg_cd))
                      or (n.bjd_cd is not null and not (n.bjd_cd ~ '^[0-9]{10}$' and left(n.bjd_cd, 5) = n.sgg_cd))))
                then 'MV4' end
       ], ',') as reasons
from n
