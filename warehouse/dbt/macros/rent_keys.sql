{#- 전월세 원천 키 목록 (결정 64, 72). 정제 모델(stg_dbt/rent_rows)과 테스트가 함께 쓴다.
    쓰는 키는 별칭 묶음이다. 원천이 이름을 바꾸면 그 묶음에 새 이름을 더하고 rent_rows의 그 줄을 coalesce로 바꾼다.
    옛 transform/rent.py의 EXPECTED_FIELDS(F1)를 둘로 나눈 것이다: 쓰는 키(없으면 실패)와 알지만 안 쓰는 키(없어도 됨). -#}
{% macro rent_used_keys(kind) -%}
    {% set common = [['buildYear'], ['contractTerm'], ['contractType'], ['dealDay'], ['dealMonth'], ['dealYear'],
                     ['deposit'], ['excluUseAr'], ['floor'], ['jibun'], ['monthlyRent'], ['preDeposit'],
                     ['preMonthlyRent'], ['umdNm'], ['useRRRight']] %}
    {% set extra = {'offi': [['offiNm']], 'rh': [['mhouseNm'], ['houseType']]} %}
    {{ return(common + extra[kind]) }}
{%- endmacro %}

{% macro rent_other_keys(kind) -%}
    {{ return({'offi': ['sggCd', 'sggNm'], 'rh': ['sggCd']}[kind]) }}
{%- endmacro %}

{#- C2: 앞뒤 공백 제거(Python str.strip()과 같음), 빈 값은 null -#}
{% macro rent_text(key) -%}
    nullif({{ py_strip("r.raw ->> '" ~ key ~ "'") }}, '')
{%- endmacro %}

{#- C3: 쉼표를 지우고 정수로. 정수 꼴이 아니면 null(그 칸의 정제 규칙 번호가 사유가 된다).
    ponytail: int 범위를 넘는 숫자는 여기서 빌드가 실패한다(옛 코드는 그 처리 단위 적재가 실패했다) -#}
{% macro rent_int(col) -%}
    case when replace({{ col }}, ',', '') ~ '^[+-]?[0-9]+$' then replace({{ col }}, ',', '')::int end
{%- endmacro %}

{#- Python float()이 읽는 꼴(부호, 소수, 지수). ponytail: nan, inf는 여기서 null(C5)이다. 옛 코드는 float으로 읽고 V5로 격리했다 -#}
{% macro rent_float(col) -%}
    case when replace({{ col }}, ',', '') ~ '^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?$'
         then replace({{ col }}, ',', '')::double precision end
{%- endmacro %}

{#- Python str.zfill(4): 4자리보다 짧을 때만 앞에 0. lpad는 긴 값을 자르므로 쓰지 않는다 -#}
{% macro zfill4(col) -%}
    case when length({{ col }}) >= 4 then {{ col }} else lpad({{ col }}, 4, '0') end
{%- endmacro %}

{#- 배치 관문의 전월세 배치 키(결정 75). batch_status와 정제 결과(rent, rent_quarantine)가 같은 식으로 맞춘다 -#}
{% macro rent_batch_key() -%}
    concat_ws('/', source_type, collected_date, sgg_cd, deal_ymd)
{%- endmacro %}
