{#- 혼잡도 원천 키 목록 (결정 64). 정제 모델(stg_dbt/congestion_rows)과 테스트가 함께 쓴다.
    키마다 별칭 묶음이다. 원천이 이름을 바꾸면(S1) 그 묶음에 새 이름을 더하고, congestion_rows의 그 줄을
    coalesce(raw ->> '새이름', raw ->> '옛이름')으로 바꾼다. 고칠 곳은 이 두 줄이고 옛 수집일과 새 수집일이 함께 처리된다.
    키 이름은 모델 SQL에 글자로 있어 리니지 파서가 키 단위로 읽는다. -#}
{% macro congestion_id_keys() -%}
    {{ return([['DOW_SE'], ['LINE'], ['STTN_NO'], ['DPTRE_STTN'], ['UP_DOWN_SE']]) }}
{%- endmacro %}

{#- 05:30부터 30분 간격 39칸. 마지막 두 칸은 다음 날 00:00, 00:30 (TIME0000, TIME0030) -#}
{% macro congestion_time_keys() -%}
    {% set keys = [] %}
    {% for i in range(39) %}
        {% set m = 330 + 30 * i %}
        {% do keys.append(['TIME%02d%02d' | format((m // 60) % 24, m % 60)]) %}
    {% endfor %}
    {{ return(keys) }}
{%- endmacro %}
