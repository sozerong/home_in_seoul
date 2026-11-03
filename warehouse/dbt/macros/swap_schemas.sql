{#- 빌드 스키마를 실제 스키마로 교체한다(blue/green, 결정 30 D2). `dbt build`가 모두 통과한 뒤에만 부른다.
    모든 문장을 한 트랜잭션으로 보내서 중간에 하나라도 실패하면 아무것도 바뀌지 않는다. 옛 스키마는 바로 지운다(결정 32).
    사용법: dbt run-operation swap_schemas -#}
{% macro swap_schemas(schemas=['wh', 'mart']) %}
    {% set sql %}
        {% for s in schemas %}
        create schema if not exists {{ s }};  -- 빈 DB에서 처음 돌 때도 이름 바꾸기가 되도록
        alter schema {{ s }} rename to {{ s }}_old;
        alter schema {{ s }}_build rename to {{ s }};
        drop schema {{ s }}_old cascade;
        {% endfor %}
    {% endset %}
    {% do run_query(sql) %}  {#- run_query가 끝나며 커밋한다. 문장 묶음 하나라 PostgreSQL이 한 트랜잭션으로 실행한다 -#}
    {{ log('교체 완료: ' ~ schemas | join(', '), info=True) }}
{% endmacro %}

{#- 빌드 전에 지난번 실패로 남은 빌드 스키마를 지운다. 지운 모델의 옛 테이블이 남아 교체 때 함께 들어가지 않게 한다.
    사용법: dbt run-operation drop_build_schemas -#}
{% macro drop_build_schemas(schemas=['wh', 'mart']) %}
    {% for s in schemas %}
        {% do run_query('drop schema if exists ' ~ s ~ '_build cascade') %}
    {% endfor %}
{% endmacro %}
