{#- 모델을 실제 스키마가 아니라 빌드용 스키마(wh_build, mart_build)에 만든다. 검사를 모두 통과하면 교체한다 (결정 30 D2). -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name ~ '_build' if custom_schema_name else target.schema }}
{%- endmacro %}
