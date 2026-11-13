{#- 모델을 실제 스키마가 아니라 빌드용 스키마(wh_build, mart_build)에 만든다. 검사를 모두 통과하면 교체한다 (결정 30 D2).
    변이 시험은 build_suffix를 _mut로 넘겨 wh_mut, mart_mut에 만든다(결정 59). -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name ~ var('build_suffix', '_build') if custom_schema_name else target.schema }}
{%- endmacro %}
