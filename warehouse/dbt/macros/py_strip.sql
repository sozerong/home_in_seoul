{#- Python str.strip()과 같은 앞뒤 공백 제거. str.isspace()가 참인 29글자(전각 공백 U+3000, U+00A0 등 포함).
    옛 Python 정제와 결과를 맞추려고 둔다. ASCII 공백만 지우면 전월세 건물 이름 27행이 달랐다(측정 24) -#}
{% macro py_strip(expr) -%}
    btrim({{ expr }}, E'\u0009\u000a\u000b\u000c\u000d\u001c\u001d\u001e\u001f\u0020\u0085\u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000')
{%- endmacro %}
