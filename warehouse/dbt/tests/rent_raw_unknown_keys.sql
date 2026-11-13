-- F1 (2) 경고: 모르는 원천 키. 새 필드는 raw 안에 남아 잃는 것이 없으므로 멈추지 않는다(결정 63, 72)
{{ config(severity='warn') }}
select distinct r.source_type, r.collected_date, k.key
from {{ source('raw', 'rent') }} r
cross join lateral jsonb_object_keys(r.raw) k(key)
where false
{%- for kind in ['offi', 'rh'] %}
   or (r.source_type = '{{ kind }}' and k.key not in (
       {%- for group in rent_used_keys(kind) %}{% for key in group %}'{{ key }}', {% endfor %}{% endfor -%}
       '{{ rent_other_keys(kind) | join("', '") }}'))
{%- endfor %}
