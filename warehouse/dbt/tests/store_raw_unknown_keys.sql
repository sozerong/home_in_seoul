-- 경고: 모르는 상가정보 원천 열. 새 열은 raw 안에 남아 잃는 것이 없으므로 멈추지 않는다(결정 60, 옛 MF2는 실패였다)
{{ config(severity='warn') }}
select distinct r.collected_date, r.source_file, k.key
from {{ source('raw', 'sbiz_store') }} r
cross join lateral jsonb_object_keys(r.raw) k(key)
where k.key not in ({%- for group in sbiz_used_keys() %}{% for key in group %}'{{ key }}', {% endfor %}{% endfor -%}
                    '{{ sbiz_other_keys() | join("', '") }}')
