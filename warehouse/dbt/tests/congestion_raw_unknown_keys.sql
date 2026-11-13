-- CF1 (2) 경고: 모르는 원천 키. 새 필드는 raw 안에 남아 잃는 것이 없으므로 멈추지 않는다(결정 63)
{{ config(severity='warn') }}
select distinct r.collected_date, k.key
from {{ source('raw', 'congestion') }} r
cross join lateral jsonb_object_keys(r.raw) k(key)
where k.key not in ({% for group in congestion_id_keys() + congestion_time_keys() %}{% for k in group %}'{{ k }}'{{ ", " if not loop.last }}{% endfor %}{{ ", " if not loop.last }}{% endfor %})
