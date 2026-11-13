-- CF1 (1) 실패: 쓰는 원천 키(신원 5 + 시간 39) 중 하나가 없는 원본 row, 또는 같은 키의 별칭이 둘 이상 함께 있는 row.
-- 원천 이름이 바뀌었으면 매크로 congestion_keys의 별칭 묶음에 새 이름을 더한다(결정 63, 64)
select r.collected_date, r.source_file, r.row_no
from {{ source('raw', 'congestion') }} r
where false
{%- for group in congestion_id_keys() + congestion_time_keys() %}
   or not (r.raw ?| array['{{ group | join("', '") }}'])
   {%- if group | length > 1 %}
   or (select count(*) from unnest(array['{{ group | join("', '") }}']) k where r.raw ? k) > 1
   {%- endif %}
{%- endfor %}
