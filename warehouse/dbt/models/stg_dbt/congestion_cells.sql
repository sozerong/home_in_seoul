-- 한 행 = 행 단위 규칙을 통과한 원본 row 하나의 시간대 칸 하나(39칸 중 하나). 세로로 풀기(CC3)와 숫자 읽기(CC5).
-- 값이 숫자가 아니면 number가 null이다. 0~300 밖이거나 null이면 격리(CV5), 아니면 정제 결과로 간다.
{{ config(materialized='view') }}

with cells as (
    select r.collected_date, r.source_file, r.row_no, r.raw, r.dow, r.line, r.sttn_no, r.sttn_nm, r.direction,
           t.key, {{ py_strip('t.value') }} as value
    from {{ ref('congestion_rows') }} r
    cross join lateral jsonb_each_text(r.raw) t
    where cardinality(r.reasons) = 0
      and t.key in ({% for group in congestion_time_keys() %}'{{ group[0] }}'{{ ", " if not loop.last }}{% endfor %})  -- 시간 키는 별칭 없음
)
select c.*,
       -- Python float()이 읽는 꼴(부호, 소수, 지수). ponytail: 1e400처럼 double 범위를 넘는 값은 여기서 빌드가 실패한다. 그런 값이 오면 길이 검사를 더한다
       case when c.value ~ '^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?$' then c.value::double precision end as number
from cells c
