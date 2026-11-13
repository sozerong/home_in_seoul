-- P3 스키마 이력 (결정 82). 한 행 = (출처, 수집일, 키 집합). 원본(raw.*)은 지우지 않으므로 매 빌드 다시 계산해도 과거 이력이 나온다.
-- 한 수집일에 모양이 둘 섞여 들어오면 두 줄이 된다. 전월세는 offi, rh를 출처에 넣는다(키가 다르다).
-- ponytail: 관문(batch_status)의 키 검사와 계산을 공유하지 않는다. 그쪽은 쓰는 키 묶음의 있음/없음, 여기는 키 집합 전체다.
with shapes as (
    select 'congestion' as source, collected_date, array(select jsonb_object_keys(raw) order by 1) as keys
    from {{ source('raw', 'congestion') }}
    union all
    select 'rent_' || source_type, collected_date, array(select jsonb_object_keys(raw) order by 1)
    from {{ source('raw', 'rent') }}
    union all
    select 'store', collected_date, array(select jsonb_object_keys(raw) order by 1)
    from {{ source('raw', 'sbiz_store') }}
)
select source, collected_date, md5(array_to_string(keys, chr(31))) as keyset_hash, keys, count(*) as items
from shapes
group by source, collected_date, keys
