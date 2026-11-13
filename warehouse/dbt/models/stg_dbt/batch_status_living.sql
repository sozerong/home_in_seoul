-- 생활이동 배치 관문 (결정 90, 76, 판단 64). 한 행 = 배치(대상연월, 수집일). 판정은 Spark SQL(spark_sql/living/22_batch_status.sql)이
-- 하고 실행기가 stg.living_batch_status에 남긴다. 여기서는 운영 카드용 합집합 batch_status와 같은 열로 바꾸기만 한다.
-- 열: source, batch_key, collected_date, period, approved, failed_checks, checked_at
select 'living' as source, target_ym || '/' || collected_date as batch_key, collected_date, target_ym::text as period,
       approved, string_to_array(failed_checks, ',')::text[] as failed_checks, checked_at
from {{ source('stg', 'living_batch_status') }}
