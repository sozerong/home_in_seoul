-- 생활이동 운영 표 세 개를 keep 보관본(*_pre_f9)과 두 방향 EXCEPT ALL로 비교한다 (측정 42, handoff_judgments.md J-13).
-- 같은 zip을 다시 넣으면 수집일이 새로 생기고, 판정 표는 적재 시각(checked_at)이 바뀌므로 두 열은 빼고 본다.
-- 보관: CREATE TABLE keep.living_<표>_pre_f9 AS TABLE stg.living_<표> (실행 전에, 판단 102).
-- 실행: docker compose exec -T db psql -U seoul -d seoul < measure/living_keep_compare.sql   (경계·격리 표 각 4~5분)
\timing on
\set ON_ERROR_STOP 1
SELECT format($q$SELECT %L AS t, (SELECT count(*) FROM (SELECT %s FROM stg.%I EXCEPT ALL SELECT %s FROM keep.%I) a) AS new_minus_keep,
                (SELECT count(*) FROM (SELECT %s FROM keep.%I EXCEPT ALL SELECT %s FROM stg.%I) b) AS keep_minus_new,
                (SELECT count(*) FROM stg.%I) AS new_rows, (SELECT count(*) FROM keep.%I) AS keep_rows$q$,
              t, c, t, c, k, c, k, c, t, t, k)
FROM (SELECT t, t || '_pre_f9' AS k, string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position) AS c
      FROM information_schema.columns, unnest(array['living_batch_status', 'living_commute', 'living_quarantine']) t
      WHERE table_schema = 'stg' AND table_name = t AND column_name NOT IN ('collected_date', 'checked_at') GROUP BY t) x
\gexec
-- 열 이름·순서·타입이 같은가 (0이면 같음)
SELECT count(*) AS column_diff FROM (
  (SELECT replace(table_name, '_pre_f9', '') t, column_name, ordinal_position, data_type FROM information_schema.columns
   WHERE table_schema = 'keep' AND table_name LIKE 'living\_%\_pre\_f9'
   EXCEPT SELECT table_name, column_name, ordinal_position, data_type FROM information_schema.columns
   WHERE table_schema = 'stg' AND table_name IN ('living_batch_status', 'living_commute', 'living_quarantine'))
  UNION ALL
  (SELECT table_name, column_name, ordinal_position, data_type FROM information_schema.columns
   WHERE table_schema = 'stg' AND table_name IN ('living_batch_status', 'living_commute', 'living_quarantine')
   EXCEPT SELECT replace(table_name, '_pre_f9', ''), column_name, ordinal_position, data_type FROM information_schema.columns
   WHERE table_schema = 'keep' AND table_name LIKE 'living\_%\_pre\_f9')) d;
