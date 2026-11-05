-- title: 조치가 필요한 DAG
-- db: airflow
-- display: scalar
-- pos: 1 10 5 3
-- alert: 0.5
-- desc: 마지막 실행이 실패했거나, 일정이 있는데 중지됐거나, 실행이 밀린 DAG 수. 기간과 상관없이 해결될 때까지 남는다.
WITH last AS (
    SELECT DISTINCT ON (dag_id) dag_id, state FROM dag_run WHERE start_date IS NOT NULL ORDER BY dag_id, start_date DESC
)
SELECT count(*) AS "DAG" FROM dag d LEFT JOIN last l USING (dag_id)
WHERE NOT d.is_stale AND (l.state = 'failed' OR (d.is_paused AND nullif(d.timetable_summary, 'None') IS NOT NULL)
                          OR (NOT d.is_paused AND d.next_dagrun_create_after < now() - interval '1 hour'))
