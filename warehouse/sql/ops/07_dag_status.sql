-- title: DAG 상태
-- db: airflow
-- display: table
-- pos: 14 0 24 8
-- status: 상태
-- desc: 실패는 일정 작업의 실패, 확인은 수동 작업의 실패, 중지됨은 일정이 있는데 꺼진 DAG, 밀림은 예정 시각이 1시간 넘게 지난 DAG다.
WITH last AS (
    SELECT DISTINCT ON (dag_id) dag_id, state, run_type, start_date, end_date, run_after
    FROM dag_run WHERE start_date IS NOT NULL ORDER BY dag_id, start_date DESC
),
result AS (
SELECT d.dag_id AS "DAG",
       CASE WHEN d.is_paused AND nullif(d.timetable_summary, 'None') IS NOT NULL THEN '중지됨'
            WHEN l.state = 'failed' AND nullif(d.timetable_summary, 'None') IS NULL THEN '확인'
            WHEN l.state = 'failed' THEN '실패'
            WHEN NOT d.is_paused AND d.next_dagrun_create_after < now() - interval '1 hour' THEN '밀림'
            ELSE '정상' END AS "상태",
       CASE WHEN nullif(d.timetable_summary, 'None') IS NULL THEN '수동' WHEN d.timetable_summary = 'Asset' THEN '수집 뒤 자동' ELSE d.timetable_summary END AS "일정",
       coalesce(to_char(l.start_date AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI'), '기록 없음') AS "마지막 실행",
       coalesce(l.state, '-') AS "결과",
       round((extract(epoch FROM l.end_date - l.start_date) / 60)::numeric, 1) AS "소요(분)",
       CASE WHEN l.run_type = 'scheduled' THEN round((extract(epoch FROM l.start_date - l.run_after) / 3600)::numeric, 1) END AS "시작 지연(시간)",
       CASE WHEN d.is_paused THEN '-' ELSE coalesce(to_char(d.next_dagrun_create_after AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI'), '-') END AS "다음 실행"
FROM dag d LEFT JOIN last l USING (dag_id)
WHERE NOT d.is_stale
)
SELECT * FROM result ORDER BY "상태" = '정상', "DAG"
