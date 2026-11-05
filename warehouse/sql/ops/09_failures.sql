-- title: 실패 기록과 원인
-- db: seoul
-- display: table
-- pos: 22 10 14 7
-- desc: 작업이 실패하면 시각, 위치, 오류가 여기에 남는다. 같은 내용이 logs/alerts.log에도 있다.
SELECT to_char(at AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI') AS "시각", dag_id AS "DAG", task_id AS "작업", error AS "오류"
FROM ops.failure_log ORDER BY at DESC LIMIT 20
