-- title: 스케줄러 신호(분)
-- db: airflow
-- display: scalar
-- pos: 1 0 5 3
-- alert: 5
-- desc: Airflow 스케줄러가 마지막으로 살아 있다고 알린 시각. 5분을 넘으면 PC나 컨테이너가 꺼진 것이다.
SELECT round((extract(epoch FROM now() - max(latest_heartbeat)) / 60)::numeric, 1) AS "분 전"
FROM job WHERE job_type = 'SchedulerJob'
