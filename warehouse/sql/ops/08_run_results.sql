-- title: 일별 실행 결과 (최근 30일)
-- db: airflow
-- display: bar
-- pos: 22 0 10 7
-- x: 날짜
-- y: 실행
-- series: 결과
-- stack: true
-- desc: 빨간 막대가 있으면 그날 실패한 실행이 있다.
SELECT to_char(start_date AT TIME ZONE 'Asia/Seoul', 'MM-DD') AS "날짜",
       CASE state WHEN 'success' THEN '성공' WHEN 'failed' THEN '실패' ELSE '진행' END AS "결과", count(*) AS "실행"
FROM dag_run WHERE start_date > now() - interval '30 days' GROUP BY 1, 2 ORDER BY 1, 2
