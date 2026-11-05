-- title: 전월세 계약월별 누적 건수 (늦은 신고 감시)
-- db: seoul
-- display: line
-- pos: 38 0 24 7
-- x: 수집일
-- y: 건수
-- series: 계약월
-- desc: 최근 달은 신고가 늦게 쌓여 올라가는 것이 정상이다. 지난 달 선이 내려가면 수집을 확인한다.
SELECT deal_ymd AS "계약월", collected_date AS "수집일", count(*) AS "건수" FROM stg.rent
WHERE deal_ymd >= to_char(current_date - interval '5 months', 'YYYYMM') GROUP BY 1, 2 ORDER BY 2, 1
