-- title: 격리 기준 초과 출처
-- db: seoul
-- display: scalar
-- pos: 1 15 5 3
-- alert: 0.5
-- desc: 불량 행 비율이 기준 5%를 넘은 출처 수. 0이어야 한다.
SELECT count(*) AS "출처" FROM ops.quality WHERE quarantined_pct > 5
