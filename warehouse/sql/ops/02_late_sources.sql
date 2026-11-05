-- title: 늦은 출처
-- db: seoul
-- display: scalar
-- pos: 1 5 5 3
-- alert: 0.5
-- desc: 출처별 늦음 기준을 넘긴 출처 수. 0이어야 한다. 어떤 출처인지는 아래 표에 나온다.
SELECT count(*) AS "늦은 출처" FROM ops.freshness WHERE is_late
