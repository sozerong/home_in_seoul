-- title: 혼잡도 API 갱신 (최근 수집일 vs 직전 수집일)
-- db: seoul
-- display: table
-- pos: 45 0 24 4
-- status: 판정
-- desc: 결정 7. 서울교통공사 혼잡도 API가 값을 갱신했는지 본다. 같음이면 아직 갱신 전이고, 바뀜이어도 실패가 아니다. 옛 transform/congestion.py가 로그에 남기던 비교를 옮긴 것이다(결정 42).
WITH days AS (
    SELECT DISTINCT collected_date FROM stg_dbt.congestion ORDER BY collected_date DESC LIMIT 2
),
pair AS (
    SELECT max(collected_date) AS latest, min(collected_date) AS previous FROM days
),
cmp AS (
    SELECT count(*) FILTER (WHERE a.congestion = b.congestion) AS same,
           count(*) FILTER (WHERE a.congestion <> b.congestion) AS changed,
           count(*) FILTER (WHERE a.congestion IS NULL OR b.congestion IS NULL) AS one_side
    FROM (SELECT * FROM stg_dbt.congestion WHERE collected_date = (SELECT latest FROM pair)) a
    FULL JOIN (SELECT * FROM stg_dbt.congestion WHERE collected_date = (SELECT previous FROM pair)) b
        USING (dow, line, sttn_no, direction, slot)
)
SELECT p.latest AS "최근 수집일", p.previous AS "직전 수집일",
       CASE WHEN p.latest = p.previous THEN '비교 불가' WHEN c.changed = 0 AND c.one_side = 0 THEN '같음' ELSE '바뀜' END AS "판정",
       c.same AS "같은 값", c.changed AS "다른 값", c.one_side AS "한쪽에만"
FROM pair p CROSS JOIN cmp c
