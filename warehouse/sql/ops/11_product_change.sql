-- title: 상품 파일 변화 (최신 버전 vs 지난 버전)
-- db: seoul
-- display: table
-- pos: 30 12 12 8
-- status: 판정
-- desc: 행 수가 10% 넘게 줄면 확인이다. 내용은 파일 해시로 비교한다.
WITH v AS (SELECT DISTINCT version FROM ops.product_file ORDER BY version DESC LIMIT 2),
now_v AS (SELECT * FROM ops.product_file WHERE version = (SELECT max(version) FROM v) AND file LIKE '%.parquet'),
prev_v AS (SELECT * FROM ops.product_file WHERE version = (SELECT min(version) FROM v) AND file LIKE '%.parquet')
SELECT n.file AS "파일", CASE WHEN n.rows < p.rows * 0.9 THEN '확인' ELSE '정상' END AS "판정",
       n.rows AS "행 수", p.rows AS "지난 행 수",
       round(100.0 * (n.rows - p.rows) / nullif(p.rows, 0), 1) AS "증감(%)",
       CASE WHEN n.sha256 = p.sha256 THEN '같음' ELSE '바뀜' END AS "내용"
FROM now_v n LEFT JOIN prev_v p USING (file) ORDER BY n.file
