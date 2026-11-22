-- 준비 (psql -h /tmp -p 5499 -U postgres -f pg_tests.sql 로 준비만, 시험은 pg_tests.sh)
DROP TABLE IF EXISTS t, other, p, p_1, p_1n CASCADE;
CREATE TABLE t(id int, v text); INSERT INTO t SELECT g, 'old' FROM generate_series(1,100000) g;
CREATE TABLE other(x int); INSERT INTO other VALUES (1);
CREATE TABLE p(ym int, v text) PARTITION BY LIST (ym);
CREATE TABLE p_1(LIKE p); INSERT INTO p_1 SELECT 1,'old' FROM generate_series(1,1000);
ALTER TABLE p ATTACH PARTITION p_1 FOR VALUES IN (1);
CREATE TABLE p_1n(LIKE p); INSERT INTO p_1n SELECT 1,'new' FROM generate_series(1,1000);
ALTER TABLE p_1n ADD CONSTRAINT c CHECK (ym = 1 AND ym IS NOT NULL);
