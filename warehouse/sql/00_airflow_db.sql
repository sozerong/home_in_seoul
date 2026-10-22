-- 단계 7: Airflow 메타 정보를 담을 DB (결정 19). 데이터 DB(seoul)와 같은 서버, 다른 DB.
-- CREATE DATABASE는 IF NOT EXISTS를 쓸 수 없어서, 없을 때만 명령을 만들어 실행한다(\gexec). 여러 번 실행해도 된다.
SELECT 'CREATE DATABASE airflow OWNER seoul'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
