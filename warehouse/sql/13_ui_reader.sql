-- 파이프라인 UI 읽기 전용 계정 (결정 106, docs/ui.md). 소비자마다 역할을 나눈다(metabase_ro, lineage_reader와 같은 방식).
-- 실행(관리자 seoul, 여러 번 돌려도 같음):
--   docker exec -i new_proj-db-1 psql -U seoul -d seoul -v ON_ERROR_STOP=1 -v ui_pw="<.env의 UI_READER_PASSWORD>" < warehouse/sql/13_ui_reader.sql
-- ui_pw를 빼먹으면 ALTER ROLE이 문법 오류로 멈춘다(빈 비밀번호로 조용히 넘어가지 않음).

SELECT 'CREATE ROLE ui_reader LOGIN' WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ui_reader') \gexec
ALTER ROLE ui_reader LOGIN PASSWORD :'ui_pw';
ALTER ROLE ui_reader SET default_transaction_read_only = on;  -- 권한과 별개로 연결 자체도 읽기 전용

-- 화면이 읽는 스키마(docs/handoff_ui.md 3절): lineage(지도·회차), ops(품질·신선도·빌드·엔진), stg_dbt(관문 판정).
-- ops.quality의 ops.row_count()는 호출자 권한으로 stg_dbt 표를 세므로 stg_dbt는 표 하나가 아니라 스키마 전체가 필요하다.
GRANT USAGE ON SCHEMA lineage, ops, stg_dbt TO ui_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA lineage, ops, stg_dbt TO ui_reader;

-- stg_dbt는 wh_load마다 stg_dbt_build를 새로 만들어 이름을 바꿔 끼운다(swap_schemas, 결정 30). 위 GRANT는 다음 빌드에서 사라지므로
-- seoul이 앞으로 만드는 스키마와 표에 기본 권한을 준다(metabase_setup.setup_role과 같은 방식).
-- ponytail: 기본 권한은 스키마를 가리지 못해 다음 빌드부터 wh·mart도 읽게 된다. 읽기 전용 + 127.0.0.1이라 받아들임. 좁혀야 하면 SECURITY DEFINER 뷰.
ALTER DEFAULT PRIVILEGES FOR ROLE seoul GRANT SELECT ON TABLES TO ui_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE seoul GRANT USAGE ON SCHEMAS TO ui_reader;
