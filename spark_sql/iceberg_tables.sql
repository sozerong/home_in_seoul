-- Spark SQL 묶음(spark_sql/living)이 읽는 물리 표의 스키마. 리니지 스키마 추론(lineage/schema.py)만 읽는다(결정 91, design 3.7-2).
-- 실제 표를 만드는 곳은 따로 있다. 열이 바뀌면 이 파일도 고친다.

-- 생활이동 원본. transform/living_iceberg.py의 CREATE TABLE과 같은 열 (COLUMNS 순서)
CREATE TABLE lh.living.migration_rows (
    target_ym string, collected_date date, source_file string, row_no int, file_hour string, n_fields int,
    target_ym_raw string, weekday string, arrive_hour_raw string, origin_cd string, dest_cd string, sex string, age string,
    move_type string, avg_minutes_raw string, population_raw string, line string
) USING iceberg PARTITIONED BY (target_ym);

-- 생활이동 코드표(V4). 실행기가 레이크의 서울생활이동데이터_행정동코드_*.xlsx C열에서 임시 뷰 codes로 만든다(transform/living_migration.load_codes)
CREATE TABLE lake.seoul_living_migration.code_table (code string);
