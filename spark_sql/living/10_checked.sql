-- 생활이동 원본 한 행 = 한 행. 행 단위 규칙(docs/rules.md 4절 C2, C3, V1~V7)의 위반 사유를 reasons에 붙이고 버리지 않는다.
-- 규칙은 옛 transform/living_migration.py clean()과 같다(결정 105로 지움, 커밋 3efb02f에 있음). 다른 점: 범위 C(HW, 평일, 07~09시, 서울) 밖 행도 검증한다(design 3.4).
-- reasons = 규칙 번호를 이름순으로 쉼표로 이은 글자. 빈 글자 = 통과. codes = 생활이동 코드표(실행기가 xlsx에서 만든 임시 뷰, V4).
-- BROADCAST: 코드표(1,152행, 8KB)는 Python에서 만든 뷰라 Spark가 크기를 몰라 원본 14억 행을 셔플해 조인했다(F-9 원인 3, 처방 D).
SELECT /*+ BROADCAST(o, d) */ m.target_ym, m.collected_date, m.source_file, m.row_no, m.file_hour, m.n_fields, m.line,
       m.target_ym_raw, m.weekday, m.arrive_hour_raw, m.origin_cd, m.dest_cd, m.sex, m.age, m.move_type,
       m.avg_minutes_raw, m.population_raw,
       TRY_CAST(m.avg_minutes_raw AS INT) AS avg_minutes,
       TRY_CAST(NULLIF(m.population_raw, '*') AS DECIMAL(12, 2)) AS population,  -- C2: "*" = NULL(가려짐). 원본 소수 자리는 2까지(test_population_exact)
       m.pop_double,
       CASE WHEN m.n_fields <> 10 THEN 'V1' ELSE concat_ws(',',
         -- C3: 숫자로 못 바꿈. 옛 Python int()/float()에 맞춘다(NaN, 무한대도 숫자로 치지 않음)
         IF((m.avg_minutes_raw <> '' AND NOT m.avg_minutes_raw RLIKE '^ *[+-]?[0-9]+ *$')
            OR (m.population_raw NOT IN ('*', '') AND (m.pop_double IS NULL OR isnan(m.pop_double)
                                                       OR abs(m.pop_double) = CAST('Infinity' AS DOUBLE))), 'C3', NULL),
         IF('' IN (m.target_ym_raw, m.weekday, m.arrive_hour_raw, m.origin_cd, m.dest_cd, m.sex, m.age, m.move_type,
                   m.avg_minutes_raw, m.population_raw), 'V1', NULL),
         IF(m.target_ym_raw <> m.target_ym, 'V2', NULL),
         IF(m.weekday NOT IN ('월', '화', '수', '목', '금', '토', '일') OR m.arrive_hour_raw <> m.file_hour, 'V3', NULL),
         IF(o.code IS NULL OR d.code IS NULL, 'V4', NULL),
         IF(m.sex NOT IN ('M', 'F')
            OR m.age NOT IN ('00', '10', '15', '20', '25', '30', '35', '40', '45', '50', '55', '60', '65', '70', '75', '80')
            OR m.move_type NOT IN ('HW', 'HE', 'EW', 'EE', 'EH', 'WW', 'WE', 'WH', 'HH'), 'V5', NULL),
         -- V6: 정수 모양인데 INT를 넘으면 TRY_CAST가 NULL이므로 그것도 범위 밖으로 친다
         IF(m.avg_minutes_raw RLIKE '^ *[+-]?[0-9]+ *$'
            AND (TRY_CAST(m.avg_minutes_raw AS INT) IS NULL OR TRY_CAST(m.avg_minutes_raw AS INT) NOT BETWEEN 1 AND 240), 'V6', NULL),
         IF(m.pop_double < 3 AND m.population_raw <> '*', 'V7', NULL)
       ) END AS reasons
FROM (SELECT *, TRY_CAST(population_raw AS DOUBLE) AS pop_double FROM lh.living.migration_rows) m
LEFT JOIN codes o ON o.code = m.origin_cd
LEFT JOIN codes d ON d.code = m.dest_cd
