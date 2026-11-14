# 시험 케이스 기록

design_remaining_work.md의 절마다 무엇을 넣어 어떻게 확인했는지 남긴다. 수치와 조건은 `docs/measurements.md`의 측정 번호, 시험 중 만난 오류는 `docs/failure_analysis.md`의 F 번호로 가리킨다. 다시 돌릴 수 있게 넣은 SQL과 명령을 그대로 적는다.

형식: 케이스마다 목적, 넣은 것, 기대, 결과, 다시 돌리는 법.

---

## 1절. 배치 관문 (결정 75~77, 측정 25, 2026-11-13)

### 1-0. 넣기 전 확인: 지금 데이터에서 거절되는 과거 배치가 없음
- 목적: 옛 테스트는 최근 수집일(CF4)·최근 단위(F2)만 봤다. 관문은 모든 배치를 판정하므로, 넣는 순간 과거 배치가 거절되어 결과가 바뀌는지 먼저 본다.
- 넣은 것: 없음(읽기만).
- 기대: 혼잡도 수집일마다 직전 대비 10% 넘게 줄어든 날 0, 전월세 처리 단위 중 격리 5% 초과 0.
- 결과: 혼잡도 5개 수집일 모두 1,671행. 전월세 1,352단위 중 초과 0. 통과.
- 다시 돌리는 법:
  ```sql
  select collected_date, count(*) n, lag(count(*)) over (order by collected_date) prev
  from raw.congestion group by 1 order by 1;

  select count(*) filter (where bad::numeric / items > 0.05) over5
  from (select count(*) items, count(*) filter (where reasons <> '') bad
        from stg_dbt.rent_rows group by source_type, collected_date, sgg_cd, deal_ymd) u;
  ```

### 1-1. 같음 증명: 관문을 넣어도 결과가 그대로
- 목적: 거절이 없을 때 관문이 결과를 바꾸지 않는지 본다.
- 넣은 것: 관문을 넣은 dbt 빌드를 빌드용 스키마(`*_build`)에 만들고, 교체하기 전 실제 스키마와 비교.
- 기대: 모든 비교에서 차이 0.
- 결과: `stg_dbt.rent`, `stg_dbt.congestion`, 두 격리 표(빌드 시각 열 제외), `wh.fact_rent`, `wh.fact_congestion`, 상품 표 4개 모두 두 방향 `EXCEPT ALL` 차이 0. wh_load 뒤 상품 파일 8개 sha256도 오전 실행과 같음. 통과.
- 다시 돌리는 법: `dbt run-operation drop_build_schemas; dbt build` 뒤 표마다
  ```sql
  select count(*) from ((select * from stg_dbt.rent except all select * from stg_dbt_build.rent)
                        union all (select * from stg_dbt_build.rent except all select * from stg_dbt.rent)) d;
  ```

### 1-2. unit test: 거절된 수집일의 칸은 정제 결과에서 빠짐
- 목적: 정제 모델이 승인 배치만 읽는 규칙을 DB 없이 고정한다.
- 넣은 것: `stg_dbt.yml`의 `congestion_values`에 거절된 수집일(2026-11-02, `{congestion_raw_shrink}`)의 칸 하나와 그 판정을 입력으로 더함.
- 기대: 기대값에 11-02 칸이 없다.
- 결과: 통과. 처음에는 입력을 dict로 줘서 `cast(null as ARRAY)` 오류(F-1), sql 형식으로 고침.
- 다시 돌리는 법: `dbt test -s congestion_values`

### 1-3. 체크포인트: 나쁜 배치 하나는 그 배치만 거절
- 목적: 데이터 문제가 그 배치에서 끝나고, 다른 배치와 상품은 갱신되는지 본다(결정 75).
- 넣은 것: `raw.rent`에 새 배치 키 `rh/2026-11-14/11710/202610`. 2026-11-13 수집의 같은 단위(1,206 item)를 복사하고 10개 중 1개 item의 `deposit`을 `abc`로 바꿈(120개, 9.95%, 기준 5% 초과). 원천 파일 이름은 `checkpoint_bad_batch.xml`. 기존 행은 건드리지 않음. 그 뒤 wh_load DAG 실행.
- 기대: DAG 성공. 그 배치만 `rent_quarantine_ratio`로 거절. 그 단위의 하류는 직전 승인 배치(11-13)를 쓴다. 상품 그대로. 운영 카드 14에 한 줄.
- 결과 (모두 통과):

  | 본 것 | 값 |
  |---|---|
  | wh_load | 성공 (dbt_build 2분 19초, swap, export) |
  | `stg_dbt.batch_status` 전월세 | 1,353배치 중 거절 1, `{rent_quarantine_ratio}` |
  | `stg_dbt.rent`·`rent_quarantine`의 11-14 행 | 0, 0 |
  | 거르지 않은 `stg_dbt.rent_rows`의 11-14 사유 있는 행 | 120 |
  | 그 단위의 `wh.fact_rent` | 11-13 수집분 1,206행 그대로 |
  | `wh.fact_rent` 전체 | 86,903행 그대로 |
  | `ops.freshness` 전월세 마지막 수집일 | 2026-11-13 그대로 |
  | 상품 파일 8개 sha256 | 모두 같음 |
  | 카드 `14_rejected_batches` | 한 줄 |

- 시험 준비 중 오류: 처음 복사 때 기본 키 중복(F-2). 단위 하나가 원천 파일 2개(1,000건 + 206건)이고 `item_no`는 파일 안 번호였다. 복사본 번호를 다시 매겨 해결.
- 다시 돌리는 법:
  ```sql
  insert into raw.rent (source_type, collected_date, sgg_cd, deal_ymd, source_file, item_no, raw, loaded_at)
  select source_type, date '2026-11-14', sgg_cd, deal_ymd, 'checkpoint_bad_batch.xml', n,
         case when n % 10 = 0 then jsonb_set(raw, '{deposit}', '"abc"') else raw end, now()
  from (select *, row_number() over (order by source_file, item_no) n from raw.rent
        where source_type = 'rh' and collected_date = '2026-11-13' and sgg_cd = '11710' and deal_ymd = '202610') s;
  ```
  그 뒤 `airflow dags trigger wh_load`, 확인은 `select * from stg_dbt.batch_status where not approved;`
- 정리: 시험 행은 `delete from raw.rent where source_file = 'checkpoint_bad_batch.xml';`(1,206행). 지우기 전 사용자 확인(design_remaining_work.md 0절). **2026-11-13 현재 남아 있음.**

### 1-4. 체크포인트: 모델 SQL 오류는 빌드 전체를 멈춤
- 목적: 코드 문제는 지금처럼 모든 출처의 갱신을 막는지 본다(결정 75).
- 넣은 것: `warehouse/dbt/models/stg_dbt/congestion.sql`의 `c.number`를 없는 컬럼 `c.number_typo`로 바꾸고 wh_load DAG 실행.
- 기대: dbt_build 실패, 교체와 내보내기 안 함, 실제 표 그대로.
- 결과 (통과): dbt_build 실패(PASS 85, ERROR 1, SKIP 19). swap_schemas·export_product는 upstream_failed. 먼저 막은 것은 모델이 아니라 unit test `congestion_values`(`column c.number_typo does not exist`)였다. 실제 `stg_dbt.batch_status`의 `checked_at`이 앞 시험(03:33:52 UTC) 그대로라 교체가 없었다. `ops.failure_log`에 dbt_build 실패 한 줄. 실패한 빌드가 남긴 `*_build` 스키마 3개는 다음 빌드가 지운다.
- 정리: 파일을 시험 전 백업과 바이트 단위로 같게 되돌림(`cmp`로 확인). Git Bash의 `sed -i`가 줄 끝을 CRLF에서 LF로 바꿔서, 글자만 되돌리면 파일이 달라진다.

### 1절에서 시험하지 않은 것
- 혼잡도 배치 거절(행 수 급감, 같은 칸 중복, 쓰는 키 없음). 판정식은 옮긴 테스트 SQL과 같지만 거절을 실제로 일으켜 보지는 않았다. [확인 필요]
- 법정동 코드표가 빈 경우 전월세 단위가 모두 거절되어 `rent_listing_not_empty`가 실패하는지(README에 [확인 필요]로 적음).

---

## 2절. 상가정보 이전 (결정 78~80)

### 2.1-1. 원본 적재: 행 수와 옛 경로 수 맞추기 (측정 26)
- 목적: 파싱만 하는 적재기가 서울 CSV 전체를 빠짐없이 담는지 본다.
- 넣은 것: `python -m transform.sbiz_store_raw 2026-10-21`
- 기대: 행 수 = CSV 데이터 행 수, row_no 연속, 6코드 행 수 = 옛 `stg.sbiz_store` 30,394, 법정동 세 값 조합 수 = 옛 `stg.bjd_code` 466.
- 결과: 554,092행, row_no 1~554,092 겹침 없음, 행마다 키 수 같음, 6코드 30,394, 조합 466. 통과.
- 다시 돌리는 법:
  ```sql
  select count(*), count(distinct row_no), min(row_no), max(row_no),
         count(distinct (select count(*) from jsonb_object_keys(raw))) key_counts from raw.sbiz_store;
  select count(*) filter (where raw->>'상권업종소분류코드' in ('G20405','G20404','G21501','Q10201','S20901','S20902')),
         count(distinct (raw->>'시군구코드', raw->>'법정동명', raw->>'법정동코드')) from raw.sbiz_store;
  ```

### 2.1-2. 원본 적재: 다시 넣어도 같은 결과 (멱등)
- 넣은 것: 같은 명령을 7번.
- 기대: 매번 554,092행.
- 결과: 매번 554,092행. 통과. 다만 표 크기가 불어나는 것을 찾았다(F-3).

### 2.1-3. 원본 적재: 다시 넣을 때 표 크기 (F-3)
- 목적: 죽은 행이 쌓이는지, VACUUM이 사이에 돌면 멈추는지 본다.
- 넣은 것: 연달아 4번 적재 → `VACUUM FULL` → 적재 → `VACUUM` → 적재 → `VACUUM` + 적재.
- 결과: 3,464MB → 866MB → 1,732MB → 1,732MB → 1,732MB → 1,732MB (heap). 사이에 VACUUM이 돌면 2배치분에서 멈춘다.
- 다시 돌리는 법: `select pg_size_pretty(pg_relation_size('raw.sbiz_store')), n_dead_tup from pg_stat_user_tables where relid = 'raw.sbiz_store'::regclass;`

### 2.4-1. 같음 증명: 상가정보를 SQL로 옮겨도 결과가 그대로 (측정 27)
- 넣은 것: 같은 원본(상가정보 수집일 2026-10-21)으로 옛 경로(`stg.sbiz_store`)와 새 경로(`stg_dbt.store`)를 둘 다 둔 채 빌드.
- 기대: 새 `store`의 6코드 부분과 옛 표 차이 0, `bjd_code` 466행 차이 0, 격리 0 대 0. 상가정보와 무관한 표(rent, congestion, dim_dong, fact_rent, dong_amenity, dong_profile, rent_listing, commute_od) 차이 0. `map_dong_mois`는 다수결 표본이 바뀌어(결정 79) `stores`, `share`만 바뀌고 대응은 그대로.
- 결과: 모두 기대대로. `map_dong_mois` 427행 모두 `stores`, `share`가 바뀜, 코드 이동 0, 대표 코드 뒤집힘 0.

### 2.5-1. 리니지 행 집합 간선: 정답 대조
- 넣은 것: `python -m measure.lineage_score` (정답 6개 모델, ROWSET 정답 45줄).
- 기대: DIRECT, INDIRECT 놓침·틀림 0이 그대로이고 ROWSET도 놓침·틀림 0.
- 결과: DIRECT 78/78, INDIRECT 218/218, ROWSET 45/45. 통과.

### 2.5-2. 리니지 행 집합 간선: 실제 회차와 하류 조회
- 넣은 것: wh_load 교체 뒤 자동으로 돈 lineage 회차 25. `lineage.downstream`에서 `wh.fact_rent.contract_type`.
- 기대: `product.dong_profile.monthly_deals`가 `rowset_path`로 나온다.
- 결과: 나옴(`jeonse_deals`, `monthly_deals` 2개). 다만 `product.rent_listing`에는 `*` 노드가 없어 ROWSET 경로가 마트에서 멈춘다(F-5, 고치지 않음).

### 2.6-1. S6a 재측정: 업종 하나 추가 (측정 28)
- 넣은 것: `python -m measure.s6a_sql`. 사본 프로젝트에 헬스장 R10307(seed 1줄, 마트 SQL 3줄)을 더해 `_s6a` 스키마로 빌드.
- 기대: Python 수정 0, 원본 다시 적재 0, `dong_profile`에 컬럼 2개만 더해지고, `dong_mois_map`, `commute_od`, `rent_listing` 차이 0.
- 결과: 모두 기대대로. dbt 116개 PASS, 338.3초. 헬스장 3,570곳(측정 17과 같음).
- 다시 돌리는 법: `docker compose exec -w /opt/project airflow-scheduler python -m measure.s6a_sql` (결정 109 뒤 서비스 이름)

## 3절. 생활이동 이전 (결정 87~94)

### 3.2-1. 첫 측정: 경계 표 그룹 수가 기준을 넘으면 멈춤 (측정 29)
- 넣은 것: `docker compose run --rm spark-job python -m measure.living_g3` (결정 109 뒤 실행 명령. 측정 당시는 Airflow 컨테이너 안 Spark 가상환경) (Iceberg 원본 10개월, 1,405,791,964행).
- 기대(설계 3.2): G3 그룹 수가 40,382,008 이하면 진행, 넘으면 멈추고 차원 줄이기 선택지를 보인다.
- 결과: 816,203,740(20.2배). 멈춤. 후보 6개를 더 재서 기준 아래는 f(13,842,750) 하나였다. 사용자 결정 대기.

## 4절. P6 규칙 버전 (결정 81, 측정 30)

### 4-1. 같은 코드면 같은 해시, 바뀐 단위 0
- 넣은 것: 코드 그대로 빌드 세 번(wh_load 한 번, 손 빌드 두 번).
- 결과: sql_hash 세 번 같음(9d8b25cc4df9), `ops.build_diff` 0행.

### 4-2. 매크로 한 줄을 고치면 그 모델 해시와 바뀐 단위가 나옴
- 넣은 것: `py_strip` 매크로의 지울 글자를 29개에서 ASCII 6개로. 빌드 스키마에만 빌드, 교체 안 함.
- 기대: 매크로를 쓰는 모델 해시만 바뀌고, 전월세 건물 이름이 달라진 단위가 나온다.
- 결과: 모델 4개 해시만 바뀜. 전월세 24단위 27행, 상가정보 1단위 13행. 통과. 되돌린 뒤 wh_load 다시 실행.

### 4-3. 커밋하지 않은 코드로 만든 지도는 MCP가 stale
- 결과: `stale: True`(clean=false, 해시도 다름). clean=true에서 false가 나오는지는 커밋 뒤에 볼 일이다.

## 5절. P3 스키마 이력 (결정 82)

### 5-1. 같은 수집일 두 번 → 한 줄, 키를 바꾼 배치 → 새 해시
- 넣은 것: `python -m measure.raw_shape_check` (`raw_t` 뷰, raw는 그대로).
- 결과: 통과(2026-11-13 한 줄 items 두 배, 2099-01-01 새 해시).

## 6절. P5 상품 계약 (결정 83)

### 6-1. 계약과 다르면 실패, 의도한 변경은 깨는 변경으로 기록
- 넣은 것: `python -m measure.contract_check` (계약 파일 사본에서 컬럼 이름 하나 바꿈).
- 결과: 내보내기가 파일을 쓰기 전 실패, `breaking: true`. 통과. 실제 마트 컬럼을 바꾼 시험은 하지 않았다.

## 9절. P7 알림 (결정 86)

### 9-1. 요약 작업이 경고를 모음 (웹훅 없이)
- 결과: 늦은 출처 1건(생활이동)을 모아 글을 만들고, 웹훅 주소가 없어 보내지 않음. 슬랙 도착은 시험하지 못했다.

## 7절. 변이 시험 다시 쓰기 (결정 84, 측정 31)

### 7-1. raw 키 값 변이마다 정답 영향 컬럼이 실제로 바뀌고 지도가 놓치지 않음
- 넣은 것: `python -m measure.lineage_mutation` (M1~M7, M1·M2·M4·M6은 raw 변이로 다시 씀, M7은 contract_type).
- 기대: 변이마다 설계서가 예상한 상품 컬럼이 바뀌고, 지도가 놓친 것 0.
- 결과: 7개 모두 예상 컬럼이 바뀌고 놓침 0. 통과. 지도가 넓게 잡는 문제(F-7)와 M7에서 `rent_listing`이 0행이 된 것(관문 거절)을 함께 찾았다.
