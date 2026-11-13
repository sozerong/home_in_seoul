# 상세 설계 2: 남은 구현 (결정 75~95)

2026-11-13에 정한 결정을 구현 순서대로 옮긴 작업 지시서다. Claude Code가 이 문서와 `docs/decisions.md`를 함께 읽고 위에서부터 한 절씩 구현한다. 앞선 설계서는 `docs/design_change_resilience.md`(리니지, 혼잡도 이전), 결정 목록은 `docs/design_backlog.md` 0절 "결정 현황"이다.

## 0. 작업 규칙

- CLAUDE.md를 따른다. 코드, 설정, SQL, docs 기록은 Claude Code가 직접 쓰고 실행해 확인한 뒤 무엇을 왜 그렇게 썼는지 설명한다. 각 절의 **[체크포인트]**에서 멈추고 사용자 확인을 받는다.
- 결정과 다르게 해야 하는 일이 생기면(실측이 가정과 다를 때 포함) 진행하지 말고 선택지를 표로 보여 사용자에게 묻는다. 고른 것은 결정 96부터 적는다.
- 수치는 실행해서 얻은 값만, 조건(데이터 범위, 반복 횟수, 환경, 도구 버전)과 함께 `docs/measurements.md`에 적는다.
- **구현과 측정 중 만나는 모든 오류는 `docs/failure_analysis.md`에 분석해 남긴다(결정 95).** 고치고 넘어간 작은 오류도 원인이 자명하지 않으면 남긴다. 형식은 그 파일 1절.
- 원본(레이크, `raw` 스키마, Iceberg 원본 표)은 고치지 않는다. 표나 파일을 지우는 일은 지우기 전에 사용자에게 확인받는다.
- 커밋은 절마다 나눈다.

| 순서 | 절 | 결정 | 끝나면 서는 것 |
|---|---|---|---|
| 1 | 배치 관문 | 75~77 | 데이터 문제는 그 배치에서 끝난다 |
| 2 | 상가정보 이전, 다수결 분리, 행 집합 간선 | 78~80 | S6a 재측정 |
| 3 | 생활이동 이전과 엔진 | 87~94 | S6b 재측정, 엔진 분석 |
| 4 | P6 규칙 버전 | 81 | 규칙을 고치면 바뀐 범위가 자동으로 남는다 |
| 5 | P3 스키마 이력 | 82 | 원천 모양이 언제 바뀌었는지 남는다 |
| 6 | P5 상품 계약 | 83 | 깨는 변경이 내보내기에서 막힌다 |
| 7 | 변이 시험 다시 쓰기 | 84 | 리니지 시험이 지금 경로를 덮는다 |
| 8 | 옛 경로 은퇴 | 85 | 규칙이 한곳에만 있다 |
| 9 | P7 알림 | 86 | 경고가 사람에게 닿는다 |

결정 74-3: 3절까지 끝나면 서류 작성을 시작하고 4절부터는 병행한다.

---

## 1. 배치 관문 (결정 75~77)

**목표.** 원천 배치 하나의 데이터 문제는 그 배치만 거절하고, 코드 문제는 지금처럼 빌드 전체를 멈춘다.

1. dbt 모델 `stg_dbt.batch_status`: 한 행 = (출처, 배치 키). 혼잡도는 수집일, 전월세는 처리 단위(`raw.rent`의 키). 열: `source`, `batch_key`, `approved`(boolean), `failed_checks`(text[]), `checked_at`.
2. 관문 검사(결정 76): `congestion_raw_keys_present`, `rent_raw_keys_present`, `congestion_raw_shrink`(CF4), `congestion_cell_unique`(CF2), `congestion_quarantine_ratio`(CF3), `rent_quarantine_ratio`(F2). 지금 dbt 테스트 SQL을 배치별 판정식으로 옮긴다. 옮긴 테스트는 빌드 테스트에서 빼고, `batch_status`에 "판정이 하나 이상 있다" 같은 최소 테스트만 남긴다.
3. 정제 모델(`stg_dbt.congestion*`, `stg_dbt.rent*`)이 `approved` 배치만 읽는다. 같은 대상 기간에 거절된 배치가 있으면 직전 승인 배치가 쓰인다.
4. 그대로 둘 것: `*_raw_unknown_keys`(경고), unit test, wh·mart 테스트, `congestion_station_all_mapped`, `rent_pnu_length`.
5. 운영 카드: 거절된 배치 목록 카드 하나, `ops.freshness`는 마지막 **승인** 수집일을 보인다.

**[체크포인트]** 일부러 나쁜 배치 하나(복사본에서 격리 비율을 넘기게 만든 배치)를 넣어 그 배치만 거절되고 다른 출처와 상품은 갱신되는지, 모델 SQL에 오류를 넣으면 전체가 멈추는지 사용자와 함께 본다. 원본을 고치지 않도록 나쁜 배치는 새 배치 키로 넣고 끝나면 사용자 확인 뒤 지운다.

---

## 2. 상가정보 이전, 다수결 분리, 행 집합 간선 (결정 78~80)

### 2.1 원본 (결정 78)
- `raw.sbiz_store`: 행 하나 = 업소 하나의 jsonb(모든 컬럼), 배치 키 = (수집일, 원천 파일). 서울 전체(202606 기준 554,092행). 적재기 `transform/sbiz_store_raw.py`는 파싱만 한다(`congestion_raw.py`, `rent_raw.py`와 같은 방식, 배치를 지우고 다시 넣기).
- 적재 뒤 원본 크기와 행 수를 잰다(결정 78의 크기는 추정이었다).

### 2.2 정제 (dbt)
- `stg_dbt.store_rows`, `stg_dbt.store`, `stg_dbt.store_quarantine`: 지금 `transform/sbiz_store.py`의 MF1~MF3, 검증 규칙을 SQL로. 키 목록은 매크로로(결정 64).
- 업종 6코드(`ESSENTIAL`)는 정제에서 빼고 하류(편의 지표)로 옮긴다. 정제 결과는 서울 전체 업소다.
- `stg.bjd_code`를 원본에서 만드는 SQL 모델로 바꾼다(전월세 리니지가 원천까지 이어지게).
- 관문(1절)에 상가정보 배치를 더한다: 쓰는 키 있음, 행 수 급감, 격리 비율.

### 2.3 다수결 분리 (결정 79)
- `wh.fact_store`는 서울 전체 업소. `wh.map_dong_mois`는 다수결 SQL 그대로, 읽는 표만 전체 업소.
- 편의 지표(`mart.dong_amenity`, `mart.dong_profile`)만 고른 업종을 거른다. 업종 목록은 seed 또는 매크로 한곳.

### 2.4 같음 증명
1. 옛 `stg.sbiz_store`(30,394행)와 새 정제 결과의 6코드 부분 두 방향 `EXCEPT ALL` 차이 0.
2. `stg.bjd_code` 466행과 SQL 모델 결과 차이 0.
3. `map_dong_mois` 바꾸기 전후를 두 방향 `EXCEPT ALL`로 비교해 바뀐 행 수, 1위 코드가 뒤집힌 동의 수와 목록을 `docs/measurements.md`에 남긴다. 이것은 의도한 변경이다. `docs/data_contract.md` 변경 기록에 적는다.
4. 공간 조인 시간(3만 → 55만 곳)을 잰다.

### 2.5 행 집합 간선 (결정 80)
1. 정답 기준(결정 66)에 ROWSET 선을 먼저 더해 커밋한다: 모델마다 FROM·JOIN 원천표 `원천.* → 대상.*`, 그 쿼리의 WHERE·JOIN·GROUP BY 컬럼 `→ 대상.*`.
2. 추출기에 종류 `ROWSET`을 더한다. 행 조건 컬럼은 이미 scope로 뽑고 있다(결정 54).
3. 하류·상류 조회와 MCP 결과에서 컬럼 단위 영향과 ROWSET 경로 영향을 나눠 표시한다.
4. 변이: `contract_type`의 '신규' 표기를 바꿔 `dong_profile.monthly_deals`가 실제로 바뀌고 지도에 나오는지(7절 묶음과 함께 둬도 된다).

### 2.6 S6a 재측정
측정 17과 같은 방법으로 업종 하나를 더한다. 고친 곳이 SQL(업종 목록, 마트, 계약 문서)뿐인지, `dong_mois_map`이 바뀌지 않는지 본다.

**[체크포인트]** 2.1 적재 뒤(크기), 2.4 같음 증명 뒤, 2.6 재측정 뒤.

---

## 3. 생활이동 이전과 엔진 (결정 87~94)

### 3.1 바뀌는 흐름
```
레이크 zip → living_iceberg.py(파싱, 그대로) → Iceberg lh.living.migration_rows(원본, 그대로)
  → spark_sql/living/*.sql (Spark SQL: 관문, 정제, 집계)          ← 새로
  → PostgreSQL stg.living_commute (경계 표, grain G3)             ← 새로
  → dbt: wh.fact_commute → mart.commute_od (시간대·유형·나이 고르기)  ← 고침
옛 경로 transform/living_migration.py → stg.living_migration 은 3.6 증명까지 그대로 둔다.
```

### 3.2 첫 측정: 경계 표 행 수와 데이터 성질 (결정 89 조건, 94-2)
구현 전에 Spark로 한 번 잰다.
- G3 grain(대상연월 × 출발 × 도착 × 평일/주말 × 도착시간 × 이동유형 × 나이)의 그룹 수. 범위는 지금 원본 전체(10달).
- 키 분포: 도착 행정동·출발 행정동 상위 20개 키의 행 비중, 대상연월별 행 수, 가려진 값(`*`)과 NULL 비중.
- **그룹 수가 40,382,008(지금 `stg.living_migration`)을 넘으면 멈추고 사용자에게 차원 줄이는 선택지를 보인다.**

**[체크포인트]** 측정 결과를 보고 진행 여부를 정한다.

### 3.3 실행기 (결정 88)
- `spark_sql/living/`: 파일 이름 순서대로 실행한다. 파일 하나 = 임시 뷰 하나, 뷰 이름 = 파일 이름(번호 뺀 부분). 예: `10_batch_status.sql`, `20_clean.sql`, `21_quarantine.sql`, `30_commute.sql`. 마지막 결과 파일이 경계 표.
- `test_*.sql`: 위반 행을 돌려주는 SQL. 하나라도 행이 있으면 실행기가 종료 코드 1로 끝난다(dbt 테스트와 같은 모양).
- 실행기 `transform/spark_sql_runner.py`: SparkSession을 직접 만든다(설정은 `living_iceberg.py`와 같은 카탈로그, 메모리·셔플 파티션·AQE는 인자로 바꿀 수 있게). 결과를 JDBC로 `stg.living_commute_new`에 쓰고 PostgreSQL 트랜잭션 하나에서 `stg.living_commute`와 이름을 바꾼다(결정 90). 매번 전체 재계산.
- Spark 이벤트 로그를 켠다. 실행이 끝나면 단계별 시간, 셔플 읽기·쓰기, spill(메모리·디스크), 태스크 시간 최대/중앙값, 최대 실행 메모리를 `ops.engine_run`에 남긴다(결정 94-1). 한 행 = 실행 × 단계.
- DAG: file_sources의 생활이동 작업이 `living_iceberg` 다음에 실행기를 부른다. 경계 표 Asset을 갱신한다.

### 3.4 관문과 정제 (결정 90)
- `10_batch_status`: 배치 = (수집일, 대상연월). 직전 대상연월 대비 행 수 급감, 격리 비율 5% 초과(F2)면 거절. 쓰는 열 없음은 파서가 이미 실패시킨다(F1).
- `20_clean`, `21_quarantine`: `living_migration.py`의 V3~V6 규칙을 SQL로. 범위 C 필터는 넣지 않는다(시간대·유형 고르기는 dbt로 간다).
- `30_commute`: 대상연월마다 승인된 가장 최근 수집일만. 값은 더할 수 있는 것만: 가려지지 않은 이동인구 합, 이동인구×이동 시간 합, 가려진 행 수, 원본 행 수(결정 89 G-c).

### 3.5 dbt 쪽
- `sources.yml`에 `stg.living_commute`. `wh.fact_commute`는 이 표를 읽고, 최신 수집일 고르기는 빠진다(Spark로 옮겨 감).
- `mart.commute_od`: 범위 C(HW, 평일, 07~09시)를 여기서 고른다. 평균은 합에서 다시 계산한다. 20~34세 기준 30(결정 22)과 `*` 처리는 그대로.
- `map_dong_2021`의 코드 목록은 경계 표에서.

### 3.6 같음 증명
1. 옛 `commute_od`(지금 표)를 보관한다.
2. 새 경로로 만든 `commute_od`와 두 방향 `EXCEPT ALL` 차이 0. 부동소수 합 순서 때문에 끝자리가 다르면 그것도 오류로 `failure_analysis.md`에 남기고, 고칠 방법(numeric 형변환 위치 등)을 사용자에게 보인다.
3. 상품 파일 sha256이 바뀌기 전과 같은지.

### 3.7 리니지 (결정 91)
1. 정답 기준을 먼저 고친다: `commute_od` 상류에 Iceberg 원본 열이 나온다.
2. Iceberg 원본 표 스키마를 DDL 파일 하나로 둔다(`living_iceberg.py`와 같은 열). 스키마 추론 입력에 더한다.
3. `spark_sql/living/*.sql`을 sqlglot spark 방언으로 파싱한다. 파일 이름이 대상 뷰, 마지막 파일의 대상은 `stg.living_commute`.
4. `lineage.run_diff`로 이전 전후를 비교한다.

### 3.8 S6b 재측정
저녁 귀가 18~20시를 dbt SQL만 고쳐 더한다(측정 18과 같은 방법으로 잰다). 이동유형은 WH(주간상주지 → 야간상주지, 측정 18에서 확인).

### 3.9 DuckDB로 같은 SQL (결정 87 검증)
같은 `spark_sql/living/*.sql`을 DuckDB로 돌린다. 방언 차이로 고친 곳은 모두 적는다(그 자체가 분석 대상이다). Iceberg 읽기가 JDBC 카탈로그로 안 되면 메타데이터 파일 경로로 읽고 그 사실을 적는다.

### 3.10 엔진 분석 (결정 94, 95)
속도와 규모를 재는 일이 아니다. 엔진이 어디서 어떻게 무너지는지, 왜 그런지, 무엇으로 고치는지를 찾는다.
1. 기준 실행: 기본 설정으로 Spark와 DuckDB 각각 3회. `ops.engine_run` 지표와 실행 계획(Spark `EXPLAIN FORMATTED`, DuckDB `EXPLAIN ANALYZE`)을 남긴다.
2. 압력: 메모리 상한을 단계적으로 낮춘다(Spark driver memory, DuckDB `memory_limit`). 단계마다 결과(성공, spill로 버팀, 느려짐, 실패)와 무너진 연산자를 기록한다. 셔플 파티션 수, AQE 켜고 끔, 스레드 수도 원인 확인에 필요한 만큼 바꾼다.
3. 무너지거나 이상한 지점마다 `failure_analysis.md` 항목 하나: 증상 → 재현 → 가설 여럿 → 지표·계획으로 확인 → 엔진 내부 수준 원인 → 처방 선택지 → 전후 수치.
4. 살펴볼 후보(가설, 사실 아님): 부분 집계가 쏠림을 얼마나 줄이는지, 부분 집계가 안 되는 연산(창 함수, 중복 제거), 카디널리티가 낮은 키로 다시 나누는 쓰기, JDBC 쓰기 병렬도, Iceberg 파일 수와 크기, driver 쪽으로 모이는 결과, DuckDB의 막는 연산자 겹침과 insertion order. 스큐는 예시일 뿐이다. 실제로 나오는 오류를 따라간다.
5. 처방은 지표가 가리키는 원인에 따라 고르고, 개선이 없으면 없다고 쓴다.

**[체크포인트]** 3.2 측정 뒤, 3.6 같음 증명 뒤, 3.8 재측정 뒤, 3.10에서 분석 항목이 하나 나올 때마다.

---

## 4. P6 규칙 버전 (결정 81)
- wh_load가 dbt 빌드 뒤 `target/compiled`의 모델별 SQL sha256, git 커밋, 작업 트리 깨끗 여부(`git status --porcelain`가 비었나)를 `ops.build_run`에 한 줄 남긴다. 표별 행 수, 빌드 시간도 같은 줄에.
- 교체 직전 새 빌드 스키마와 지금 스키마의 정제 표(rent, congestion, store, 그리고 Spark 실행기 결과 `stg.living_commute`는 `_new`와 지금 표)를 처리 단위별로 비교해 바뀐 행 수를 `ops.build_diff`에 남긴다. 비교 시간도 남긴다.
- `lineage.run`에 같은 해시와 깨끗 여부를 남기고, MCP의 stale 판정을 해시 비교로 바꾼다.
- **[체크포인트]** 매크로 한 줄만 고쳐 빌드하면 해당 모델 해시가 바뀌고 `ops.build_diff`에 바뀐 단위가 나온다. 커밋하지 않은 변경으로 리니지를 돌리면 MCP가 stale이라고 답한다.

## 5. P3 스키마 이력 (결정 82)
- dbt 모델 `stg_dbt.raw_shape`: 한 행 = (출처, 수집일, 키 집합 해시). 열: 키 목록, item 수. 전월세는 offi·rh를 출처 구분에 넣는다. 혼잡도, 전월세, 상가정보. 키 훑기는 `batch_status`와 같은 CTE를 쓴다.
- **[체크포인트]** 같은 수집일 두 번 → 한 줄. 키 하나를 바꾼 입력(새 배치 키로) → 새 해시와 그 수집일.

## 6. P5 상품 계약 (결정 83)
- 계약 스키마 파일 하나(표별 컬럼 이름, 타입)를 저장소에 둔다. 위치는 `docs/data_contract.md`와 나란히.
- `export_product.py`: 실제 표 스키마가 계약 파일과 다르면 실패. 같으면 manifest에 스키마와 직전 버전 대비 변경 목록(추가, 삭제, 타입 변경, 깨는지)을 쓴다. 버전 규칙(날짜)은 그대로.
- 리니지는 읽지 않는다(원천 추적은 MCP).
- **[체크포인트]** 마트 컬럼 하나를 바꾸고 계약 파일을 안 고치면 실패, 고치면 manifest에 깨는 변경으로 기록(S4 재측정).

## 7. 변이 시험 다시 쓰기 (결정 84)
- M1, M2, M4를 raw jsonb 키 값을 바꾸는 변이로 다시 쓴다(dbt var 방식 그대로). 2.5의 `contract_type` 변이를 같은 묶음에.
- **[체크포인트]** 변이마다 정답의 영향 컬럼이 실제로 바뀐다.

## 8. 옛 경로 은퇴 (결정 85)
- 지우기 직전 커밋에 태그 `pre-retire-python`.
- 혼잡도·전월세·상가정보·생활이동의 옛 정제 코드, 테스트, DDL, 표를 지운다. 쓰는 함수(`measure/rent_fingerprint.py`의 `read_items`)는 쓰는 곳으로 옮긴다. 지울 목록은 사용자에게 먼저 보인다.
- 스키마 이름: `stg` = dbt 밖(Python, Spark)에서 쓰는 표, `stg_dbt` = dbt가 만드는 표(결정 85, 90). README와 `docs/data_contract.md`에 한 줄씩.
- **[체크포인트]** CI와 wh_load 통과, 태그에서 측정 재현 스크립트가 돈다.

## 9. P7 알림 (결정 86)
- 사용자가 슬랙 웹훅 주소를 `.env`의 `SLACK_WEBHOOK_URL`에 넣는다. 없으면 지금처럼 파일과 `ops.failure_log`에만.
- `dags/alerting.py`의 실패 알림이 슬랙에도 보낸다(표준 라이브러리 HTTP).
- wh_load 마지막 작업이 dbt `run_results.json`의 warn, `batch_status` 거절 배치, `ops.freshness` 늦은 출처를 모아 메시지 하나로 보낸다.
- **[체크포인트]** 일부러 넣은 실패, 경고, 관문 거절, 늦음이 슬랙에 도착한다.

---

## 10. 확인만 남은 일 (design_backlog.md 11절)
정답 검토, 새 세션에서 MCP 연결, molit_rent 정기 실행 성공, Airflow 화면에서 성공·실패·재실행.

## 11. 하지 않는 것
- Airflow 밖 "실행 안 됨" 감시, 알림 목표 시간(결정 86).
- 재빌드 한계선 숫자(결정 92).
- 역사마스터, 행정동 경계, 연속지적도 이전(결정 93).
- dbt-spark, Spark Thrift Server(결정 88). 경계 결과를 Iceberg에 따로 저장(결정 90).
- 클러스터 문제(네트워크 셔플, 실행기 유실) 재현(결정 94 한계).
- 스큐를 일부러 만든 데이터. 압력은 메모리·설정으로만 걸고 "유도"라고 밝힌다.
