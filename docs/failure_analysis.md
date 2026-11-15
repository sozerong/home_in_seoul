# 오류 분석 기록

구현과 측정 중 실제로 만난 오류를 원인까지 파고들어 남긴다(결정 95). 이 기록이 이력서와 포트폴리오의 재료다. "무엇을 만들었다"가 아니라 "무엇이 왜 무너졌고, 어떻게 확인했고, 무엇을 골라 고쳤는지"를 남기는 곳이다.

## 0. 규칙

- **실제로 일어난 것만.** 일부러 압력을 걸어 일으킨 오류(메모리 상한을 낮추는 등)는 제목에 `[유도]`를 붙이고, 어떻게 일으켰는지 적는다. 데이터를 조작해 오류를 만들지 않는다.
- **원인은 한 단계 더 아래까지.** "메모리 부족"에서 멈추지 않고, 어느 연산자가 왜 그 메모리를 썼는지(해시 표 크기, 부분 집계가 안 되는 이유, 파티션 하나에 몰린 행 수 등)를 지표나 실행 계획으로 보인다.
- **가설은 여럿 세우고 하나씩 지운다.** 지운 가설과 지운 근거도 남긴다. 틀린 가설이 기록의 절반이다.
- **수치는 실측만.** 조건(데이터 범위, 반복, 환경, 버전, 설정)과 함께. 측정은 `docs/measurements.md`에 번호로 남기고 여기서는 번호로 가리킨다.
- **개선이 없으면 없다고 쓴다.** 처방이 다른 것을 나쁘게 만들었으면 그것도 쓴다.
- **넣지 않는 것:** 실행하는 PC 환경 탓인 일(절전, Docker Desktop 자동 시작 등), 오타처럼 원인이 자명하고 배울 것이 없는 일.
- 결정이 필요하면 `docs/decisions.md`에 결정 번호로 옮기고 여기서 가리킨다.

분류(태그, 여러 개 가능): `메모리`(OOM, GC, spill) / `분포`(스큐, 병렬도, 파티션) / `저장`(작은 파일, 커밋, 스냅샷) / `연결`(JDBC, 타임아웃, 재시도) / `타입·스키마`(형변환, 방언, 새 필드) / `정확성`(값 어긋남, 중복, 누락) / `운영`(멱등, 재실행, 순서) / `계산량·인덱스`(실행 계획, 인덱스 미사용, 조인 방식) / `설계 가정`(재 보기 전에 세운 추정이 틀림)

## 1. 항목 형식

```
## F-N. 제목 (증상 한 줄) [유도 여부]
- 태그:
- 언제·어디: 날짜, 설계 절(design_remaining_work.md), 실행 명령, 커밋
- 증상: 에러 메시지에서 필요한 줄만, 관찰한 지표
- 재현: 최소 재현 조건(데이터 범위, 설정), 몇 번 중 몇 번
- 가설:
  - H1 ... → 확인 방법 → 결과(지움/남김, 근거)
  - H2 ...
- 원인: 엔진·시스템 내부 수준 설명. 근거가 된 지표, 실행 계획 부분
- 처방 선택지:
  | 선택지 | 장점 | 단점 |
- 처방과 결과: 고른 것과 이유, 전후 수치(측정 번호)
- 재발 방지: 테스트, 관문, 설정, 문서 중 무엇을 남겼나
- 남은 것: 모르는 것은 [확인 필요]
```

## 2. 항목

## F-1. 배열 컬럼이 있는 모델을 unit test 입력으로 주면 `cast(null as ARRAY)` 문법 오류
- 태그: 타입·스키마
- 언제·어디: 2026-11-13, 1절 배치 관문. `dbt build -s +batch_status+` (dbt 1.12.6, dbt-postgres 1.11.0, PostgreSQL), 커밋 전 작업 트리(34cfd5f 위)
- 증상: unit test `congestion_values`, `congestion_quarantine_pieces` 두 개가 실패했다.
  `syntax error at or near "ARRAY"` / `LINE 35: as "approved", cast(null as ARRAY) as "failed_checks", ...`
  두 테스트의 모델(`congestion`, `congestion_quarantine`)이 새로 `ref('batch_status')`를 읽게 되어 입력에 `batch_status`를 dict 행(source, collected_date, approved만)으로 줬을 때다.
- 재현: `failed_checks text[]` 컬럼이 있는 모델을 unit test 입력으로 dict 형식으로 주고, 그 배열 컬럼을 행에 쓰지 않는다. 2회 중 2회(두 테스트 모두).
- 가설:
  - H1 입력 행의 값 타입이 틀렸다(날짜를 글자로 줌 등) → 오류 위치가 값이 아니라 빠진 컬럼 `failed_checks`의 `cast(null as ...)`이다 → 지움.
  - H2 dbt가 빠진 컬럼을 null로 채울 때 쓰는 타입 이름이 PostgreSQL이 받지 않는 글자다 → 아래 확인 → 남김.
- 원인: dbt는 unit test 입력에서 빠진 컬럼을 `cast(null as <타입>)`으로 채우고, 타입은 실제 표를 조회해 얻는다. dbt-postgres의 `postgres__get_columns_in_relation`은 `information_schema.columns`의 `data_type`을 읽는데, PostgreSQL은 배열 컬럼의 `data_type`을 원소 타입이 아니라 글자 그대로 `ARRAY`로 준다. 실제 타입은 `udt_name`(`_text`)에만 있다. 같은 표를 조회한 결과: `failed_checks | ARRAY | _text`. 그래서 `cast(null as ARRAY)`라는, 타입 이름이 아닌 SQL이 만들어진다.
  - 두 번째로 만난 것이다. 결정 71 2번(혼잡도 unit test)에서 "dbt가 `ARRAY`로 캐스트하려다 실패"해 기대값을 sql 형식으로 썼고, 결정 73 3번은 같은 이유로 `rent_rows.reasons`를 글자로 뒀다. 그때는 왜 `ARRAY`라는 글자가 나오는지 확인하지 않았다. 이번에 확인한 것은 그 아래 원인(`data_type`과 `udt_name`)이다.
- 처방 선택지:
  | 선택지 | 장점 | 단점 |
  |---|---|---|
  | A. 입력을 `format: sql`로 주고 모든 컬럼을 타입과 함께 쓴다 | 모델의 `text[]` 설계를 그대로 둔다. 고칠 곳은 테스트 정의 두 곳 | 입력이 길어진다. 모델에 컬럼이 늘면 입력도 고쳐야 한다 |
  | B. `failed_checks`를 쉼표로 이은 글자로 바꾼다(`rent_rows.reasons`와 같은 방식) | dict 입력이 그대로 된다 | 설계(결정 77, 열 `failed_checks text[]`)와 다르다. 운영 카드와 freshness가 글자를 다시 쪼개야 한다 |
  | C. dbt 매크로를 덮어써 `udt_name`을 읽게 한다 | 모든 unit test가 배열을 다룬다 | dbt 내부 매크로를 프로젝트에서 바꾸면 업그레이드 때 깨질 수 있다. 다른 곳(스냅샷, 증분 모델)의 컬럼 조회까지 바뀐다 |
- 처방과 결과: A. 설계를 바꾸지 않고 테스트 정의만 고치는 가장 작은 변경이다. 고친 뒤 전체 빌드 105개 통과(131.8초).
- 재발 방지: 입력 정의 옆에 이 항목 번호를 주석으로 남겼다(`stg_dbt.yml`). 앞으로 배열 컬럼이 있는 모델을 unit test 입력으로 쓰면 `format: sql`로 준다.
- 남은 것: 결정 73의 `reasons` 글자 컬럼도 같은 방법(A)이면 배열로 둘 수 있었다. 바꿀지는 정하지 않았다(지금 동작에는 문제가 없다).
- 외부 이슈 검색 (결정 97, 2026-11-13, 웹 검색과 `gh search issues`, dbt-labs 저장소):
  - 같은 뿌리 원인(`information_schema.columns.data_type`이 배열을 `ARRAY`로 줌)을 다룬 열린 이슈가 있다: [dbt-labs/dbt-adapters#664](https://github.com/dbt-labs/dbt-adapters/issues/664) "Snapshots and incremental models cannot add array columns"(pkg:dbt-postgres, OPEN). 증상은 스냅샷·증분 모델의 `alter table ... add column "x" ARRAY`다.
  - 계약(contract)에 `ARRAY`를 쓴 경우는 [dbt-labs/dbt-adapters#939](https://github.com/dbt-labs/dbt-adapters/issues/939)(CLOSED)에 있다.
  - unit test의 `cast(null as ARRAY)`(빠진 입력 컬럼 채우기)를 다룬 dbt-postgres 이슈는 찾지 못했다. 다른 웨어하우스(BigQuery, Snowflake, Databricks)의 배열 unit test 문제는 dbt Discourse에 있다.
  - 올릴지는 사용자에게 묻는다(외부에 올리는 일). 새 이슈보다 #664에 "unit test 입력 채우기에서도 같은 원인으로 난다"는 재현을 덧붙이는 쪽이 중복이 적다.
- 우회를 그대로 둘지 (2026-11-13, handoff_finish.md 4절 6번). 지금 우회(A, 입력을 `format: sql`로)는 배열 열이 있는 unit test 입력 2곳(`stg_dbt.yml`)과 `batch_status_living_shape`에 있고 매 빌드 통과한다. dbt-core 1.12.6, dbt-postgres 1.11.0, dbt-adapters 1.24.5.
  | 선택지 | 장점 | 단점 |
  |---|---|---|
  | A. 우회 그대로 | 일이 없다. 모델 설계(`text[]`)가 그대로다. 증상이 테스트 정의에만 머문다 | 배열 열이 있는 모델을 새로 unit test 입력으로 쓸 때마다 모든 열을 sql 형식으로 써야 한다(규칙은 F-1 재발 방지에 있음) |
  | B. 프로젝트에 `postgres__get_columns_in_relation`을 덮어써 `udt_name`으로 배열 타입을 낸다 | dict 입력이 된다. 스냅샷·증분 모델의 같은 증상(#664)도 함께 피한다 | dbt 내부 매크로를 프로젝트가 떠안는다. 업그레이드 때 깨질 수 있고, 컬럼 조회를 쓰는 다른 곳(스키마 교체 `swap_schemas` 등)의 동작도 바뀐다. 시험 범위가 넓다 |
  | C. A를 두고, 외부 이슈 #664에 unit test 재현을 덧붙인다 | 고쳐지면 우회를 걷을 수 있다. 코드 변경 없음 | 외부에 글을 올리는 일(사용자 확인 필요). 언제 고쳐질지 모른다 |
  | D. A를 두고, dbt-postgres를 올렸을 때 고쳐졌는지 본다 | 고쳐졌으면 우회를 걷는다 | 이 PC에서 최신 버전 변경 기록을 확인하지 않았다 [확인 필요]. 업그레이드는 다른 모델 동작도 바꿀 수 있다 |
- 상태: **우회 유지(결정 104, 사용자 2026-11-14).**

## F-2. 전월세 처리 단위 하나를 새 배치 키로 복사하다 기본 키 중복
- 태그: 정확성, 운영
- 언제·어디: 2026-11-13, 1절 체크포인트(나쁜 배치 만들기). psql에서 `insert into raw.rent ... select ... from raw.rent where (rh, 2026-11-13, 11710, 202610)`, 원천 파일 이름을 `checkpoint_bad_batch.xml` 하나로 바꾸고 `item_no`는 그대로 둠
- 증상: `duplicate key value violates unique constraint "rent_pkey"` / `Key (source_type, collected_date, source_file, item_no)=(rh, 2026-11-14, checkpoint_bad_batch.xml, 1) already exists.`
- 재현: 원천 파일이 둘 이상인 처리 단위를 파일 하나로 합쳐 복사한다. 1회 중 1회.
- 가설:
  - H1 같은 배치를 이미 한 번 넣었다 → 넣기 전 `collected_date > 2026-11-13` 행이 0이었다 → 지움.
  - H2 `item_no`가 단위 안이 아니라 원천 파일 안의 번호라 파일을 합치면 겹친다 → 그 단위의 원천 파일 수를 셌다 → 2개, 남김.
- 원인: 처리 단위(출처, 수집일, 구, 계약년월)는 API 응답 페이지마다 파일 하나로 레이크에 저장되고(1,000건 단위), `transform/rent_raw.py`는 `item_no`를 파일 안 순번으로 매긴다. 그래서 원본 표의 기본 키에 `source_file`이 들어 있고, 단위 하나 안에서 `item_no`는 유일하지 않다. 배치 관문의 전월세 배치 키(`rent_batch_key`)는 원천 파일을 빼고 단위로 묶으므로 판정에는 영향이 없다.
- 처방 선택지:
  | 선택지 | 장점 | 단점 |
  |---|---|---|
  | A. 복사본의 `item_no`를 단위 안에서 다시 매긴다(`row_number()`) | 시험 SQL 한 줄. 원본 모양을 흉내 내는 데 충분 | 원천 파일 구성은 원본과 다르다(시험에는 상관없음) |
  | B. 원천 파일 이름을 원본 것을 따라 둘로 나눠 복사 | 원본과 같은 모양 | 시험 SQL이 길어진다 |
- 처방과 결과: A. 1,206 item을 넣었고 시험은 측정 25대로 끝났다.
- 재발 방지: 2.1 상가정보 원본은 배치 키가 (수집일, 원천 파일)이다(결정 78). 원본 표 행 번호가 파일 안 번호인지 배치 안 번호인지를 적재기 주석과 DDL에 적는다.
- 남은 것: 없음.

## F-3. 상가정보 원본을 다시 넣을 때마다 표가 한 배치분(866MB)씩 커짐
- 태그: 저장, 운영
- 언제·어디: 2026-11-13, 2.1 상가정보 원본 적재. `python -m transform.sbiz_store_raw 2026-10-21`을 멱등 확인과 시간 재기로 4번 돌림. PostgreSQL 17.5
- 증상: 554,092행 표의 heap이 3,464MB였다(행 평균 1,462바이트면 약 0.8GB여야 한다). TOAST는 1MB 미만이라 큰 값이 따로 저장된 것도 아니었다. `pg_stat_user_tables`: `n_live_tup` 543,464, `n_dead_tup` 1,669,336.
- 재현: 같은 배치를 지우고 다시 넣기를 연달아 반복한다. 측정 26 표대로 반복할 때마다 같은 결과.
- 가설:
  - H1 jsonb가 커서 행이 원래 크다 → 행 평균 1,462바이트 × 554,092 ≈ 0.81GB. 3.4GB를 설명하지 못한다 → 지움.
  - H2 큰 값이 TOAST로 빠져 크기가 부풀었다 → TOAST 1MB 미만 → 지움.
  - H3 지우고 다시 넣기가 남긴 죽은 행이다 → 죽은 행 1,669,336 ≈ 3 × 554,092. `VACUUM FULL` 뒤 866MB → 남김.
- 원인: PostgreSQL은 MVCC라 `DELETE`가 행을 지우지 않고 "이 트랜잭션 뒤로는 안 보임"으로 표시만 한다. 공간은 `VACUUM`이 돌아야 다시 쓸 수 있게 되고, 일반 `VACUUM`은 그 공간을 운영체제에 돌려주지 않고 표 안에서 재사용만 한다. 적재기는 배치를 `DELETE` 후 `COPY`로 다시 넣으므로(멱등) 한 번 다시 넣을 때마다 배치 하나만큼 죽은 행이 생긴다. autovacuum이 돌기 전에 다시 넣으면 새 행은 빈 곳이 없어 표 끝에 붙는다. 그래서 연달아 4번이면 4배치분이 됐다. `VACUUM`이 사이에 돌면 다음 적재가 그 빈 곳을 재사용해 2배치분(1,732MB)에서 멈춘다(측정 26).
  - 같은 방식인 `raw.rent`(250MB)에서는 처리 단위가 작아(단위당 수백 item) 눈에 띄지 않았다. 상가정보는 배치 하나가 표 전체다.
- 처방 선택지:
  | 선택지 | 장점 | 단점 |
  |---|---|---|
  | A. 그대로 둔다(autovacuum에 맡김) | 일이 없다. 분기 갱신이라 다시 넣기가 드물고, 다시 넣어도 VACUUM이 돌면 2배치분에서 멈춘다 | 표가 실제 데이터의 약 2배를 차지한다. autovacuum보다 빨리 거듭 다시 넣으면 더 커진다 |
  | B. 적재기가 끝에 `VACUUM`을 부른다 | 2배치분 상한이 확실해진다. 한 줄 | 크기는 여전히 2배다. 분기마다 배치가 늘면 VACUUM이 표 전체를 훑는다 |
  | C. 원본 표를 수집일로 나눈 파티션으로 두고, 배치를 지울 때 `DELETE` 대신 그 파티션을 비우거나 바꿔 끼운다(`TRUNCATE`/`DETACH`) | 죽은 행이 생기지 않는다. 옛 배치를 지울 때도 즉시 공간이 돌아온다 | 표 구조가 바뀐다. 다른 원본 표(`raw.rent`, `raw.congestion`)와 방식이 갈린다. 파티션 만들기를 적재기가 해야 한다 |
  | D. 파일이 바뀌지 않았으면 다시 넣지 않는다(해시 비교) | 같은 배치 다시 넣기 자체가 없어진다 | 파일이 바뀐 다시 넣기에는 그대로다. 비교할 해시를 어딘가 남겨야 한다 |
- 처방과 결과: 사용자 결정을 기다린다(2.1 체크포인트). 지금 표는 `VACUUM FULL`로 922MB다.
- 재발 방지: 측정 26에 크기를 잴 때는 죽은 행을 확인하고 `VACUUM FULL` 뒤 값을 쓴다고 남겼다.
- 남은 것: 적재 중 `DELETE`가 행 55만 개를 표시하는 시간과 WAL 크기는 재지 않았다.
- 근거 다시 보기 (2026-11-13, handoff_finish.md 4절 5번, 결정 97 A "그대로 둠"):
  - 지금 `raw.sbiz_store`: 배치 1개(수집일 2026-10-21) 554,092행, 표 크기 866MB = 한 배치분. 처방 전 `VACUUM FULL` 뒤 크기(922MB)보다 작고 부풀지 않았다. 그 뒤 같은 배치를 다시 넣은 적이 없다.
  - 새로 본 점: `pg_stat_user_tables`에서 `raw.sbiz_store`, `raw.rent`, `raw.congestion`의 `n_live_tup`, `n_dead_tup`가 모두 0이고 `last_autovacuum`이 비어 있다. F-16 뒤 PostgreSQL이 비정상 종료에서 복구하며 누적 통계를 지웠기 때문이다(PostgreSQL은 비정상 종료 뒤 통계를 초기화한다). autovacuum은 이 통계로 대상을 고르므로, 멈추기 전에 생긴 죽은 행은 통계에 없어 autovacuum이 보지 못한다. 지금은 죽은 행이 없어(크기 = 한 배치) 문제가 없지만, "autovacuum에 맡긴다"는 근거는 비정상 종료 뒤에는 성립하지 않는다.
  - 판단: A 그대로 둔다. 근거(분기 1회 적재, 지금 부풀지 않음)는 유지된다. 덧붙임: 비정상 종료에서 복구한 뒤에는 `ANALYZE`(통계 다시 모으기)를 한 번 돌린다. 운영 문서(README 복구 순서)에 한 줄로 남긴다.
- 상태: **그대로 둠(근거 다시 확인, 2026-11-13).**

## F-4. 상가정보를 서울 전체로 넓히자 `fact_store`가 140초: 행정동 경계에 공간 인덱스가 없어 모든 업소를 모든 동과 비교
- 태그: 계산량·인덱스
- 언제·어디: 2026-11-13, 2.2·2.3. `dbt build` (dbt 1.12.6, PostgreSQL 17.5, PostGIS), 빌드용 스키마. 커밋 전 작업 트리
- 증상: 빌드 전체가 131.8초에서 339.7초로 늘었다. 노드별 시간(`run_results.json`)에서 `fact_store` 140.5초가 가장 길었다(다음은 `commute_od` 67.0초, `map_dong_2021` 41.8초).
- 재현: `fact_store`와 같은 공간 조인 SQL을 업소 표만 바꿔 `EXPLAIN ANALYZE`로 1회씩.

  | 업소 | 실행 시간 | 계획 | 조인 필터로 버린 행 |
  |---|---|---|---|
  | 옛 `stg.sbiz_store` 30,394곳 | 17.8초 | Nested Loop Left Join, 병렬 없음 | 12,917,450 |
  | 새 `stg_dbt.store` 554,092곳 | 132.2초 | Nested Loop Left Join, 작업자 3개 | 78,496,367 × 3 |

- 가설:
  - H1 업소가 18배라 그만큼 느리다 → 시간은 7.4배(병렬 3개). 비례는 맞지만 왜 업소 수에 비례해 비교가 느는지가 남는다 → 원인 아님(결과).
  - H2 `ST_Transform`(4326 → 5179)이 비싸다 → 업소마다 한 번이라 55만 번. 버린 행 2.4억과 비교하면 작다 [확인 필요: 따로 재지 않음] → 주원인으로 보지 않음.
  - H3 경계 쪽 공간 인덱스가 없어 업소마다 경계 426개 모두와 `ST_Contains`를 계산한다 → `pg_indexes`에 `stg.sgis_dong`은 기본 키(btree)뿐이고, 계획이 Index Scan이 아니라 Join Filter, 버린 행 = 업소 × 425(30,394 × 425 = 12,917,450) → 남김.
- 원인: `fact_store`는 `wh.dim_dong`(GiST 인덱스 있음, 결정 31)이 아니라 원천 표 `stg.sgis_dong`을 CTE로 읽어 조인한다. 그 표의 `geom`에는 GiST 인덱스가 없다. 그래서 PostgreSQL이 고를 수 있는 조인은 업소 한 행마다 경계 전체를 훑는 Nested Loop뿐이고, `ST_Contains`(다각형 안 점 판정)를 업소 수 × 426번 부른다. 결정 79는 "공간 조인 방식과 `dim_dong.geom` GiST 인덱스(결정 31)가 있다"를 근거로 들었는데, 이 조인은 그 인덱스를 쓰지 않는다. 업소가 3만일 때는 18초라 드러나지 않았다.
- 처방 선택지:
  | 선택지 | 장점 | 단점 |
  |---|---|---|
  | A. `fact_store`가 `stg.sgis_dong` 대신 `ref('dim_dong')`(GiST 있음)과 조인 | 고칠 곳이 모델 한 곳. 다른 공간 조인(`dim_parcel`, `dim_station`)과 같은 경로가 된다 | `dim_dong`의 경계 정의가 원천과 같은지 확인해야 한다 |
  | B. `stg.sgis_dong.geom`에 GiST 인덱스 | 모델을 안 바꾼다 | dbt 밖(Python 적재 DDL)을 고친다. 수집일이 쌓이면 다른 수집일 경계까지 인덱스에 든다 |
  | C. 그대로 둔다 | 일이 없다 | 상가정보는 분기 갱신이지만 wh_load는 어느 출처가 갱신돼도 전체를 다시 만들어 매번 2분 넘게 쓴다(결정 62의 재빌드 한계선과 이어진다) |
- 처방과 결과: 사용자 지시("오류는 기록하고 해결하지 말 것", 2026-11-13)에 따라 고치지 않았다. 측정 27에 시간만 남겼다.
  - 2026-11-13 결정 97로 A를 골랐으나, 적용하려 하자 dbt가 의존 순환으로 거절했다(아래). 그래서 처방하지 않았고 전후 수치도 없다. 처방을 다시 고른다.
  - 재현: dbt 프로젝트 사본(`/tmp/dbt_f4`)에서 `fact_store`의 `boundary` CTE를 `select dong_id as adm_cd, geom from {{ ref('dim_dong') }}`로 바꾸고 `dbt ls --select fact_store`. `Compilation Error / Found a cycle: model.seoul_dong.fact_store --> model.seoul_dong.map_dong_mois --> model.seoul_dong.dim_dong`. 1회, 레포는 고치지 않았다.
  - 원인: `dim_dong`은 행정동마다 업소 다수결 1위 행정안전부 코드(`mois_dong_cd`, 결정 13, 25)를 붙이려고 `map_dong_mois`를 읽고, `map_dong_mois`는 업소를 행정동에 붙인 `fact_store`를 읽는다. 결정 97의 A는 그 `fact_store`가 다시 `dim_dong`을 읽게 하므로 고리가 된다. 처방 표의 A 단점("경계 정의가 같은지 확인")이 실제로는 경계 정의가 아니라 의존 방향 문제였다. 처방을 고를 때 모델 의존 그래프를 보지 않았다.
  - 다시 고를 선택지:
    | 선택지 | 장점 | 단점 |
    |---|---|---|
    | A'. 경계만 담는 새 모델(`dim_dong_geom`: 최신 수집일 `sgis_dong`의 adm_cd, geom, GiST)을 두고 `fact_store`와 `dim_dong`이 함께 읽는다 | 순환 없음. 인덱스가 dbt 안에 있고 수집일 고르기가 한 곳 | 모델이 하나 는다. 리니지 정답 기준에 영향 없음(정답 6개 모델 밖) |
    | B. `stg.sgis_dong.geom`에 GiST 인덱스(DDL 06) | 모델을 안 바꾼다 | dbt 밖 DDL. 수집일이 쌓이면 모든 수집일 경계가 인덱스에 든다(지금 1개) |
    | C. 그대로 둔다 | 일이 없다 | wh_load 매번 2분 넘게(측정 27) |
- 처방 (2026-11-14, 결정 102): B. `stg.sgis_dong.geom`에 GiST 인덱스(`warehouse/sql/06_stg_boundary.sql`). 측정 40: 같은 조인 137.3초 → 4.55초, 계획이 Join Filter에서 업소마다 GiST Index Scan으로 바뀜, 결과 두 방향 `EXCEPT ALL` 0/0, dbt 전체 빌드 233.1초 → 95.4초(`fact_store` 노드 140.5초 → 5.41초). wh_load 1회(Airflow, 2026-11-14 00:12:53~00:15:05, 인덱스 뒤 첫 실제 실행): 성공, `dbt_build` 작업 103초(같은 날 인덱스 전 233.1초, 측정 38 2.3), 상품 데이터 파일 8개 sha256이 v2026.11.13과 모두 같음(v2026.11.14).
- 재발 방지: DDL에 인덱스(`IF NOT EXISTS`). 공간 조인을 새로 짤 때 `EXPLAIN`에서 `Join Filter: st_contains`가 보이면 인덱스가 없다는 뜻이다.
- 남은 것: `ST_Transform` 몫은 205ms로 쟀다(주원인 아님). 다른 공간 조인 중 `dim_parcel`은 `EXPLAIN`(실행 안 함)에서 `wh.dim_dong`의 GiST로 `Index Scan`(`Index Cond: geom ~ point`)을 쓴다(2026-11-14). `dim_station`은 같은 모양(`dim_dong`과 `ST_Contains`)이라 같다고 보지만 계획을 따로 보지 않았다(역 784곳이라 영향 작음).
- 상태: **처방함, 확인됨**(결정 102는 사용자 확인 전).


## F-5. 상품 파일 층에는 행 집합 노드(`*`)가 없어, 마트 WHERE 조건의 ROWSET 경로가 마트에서 멈춤
- 태그: 정확성(리니지 누락)
- 언제·어디: 2026-11-13, 2.5-3. wh_load(manual__2026-11-13T04:26:15) 뒤 lineage 회차 25(간선 2,051개, 그중 ROWSET 246개). 커밋 전 작업 트리
- 증상: `lineage.downstream`에서 `wh.fact_rent.contract_type`의 하류를 보면 `mart.rent_listing.*`까지는 `rowset_path = true`로 나오지만, `product.rent_listing`에는 `*` 대상이 없다. 상품 층에서 `rowset_path`가 참인 행은 `product.dong_profile`의 `jeonse_deals`, `monthly_deals` 2개뿐이다(count(*) 집계를 거친 컬럼). `product.rent_listing` 13개 컬럼에는 `row_path`(INDIRECT FILTER)로만 닿는다.
- 재현: `select to_table, to_column, row_path, rowset_path from lineage.downstream where from_table = 'wh.fact_rent' and from_column = 'contract_type' and is_product` (18행, rowset 2행). `edge_raw`에서 run_id 25, `target_table = 'product.rent_listing'`이고 `*`가 들어간 간선 0개. 1회 확인.
- 가설:
  - H1 추출기가 `rent_listing.sql`의 WHERE에서 ROWSET을 못 뽑았다 → `mart.rent_listing.*` 대상 ROWSET 간선이 있다 → 지움.
  - H2 상품 층 간선은 SQL에서 뽑지 않는다 → `lineage/product.py`가 `export_product.py`의 TABLES를 읽어 `product.<파일>.<컬럼> <- mart.<표>.<컬럼> IDENTITY`만 만든다. `* -> *` 간선은 만들지 않는다 → 남김.
- 원인: 상품 층 간선은 SQL 파서가 아니라 `lineage/product.py`가 컬럼 목록으로 만든다. 결정 80에서 ROWSET을 더할 때 SQL 추출기(`lineage/edges.py`)만 바꾸고 상품 드라이버는 바꾸지 않았다. 그래서 행 집합 경로는 마트 표의 `*`에서 끝나고, "상품 파일의 행이 바뀌는가"는 count(*)처럼 행 수가 값으로 바뀐 컬럼을 거쳐서만 보인다. 컬럼 단위 영향(`row_path`)은 상품 컬럼 전체에 닿으므로 영향 범위 자체는 빠지지 않는다.
- 처방 선택지:
  | 선택지 | 장점 | 단점 |
  |---|---|---|
  | A. `product.py`가 표마다 `product.<파일>.* <- mart.<표>.*` ROWSET FROM 한 줄을 더한다 | 상품 파일의 행 집합 영향이 그대로 이어진다. 고칠 곳 한 곳 | 정답 기준(tests/lineage_truth)은 SQL 모델 6개라 상품 층 정답이 따로 없다 |
  | B. 그대로 둔다 | 일이 없다. 컬럼 단위로는 이미 닿는다 | MCP 질문 "이 조건을 바꾸면 상품 행이 바뀌나"에 rowset으로 답하지 못한다 |
- 처방과 결과: 처음에는 사용자 지시("오류는 기록하고 해결하지 말 것")에 따라 두었고, 결정 97로 A를 적용했다(2026-11-13). `lineage/product.py`가 상품 파일마다 `product.<파일>.* <- mart.<표>.*` ROWSET FROM 한 줄을 더한다.
  - 전후(측정 33): `wh.fact_rent.contract_type`의 상품 하류 중 `rowset_path` 참 2/18행(회차 27) → 4/20행(회차 31, `product.rent_listing.*`, `product.dong_profile.*`). 상품 층 `rowset_path` 참 전체 2,234 → 2,795.
- 재발 방지: 상품 층 간선도 같은 간선 종류(DIRECT, ROWSET)를 낸다. 정답 기준에 상품 층 파일은 아직 없다.
- 남은 것: 2.5-4 변이(`contract_type` 표기 변경)는 7절 묶음과 함께 한다.

## F-6. 상품 manifest의 전월세 격리 지표가 더는 채워지지 않는 옛 표(`stg.rent_quarantine`, `stg.rent`)를 읽음
- 태그: 정확성(낡은 지표), 운영
- 언제·어디: 2026-11-13, 8절 지울 목록을 만들다 찾음(읽기만). `warehouse/export_product.py` 119~120줄. 커밋 전 작업 트리
- 증상: manifest `quality.rent_quarantine_rows`가 `stg.rent_quarantine`을 `stg.rent`의 처리 단위별 최신 수집일로 걸러 센다. 전월세 정제는 결정 72로 `stg_dbt.rent`, `stg_dbt.rent_quarantine`으로 옮겼고, 지금 `molit_rent` DAG는 `transform.rent_raw`(원본 적재)만 부른다. 지금은 두 표 내용이 같다(최근 수집일 2026-11-13, `stg.rent` 450,957행 = `stg_dbt.rent` 450,957행, 격리 11행 = 11행).
- 재현: `select max(collected_date), count(*) from stg.rent` 와 `stg_dbt.rent` 비교. 1회.
- 가설:
  - H1 옛 표가 아직 다른 경로로 채워진다 → DAG 명령에 `transform.rent`가 없다(`dags/molit_rent.py` 25줄은 `transform.rent_raw`). 11-13 행은 2026-11-12T16:02(UTC) 정기 실행에서 들어왔다. 그 실행이 옛 DAG 코드로 돌았는지는 [확인 필요] → 앞으로는 채워지지 않는 것으로 봄.
  - H2 내보내기가 이전 때 함께 고쳐지지 않았다 → 결정 72 작업에서 정제 모델과 DAG만 바꾸고 `export_product.py`의 품질 쿼리는 그대로였다 → 남김.
- 원인: 정제 경로를 옮길 때 그 결과를 읽는 쪽을 dbt `ref` 바깥(Python 내보내기의 SQL 글자)에서 찾지 않았다. dbt 의존 그래프와 컬럼 리니지는 모델 SQL만 읽으므로 이 의존이 보이지 않는다(리니지 상품 층은 마트 → 상품 파일 컬럼만 본다, F-5).
- 처방 선택지:
  | 선택지 | 장점 | 단점 |
  |---|---|---|
  | A. 품질 쿼리를 `stg_dbt.rent_quarantine`, `stg_dbt.rent`로 바꾼다 | 한 줄. 8절 옛 표 삭제 전에 꼭 필요하다 | 같은 종류의 의존이 다른 곳에 더 있는지는 따로 찾아야 한다 |
  | B. 품질 지표를 `stg_dbt.batch_status`(관문 판정)에서 읽는다 | 관문과 같은 기준이 된다 | 지표 뜻이 바뀌어 데이터 계약 문서를 고쳐야 한다 |
- 처방과 결과: A (결정 97, 2026-11-13). `export_product.py` 119~120줄을 `stg_dbt.rent_quarantine`, `stg_dbt.rent`로 바꿨다. 같은 시점 지표 값은 옛 표 3, 새 표 3으로 같았다.
- 전수 탐색 (결정 97): dbt 모델 밖의 Python 전체(`.venv` 등 제외)와 `warehouse/sql/**/*.sql`(Metabase 카드 원본 `warehouse/sql/ops/` 포함)에서 `스키마.표` 글자를 정규식으로 찾았다(`python -m measure.table_refs`, 1회). 파일 51개, (파일, 표) 쌍 190개(F-6 처방 뒤).
  | 분류 | 옛 표를 읽거나 쓰는 쌍 |
  |---|---|
  | 옛 경로 자체(쓰는 쪽): `transform/congestion.py`, `rent.py`, `sbiz_store.py`, `living_migration.py`, DDL `01`·`03`·`04`·`05` | 18 |
  | 옛 경로 밖에서 읽는 쪽, 처방 전 | 9: `export_product.py`(전월세 2, 생활이동 격리 1), `07_ops.sql`(생활이동 2), `ops/06_source_status.sql`, `ops/10_quality.sql`, `measure/lineage_mutation.py`(M5), `measure/living_g3.py`(주석의 글자, 읽지 않음) |
  | 옛 경로 밖에서 읽는 쪽, 처방 뒤 | 7. 모두 생활이동이다. 3절 이전으로 바뀌어야 할 곳이고, 전월세·혼잡도·상가정보 옛 표를 읽는 곳은 0이다 |
- 재발 방지: 8절에서 옛 표를 지우기 전에 같은 탐색을 다시 돌려 "옛 경로 밖 0"을 확인한다.
- 남은 것: F-5와 같은 경계에서 나왔다. 리니지와 dbt 의존 그래프는 모델 SQL만 보므로, 모델 밖 SQL 글자(내보내기, 운영 카드, 측정 도구)의 의존은 지도에 없다. 지금은 위 탐색처럼 글자 검색으로만 찾는다.

## F-7. 관문 표 하나에 모든 출처 판정이 있어, 리니지가 한 출처의 변화를 다른 출처 정제 결과에도 닿는다고 넓게 잡음
- 태그: 정확성(리니지 과다 귀속)
- 언제·어디: 2026-11-13, 7절 변이 시험(`python -m measure.lineage_mutation`, lineage 회차 27). 커밋 전 작업 트리
- 증상: raw 변이 5개(M1, M2, M4, M6, M7)는 모두 지도가 상품 컬럼 40개를 예측했다. 실제로 바뀐 것은 1~18개다. 예: M4(`raw.rent`의 `monthlyRent`)는 실제로 `dong_profile` 시세 5개와 `rent_listing` 2개만 바뀌었는데, 지도는 `dong_mois_map` 6개, 편의 업종 10개, `am_peak_congestion` 등도 예측했다. 놓친 것은 0이라 시험은 통과했다. stg 변이(M3, M5)는 예측 = 실제(7개, 4개)였다.
- 재현: `select to_table, bool_or(row_path), bool_or(rowset_path) from lineage.downstream where from_table = 'raw.rent' and from_column = 'raw.monthlyRent' group by 1` → `stg_dbt.batch_status`(깊이 2) 다음에 `stg_dbt.store`, `stg_dbt.congestion`(깊이 3)이 row/rowset 경로로 나온다. 1회.
- 가설:
  - H1 sqlglot 추출기가 FILTER를 잘못 뽑았다 → `stg_dbt.store`의 WHERE는 실제로 `batch_key in (select batch_key from batch_status where source = 'store' and approved)`이고, 이 하위 쿼리는 `batch_status`의 `batch_key`, `source`, `approved`를 읽는다. 간선은 SQL 그대로다 → 지움.
  - H2 `batch_status`의 컬럼 단위 리니지가 출처를 구분하지 못한다 → `batch_status`는 세 출처를 `union all`로 합친 한 표라서 `approved`는 전월세·혼잡도·상가정보 판정 모두에서 온다. 컬럼 리니지는 행 조건(`source = 'store'`)으로 갈래를 자르지 않는다 → 남김.
- 원인: 결정 75~77이 관문 판정을 한 표(`stg_dbt.batch_status`)에 모았다. 실행에서는 `source = 'store'` 조건이 다른 출처의 판정 행을 거르지만, 컬럼 리니지는 값의 갈래(union의 어느 가지에서 온 행인가)를 행 조건으로 나누지 못한다. 그래서 어느 출처의 관문 재료든 `approved` 하나로 모여 세 출처의 정제 결과 전체로 번진다. 정확도(놓침 0)는 그대로이고 정밀도가 떨어진다.
- 처방 선택지:
  | 선택지 | 장점 | 단점 |
  |---|---|---|
  | A. 출처마다 관문 뷰를 둔다(`batch_status_rent` 등)와 `batch_status`는 그 합집합 | 지도가 출처별로 갈린다. 판정 한 표(운영 카드)는 그대로 | 모델 3개가 늘고 정제 모델 6개의 참조를 바꾼다. 1절 같음 증명을 다시 한다 |
  | B. 추출기가 `where source = '상수'`를 보고 union 갈래를 자른다 | 모델은 그대로 | 추출기에 특수 규칙. 다른 모양의 조건에는 안 맞는다 |
  | C. 그대로 둔다 | 일이 없다. 놓침은 없다 | 영향 범위 질문에 넓은 답이 나와 사람이 다시 걸러야 한다 |
- 처방과 결과: 결정 97에 따라 A (2026-11-13). 출처별 관문 모델 3개 + 합집합 `batch_status`, 정제 모델 6개는 자기 출처 모델을 읽는다. 같음 증명은 표 14개 모두 두 방향 차이 0, wh_load 배포 뒤 상품 데이터 파일 sha256 같음(측정 35). 지도 예측 상품 컬럼: M1 40 → 5, M2 40 → 5, M4 40 → 20, M7 40 → 20(정밀도 M1 10% → 80%, M7 45% → 90%). **M6은 40 그대로다.** 상가정보는 관문이 아니라 두 길로 다른 출처에 닿는다: (1) `store → bjd_code`(승인된 상가 배치로 만든 법정동 코드표) `→ rent_rows → rent`, 행 경로, 깊이 3~5 (2) `fact_store → map_dong_mois → dim_dong`(다수결 대응표), 행 집합 경로, 깊이 5. `lineage.downstream`에서 `raw.sbiz_store`의 `raw.상권업종소분류코드`로 확인(회차 32). 둘 다 출처를 실제로 잇는 길이라 F-7과는 다른 넓힘이다. 업종 코드가 코드표나 다수결을 바꾸는지는 컬럼 리니지가 구분하지 못한다(처방 없음).
- 재발 방지: 관문 모델은 출처마다 둔다(`batch_status.sql` 머리 주석). 정제 모델이 합집합을 읽으면 다시 생긴다. 그것을 막는 시험은 없다.
- 남은 것: 관문 이전(측정 22)의 M1·M2·M4·M6은 stg 변이라 정밀도를 바로 비교할 수 없다.

## F-8. 생활이동 경계 표를 묶으면 크게 줄 것이라는 설계 가정이 틀림: 그룹/행 0.58
- 태그: 설계 가정, 분포
- 언제·어디: 2026-11-13, design_remaining_work.md 3.2. `python -m measure.living_g3` (Spark 3.5.9 local[*], driver 8g, Iceberg `lh.living.migration_rows` 10달). 측정 29
- 증상: 결정 89의 G3 grain(대상연월 × 출발 × 도착 × 평일/주말 × 도착시간 × 이동유형 × 나이) 그룹 수가 816,203,740으로 기준(40,382,008)의 20.2배였다. 원본 1,405,791,964행 대비 0.58. 같은 실행에서 G3 그룹 세기가 336.3초로 가장 긴 단계였다.
- 재현: 대상연월마다 그룹/행이 0.57~0.59로 같다(측정 29 표). 1회 실행, 10개 대상연월.
- 가설:
  - H1(결정 89에서 세운 가정) 성별 2개와 요일 7개를 접으면 크게 준다 → 그룹/행 0.58. 접히는 행이 적다 → 지움.
  - H2 원본이 이미 거의 그룹 단위로 성겨서(대부분의 칸에 행이 하나뿐) 접을 짝이 없다 → 이동인구가 3명 이하이면 "*"로 가려지는데(`docs/data_sources.md` 생활이동 절, 설명서) 가려진 비중이 대상연월마다 23.6~24.8%다(측정 29). 인구가 아주 작은 칸이 많다는 뜻이고, 같은 (출발, 도착, 시간, 유형, 나이)에 성별·요일 짝이 모두 있는 경우가 드물다는 H2와 맞는다 → 남김. 칸마다 짝이 몇 개인지 분포는 재지 않았다 [확인 필요].
  - H3 키 쏠림 때문에 그룹이 많다 → 가장 큰 도착·출발 행정동도 전체의 1% 남짓(측정 29) → 그룹 수와 관계없음, 지움.
- 원인: 원본 한 행이 이미 거의 하나의 집계 단위라, 차원을 접는 집계가 행을 줄이지 못한다. 결정 89 표의 G-b 칸을 [확인 필요]로 두고 "줄기도, 늘기도 한다"고만 적은 채 G3를 골랐다. 재 보고 고를 수 있었던 값이다.
  - 엔진 쪽 뜻(3.10에서 확인할 가설): Spark의 부분 집계(셔플 전 HashAggregate partial)는 같은 키를 한 파티션 안에서 미리 합쳐 셔플 양을 줄이는데, 전체로도 0.58까지만 주는 데이터라면 파티션 안에서는 그보다 덜 줄어 셔플 양이 입력과 비슷할 것이다. 결정 94의 가설("부분 집계가 쏠림을 대부분 줄인다")은 이 데이터에서 맞지 않을 가능성이 크다. 병목 후보는 스큐보다 셔플 양과 spill이다.
- 처방 선택지: 측정 29의 후보 a~f(결정 96에 표).
- 처방과 결과: 결정 96, f(13,842,750그룹, 기준의 0.34배).
- 재발 방지: 설계 표에서 [확인 필요]로 둔 크기 칸은 고르기 전에 한 번 잰다. 이번 측정 도구(`measure/living_g3`)는 후보별 그룹 수를 바로 잰다.
- 남은 것: 칸마다 성별·요일 짝 수의 분포, 3.10에서 부분 집계 전후 행 수(Spark SQL 지표 `number of output rows`)로 위 엔진 쪽 가설 확인.

## F-9. 생활이동 실행기 첫 실행이 34.9분: 관문 계산에서 원본을 다섯 번 읽고, 정제 묶기가 정렬 집계로 220GB를 흘림
- 태그: 계산량·인덱스, 메모리
- 언제·어디: 2026-11-13, design_remaining_work.md 3.3~3.4 첫 실행. `python -m transform.spark_sql_runner spark_sql/living stg.living_commute --show batch_status` (Spark 3.5.9 local[*], driver 8g, 셔플 파티션 200, AQE 켬, Iceberg `lh.living.migration_rows` 10달 1,405,791,964행). `ops.engine_run` run_id `local-1794556106209`. 커밋 ef06529 위 작업 트리
- 증상: 실행 전체 2,094.0초. 그중 `show batch_status`(관문 계산, 캐시)가 약 30분, 경계 표 쓰기(`write`)는 약 4분. 단계 지표(`ops.engine_run`):
  | 단계 | 태스크 | 단계 시간 | 셔플 쓰기 | spill(메모리 / 디스크) | 태스크 최대 / 중앙 |
  |---|---|---|---|---|---|
  | 0, 2, 3 (원본 읽기 + 코드표 조인용 셔플) | 118 | 192~546초 | 각 36GB | 0 | 35~37초 / 23~25초 |
  | 9, 12 (`20_clean` 묶기) | 198 | 640초, 1,227초 | 0 | 각 220GB / 24GB | 137초, 134초 / 29~30초 |
  | 11 | 118 | 773초 | 20GB | 0 | 39초 / 25초 |
  | 53 (쓰기의 `30_commute` 묶기) | 114 | 135초 | 0 | 30GB / 4.8GB | 39초 / 18초 |
  컨테이너 `/tmp/blockmgr-*`(셔플·spill 파일)가 실행 중 129GB까지 찼다.
- 재현: 같은 명령 2회(두 번째는 F-10 확인용, 측정 32). 실행 계획은 `EXPLAIN FORMATTED SELECT * FROM batch_status`(실행하지 않고 계획만).
- 가설:
  - H1 데이터 쏠림(특정 도착 행정동에 몰림) → 태스크 시간 최대/중앙이 원본 읽기 단계 1.5배, 묶기 단계 4.5배다. 묶기 단계가 더 벌어지지만 가장 큰 키도 전체의 1% 남짓이고(측정 29) 셔플 파티션 200개에 비해 작다 → 주원인 아님, 지움. 묶기 단계 최대 태스크가 왜 4.5배인지는 [확인 필요](3.10).
  - H2 부분 집계가 줄이지 못해 셔플이 크다(F-8 엔진 쪽 가설) → 실행 계획에서 `20_clean`은 HashAggregate가 아니라 `SortAggregate`(82, 83)다. 묶기 키 9개가 거의 행마다 달라 부분 집계로 줄 것이 없고, 입력 전체를 키 9개로 정렬한다 → 남김(아래 원인 2).
  - H3 같은 계산을 여러 번 한다 → 관문 계획에 `BatchScan lh.living.migration_rows`가 5개(원본 행 수, 격리 사유 행, V8 격리용 원본, V8용 `clean`, 통과 수용 `clean`). 단계 지표에서도 36GB 셔플 쓰기가 3번, 220GB spill 묶기가 2번(9, 12) 나온다. Spark는 같은 임시 뷰를 쿼리 안에서 여러 번 참조하면 계획을 각각 펼치고, 셔플 재사용(ReuseExchange)은 셔플 아래 계획이 똑같을 때만 된다 → 남김(원인 1).
  - H4 코드표(V4) 조인이 원본을 셔플한다 → 계획에서 코드표가 `Scan ExistingRDD`(Python `createDataFrame`)라 크기 통계가 없어 브로드캐스트가 아닌 `SortMergeJoin`이 됐고, 원본 14억 행 15개 열이 출발 코드로 한 번, 도착 코드로 한 번 셔플된다(36GB). AQE가 실행 중 1태스크 브로드캐스트 단계(5, 15)를 만들었지만 큰 쪽 셔플은 이미 쓰인 뒤다 → 남김(원인 3). 다만 `checked`만 한 번 읽어 묶는 측정(`measure.living_boundary_gap`)은 같은 조인을 거쳐도 139.5초라, 이것이 34.9분의 대부분은 아니다.
- 원인:
  1. 관문(`22_batch_status`)이 `checked`, `clean`, `quarantine`을 따로 세어 원본을 다섯 번 펼친다. 실행기는 `batch_status`만 캐시하므로 그 안의 중간 뷰는 재사용되지 않는다.
  2. `20_clean`의 `min/max(문자열)`은 Spark HashAggregate가 다루지 못하는 집계 버퍼(가변 길이 문자열)라 정렬 기반 `SortAggregate`로 바뀐다. 거의 유일한 키로 14억 행을 정렬하므로 실행 메모리(driver 8g의 일부)를 넘겨 매번 220GB(직렬화 전 크기)를 흘리고 디스크에 24GB를 쓴다. 이 묶기가 관문에서 2번 돈다.
  3. 코드표 조인이 브로드캐스트되지 않아 원본 전체를 두 번 셔플한다.
- 처방 선택지:
  | 선택지 | 장점 | 단점 |
  |---|---|---|
  | A. `20_clean`의 충돌 검사를 숫자로 바꾼다(`min/max(avg_minutes)`, `min/max(population)`, 가려짐 수) → HashAggregate | 정렬이 사라진다. SQL 한 파일 | `*`와 숫자 섞임, 숫자로는 같고 글자로는 다른 값("05"와 "5")을 옛 코드와 똑같이 판정하려면 열을 하나 더 세야 한다 |
  | B. 실행기가 `checked`(또는 `clean`)를 캐시·체크포인트한다 | 원본 다섯 번 읽기가 한 번 | 14억 행 캐시는 메모리에 안 들어가 디스크에 쓴다. 실행기에 뷰별 설정이 생긴다 |
  | C. 관문 SQL을 한 번 읽기로 다시 쓴다(`checked`에서 한 번에 행 수·격리 수, `clean`은 한 번만) | 계산이 설계 수준에서 준다 | V8 격리 행 수를 `clean`에서 세면 `21_quarantine` 뷰와 셈이 둘이 된다 |
  | D. 코드표에 `BROADCAST` 힌트(또는 SQL `VALUES`로 만들어 크기 통계가 있게) | 셔플 2번 x 원본 읽기 수가 사라진다 | 단독으로는 효과가 작을 수 있다(H4의 139.5초) |
  | E. 그대로 둔다 | 월 1회 실행이라 35분은 견딘다 | 3.10의 압력 실험이 이 낭비를 재게 된다. 셔플·spill 파일이 /tmp를 129GB 쓴다 |
- 처방과 결과: 사용자 지시("오류는 기록하고 해결하지 말 것", 2026-11-13)에 따라 고치지 않았다. 3.10 엔진 분석의 첫 항목 후보다.
- 재발 방지: 없음(처방 전). 실행기가 매 실행 `ops.engine_run`에 단계별 spill과 셔플을 남기므로 처방 전후를 같은 표로 비교할 수 있다.
- 남은 것: 묶기 단계 최대 태스크 4.5배의 원인, spill 220GB가 실제 디스크 24GB와 다른 이유(직렬화·압축 비율) [확인 필요].
- 원인 다시 확인 (2026-11-14 00:10, handoff_finish.md 4절 2번 "실행 계획으로 원인을 다시 확인"): `python -m transform.spark_sql_runner spark_sql/living stg.living_commute --show batch_status quarantine --explain`(계산하지 않음, 7초, 출력 `logs/f9_explain.txt`). 계획의 연산자 번호 줄을 뷰마다 셌다:
  | 뷰 | BatchScan(원본 읽기) | SortAggregate | HashAggregate | SortMergeJoin | BroadcastHashJoin | Exchange(셔플) |
  |---|---|---|---|---|---|---|
  | `batch_status` | 5 | 4 | 6 | 13 | 0 | 26 |
  | `quarantine` | 3 | 2 | 0 | 7 | 0 | 14 |
  - 원인 1(원본을 여러 번 펼침): 그대로. 관문 하나가 원본을 5번, 격리 뷰가 따로 3번 읽는다. 실제 실행(측정 38)에서는 격리 뷰를 먼저 캐시해 관문이 그것을 재사용하지만, 격리 뷰 자체가 원본을 3번 읽는다.
  - 원인 2(글자 min/max → SortAggregate): 그대로. `clean` 한 번 = 부분·최종 SortAggregate 2개이고, 관문에 4개(2번), 격리 뷰에 2개(1번).
  - 원인 3(코드표 조인이 브로드캐스트되지 않음): 그대로. BroadcastHashJoin 0, 코드표 조인이 모두 SortMergeJoin이다.
  - DuckDB에서도 같은 SQL 구조가 메모리·임시 공간을 넘겼다(F-19). 처방은 두 엔진에 함께 듣는다.
- 처방과 결과 (2026-11-14, 결정 103 A + C + D): 원인 1~3은 계획에서 사라졌다(측정 41: 관문 원본 읽기 5 → 4, 실제 실행에서는 `clean` 1번; SortAggregate 0; 코드표 조인 BroadcastHashJoin). 전체 실행(측정 42): 결과 같음(세 표 두 방향 0), 셔플 쓰기 248.1 → 122.7GB, VM 디스크 최대 증가 259.0 → 143.9GB. 실행 시간 3,201 → 3,329초로 줄지 않았다. 메모리 흘림 460 → 1,584GB, 디스크 흘림 55 → 178GB.
  - 새로 드러난 원인(가설): 묶기 키 9개가 거의 유일해(H2) HashAggregate도 부분 집계로 줄일 것이 없고 해시 표가 메모리를 넘기면 정렬로 바꿔 흘린다. 정렬 집계를 해시로 바꾸는 것만으로는 '거의 유일한 키로 14억 행 묶기'라는 일 자체가 줄지 않는다. 처방 전 없던 흘림이 원본 읽기 단계(부분 집계 자리)에 생긴 것이 근거다 [확인 필요: 정렬 대체 횟수].
- 상태: **처방함(원인 1~3), 시간 개선 확인 안 됨.** 셔플·작업 공간은 줄었다. 시간을 줄이려면 다음 후보(부분 집계를 건너뛰기, 메모리·파티션 조정, `clean`을 키 9개 대신 필요한 셈만 묶기)를 따로 재야 한다. 고르지 않았다.
- 덧붙임 (2026-11-13, 측정 38): 판단 64로 격리 뷰도 `--save`로 저장하자 실행기가 격리 뷰를 따로 캐시해, 같은 원본 읽기와 조인 셔플이 한 번 더 돈다. 셔플 쓰기 143.0GB → 248.1GB, 단계 32개, 실행 2,094초 → 3,201초(CPU 상한 12개 조건 포함). 원인 1(뷰를 여러 번 펼침)이 더 커진 것이라 처방 선택지는 그대로이고, B(`checked` 캐시)나 C(관문을 한 번 읽기로)의 효과가 커진다.
- 덧붙임 (2026-11-13, F-16에서 확인): 제목과 표의 "220GB"는 Spark 지표 Memory Bytes Spilled(흘리기 전 메모리 안 크기)다. 디스크에 실제로 쓴 spill은 단계마다 24GB(Disk Bytes Spilled)이고, 셔플 쓰기와 함께 컨테이너 /tmp/blockmgr-*가 최대 129GB였다. 그 /tmp는 Docker 가상 디스크(호스트 C:의 VHDX)에 있다.

## F-10. 리니지 추출이 5절 뒤로 매번 실패: `raw_shape`의 `count(*)`가 UNION 위에 있어 `KeyError: 'from_'`
- 태그: 정확성(리니지 누락), 운영
- 언제·어디: 2026-11-13 발견(3.5 작업 중). 처음 난 때는 5절 `raw_shape` 모델을 더한 뒤의 lineage 회차 26(05:14)이다. 회차 26, 27, 28 모두 같은 오류. lineage DAG `extract_lineage`가 두 번 실패했고 `ops.failure_log`에 남았지만, 슬랙 웹훅이 없어(9절) 사람에게 닿지 않았다.
- 증상: `lineage.edge_raw`의 `models/stg_dbt/raw_shape.sql` 행 하나가 `status = parse_error`, `detail = KeyError: 'from_'`. 회차 자체는 `success`로 남고 추출기가 끝에서 `SystemExit`로 실패한다(설계대로).
- 재현: Airflow 컨테이너에서 `S.build(manifest)` 뒤 `Resolver.output(root, 'items', seen)`. 매번(3/3 회차).
- 가설:
  - H1 sqlglot 버전이 바뀌어 `from` 인자 이름이 달라졌다 → 같은 sqlglot 30.31.0에서 다른 모델의 `count(*)`(`dong_profile`)는 된다 → 지움.
  - H2 `count(*)`의 FROM이 UNION 결과다 → `raw_shape`는 `from shapes`이고 `shapes`는 `union all` 셋이다. `Resolver.star()`가 FROM 표를 따라 `inner(source)`로 들어가면 그 scope의 식이 `Select`가 아니라 `Union`이라 `args["from_"]`이 없다 → 남김.
- 원인: `lineage/edges.py` `star()`는 FROM이 표이거나 SELECT 서브쿼리인 경우만 다룬다. `output()`은 UNION을 갈래마다 나누지만 `star()`에는 같은 처리가 없다. 정답 기준 6개 모델에 UNION 위 `count(*)`가 없어 시험에서 드러나지 않았다.
- 처방 선택지:
  | 선택지 | 장점 | 단점 |
  |---|---|---|
  | A. `star()`가 UNION이면 `branches()`로 갈래마다 FROM을 따라간다(`output()`과 같은 모양) | 세 원본 표의 행 `*`이 모두 잎으로 나온다. 몇 줄 | 정답 기준에 UNION 위 count(*) 예가 없어 정답부터 더해야 한다(결정 49 순서) |
  | B. `raw_shape`를 리니지 대상에서 뺀다 | 일이 없다 | 운영 표라 상품 경로는 없지만, "모든 모델을 본다"는 원칙이 깨진다 |
- 처방과 결과: 처음에는 사용자 지시에 따라 두었다. 3.7에서 Spark SQL 묶음(`22_batch_status`가 UNION인 `21_quarantine` 위에서 `count(*)`)이 같은 오류로 막혀, 결정 91의 "추출기를 고친다"에 따라 A를 적용했다. 회차 31에서 `raw_shape` 포함 실패 0, `stg_dbt.raw_shape` 간선 34개가 새로 나왔다(측정 33). `tests.test_lineage_edges` 통과.
- 재발 방지: 정답 기준 `stg_living_commute.csv`가 UNION 위 count(*)를 담는다. 실패가 사람에게 닿지 않은 것은 9절(슬랙) 남은 일과 같은 원인이다.
- 남은 것: 5절 체크포인트에서 리니지 회차 결과를 보지 않았다(판단 기록에 남김).

## F-11. 3.5 뒤 리니지가 `fact_commute`, `map_dong_2021`에서 `RecursionError`: 새 원천 표가 스키마 추론에 없고, 별칭 지름길이 자기 자신을 다시 부름
- 태그: 정확성(리니지 누락), 타입·스키마
- 언제·어디: 2026-11-13, design_remaining_work.md 3.5 뒤 wh_load(manual__2026-11-13T08:24:28)가 부른 lineage 회차 28.
- 증상: `lineage.edge_raw` 회차 28에서 `models/wh/fact_commute.sql`, `models/wh/map_dong_2021.sql`이 `RecursionError: maximum recursion depth exceeded`. 간선 2,052개(회차 27) → 2,022개.
- 재현: 컨테이너에서 같은 manifest로 `extract_edges` 2회, 2회 모두.
- 가설:
  - H1 새 SQL이 복잡해 재귀가 깊다 → `fact_commute`는 표 하나를 그대로 고르는 SELECT다 → 지움.
  - H2 `stg.living_commute`가 리니지 스키마에 없다 → `lineage/schema.py`는 `warehouse/sql/0[1-6]_stg_*.sql`과 `*_raw_*.sql`만 읽는다. 새 DDL은 `12_stg_living_commute.sql`이라 빠졌다. 스키마에 없는 표의 컬럼은 `qualify`가 표 별칭을 붙이지 못한다 → 남김.
  - H3 표 별칭이 없는 컬럼이 무한 반복을 만든다 → `Resolver.column()`은 별칭 없는 컬럼 이름이 그 SELECT의 출력 이름과 같으면 "SELECT 별칭을 가리킨다"(ORDER BY용 지름길)고 보고 `output()`을 다시 부른다. `select target_ym ... from 모르는표`에서는 출력 `target_ym`의 식이 바로 그 컬럼이라 자기 자신을 부른다 → 남김.
- 원인: H2가 방아쇠, H3가 증상을 키웠다. 모르는 표면 원래 `unresolved` 간선으로 남아야 하는데(설계 원칙: 실패는 드러나게), 지름길에 이미 지나온 (scope, 이름)을 막는 장치가 없어 파싱 실패로 바뀌고 그 파일의 다른 간선까지 모두 잃었다.
- 처방 선택지:
  | 선택지 | 장점 | 단점 |
  |---|---|---|
  | A. 스키마 추론 입력에 새 DDL을 넣는다(3.7-2와 함께, 파일 이름 규칙을 `*_stg_*.sql`로) | 방아쇠가 사라진다. 3.7 설계에 이미 있는 일 | H3는 남아 다음 새 표에서 또 난다 |
  | B. `column()`의 별칭 지름길에 (scope, 이름) 방문 표시를 두고, 다시 오면 `unresolved` 잎을 낸다 | 모르는 표가 와도 파일 전체를 잃지 않는다 | 정답 기준 재대조가 필요하다 |
  | C. 스키마 추론이 dbt `sources.yml`의 표 목록과 DDL을 대조해 빠진 표가 있으면 실패한다 | 같은 종류의 누락을 처음부터 막는다 | 검사 하나가 더 생긴다 |
- 처방과 결과: A를 3.7에서 적용했다(`lineage/schema.py`가 `[0-9][0-9]_stg_*.sql`을 읽음). 회차 31에서 `fact_commute`, `map_dong_2021` 실패 0, 간선 각 13개, 15개(측정 33). B, C는 사용자 지시에 따라 고치지 않았다.
- 재발 방지: 없음. H3(별칭 지름길의 무한 반복)는 남아 있어 스키마에 없는 표가 오면 다시 난다.
- 남은 것: 없음.

## F-12. 전월세 정제를 SQL로 옮기자 54행이 다름: Python `strip()`과 SQL `btrim`의 "공백" 정의가 다름
- 태그: 정확성, 타입·스키마
- 언제·어디: 2026-11-13, 전월세 정제 SQL 이전 같음 증명(결정 72, 측정 24). 이 기록 파일을 만들기 전에 일어나 측정 24에서 옮겨 적었다.
- 증상: 옛 `stg.rent`과 새 dbt 결과의 두 방향 `EXCEPT ALL`이 0이 아니라 54행(한쪽 27행씩)이었다. 모두 `building_name` 끝에 전각 공백(U+3000)이 있는 행이다. 예: "금강프라임빌　".
- 재현: 같은 원본으로 두 경로를 돌려 비교. 54행 모두 같은 패턴.
- 가설:
  - H1 SQL 정제 규칙을 잘못 옮겼다 → 54행 모두 `building_name` 끝 한 글자 차이뿐이다 → 규칙 자체는 같다, 지움.
  - H2 "앞뒤 공백 지우기"라는 같은 규칙을 두 언어가 다르게 구현한다 → Python `str.strip()`은 인자 없이 부르면 유니코드 공백 문자 전체(U+3000 포함)를 지우고, SQL은 `btrim(x, E' \t\n\r\f\v')`처럼 지울 글자를 직접 적어 그 6글자만 지운다 → 남김.
- 원인: 규칙 문서(`docs/rules.md`)는 "앞뒤 공백 제거"라고만 적었고, "공백"의 정의는 구현 언어가 정했다. 옮기기 전에는 Python의 정의가 곧 규칙이었다. 원천(국토교통부 API)이 건물 이름에 전각 공백을 넣어 보내는 경우가 있어 차이가 드러났다.
- 처방 선택지:
  | 선택지 | 장점 | 단점 |
  |---|---|---|
  | A. SQL이 Python과 같은 글자 집합을 지우게 한다(매크로 `py_strip`, Python이 공백으로 보는 29글자) | 옮기기 전 결과와 같다(동등 증명이 성립) | 29글자 목록을 직접 관리한다 |
  | B. SQL 쪽 정의를 규칙으로 삼고 차이를 의도한 변경으로 기록한다 | 매크로가 없다 | 같은 건물이 두 이름으로 갈려 `rent_listing` 묶음과 시세가 바뀐다 |
- 처방과 결과: A. 차이 0. 같은 매크로를 혼잡도 모델에도 적용해 혼잡도도 차이 0(측정 24).
- 재발 방지: 매크로 `py_strip`. 규칙 문서의 "공백"이 어떤 글자를 뜻하는지 매크로를 가리키게 했는지는 [확인 필요].
- 남은 것: 생활이동 Spark SQL(3절)과 DuckDB(3.9)의 `trim` 정의도 각각 다를 수 있다. 같은 규칙을 세 엔진으로 옮길 때 같은 시험을 한다.

## F-13. 실행기 두 번째 실행이 교체에서 실패: dbt 뷰가 경계 표에 붙어 있어 `DROP TABLE`이 거절됨
- 태그: 운영
- 언제·어디: 2026-11-13, design_remaining_work.md 3.3(결정 90의 "이름을 바꾼다"). 3.5로 `wh.fact_commute`(뷰)가 `stg.living_commute`를 읽게 된 뒤의 첫 실행기 실행. `ops.engine_run` run_id `local-1794558670739`, `ok = false`, 2,057.8초
- 증상: 계산·테스트·`stg.living_commute_new` 쓰기까지 끝난 뒤 교체 트랜잭션에서
  `psycopg.errors.DependentObjectsStillExist: cannot drop table stg.living_commute because other objects depend on it / DETAIL: view wh.fact_commute depends on table stg.living_commute`. 종료 코드 1.
- 재현: 3.5 뒤 실행기 1회(1/1). 첫 실행(3.5 전)은 같은 코드로 성공했다. 그때는 이 표를 읽는 뷰가 없었다.
- 가설:
  - H1 PostgreSQL 뷰는 표를 이름이 아니라 OID로 붙잡는다 → 실행기는 `DROP TABLE stg.living_commute; ALTER TABLE stg.living_commute_new RENAME TO living_commute` 순서다. 뷰 `wh.fact_commute`가 지금 표(OID)에 걸려 있어 DROP이 거절된다(CASCADE면 뷰까지 지워진다). 이름 바꾸기가 먼저였어도 뷰는 옛 OID(이름이 바뀐 옛 표)를 따라가 새 데이터를 보지 못한다 → 남김.
  - H2 권한이나 잠금 문제 → 오류가 의존 객체(2BP01)다 → 지움.
- 원인: 결정 90은 "dbt 밖에서 쓴 표를 dbt가 source로 읽는다"와 "JDBC로 `_new`에 쓰고 이름을 바꾼다"를 함께 골랐는데, dbt 쪽 첫 모델이 뷰(`fact_commute`, materialized view)라 source 표에 OID 의존이 생긴다. dbt의 스키마 교체(결정 32)는 뷰를 매 빌드 다시 만드니 문제가 없지만, 이 표는 dbt 밖에서 바뀐다. 설계 때 "이름 바꾸기 = 원자적 교체"만 보고 의존 객체를 보지 않았다.
- 결과: 트랜잭션이 되돌려져 `stg.living_commute`(첫 실행 결과)는 그대로이고 상품도 그대로다. `stg.living_commute_new`(이번 계산, 13,782,826행)가 남았다. 실패는 종료 코드와 `ops.engine_run.ok`로 드러났다(조용히 옛 데이터를 보는 일은 없었다).
- 처방 선택지:
  | 선택지 | 장점 | 단점 |
  |---|---|---|
  | A. 한 트랜잭션에서 `TRUNCATE stg.living_commute; INSERT INTO stg.living_commute SELECT * FROM stg.living_commute_new; DROP TABLE stg.living_commute_new` | 뷰가 그대로 붙어 있다. 원자적. 고칠 곳 실행기 한 곳 | 1,378만 행을 한 번 더 복사한다(시간은 [미측정]). TRUNCATE는 다른 트랜잭션의 읽기를 잠깐 막는다 |
  | B. `fact_commute`를 표(materialized table)로 바꾼다 | 이름 바꾸기 교체가 그대로 된다 | wh_load마다 1,378만 행 복사. 다음에 누가 뷰로 이 표를 읽으면 다시 난다 |
  | C. 이름 바꾸기 뒤 의존 뷰를 다시 만든다(`CREATE OR REPLACE VIEW`를 dbt로 다시 빌드) | 복사 없음 | 실행기가 dbt를 알아야 한다. 그 사이 뷰가 옛 표를 본다 |
  | D. JDBC로 지금 표에 바로 덮어쓰기(Spark `truncate` 옵션) | 단순 | 트랜잭션이 아니다(결정 90이 피하려던 것). 실패하면 반쯤 쓴 표가 남는다 |
- 처방과 결과: 사용자 지시("오류는 기록하고 해결하지 말 것")에 따라 고치지 않았다. 남은 `stg.living_commute_new`는 지우지 않았다(표 삭제는 확인받는다).
  - 2026-11-13 처방 A를 넣었다(결정 99의 "남은 구현을 막으면 가장 작은 처방"). 판단 64(관문 판정·격리 행을 PostgreSQL에)가 같은 교체 코드를 쓰고, 새 표도 dbt 뷰(`batch_status_living`)가 읽어 같은 이유로 막힌다. `transform/spark_sql_runner.py`의 `swap()`: 표마다 `TRUNCATE` → `INSERT ... SELECT * FROM {표}_new` → `DROP {표}_new`, 모든 표를 한 트랜잭션에서. 시험: `tests/check_runner_swap.py`(시험 스키마, 의존 뷰 유지와 0행 실패 주입, 통과). 실제 실행(1,378만 행 복사 시간)은 [미측정]: 실행기 한 번이 `/tmp`에 셔플 143GB와 spill 57GB를 쓰는데(측정 32 `ops.engine_run`) 호스트 C: 여유가 11GB라 F-16이 다시 난다. 공간 회수 뒤 돌린다.
  - **처방 확인 (2026-11-13, 측정 38):** 실제 실행기 1회(run `local-1794576950933`)에서 교체가 성공했다. `wh.fact_commute` 뷰가 붙은 채 `stg.living_commute` 13,782,826행, 새 판정·격리 표 10행·31,935,758행이 한 트랜잭션으로 바뀌었다. 복사 시간: 경계 표 72.3초, 격리 표 257.8초, 커밋 포함 333.9초.
- 재발 방지: `tests/check_runner_swap.py`(의존 뷰가 붙은 표 교체, 0행 실패 주입). 실행기가 교체 시간을 매번 출력한다.
- 남은 것: 처방 A의 단점이 실측으로 드러났다. 교체 334초 동안 `TRUNCATE` 잠금으로 이 표와 `wh.fact_commute`를 읽는 쿼리(wh_load, Metabase 카드)가 기다린다. 생활이동은 월 1회라 그대로 둔다. 격리 표가 커지면 F-13 선택지 C나 격리 표만 파티션 교체를 다시 본다. 결정 90 문구("이름을 바꾼다")는 "비우고 복사한다"로 고친다(이번에 고침).
- 상태: **처방 확인.**

## F-14. S6b 재측정 도구가 "고친 파일" 목록을 빈칸으로 찍음: 새 파일이 지운 컴파일 결과와 이름 바꾸기로 짝지어짐
- 태그: 측정 도구
- 언제·어디: 2026-11-13, design_remaining_work.md 3.8, `measure/s6b_sql.py` 37~41줄
- 증상: `python -m measure.s6b_sql` 출력의 "고친 파일(더한 줄, 지운 줄):" 아래가 비어 있다. 종료 코드 0, 빌드와 비교는 정상.
- 재현: 같은 비교를 따로 3회 돌림. 새 파일이 `x` 한 줄이면 `1 0 /{dev/null => .../models/mart/zz.sql}`로 잡힌다(2/2). 새 파일이 `commute_od.sql` 사본에 가까우면 잡히지 않는다(1/1).
- 가설:
  - H1 git이 컨테이너에 없다 → `/usr/bin/git` 있음, 다른 비교는 줄이 나옴 → 지움.
  - H2 `git diff --no-index`의 이름 바꾸기 찾기가 새 파일을 원본 쪽에만 있는 `target/compiled/.../commute_od.sql`과 짝지었다 → 원본 쪽(`/opt/project/warehouse/dbt`)은 `target/`을 그대로 두고 사본만 `target/`을 뺀다. 출력 줄이 `8 7 /{opt/project/warehouse/dbt/target/compiled/seoul_dong/models/mart/commute_od.sql => tmp/.../dbt/models/mart/commute_od_evening.sql}`이고, 도구는 `/target/`이 든 줄을 거른다 → 남김.
- 원인: 두 쪽 폴더의 범위가 다르다(원본 쪽에 `target/`, `logs/`가 남음). 비슷한 내용의 새 파일은 "지워진 컴파일 결과의 이름 바꾸기"로 보고되고, 그 줄이 거르는 조건에 걸린다. S6a 도구(측정 25)는 기존 파일만 고쳐서 드러나지 않았다.
- 결과: 측정값 "고친 파일과 줄 수"를 도구가 내지 못했다. 손으로 셌다(측정 34): 새 파일 1개(`commute_od.sql` 51줄 + 머리 주석 1줄 = 52줄, 그중 범위 조건 1줄만 다름).
- 처방 선택지: A. 원본 쪽도 `target/`, `logs/`를 뺀 사본으로 비교 / B. `--no-renames` 옵션 추가. 둘 다 한 줄 고침.
- 처방과 결과 (2026-11-13~14, handoff_finish.md 4절 5번 "측정 도구를 고치고"): A. 원인(두 쪽 범위가 다름)을 없애는 쪽이다. B는 짝짓기만 끄고 원본 쪽 `target/` 차이는 그대로 남아 거르는 조건이 계속 필요하다. `measure/s6b_sql.py`가 원본도 같은 `ignore_patterns("target", "logs")`로 사본을 떠 두 사본을 비교하고, `/target/` 거르기를 뺐다. 재측정(2026-11-14 00:07~00:09, 1회): 출력 `+52 -0  models/mart/commute_od_evening.sql`(측정 34에서 손으로 센 52줄과 같음), `dbt build` 종료 코드 0, 98.9초, PASS 123, 기존 상품 4개 지금 mart와 두 방향 차이 0, `commute_od_evening` 142,777행. 빌드 시간은 측정 34(221.1초)보다 짧다(측정 40의 F-4 인덱스 뒤).
- 재발 방지: 두 쪽을 같은 규칙으로 뜬 사본끼리 비교한다(도구 주석).
- 상태: **처방함, 확인됨.**

## F-15. DuckDB 실행기(3.9)를 만들며 난 도구 오류 세 건: 프로파일 설정 이름, 방언 바꾸기 정규식, 프로파일 덮어쓰기
- 태그: 측정 도구
- 언제·어디: 2026-11-13, design_remaining_work.md 3.9, `measure/duckdb_sql.py`(이번에 새로 만든 도구). DuckDB 2.0.0, Airflow 컨테이너
- 증상과 원인 (각 1회):
  1. `PRAGMA custom_profiling_settings` → `Catalog Error: unrecognized configuration parameter`. DuckDB 1.x의 설정 이름을 썼다. 2.0.0은 `tracked_metrics`이고, 기본으로 켜진 지표(`system.peak_buffer_memory`, `system.peak_temp_dir_size`, `query.total_time`)로 충분해 설정을 뺐다. 지표 키 이름도 1.x(`system_peak_buffer_memory`)와 달라 2.0.0 JSON 모양(`system` 아래)으로 읽게 고쳤다.
  2. RLIKE를 `regexp_matches`로 바꾸는 정규식 `(\S+) RLIKE`가 `IF(m.avg_minutes_raw RLIKE ...`에서 `IF(m.avg_minutes_raw` 전체를 잡아 `regexp_matches(IF(m.avg_minutes_raw, ...)`를 만들었다(`Parser Error: syntax error at or near ","`). 앞 글자 범위를 `[\w.]+`로 좁혔다. 덧붙여 Git Bash heredoc 안의 파이썬 문자열에서 `\1`이 제어 문자(코드 1)로 바뀌어 한 번 더 같은 오류가 났다(Write 도구가 아닌 셸로 파일을 고친 탓).
  3. 문장마다 프로파일 파일 경로를 바꾸고 그대로 두어, 다음 문장(관문 결과 출력 `SELECT * FROM batch_status`)이 같은 파일을 덮어썼다. 지표 행(시간, 버퍼 메모리, 임시 폴더 크기)은 덮어쓰기 전에 읽어 남았지만 관문 계산의 연산자별 계획은 잃었다(run `duckdb-a29f84eb52ab`). 문장이 끝나면 경로를 `setup.json`으로 돌리게 고쳤다.
- 처방과 결과: 셋 다 고쳤다. 사용자 지시("오류는 기록하고 해결하지 말 것")의 대상은 설계대로 만든 파이프라인의 오류로 보고, 아직 한 번도 돌지 않은 새 측정 도구의 구현 오류는 고치며 진행했다(판단 기록 73).
- 재발 방지: 없음. 셸 heredoc으로 백슬래시가 든 코드를 고치지 않는다(Write/Edit 도구를 쓴다).

## F-16. DuckDB 실행(3.9) 중 Docker VM 전체가 멈춤: 엔진, PostgreSQL, Airflow가 함께 응답 없음
- 태그: 메모리, 분포(자원 경합), 디스크. 원인 잠정(메모리 → 스왑 소진이 가장 유력, 2026-11-13 정정), 처방 결정 101
- 언제·어디: 2026-11-13, design_remaining_work.md 3.9. `measure/duckdb_sql.py spark_sql/living --show batch_status --compare`(방언 고침 2곳 뒤 세 번째 실행), Airflow 컨테이너 안 DuckDB 2.0.0. 18:43 KST 시작. 설정은 DuckDB 기본값(threads 16, memory_limit 24.7GiB = 컨테이너가 보는 30.9GiB의 80%, preserve_insertion_order true), 임시 폴더 `/tmp/duckdb-tmp`(컨테이너 안).
- 증상 (시각 KST):
  - 18:54:59 Docker VM 디스크 파일 `C:\Users\dudfo\AppData\Local\Docker\wsl\disk\docker_data.vhdx`(669.0GB)의 마지막 쓰기. 19:28에도 같다.
  - 18:56:15 VM 쪽 로그(`Docker\log\vm\init.log`)의 마지막 줄.
  - 18:57:18 호스트 백엔드 로그에 `still dialing 192.168.65.7:2376 ... no route to host`가 처음 나오고 19:28까지 이어진다. `docker version`, `docker exec`, `docker stats`는 500 오류 또는 30초 제한 초과(19:09~19:28 폴링 10회 모두 응답 없음).
  - 19:04 사용자가 Docker Desktop 화면에서 본 값: 컨테이너 CPU 1288%, 메모리 22.98GB, 전체 칸 "0 CPUs available", "0B". 이 값이 그 시각에 새로 읽힌 것인지 마지막으로 받은 값인지는 모른다 [확인 필요].
  - 19:11 `wsl -d docker-desktop -e sh -c ...` 60초 안에 응답 없음.
  - 19:26 호스트 전체 CPU 24.7%(30초 평균), Hyper-V 가상 CPU 게스트 실행 13.3%(코어 약 2개분). 호스트 디스크 읽기 0.02MB/s, 쓰기 0.39MB/s, 디스크 시간 3%. vmmemWSL 작업 집합 23.14GB(18:5x에는 30.45GB). **호스트 C: 여유 5.9GB**(사용 1,900.2GB).
  - 19:28 PostgreSQL(localhost:5433)은 TCP 연결은 되지만 쿼리 연결이 30초 제한 초과. Airflow API(localhost:8080) 20초 제한 초과.
  - DuckDB 실행 출력은 파이썬이 블록 버퍼로 잡고 있어 아직 한 줄도 오지 않았다(로그 0바이트). 종료 코드 없음.
- 같은 날 앞선 DuckDB 실행(run `duckdb-a29f84eb52ab`, 18:28:42 시작, 관문까지 860.8초 뒤 `collect_list` 방언 오류로 끝남, `ops.engine_run`): 관문 계산 한 문장의 최대 버퍼 메모리 26.66GB, **임시 폴더 최대 150.09GB**, 프로세스 최대 RSS 27.03GB. 그때는 `docker exec`가 응답했다.
- 재현: 1회(이번 실행). 앞선 실행(같은 설정, 관문까지)은 멈추지 않았다.
- 가설:
  - H1 DuckDB 기본 스레드 수 = 코어 16개라 CPU를 다 잡아 엔진이 굶었다 → 19:04 1288%(약 13코어)는 맞는 모양이다. 그런데 19:26 가상 CPU는 13.3%(약 2코어)로 내려갔는데도 엔진·DB·WSL 셸이 모두 계속 응답하지 않는다. CPU 경합만으로는 CPU가 풀린 뒤의 멈춤을 설명하지 못한다 → 주원인에서 지움(초반 지연의 한 몫일 수는 있음).
  - H2 기본 memory_limit(보이는 메모리의 80%, 24.7GiB)가 같은 VM의 PostgreSQL, Airflow, 페이지 캐시를 계산에 넣지 않아 VM 메모리가 바닥났다 → 앞선 실행의 RSS 27.03GB에 PostgreSQL과 Airflow를 더하면 VM 상한(30.9GiB)에 닿는다. 18:5x vmmemWSL 30.45GB. 하지만 메모리만 바닥났다면 커널 OOM killer가 프로세스를 죽이고 VM이 돌아와야 하는데 30분 넘게 그대로다. OOMKilled 여부는 엔진이 응답해야 볼 수 있다 → 남김(확인 못 함).
  - H3 흘림(spill)이 같은 VM 디스크로 가고, 그 VM 디스크는 호스트 C:에서 늘어나는 VHDX 파일이라 C:가 차서 VM의 쓰기가 막혔다 → 컨테이너 안 `df /tmp`는 886G 여유로 보였지만(VHDX의 가상 크기) 실제 한계는 호스트 C: 여유다. 앞선 실행이 임시 폴더를 150GB까지 썼고, Spark 실행 두 번도 디스크 흘림이 각 약 57GB였다(측정 32). 지금 C: 여유 5.9GB, VHDX 마지막 쓰기 18:54:59, 그 뒤 VM 로그와 엔진이 1~2분 안에 차례로 멈췄고 디스크 입출력이 거의 0이다. 쓰기가 막히면 디스크를 쓰는 모든 프로세스(DuckDB, PostgreSQL, 엔진, 로그)가 함께 멈추고 OOM처럼 풀리지도 않는다 → **남김(가장 유력)**. 호스트 C:가 언제 얼마였는지의 기록은 없다 [확인 필요].
  - H4 DuckDB 실행이 둘 겹쳤다 → 호스트에서 띄운 실행은 하나뿐이다(18:43). 앞선 실행 둘은 0.7초 만에 끝났고(방언 오류), `duckdb-a29f84eb52ab`는 18:43:03에 끝났다(시작 + 860.8초). 18:43 확인 때 `/tmp/duckdb-tmp`는 없었다(앞선 실행이 지움). 컨테이너 안 프로세스 목록은 엔진이 응답하지 않아 보지 못했다 → 호스트 쪽 근거로는 겹침 없음, 컨테이너 안은 [확인 필요].
  - H5 PC 자체 문제(하드웨어, Windows 업데이트) → 멈춘 시각이 DuckDB가 크게 흘리는 실행 도중이고, 같은 실행이 없던 낮에는 같은 VM이 문제없었다 → 지움.
- 원인 (잠정): 워크로드가 공유 자원을 고갈시켰다. DuckDB 기본 설정은 "이 기계를 혼자 쓴다"고 가정한다(스레드 = 코어 수, 메모리 = 보이는 것의 80%, 흘림 = 보이는 디스크 여유). 같은 VM에 PostgreSQL과 Airflow가 있고, VM의 디스크는 호스트 C:를 나눠 쓰는 늘어나는 파일이라 세 가정이 모두 틀린다. 그중 디스크(H3)는 막히면 스스로 풀리지 않아 VM 전체를 멈춘 것으로 본다. 메모리(H2)가 함께 걸렸는지는 엔진이 돌아와야 확인된다.
- 결과: DuckDB 결과와 `stg.living_commute` 비교는 나오지 않았다. 동등 증명(guarantees.md 2.2)은 아직 없다. Spark 경계 표와 상품은 이 실행이 쓰지 않으므로 그대로일 것이다(DB 응답이 없어 확인 못 함).
- 처방 선택지 (사용자가 정함, 기록만):
  | 선택지 | 장점 | 단점 |
  |---|---|---|
  | A. DuckDB에 `threads`(예: 12), `memory_limit`(예: 16GB), `max_temp_directory_size`를 명시 | 이웃 서비스 몫을 남긴다. 흘림이 상한에서 오류로 끝나 VM을 멈추지 않는다 | 흘림 상한에 걸리면 실행이 실패한다(드러나는 실패다) |
  | B. 흘림 위치를 여유를 아는 곳으로 | VM 디스크가 차지 않는다 | 바인드 마운트(Windows 파일 시스템) 쓰기는 느리다 |
  | C. 흘림을 줄이도록 SQL을 고침(F-9와 같은 원인: 원본 여러 번 읽기, 글자 min/max 묶기) | 두 엔진 모두 이득 | 같은 SQL을 두 엔진으로 비교한다는 3.9의 전제가 바뀐다 |
  | D. Docker Desktop 가상 디스크 상한을 호스트 여유보다 작게 | VM이 디스크 부족을 스스로 알아 프로세스 하나가 오류로 끝난다 | Docker 설정 변경(사용자 몫) |
- **재시작 뒤 (사용자가 `wsl --shutdown`, 2026-11-13 19:3x)**:
  - 19:35~19:37 ~~WSL VM은 새로 떴지만~~ **(정정, 아래 "원인 다시 보기") 새 VM이 아니었다.** vmmemWSL 작업 집합은 2.81GB(멈추기 전 23~30GB)로 줄었지만 Docker 엔진은 계속 `no route to host`이고 `docker ps`, `wsl -d docker-desktop -e sh`가 40초 안에 응답하지 않는다. `docker_data.vhdx` 마지막 쓰기는 여전히 18:54:59다. 새 VM도 데이터 디스크에 한 번도 쓰지 못했다. C: 여유 5.9GB 그대로.
  - 해석(가설): ext4 데이터 디스크를 붙이려면 저널 복구 등 쓰기가 필요하다. 호스트 C:에 VHDX가 늘어날 자리가 없어 새 VM도 같은 자리에서 멈춘다. H3과 같은 원인이다. 확인은 C: 공간을 비운 뒤 엔진이 뜨는지로 한다 [확인 필요]. **(정정) 이 해석은 "새 VM"을 전제로 했는데 그 전제가 틀렸다. 그 시각은 멈춘 옛 VM이 그대로였다.**
  - 증거를 둘 "호스트 C:가 아닌 곳"은 이 PC에 없다. 드라이브가 C: 하나다(`Get-PSDrive`).
  - C: 사용 중 확인한 것(읽기만): `docker_data.vhdx` 669.0GB, 레포 `lake` 18.2GB, `lakehouse` 11.6GB, `AppData\Local\Temp` 11.5GB. 나머지 약 1.19TB는 이 프로젝트 밖이다.
- **한 단계 더 내려간 확인 (VM 없이 할 수 있는 것)**:
  - Docker Desktop 설정 `settings-store.json`의 `DiskSizeMiB = 1048576`(가상 디스크 상한 1TB). 컨테이너 안 `df -h /tmp`가 1007G 중 886G 여유로 보고한 것(측정 시각 17:5x)이 이 값이다. VHDX는 쓸 때 늘어나는 파일이고(`fsutil sparse queryflag` → sparse 아님) 실제 한계는 호스트 C: 여유인데, VM 안에서는 보이지 않는다.
  - DuckDB 2.0.0 `max_temp_directory_size` 기본값은 `90% of available disk space`(호스트 `.venv`의 DuckDB 2.0.0에서 `current_setting`으로 확인). 임시 폴더가 있는 파일 시스템의 여유를 기준으로 하므로, 컨테이너 안에서는 886G의 90%(약 797GB)가 상한이 된다. 실제 C: 여유보다 훨씬 큰 거짓 용량을 기준으로 안전 상한이 계산되었다. 컨테이너 안에서 이 값을 직접 읽는 것은 VM이 돌아온 뒤 한다 [확인 필요].
  - Spark: `spark.local.dir`을 설정한 곳이 없다(`transform/`, `dags/`, `docker-compose.yml` 검색, `SPARK_LOCAL_DIRS`도 없음). Spark 기본값 `/tmp`라 셔플·spill 파일(`/tmp/blockmgr-*`)이 DuckDB 임시 폴더와 같은 컨테이너 쓰기 층, 곧 같은 VHDX에 쓰인다. `docker-compose.yml`에 tmpfs나 자원 상한(memory, cpus)도 없다.
  - F-9의 "220GB"는 디스크에 쓴 양이 아니다. Spark 지표 `Memory Bytes Spilled`(흘리기 전 메모리 안 크기, 역직렬화 기준)이고, 같은 단계의 `Disk Bytes Spilled`(실제로 쓴 직렬화 크기)는 각 24GB다. 실제 디스크에는 spill에 더해 셔플 쓰기(36GB × 3)도 같은 곳에 쓰이며, 첫 실행 중 `/tmp/blockmgr-*`가 129GB까지 찼다(F-9 본문). F-9 제목의 "220GB를 흘림"은 메모리 기준 값이라 오해의 여지가 있다.
- **가상 디스크 공간 회수 방법 (조사만, 실행하지 않음, 출처는 커뮤니티 글이라 [확인 필요])**: 공통 순서는 (1) VM 안에서 불필요한 파일 지우기(`/tmp/duckdb-tmp` 등) (2) Docker Desktop 종료와 `wsl --shutdown` (3) VHDX 압축. 압축 방법은 `diskpart`의 `select vdisk file=...` → `attach vdisk readonly` → `compact vdisk` → `detach vdisk`(Windows Home 가능, 관리자 권한 필요), Hyper-V 모듈의 `Optimize-VHD -Mode Full`(이 PC에는 명령이 없음, 관리자 아님), WSL 2.x의 `wsl --manage <배포판> --set-sparse true`(이 PC WSL 2.3.26, Docker 데이터 디스크에 쓸 수 있는지 모름), 이미지 `docker/desktop-reclaim-space`(TRIM). 압축 중 VHDX 손상 사례가 있어 백업을 권하는 글이 있다. 그런데 백업할 자리(669GB)가 C:에 없다.
- 19:46 호스트에서 띄운 `docker exec`(DuckDB 실행)가 끝났다. 남은 출력은 `exec/<id>/json` 요청의 500 오류 한 줄뿐이다. 실행 결과를 담은 출력은 파이썬이 블록 버퍼로 잡고 있다가 VM과 함께 사라져 한 줄도 오지 않았다. 종료 코드는 엔진이 응답하지 않아 알 수 없다 [확인 필요: 엔진이 돌아오면 `docker inspect`와 `ops.engine_run`으로]. 재발 방지 후보: 측정 도구를 `python -u`(버퍼 없음)로 돌린다.
- **복구 뒤 증거 (2026-11-13 19:56~20:05, 사용자가 Docker를 다시 켠 뒤. 컨테이너는 다시 만들지 않고 `start`만 했다)**. 원본 파일은 레포 밖 `C:\Users\dudfo\pg_backup_20261113\f16_evidence\`(이 PC에 C: 말고 다른 드라이브는 없다):
  - 켜기 전 `docker inspect`: `new_proj-db-1`, `new_proj-airflow-1`, `new_proj-metabase-1` 모두 `OOMKilled=false`, 종료 코드 255, 끝난 시각 19:55:47 KST(VM 재시작 때 엔진이 정리한 시각). 재시작 정책 no, 메모리 상한 0(없음), tmpfs 없음. 엔진이 본 VM: 메모리 33,189,863,424B(30.9GiB), CPU 16. **OOM으로 죽은 기록은 없다.** VM이 통째로 멈춰 커널 OOM killer가 돌 기회가 있었는지는 이것만으로 알 수 없다 → H2는 "확인된 원인 아님"으로 남긴다.
  - Airflow 메타데이터(켜기 전): 스케줄러 마지막 하트비트 18:54:29, DAG 처리기 18:54:30, 트리거러 18:54:33. 그 시각에 돌던 태스크 0개. 18:55~20:00 사이 예정 실행 없음(다음 예정 11-16 13:00, 생활이동 DAG는 멈춤 상태) → **놓친 실행 없음**. 응답 없던 구간은 스케줄러 기준 18:54:29 ~ 20:03(다시 켬).
  - PostgreSQL 로그: 마지막 정상 로그가 18:53:35 KST(정기 체크포인트 완료). 다음 정기 체크포인트(18:58:20 예정)가 없다. 다시 켤 때 `database system was not properly shut down; automatic recovery in progress`, `redo starts at 13/91A12288`, `redo done at 13/91A3E1E8`(약 175kB), 끝 체크포인트 뒤 `ready to accept connections`(19:57:04). 복구 뒤 행 수: `stg.living_commute` 13,782,826, `stg.living_commute_new` 13,782,826, `mart.commute_od` 141,411, `mart.dong_profile` 426, `lineage.run` 최대 32. 멈추기 전과 같다.
  - DuckDB 세 번째 실행(run `duckdb-4ae32d9bc64a`): 프로파일 폴더는 비어 있고(첫 문장 관문 계산을 끝내지 못함) `ops.engine_run`에도 행이 없다(기록은 실행이 끝날 때 쓰는데 VM과 함께 프로세스가 사라짐). 남은 임시 폴더 `/tmp/duckdb-tmp`: 파일 23개, **134,544,003,072B(125.3GiB)**, 프로세스 번호 29903. 파일 생성 18:44:29, 마지막 수정 18:54:36. 18:44~18:54 약 10분 동안 125GiB를 썼다(평균 약 210MiB/s). 마지막 수정 18:54:36은 VHDX 마지막 쓰기 18:54:59, 스케줄러 마지막 하트비트 18:54:29와 같은 분이다.
  - 앞선 실행(`duckdb-a29f84eb52ab`)의 `ops.engine_run` 2행은 그대로다(관문 문장: 최대 버퍼 26.66GB, 임시 폴더 최대 150.09GB).
- **임시 폴더 삭제 전후 (20:04)**: VM 안 `df /`는 사용 209.4GB → 74.9GB(134.5GB 비움). **호스트 C: 여유 10.76GB → 10.76GB, `docker_data.vhdx` 668.99GB → 668.99GB(변화 없음).** VM 안에서 지워도 늘어난 VHDX는 줄지 않는다. VM 안 실제 사용 74.9GB와 VHDX 669GB의 차이(약 594GB)가 압축으로 돌려받을 수 있는 최대치로 보이나 실행하지 않았다 [확인 필요]. 회수 방법은 위 조사(diskpart compact 등)대로 사용자에게 묻는다.
- **원인 확인 한 단계 더 (컨테이너 안에서 실측, 20:04)**:
  - 컨테이너 안 DuckDB 2.0.0: `max_temp_directory_size = 90% of available disk space`, `memory_limit = 24.7 GiB`, `threads = 16`. 같은 순간 `/tmp`가 보는 디스크는 전체 1006.9GiB, 여유 760.6GiB라 상한은 약 684.6GiB가 된다. 실제로 쓸 수 있던 것은 호스트 C: 여유(그 시각 10.76GB)다. **안전 상한이 가상 디스크의 거짓 용량으로 계산되어, 디스크 부족을 오류로 알리지 못하고 VM 전체를 멈췄다.** 18:43 시작 때 C: 여유는 기록이 없다 [확인 필요]. 다만 세 번째 실행이 125.3GiB를 쓰고 VHDX가 669GB에서 멈춘 것을 보면, 시작 때 C: 여유는 125GiB 남짓이었고 이 실행이 그것을 다 썼다고 본다(추론).
  - Spark: 컨테이너에 `SPARK_LOCAL_DIRS` 없음, `spark.local.dir` 설정 없음, `/tmp`는 별도 마운트가 아닌 컨테이너 쓰기 층(overlay). 그래서 Spark 셔플·spill 파일도 같은 VHDX에 쓰이고, F-9 첫 실행의 `/tmp/blockmgr-*` 최대 129GB도 같은 거짓 여유 아래에서 썼다. 그때 멈추지 않은 것은 C:에 그만큼 자리가 있었기 때문이다(추론, 그때 C: 여유 기록 없음).
- **가설 정리 (복구 뒤 증거 반영)** ~~H3(디스크)이 원인으로 확인됐다.~~ **(정정, 아래 "원인 다시 보기": 이 단락의 결론은 근거가 약하다)** 쓰기 시각(임시 파일 18:54:36, VHDX 18:54:59)과 멈춘 시각(하트비트 18:54:29~33, PostgreSQL 마지막 로그 18:53:35, VM 로그 18:56:15)이 같은 분이고, ~~C:가 다시 몇 GB 비자 VM이 떴다.~~ H2(메모리)는 OOM 기록이 없어 원인으로 확인되지 않았다. H1(CPU)은 주원인에서 지운 그대로다. H4(실행 겹침)는 임시 파일이 한 프로세스(29903)의 것뿐이고 프로파일 폴더 순서로도 겹침이 없다 → 지움.
- **백업 (사용자 지시 1번)**: `pg_dump -Fc`로 DB 4개(seoul 1,112,711,738B, metabase 1,320,786B, airflow 580,714B, iceberg 3,461B)와 `pg_dumpall --globals-only`(1,069B)를 `C:\Users\dudfo\pg_backup_20261113\`에 남겼다(157초). 먼저 디스크에 쓰지 않고 바이트만 세어 크기를 확인했다(1.06GB, C: 여유 11.8GB). 각 파일은 `pg_restore --list`로 읽힌다(seoul TABLE DATA 64개). `SHA256SUMS` 함께 둠. 백업 뒤 C: 여유 10.8GB. `globals.sql`에는 역할의 비밀번호 해시가 들어 있어 레포 밖에 두었다.
- **원인 다시 보기 (2026-11-13 20:3x, Docker 로그와 Windows 이벤트로 다시 본 것, `docs/resource_guard.md` 0절)**. 앞의 "가설 정리"를 아래로 고친다.
  - 근거 파일: Docker 백엔드 로그 `%LOCALAPPDATA%\Docker\log\host\com.docker.backend.exe.log.20261113-190108.347`, `...20261113-195200.091`(10분 남짓마다 `com.docker.backend.exe.processes] parsed {vmmemWSL <PID> <CPU 시간> ... <생성 시각>}` 줄), VM 로그 `%LOCALAPPDATA%\Docker\log\vm\init.log`(지금은 재시작 뒤 내용으로 덮여 멈춘 시각 줄은 남아 있지 않다), 사용자가 PowerShell로 확인한 Windows 시스템·Hyper-V 이벤트 로그, WSL 스왑 파일 `%LOCALAPPDATA%\Temp\<GUID>\swap.vhdx`.
  - 정정 (1) "19:35 VM이 새로 떴다"는 틀렸다. 백엔드 로그의 vmmemWSL PID 28320(생성 20261113095614 = 09:56:14 KST)이 멈춘 뒤에도 그대로 찍혔다. Claude가 다시 본 마지막 줄은 10:50:12Z(19:50:12 KST)이고 PID와 생성 시각이 같다(사용자 확인은 19:46까지). 실제 재부팅은 19:55:46 무렵이다: 새 VM의 `swap.vhdx`(다른 GUID 폴더, 37,748,736B) 마지막 쓰기 19:55:43, 컨테이너 종료 시각 19:55:47(위 `docker inspect`). 19:45 무렵 시작 실패는 멈춘 옛 VM과 WSL 서비스가 응답하지 않아서였다.
  - 정정 (2) "C:가 다시 몇 GB 비자 VM이 떴다"는 로그가 뒷받침하지 않는다. VM이 다시 뜬 것은 19:55:46 재부팅이고, 그 사이 C: 여유와 VM 기동을 잇는 기록은 없다.
  - 정정 (3) 원인 순위:
    | 순위 | 후보 | 근거 | 판단 |
    |---|---|---|---|
    | 1 | 메모리 → 스왑 소진 → VM 전체 정체 (H2 확장) | 옛 VM의 `swap.vhdx`가 설정 최대치 8.00GB(8,594,128,896B)까지 늘었고 마지막 쓰기 18:45:05(DuckDB 세 번째 실행 시작 2분 뒤). 새 VM 것은 0.04GB. DuckDB 기본 `memory_limit` 24.7GiB, 앞선 실행 RSS 27.03GB, 같은 VM에 PostgreSQL·Airflow·Metabase(JVM). 멈춘 동안 VM이 코어 약 2.3개를 쓰고(백엔드 로그의 vmmemWSL CPU 시간 증가) 네트워크와 디스크 쓰기만 끊겼다 | **가장 유력.** 커널 로그가 VM과 함께 사라져 확정은 아니다 |
    | 2 | 호스트 C: 부족으로 VHDX가 못 늘어남 (H3) | Windows 시스템 로그에 디스크 부족·가상 디스크 오류 없음, Hyper-V 일시 정지 기록 없음. 19:26 C: 여유 5.9GB(디스크가 찼다면 0 근처였어야 함). VHDX 669GB 안에서 실제 사용은 209GB라 spill이 이미 할당된 블록을 다시 썼을 수 있다 | **약해짐.** 함께 작용했을 가능성은 남음 |
    | 3 | CPU 독점 (H1) | 19:26 가상 CPU 13.3%로 내려간 뒤에도 응답 없음 | 주원인 아님(그대로) |
  - 정정 (4) `OOMKilled=false`는 약한 근거다. 컨테이너에 메모리 상한이 없으면 cgroup OOM은 일어나지 않고, VM 전체가 고갈되면 커널 OOM killer보다 스왑 쓰기와 회수(reclaim)로 정체가 먼저 온다. 그래서 "OOM 기록 없음"이 "메모리 아님"을 뜻하지 않는다.
  - 확정하지 못한 이유: VM 커널 로그(`dmesg`)와 측정 도구 출력(블록 버퍼)이 VM과 함께 사라졌다. 다음에는 L0 관측(5초마다 `/proc/meminfo`, `vmstat`, `df`를 바인드 마운트 파일에, `python -u`)으로 판정한다(resource_guard.md 1절).
  - 디스크 쪽 사실(컨테이너 `/tmp`가 1TB 가상 디스크를 보고 DuckDB·Spark 안전 상한이 거짓 용량으로 계산된다)은 그대로 맞다. 다만 이번 멈춤의 주원인으로는 확인되지 않았다.
  - 처방: 결정 101(Airflow 컨테이너 상한, k8s 이식 때 작업 컨테이너 분리)과 resource_guard.md 4절 순서.
- 남은 것: 처방(A~D) 선택과 DuckDB 재실행(사용자 확인 뒤, 흘림 위치·max_temp_directory_size·memory_limit 명시, threads 12, 한 번에 하나). VHDX 공간 회수 방법 선택. 재실행 때 공간이 모자라면 DuckDB가 오류로 바로 끝나는지도 측정한다.
- 상태 갱신 (2026-11-13, handoff_finish.md 4절 4번): **처방함, 확인됨.**
  - 고른 처방: 결정 101(L2 컨테이너 상한 = 선택지 A의 메모리 쪽), L3(DuckDB `memory_limit` 16GiB·`threads`·`max_temp_directory_size` = 호스트 C: 여유 − 50GiB, Spark `spark.local.dir`과 시작 전 여유 점검), L4(공간 회수, 측정 36), L5(heavy 잠금과 pool), L0(vm_watch). 선택지 B(흘림 위치 바꾸기)와 D(Docker 가상 디스크 상한)는 하지 않았다. L1(`.wslconfig`)은 제안만(resource_guard.md 6절).
  - 확인: [유도] 시험(F-17, guarantees 2.7 ②), 그리고 유도가 아닌 실제 부하에서 Spark 실행기 3,201초 완주(측정 38)와 같은 DuckDB SQL 3회가 VM을 멈추지 않고 오류로 끝남(측정 39, F-19). F-16 때와 같은 SQL·같은 엔진에서 결과가 "VM 전체 멈춤"에서 "그 실행 하나의 오류"로 바뀌었다.
  - 남은 것: 원인 1순위(메모리 → 스왑 소진)는 여전히 추정이다(그때의 커널 로그가 없다). 이번 실행들에서는 스왑을 전혀 쓰지 않았다(컨테이너 스왑 금지). 실행마다 가상 디스크가 다시 커지는 것(측정 38·39에서 C: 여유 641GB → 177GB)은 L4를 주기적으로 해야 한다는 뜻이다.

## F-17. [유도] 자원 상한 시험: 메모리를 넘치게 하면 그 컨테이너만 OOM으로 죽고, 임시 공간을 넘치게 하면 DuckDB가 오류로 끝남
- 태그: 메모리, 디스크, 분포(자원 경합). 일부러 일으킨 오류(F-16 처방 확인, resource_guard.md 4절 5번)
- 언제·어디: 2026-11-13 21:4x~22:0x. 측정 36(공간 회수), 측정 37(예산, L2 상한 적용) 뒤. `measure/induce_guard.py`
- 어떻게 일으켰나:
  - 메모리: `seoul-airflow:3.3.2` 이미지로 일회용 컨테이너(`--memory 2g --memory-swap 2g --cpus 4 --network none`). DuckDB 2.0.0에 `memory_limit = 12GB`(컨테이너 상한보다 크게), `threads 4`, `SELECT count(*) FROM (SELECT i % 300000000 AS k, list(i) FROM range(600000000) t(i) GROUP BY k)`.
  - 디스크: Airflow 컨테이너 안 DuckDB에 `memory_limit 1GB`, `threads 4`, `max_temp_directory_size 1GiB`, 2억 행을 해시로 정렬해 임시 표로.
- 증상(기대한 것):
  - 메모리: 컨테이너가 종료 코드 137, `OOMKilled=true`로 끝남(3회, 210·192·234초). 그동안 PostgreSQL 탐침 실패 0(2·3회차 103회, 169회), 다른 컨테이너 4개 그대로, Airflow 컨테이너의 vm_watch는 줄을 빠뜨리지 않음(1회차 46줄, 5초 간격 그대로, VM MemAvailable 최소 24.49GiB, 스왑 0).
  - 디스크: 6.8~7.2초 만에 `Out of Memory Error: failed to offload data block of size 64.0 KiB (1.0 GiB/1.0 GiB used). This limit was set by the 'max_temp_directory_size' setting.` 임시 폴더는 남지 않음(0B).
- F-16과 비교: F-16은 상한이 없어(컨테이너 상한 0, DuckDB 임시 상한 = 가상 디스크 여유의 90%) 넘친 작업이 VM 전체를 1시간 멈췄다. 상한을 걸자 같은 종류의 넘침이 그 작업 하나의 드러나는 실패(종료 코드 137 또는 DuckDB 오류)로 끝났다.
- 관찰: 메모리 쪽은 상한에 닿고 OOM까지 3~4분 걸렸다. 그동안 무엇을 했는지(페이지 회수 등)는 재지 않았다 [확인 필요]. DuckDB는 상한 뒤 기본 `memory_limit`를 cgroup 상한의 80%(22g → 17.5GiB)로 스스로 잡는다(측정 37). 이번 시험은 그 자동값을 일부러 무시했을 때의 마지막 방어선을 본 것이다.
- 한계: Airflow 컨테이너 자체를 넘치게 하지는 않았다(스케줄러가 함께 죽음). VM 전체 상한(L1 `.wslconfig`)은 사용자 파일이라 아직 없다. Spark에는 임시 폴더 상한이 없다.
- 처방과 결과: 처방 자체가 이 시험의 대상(결정 101, L2·L3). 증명은 guarantees.md 2.7.

## F-18. 푸시 뒤 CI가 실패: DB가 있어야 도는 교체 시험을 CI가 도는 `tests/test_*.py` 이름으로 둠
- 태그: 운영
- 언제·어디: 2026-11-13, 커밋 5d05f47 푸시(GitHub Actions run 43434778579, 13:34:09Z). `.github/workflows/ci.yml` "정제 규칙 단위 테스트" 단계 `for t in tests/test_*.py; do python -m "tests.$(basename "$t" .py)"; done`
- 증상: `tests.test_runner_swap`에서 `FileNotFoundError: ... home_in_seoul/.env`(`common.read_env`), 종료 코드 1. 앞선 시험 5개(test_boundary 등)는 통과. 직전 성공 run은 2026-11-04.
- 재현: CI 1/1. 이 PC에서는 `.env`와 DB가 있어 통과했다(F-13 처방 때 1회).
- 가설:
  - H1 교체 시험 자체가 틀렸다 → 같은 시험이 컨테이너에서 통과(실행기 교체 시간 출력 뒤에도 통과, 2026-11-13 22:3x) → 지움.
  - H2 CI는 DB와 `.env` 없이 `tests/test_*.py`를 모두 돌리는데, 새 시험이 DB에 접속한다 → CI 설정 주석 "DB·레이크 없이 도는 검사"와 시험 첫 줄(`read_env("DATABASE_URL")`) → 남김.
- 원인: 이 레포의 규칙은 DB가 필요한 확인을 `tests/check_*.py`(예: `check_lineage_mcp.py`, "DB와 lineage 회차가 있어야 한다(CI 밖)")로 두는 것이다. F-13 처방 때 그 규칙을 보지 않고 `test_` 이름을 붙였다. 이름이 곧 CI 포함 여부라 이름 실수가 CI 실패가 된다.
- 처방과 결과: `git mv tests/test_runner_swap.py tests/check_runner_swap.py`, 문서 참조 3곳 고침, 첫 줄에 "DB가 있어야 한다(CI 밖)". CI와 같은 반복문을 이 PC에서 돌려 `tests/test_*.py` 8개 모두 통과(DB 접속하는 것 없음: `psycopg`·`read_env`를 쓰는 시험은 `check_` 둘뿐). CI 재실행은 다음 푸시 때 [확인 필요].
- 재발 방지: 시험 파일을 만들 때 DB가 필요하면 `check_`로 둔다(이 항목). CI가 실패 알림을 보냈지만 이 세션은 푸시 뒤 CI를 보지 않았다. 푸시 뒤에는 `gh run list`로 결과를 본다.
- 상태: **처방함(커밋 전, 사용자 지시로 커밋 보류).**

## F-19. DuckDB 비교 실행(3.3)이 메모리 16GiB 상한에서 오류: 스레드 12개로는 흘리지 못하는 메모리가 상한을 넘고, 4개로는 임시 공간이 117GiB를 넘음
- 태그: 메모리, 디스크, 계산량·인덱스
- 언제·어디: 2026-11-13 23:41~23:58, handoff_finish.md 3.3, `measure/duckdb_sql.py`(측정 39). 측정 38 바로 뒤, 결정 101 상한과 L3 임시 상한이 걸린 상태
- 증상: 첫 문장 `CREATE TEMP TABLE batch_status AS ...`(관문 계산)에서 (1) threads 12: 218초 뒤 `OutOfMemoryException: could not allocate block of size 256.0 KiB (16.0 GiB/16.0 GiB used)` (2) 같은 설정에 `preserve_insertion_order=false`: 213초 뒤 같은 오류 (3) threads 4: 300초 뒤 `failed to offload data block ... (116.9 GiB/117.0 GiB used). This limit was set by the 'max_temp_directory_size' setting`. 세 번 모두 VM·PostgreSQL·다른 컨테이너는 계속 응답했다(스왑 0, OOM kill 0).
- 재현: (1) 1/1, (2) 1/1, (3) 1/1. 각 실행 뒤 호스트 C: 여유가 약 40GB씩 줄어(가상 디스크가 늘어남, 측정 36) 같은 설정의 반복은 하지 않았다.
- 가설:
  - H1 삽입 순서 보존(`preserve_insertion_order=true`)이 결과를 순서대로 모으느라 메모리를 붙잡는다(DuckDB 오류 메시지의 두 번째 권고) → (2)에서 끄고 같은 시점(213초)에 같은 오류 → **지움.**
  - H2 키 9개 묶기의 글자 `min/max` 집계 상태(가변 길이)는 디스크로 흘리지 못한다(Spark F-9 원인 2의 DuckDB판) → 한 달(202607, 1억 4,118만 행) 키 8개 묶기를 `memory_limit` 2GiB, threads 12로 글자 min/max와 숫자 min/max 두 가지로 돌림: 둘 다 성공(17.5초, 18.0초, 그룹 69,500,879). 단일 묶기는 상한 안에서 흘린다 → **약해짐**(10달 전체·키 9개로는 재지 않음).
  - H3 한 문장 안에서 큰 연산자 여러 개가 동시에 메모리를 잡는다. DuckDB 계획(`EXPLAIN`, 실행하지 않음)에 Iceberg 읽기가 여럿, 키 9개 Hash Group By(예상 2.8억 그룹)가 둘 이상, 그 결과를 다시 읽는 Hash Join이 한 문장에 있다(Spark F-9 원인 1과 같은 SQL 구조). 스레드마다 묶기 표의 일부를 메모리에 두므로 스레드 수에 비례한다 → (3) threads 4에서 메모리 오류가 사라지고 흘림으로 넘어갔다 → **남김(가장 유력).**
  - H4 흘릴 디스크가 모자라다 → (3)은 임시 상한(L3) 117GiB에서 끊겼다. F-16 전 상한 없는 실행은 관문에 150.09GB를 썼다 → 디스크 예산 쪽으로는 **확인**(threads 4 기준 117GiB로 부족).
- 원인: 관문 SQL이 원본 14억 행을 한 문장에서 여러 번 펼쳐 키 9개로 두 번 묶고(F-9 원인 1·2와 같은 구조), DuckDB는 이것을 스레드마다 나눠 동시에 처리한다. 스레드 12개에서는 흘리기 전 메모리에 남겨야 하는 몫이 16GiB를 넘고, 스레드 4개로 줄이면 메모리는 버티지만 흘림이 117GiB를 넘는다. 메모리와 임시 공간을 함께 줄이는 것은 엔진 설정이 아니라 SQL 구조(F-9)다.
- 처방 선택지:
  | 선택지 | 장점 | 단점 |
  |---|---|---|
  | A. 공간 회수 뒤 임시 상한을 키워(≥ 200GiB) threads 4로 재측정 | SQL 그대로라 결정 94의 "같은 SQL" 비교가 된다 | 사용자의 diskpart 압축이 필요(측정 36). 실행마다 가상 디스크가 다시 커진다 |
  | B. F-9 처방(관문을 한 번 읽기로, 숫자 min/max) 뒤 두 엔진을 다시 잰다 | 두 엔진 모두 메모리·임시가 준다. 비교 조건이 같다 | F-9 선택이 먼저다(사용자) |
  | C. 그대로 두고 측정 39를 결론으로 | 일이 없다. "예산 안에서 DuckDB는 끝내지 못함"도 결정 94의 답이다 | 자원이 충분할 때의 DuckDB 속도는 모른 채로 남는다 |
- 처방과 결과: 고르지 않았다(사용자). 기록만.
- 재발 방지: L3(임시 상한)와 L2(컨테이너 상한)가 이번에 실제 부하에서 동작했다(guarantees 2.7 덧붙임).
- 남은 것: H2를 10달·키 9개로 확인하지 않았다. threads 4의 실행 시간과 필요한 임시 공간 전체는 모른다 [미측정].
- 상태: **처방함(결정 103 B: F-9 처방 뒤 재측정), 예산 안에서 완주 못 함으로 결론.** 측정 43: threads 12는 652.2초, threads 4는 826.6초에 메모리 16GiB 상한 오류(처방 전 218.3초, 299.8초). 같은 예산에서 더 오래 버텼지만 끝내지 못했다. J-15에 따라 예산을 넘겨 재지 않는다. 더 재려면 예산을 늘리는 결정이 필요하다(사용자).
- 처방과 결과 (결정 110, 2026-11-14~15): (1) D1 진단(측정 49): 계산 컨테이너에서 같은 조건 656.0초에 같은 오류(재현). `duckdb_memory()` 5초 기록으로 남김의 [확인 필요]가 좁혀졌다: 키 9개 해시 집계(`HASH_TABLE`, 최대 14.01GiB, 임시 125GiB를 모두 이 태그가 흘림)가 두 물결로 두 번 돌고(뷰 `clean`이 `p`와 V8 조인에서 두 번 계산됨, H3과 맞음), 물결마다 흘리지 않는 `ALLOCATOR`가 쌓여 10.36GiB까지 남아 해시 집계의 몫을 빼앗았다. `ALLOCATOR`에 무엇이 쌓이는지는 [확인 필요]. (2) D2 배치 단위 처리(측정 50): 배치 하나는 약 69초에 16GiB 안에서 끝나고 `ALLOCATOR` 누적은 사라졌지만, 누적 임시 표가 같은 버퍼 풀을 차지해 4번째 배치에서 넘쳤고, 선택지 B는 컨테이너 상한 137로 끝났다(F-30). **결론: 결정 101·109 예산(DuckDB 16GiB, 컨테이너 18GiB) 안에서 DuckDB는 이 SQL을 아직 끝내지 못했다.** 결정 94의 답은 그대로다.

- 처방과 결과 (결정 111, 2026-11-15, 측정 51): 배치마다 새 프로세스 + Parquet 넘김으로 **16GiB 안에서 658.8초에 완주**, Spark 결과 세 표와 두 방향 `EXCEPT ALL` 0행. 상태를 "처방함(완주)"으로 바꾼다. 남은 것: 202601의 threads 12 넘침(F-31).

## F-20. 무거운 실행마다 호스트 C: 여유가 그 실행의 임시 파일 최대치만큼 줄고 돌아오지 않음: 가상 디스크가 VM 안에서 지운 공간을 호스트에 돌려주지 않음
- 태그: 디스크, 설계 가정(VM이 보는 용량과 호스트 실제 용량이 다름)
- 언제·어디: 2026-11-13 21:29 공간 회수(측정 36) 뒤부터 2026-11-14 10시까지. Docker Desktop WSL2 데이터 디스크 `docker_data.vhdx`(동적 VHDX, 가상 상한 1TB). 2026-11-14 사용자 질문("왜 지금 디스크가 부족한가")으로 정리.
- 증상: 호스트 C: 여유가 실행마다 줄었다. 실행이 끝나면 VM 안의 임시 파일은 지워졌는데(컨테이너 `/tmp` 333.6GB → 79.1GB, `/tmp/spark-local` 비워짐, 측정 38) 호스트 공간은 돌아오지 않았다.
  | 시점 | C: 여유 | 근거 |
  |---|---|---|
  | 2026-11-13 21:29 공간 회수 직후 | 649.31GB | 측정 36 |
  | 생활이동 Spark 실행기 1회 뒤 | 313.00GB | 측정 38 (컨테이너 `/tmp` 최대 333.6GB) |
  | DuckDB 1·2·3회차 시작 전 | 299.7 / 221.7 / 180.2GB | 측정 39 (회마다 임시 125~133GB) |
  | 2026-11-14 10:09 KST | 141GB | 연결 폴더 `df` (C: 1.9T 중 93% 사용) |
- 원인: 임시 파일이 지워져 호스트에 돌아가기까지는 다섯 층을 거친다. 그중 3층과 4층이 이 PC에서 끊겨 있다.
  | 층 | 누가 어떻게 지우나 | 이 PC에서 |
  |---|---|---|
  | 1 엔진 | Spark: `ContextCleaner`가 셔플 의존성 객체를 약한 참조로 들고 있다가, 드라이버 JVM GC가 그 객체를 치우면 셔플 파일을 지운다. 메모리 압박이 없으면 GC가 안 돌 수 있어 `spark.cleaner.periodicGC.interval`(기본 30분)마다 GC를 요청한다. 애플리케이션이 끝나면 local dir을 지운다. DuckDB: 흘린 블록은 필요 없어지면 지우고, 임시 폴더는 인스턴스가 끝날 때 지운다. | 동작함 (`/tmp/spark-local` 비워짐, F-17 디스크 시험 뒤 임시 폴더 0B) |
  | 2 파일 시스템(VM 안 ext4) | 지운 블록을 빈 블록으로 표시 | 동작함 (VM 안 `df` 사용량이 줄어듦) |
  | 3 TRIM(discard) | 빈 블록을 아래 장치에 알림. `discard` 마운트(지울 때마다)나 주기 `fstrim`(보통 주 1회, 배포판 권장) | **끊김.** `/dev/sdc`가 `discard` 없이 붙어 있고 주기 `fstrim`도 없다(측정 36) |
  | 4 가상 디스크(VHDX) | TRIM 받은 블록을 호스트 파일에서 뺌. sparse VHDX면 자동, 아니면 압축(diskpart `compact vdisk`, `Optimize-VHD`)할 때만 | **끊김.** 동적 VHDX라 압축할 때만 줄어든다. fstrim 없이 한 압축은 1MiB만 줄었다(측정 36) |
  | 5 호스트 SSD | TRIM을 받은 블록을 컨트롤러 GC가 지움 | 이번 문제와 무관 |
  - 더해서: VM은 가상 디스크를 1TB로 알아서, 다음 실행의 임시 파일이 지운 자리 대신 한 번도 안 쓴 블록에 쓰이면 VHDX가 다시 커진다. 측정된 것은 실행마다 C: 여유가 줄었다는 것뿐이고, ext4가 어느 블록에 썼는지는 재지 않았다 [확인 필요].
  - 엔진 1층 "GC로 지운다"는 Spark의 실제 이름이 Garbage Collector다(ContextCleaner = 애플리케이션 GC). 다만 그 GC가 지우는 것은 VM 안 파일까지고, 3~4층이 없으면 호스트에서는 아무 일도 일어나지 않는다.
  - DuckDB 임시 상한(L3)을 "C: 여유 − 50GiB"로 잡은 것(측정 39)은 이 차이를 맞추려던 장치다. DuckDB 기본값은 보이는 디스크(가상 1TB) 여유의 90%다.
- 처방 선택지:
  | 선택지 | 장점 | 단점 |
  |---|---|---|
  | A. 쓰는 양을 줄인다(F-9 처방, 결정 103) | 333GB의 대부분이 생활이동 셔플·spill이라 근본 처방 | 다른 무거운 실행에도 같은 층 문제는 남는다 |
  | B. Docker Desktop 디스크 상한("Disk image max size")을 회수 직후 호스트 여유보다 작게 | VHDX가 상한 이상 못 커진다. VM이 실제 용량을 알게 되어 넘치면 VM 안 "공간 없음" 오류로 드러난다(설계 원칙 4, resource_guard L4) | 상한은 지금 사용량보다 커야 한다. 지금 크기보다 줄일 수 있는지 [확인 필요] |
  | C. 무거운 실행 뒤 fstrim + compact(측정 36 방법), 실행 전 C: 여유 점검 | 확실히 돌아온다 | 사람 손, Docker 정지 필요 |
  | D. sparse VHDX | 지우면 자동으로 돌아온다 | 지금 WSL이 데이터 손상 가능성으로 막아 두었다(`--allow-unsafe` 필요). PostgreSQL 데이터가 같은 디스크라 쓰지 않는다 |
  | E. 엔진 임시 폴더를 호스트 폴더 바인드 마운트로 | 지우면 바로 돌아온다 | WSL 파일 공유를 거쳐 셔플 수백 GB가 느려질 수 있다 [미측정] |
- 덧붙임 (2026-11-14): 원인 하나 더(가설). 엔진의 정리는 정상 종료 때만 돈다. 죽은 실행(F-16 등)의 임시 폴더는 아무도 지우지 않는다. 측정 38 실행 전 컨테이너 `/tmp`가 이미 74.6GB였고 끝난 뒤 79.1GB가 남았다 [확인 필요: 내용물]. 끝났거나 죽은 실행의 셔플·spill은 다음 실행이 읽지 않으므로(실행마다 새 `blockmgr-*`) 지워도 성능 영향이 없다. 설치물·캐시(`/tmp/duckdb_pkg` 등), 기록, 잠금 파일은 지우지 않는다.
- S0 조사 결과 (2026-11-14 10시대, handoff_decisions.md 2.5절, J-4): **컨테이너 `/tmp` 내용물은 166MB이고, 지울 셔플·spill은 없다.** "측정 38 실행 전 `/tmp` 74.6GB"는 `/tmp` 내용물이 아니라 VM 데이터 디스크 전체 사용량이었다(F-21). 그래서 위 덧붙임의 가설(죽은 실행 찌꺼기 74.6GB)은 **지움.**
  | 종류 (J-4) | `/tmp` 안 | 크기 | 마지막 수정 | 처리 |
  |---|---|---|---|---|
  | 끝났거나 죽은 실행의 셔플·spill | `spark-local/`(빈 폴더), `blockmgr-*`·DuckDB `*.tmp` 없음 | 0 | 2026-11-14 10:46(빈 폴더) | 지울 것 없음 |
  | 설치물·캐시 | `duckdb_pkg/`(측정 39 `PYTHONPATH`), `.cache/`, `hsperfdata_*`(JVM 실행 중 파일) | 70.2MB, 26.4MB, 8KB | 2026-11-13 | 남김 |
  | 기록 | `spark-events/`(Spark 이벤트 로그 5개, `ops.engine_run`의 원천), `duckdb-profiles/`(측정 39 등 6회), `explain*.txt`, `f19_*.json`, `p.json`, `runner*.log`, `test_*.log`, `vm_watch_*.out`, 시험용 스크립트 `*.py` 6개(밤사이 Claude가 둔 것) | 75.6MB, 57KB, 나머지 합 약 0.4MB | 2026-11-13~14 | 남김(정리하려면 따로 사용자 확인) |
  | 잠금 | `heavy.lock` | 0 | 2026-11-14 10:43 | 남김 |
  | 데이터 | 없음 | | | |
  - 컨테이너 쓰기 층 전체도 340MB다(`docker ps -s`). VM 데이터 디스크 사용 65GB(`df /`, 옛 표 9GB를 지운 뒤)는 Docker 이미지 25.59GB, 볼륨 34.55GB(그중 PostgreSQL), 빌드 캐시 12.32GB, 컨테이너 1.58GB다(`docker system df`, 합 74.0GB, 층 공유 때문에 `df`보다 크다).
  - 측정 38·39 뒤 VM 디스크 사용이 183GB까지 남았다가(DuckDB 3회차 vm_watch 마지막 줄 183.1GB) 지금 65GB인 것은 DuckDB 임시 폴더가 인스턴스 종료 뒤 지워졌기 때문으로 보인다. 지워진 시각은 기록이 없다 [확인 필요]. VM 안에서는 지워졌으므로 호스트 C:로 돌아오지 않은 것은 TRIM과 압축 문제(3·4층)뿐이다.
  - 결론: S1(정리)은 지울 것이 없어 하지 않는다. 호스트 공간을 돌려받는 길은 S2(fstrim + compact)뿐이다.
- S4 적용 (2026-11-14, 결정 107 A): 작업 공간을 이름 볼륨 `scratch`(`/scratch/spark`, `/scratch/duckdb`)로 옮김. `tests/check_scratch.py` 통과(셔플·흘림 파일이 `/scratch` 아래에 생김). 같은 VM 디스크라 호스트 반환은 여전히 S2가 필요하다.
- S5 적용 (2026-11-14): 잠금을 잡은 실행이 시작·끝에 `/scratch`를 비운다(`common.heavy_lock`). 죽은 실행의 찌꺼기는 다음 잠금 때 사라진다. `tests/check_scratch.py` 통과.
- S2 적용 (2026-11-14 11:14~11:59, 사용자 실행): fstrim + compact로 VHDX 563.8 → 78.1GB, 호스트 C: 여유 280.21 → 979.73GB(그중 약 214GB는 사용자가 지운 다른 파일). DB 복구 없이 기동, 행 수 같음(측정 44).
- S3 (상한): 지금 상한 `DiskSizeMiB` 1TiB가 호스트 C: 여유보다 크다. J-8대로 계산한 500.41GB는 실행기 문턱(J-7, 394GB)과 맞지 않아 사용자 결정을 기다린다(측정 44, 판단 98).
- 처방과 결과: 고르지 않았다(사용자). 제안: A + B, 당분간 C. 저장소 분류, k8s 대응, 지금 환경 처방 계획(S0~S8)은 resource_guard.md 7절.
- 운영 환경과 비교: k8s에서는 셔플 디스크가 파드의 임시 볼륨(emptyDir 등)이라 파드와 함께 지워지고, 클라우드 블록 스토리지는 thin provisioning이라도 TRIM을 넘겨받는다. 이 문제는 노트북 위 동적 가상 디스크에서 생긴다. 다만 "엔진이 보는 용량과 실제 용량이 다르면 상한이 거짓 용량을 본다"는 점은 F-16과 같은 뿌리다.
- 상태: **처방함(결정 107), S3만 적용 못 함.** S4 전용 볼륨, S5 시작·끝 비우기, S6 사전 점검(394GB), S7 알림 줄, S2 회수 적용·확인(측정 44, 42). S3 디스크 상한은 Docker Desktop(WSL2)에 메뉴가 없어 사전 점검으로 대신한다. 처방 뒤 첫 실행(측정 42)에서 VM 디스크 최대 증가는 259 → 144GB로 줄었지만 VHDX는 실행마다 다시 커진다(78 → 354GB). 호스트 반환은 여전히 사람이 하는 회수(S2)가 필요하다.

## F-21. vm_watch의 `tmp_used_b`를 "컨테이너 `/tmp` 사용량"으로 읽음: 실제로는 VM 데이터 디스크 전체 사용량
- 태그: 측정 해석, 설계 가정(파일 시스템 경계)
- 언제·어디: 2026-11-13 측정 38·39의 기록과 F-20 덧붙임, resource_guard.md 7.2·7.3. 2026-11-14 2.5절 S0 조사에서 발견.
- 증상: 기록은 "측정 38 실행 전 컨테이너 `/tmp` 74.6GB, 끝난 뒤 79.1GB"라고 했고, F-20은 이것을 죽은 실행의 찌꺼기로 추정해 지울 계획(S1)을 세웠다. S0에서 `/tmp`를 실제로 재니 166MB였고, `blockmgr-*`나 DuckDB 임시 파일은 없었다.
- 재현: `docker exec new_proj-airflow-1 df -h / /tmp` → 둘 다 같은 `overlay`(1007G 중 65G 사용). `du -sh /tmp` → 166M. `measure/vm_watch.py` 55줄 `shutil.disk_usage("/tmp")`는 `/tmp`가 놓인 파일 시스템의 사용량을 준다. 1회.
- 가설:
  - H1 `/tmp`에 큰 파일이 있었는데 그 뒤 누가 지웠다 → vm_watch 첫 줄은 유휴 상태(2026-11-13 21:30, 측정 36 회수 직후)에도 74.8GB였고, 그때 Docker 이미지·볼륨·빌드 캐시 합이 약 83GB였다(`docker system df`, 지금 74.0GB에 지운 옛 표 9.5GB를 더함). 회수 직후의 유휴 VM에 74GB 찌꺼기가 있었다고 볼 근거가 없다 → 지움.
  - H2 `tmp_used_b`는 `/tmp` 폴더가 아니라 `/tmp`가 속한 파일 시스템 전체다. 컨테이너에는 `/tmp` 마운트가 따로 없어 그 파일 시스템이 Docker 데이터 디스크(`overlay` → VM ext4)다 → `df`로 확인 → 남김.
- 원인: 열 이름과 문서가 "/tmp 사용"이라 폴더 크기로 읽었다. `disk_usage`는 경로가 아니라 파일 시스템 단위다. 지금 컨테이너는 `/tmp`가 루트와 같은 파일 시스템이라 이 값은 이미지, PostgreSQL 볼륨, 빌드 캐시까지 포함한 VM 데이터 디스크 사용량이다.
- 영향:
  - 측정 38의 "컨테이너 `/tmp` 최대 333.6GB"는 VM 디스크 사용 최대다. 실행 중 늘어난 몫은 333.6 − 74.6 = **259.0GB**이고, 그 안에 Spark 셔플·spill과 같은 시간의 PostgreSQL 쓰기(경계 표 1,378만 행 등)가 함께 있다. 측정 39 표의 "+259GB"는 이 차이라 맞다.
  - 측정 39의 "컨테이너 /tmp 최대 증가"도 VM 디스크 증가다. DuckDB 임시 폴더가 `/tmp` 아래였으므로 대부분 임시 파일이지만 정확히 같지는 않다.
  - F-20 덧붙임 가설과 resource_guard.md 7.2·7.3의 "`/tmp`에 남은 약 79GB"는 틀렸다.
  - 호스트 C: 사용(측정 38의 328GB = 641.24 → 313.00GB)은 호스트에서 직접 잰 값이라 영향 없다. J-7 문턱은 이 값을 쓴다.
- 처방과 결과: (1) 위 기록들에 정정을 덧붙였다(원문은 기록이라 지우지 않음). (2) `measure/vm_watch.py` 머리 설명을 "`/tmp`가 놓인 파일 시스템(지금은 VM 데이터 디스크 전체) 사용·여유"로 고쳤다. 열 이름 `tmp_used_b`는 지난 TSV와 맞추려고 그대로 둔다. (3) 결정 107 S4로 `/scratch` 볼륨을 붙여도 이름 볼륨은 같은 VM 디스크에 있어 `disk_usage("/scratch")`도 같은 값을 준다. 작업 공간 크기는 `du`로 따로 재야 한다(S7 관측에 반영).
- 재발 방지: 크기를 잴 때 "폴더 크기(`du`)"와 "파일 시스템 사용(`df`)"을 이름에서 구분한다. 처음 쓰는 지표는 한 번 `du`와 나란히 놓고 뜻을 확인한다.
- 남은 것: 측정 38 실행 중 Spark 작업 공간만의 최대는 따로 재지 않았다(`ops.engine_run`의 셔플 쓰기 248GB, 디스크 흘림 55GB가 가장 가까운 값).
- 상태: **처방함(기록 정정과 도구 설명).**

## F-22. 컨테이너 안에서 `git worktree prune`을 돌려 호스트 UI worktree의 git 기록을 지움 (에이전트 A1 구현 중, 2026-11-14)
- 증상: Airflow 컨테이너에서 worktree 만들기·지우기를 시험하고 `git worktree prune`을 돌린 뒤, 호스트 `git worktree list`에서 `C:/Users/dudfo/Desktop/new_proj_ui [ui]`가 사라지고 `.git/worktrees/`가 없어졌다. UI 세션의 작업 파일은 그대로였지만 그 폴더에서 git 명령이 실패하는 상태가 됐다.
- 원인: worktree 기록(`.git/worktrees/<이름>/gitdir`)은 호스트 경로 `C:/Users/.../new_proj_ui/.git`를 담는다. 컨테이너에는 그 경로가 없으므로 `prune`이 "폴더가 사라진 worktree"로 보고 기록을 지웠다. 같은 `.git`을 경로 체계가 다른 두 곳(Windows 호스트, Linux 컨테이너)이 함께 쓰는데 prune은 자기 쪽 경로만 본다.
- 처방: (1) 기록을 손으로 되살렸다: `.git/worktrees/new_proj_ui/`에 `HEAD`(`ref: refs/heads/ui`), `commondir`, `gitdir`를 쓰고, UI 폴더에서 `git reset -q`(작업 파일은 건드리지 않고 index만 HEAD로 다시 만듦). 뒤에 `git status`가 사고 전과 같은 변경(수정 2, 새 파일 6묶음)을 보였다. 사고 전에 stage해 둔 것이 있었다면 그 stage 상태는 잃었다(작업 파일은 남음). (2) `agent/run.py`는 `prune` 대신 `/scratch` 아래를 가리키는 기록만 지운다(`forget_scratch_worktrees`).
- 재발 방지: 컨테이너 안에서 저장소 전체에 걸리는 git 정리 명령(`worktree prune`, `gc`)을 돌리지 않는다. 한계: 컨테이너에서 `git commit`이 자동 `gc`를 부르면 `gc.worktreePruneExpire`(기본 3개월)보다 오래된 호스트 worktree 기록은 같은 이유로 지워질 수 있다 [미측정]. UI worktree를 3개월 넘게 두면 다시 본다.
- 상태: **처방함.**

## F-23. SQL 에이전트 첫 실행에서 드러난 도구·판정 버그 3개 (측정 45, 2026-11-14)
- ① 보고가 사라짐: `no_fix`일 때 빈 브랜치를 지우는데, 그 브랜치가 아직 worktree에 체크아웃돼 있어 `git branch -D`가 실패했다. 보고 출력이 그 뒤에 있어 신호와 에이전트 답이 남지 않았다. 처방: 보고를 `finally`에서 항상 출력하고(예외는 다시 올림), 빈 브랜치는 worktree를 지운 뒤 지운다.
- ② 에이전트가 바뀐 데이터를 못 봄: 원본 표본 도구가 운영 `raw.congestion`을 읽었다. 기준선은 변이 뷰 `raw_sbx_base`로 빌드했으므로 에이전트가 본 데이터와 신호를 만든 데이터가 달랐다. 에이전트는 추측하지 않고 "확인하지 못함"으로 멈췄다(바른 동작). 같은 실행에서 상대 경로(`Grep warehouse`)가 worktree가 아니라 프로세스 작업 폴더 기준으로 해석돼 거절됐다. 처방: 표본은 기준선이 읽은 `raw_sbx_<라벨>`과 `stg_dbt_sbx_<라벨>.*_rows`(거절 배치는 격리 표에 안 들어가므로)에서, 경로는 worktree 기준으로 푼다. selftest에 상대 경로 사례를 더했다.
- ③ G2 거짓 실패: `batch_status_*.checked_at`, `*_quarantine.quarantined_at`은 빌드마다 `now()`라 같은 코드의 두 빌드도 모든 행이 다르다. 차이 수가 행 수의 정확히 2배(1,352 → 2,704, 2 → 4, 11 → 22)라 알아봤다. 처방: 비교에서 `checked_at`, `quarantined_at`, `loaded_at`을 뺀다(`agent/sandbox.py` `VOLATILE`).
- 공통 원인: 판정 도구를 실제 데이터로 한 번도 돌리지 않고 에이전트와 함께 처음 돌렸다(F-9 실행으로 heavy 잠금이 잡혀 있었음). 재발 방지: 새 판정 코드는 에이전트 없이 사람 브랜치(`--cand-branch`)로 먼저 돌린다. 같은 코드 두 빌드의 G2·G5가 0인지 보는 대조(같은 브랜치를 기준선·후보로)를 다음 평가 전에 한 번 돌린다.
- 상태: **처방함.** 대조 실행은 아직.

## F-24. 컨테이너를 다시 만들기 전 덤프가 앞선 덤프를 덮어씀 (결정 109 H0, 2026-11-14 20:3x)
- 증상: `pg_dump`를 날짜 폴더 `C:\Users\dudfo\pg_backup_20261114\`에 썼는데, 그 폴더에 같은 날 10:55에 뜬 덤프 두 개(`seoul.dump`, `airflow.dump`)가 이미 있어 같은 이름으로 덮였다.
- 원인: 폴더 이름을 날짜로만 정하고, 쓰기 전에 있는지 보지 않았다. 같은 날 두 번째 덤프라는 경우를 생각하지 않았다.
- 처방: 새 덤프를 `pg_backup_20261114_h0\`로 옮기고, 옛 폴더에는 옛 `SHA256SUMS`(`logs/h0/old_backup_1055_SHA256SUMS`로도 보관)와 `README_lost.txt`(덮인 사실)를 남겼다. 10:55 덤프 내용은 되살릴 수 없다. 그 뒤의 H0 덤프가 더 새 상태라 복구 지점으로는 잃은 것이 없다.
- 재발 방지: 덤프는 시각까지 붙인 새 폴더에 쓰고, 폴더가 있으면 멈춘다.
- 상태: **처방함(기록).**

## F-25. `docker compose up -d`가 db 컨테이너를 다시 만듦 (결정 109 H1, 2026-11-14 20:34)
- 증상: Airflow 구성 요소를 처음 띄울 때 바꾸지 않은 `db` 서비스도 Recreated 됐다. `pgdata` 이름 볼륨이라 데이터는 그대로였다(`stg.living_commute`, `mart.dong_profile` 행 수 확인, 상품 sha256 같음).
- 가설: db의 서비스 정의는 이번 diff에 없다. 결정 101 때 상한을 `docker update`로 먼저 걸고 compose 파일에 `mem_limit`을 나중에 적었다면, 그 컨테이너의 compose 설정 해시 라벨이 파일과 달라 다음 전체 `up`에서 다시 만들어진다 [확인 필요: 옛 컨테이너의 라벨을 보지 못하고 지나감].
- 영향: 데이터 손실 없음. PostgreSQL 재시작으로 연결이 한 번 끊겼다.
- 재발 방지: 컨테이너를 다시 만들 수 있는 `up`은 서비스 이름을 적어 돌리고, 전에 `docker compose up -d --dry-run`으로 Recreate 대상을 본다. 덤프(H0)를 먼저 뜨는 규칙은 지켰다.
- 상태: **원인 확정(2026-11-15, S3).**
- 확인 (2026-11-15, Docker Compose 5.4.0, 컨테이너를 다시 만들지 않고):
  - `docker compose config --hash <서비스>`(지금 파일이 기대하는 값)와 컨테이너 라벨 `com.docker.compose.config-hash`를 비교: db, ui, airflow-scheduler, airflow-apiserver, socket-proxy는 같고 **metabase, pgadmin만 다르다**. 둘 다 재부팅 뒤 `up --dry-run`이 Recreate로 보였던 서비스다(판단 114).
  - 두 컨테이너는 상한이 compose에 들어간 커밋(5d05f47, 2026-11-13 22:32)보다 먼저 만들어졌다(metabase 2026-11-13 01:09, pgadmin 2026-10-28). 상한은 판단 85대로 `docker update`로 걸었다.
  - compose 파일에서 metabase·pgadmin의 `mem_limit`, `memswap_limit` 두 줄만 뺀 사본으로 해시를 다시 내면 두 컨테이너의 라벨과 **정확히 같다**. 해시 차이는 그 두 줄뿐이다.
  - 원인: compose 설정 해시는 `mem_limit`을 포함한다. `docker update`는 컨테이너의 실제 상한만 바꾸고 라벨은 그대로 둔다. 그래서 파일과 라벨이 어긋나 다음 `up`이 "설정이 바뀌었다"며 다시 만든다. H1의 db 재생성도 같은 경로다(db는 H1에서 다시 만들어져 지금은 해시가 같다. 옛 라벨은 못 봤으므로 db는 같은 기전으로 추정).
- 처방: 상한을 바꿀 때 `docker update`는 급할 때만 쓰고, 쓰면 "다음 `up`에서 그 서비스가 한 번 다시 만들어진다"를 함께 적는다. 계획된 때(덤프 뒤) 서비스 이름을 적어 `docker compose up -d <서비스>`로 다시 만들어 라벨을 맞춘다. metabase·pgadmin은 데이터가 DB와 `pgadmin_data` 볼륨에 있어 다시 만들어도 잃는 것이 없다(P0 덤프 있음).

## F-26. 잠금 없이 계산 컨테이너에서 Spark를 돌리다 다른 작업의 작업 공간 비우기에 셔플 파일을 잃음 (결정 109 H2, 2026-11-14 20:40)
- 증상: spark-job에서 Iceberg 한 달 `count(*)`가 `FileNotFoundException: /scratch/spark/blockmgr-.../temp_shuffle_...`로 실패했다.
- 원인: 같은 때 lineage DAG가 heavy 잠금을 잡고 `clear_scratch()`로 `/scratch`를 비웠다. 내 count는 잠금 없이 같은 `scratch` 볼륨을 썼다. 결정 107 S5의 "잠금을 잡은 쪽만 작업 공간을 쓴다"는 전제를 내가 어겼다. 컨테이너가 나뉘어도 `scratch` 볼륨은 하나라 이 전제는 그대로다.
- 처방: `python -m common lock -- ...`로 다시 돌려 141,178,148행(8초). 실행 안내(README, 각 도구 머리말)에 무거운 명령은 래퍼를 붙여 적었다.
- 재발 방지: 계산 컨테이너에서 `/scratch`를 쓰는 명령은 잠금 래퍼로 돌린다. 강제하지는 않는다(잠금 없는 `docker compose run`은 여전히 가능) [한계].
- 상태: **처방함.**

## F-27. DockerOperator가 socket-proxy에서 403: 이미지 조회 API가 닫혀 있음 (결정 109 H4)
- 증상: 첫 DockerOperator 실행이 `GET /v1.55/images/json?...reference=seoul-spark-job:3.5.9` 403 Forbidden으로 실패.
- 원인: provider는 컨테이너를 만들기 전에 이미지가 있는지 본다(없으면 pull). socket-proxy는 `CONTAINERS`, `POST`만 열었고 `IMAGES`는 기본 0이다.
- 처방: `IMAGES: 1`. `info`, `volumes`, `networks`, `EXEC` 등은 닫은 채로 두고 생활이동 전체 실행까지 다른 403은 없었다.
- 재발 방지: 여는 API 묶음과 그 이유를 compose 주석에 적었다.
- 상태: **처방함.**

## F-28. DockerOperator `auto_remove="success"`가 실패한 계산 컨테이너도 지움 (결정 109 H4)
- 증상: 중단 시험에서 failed로 표시한 compute의 컨테이너(ba40ad2b46bf)가 끝난 뒤 `docker inspect`로 찾을 수 없었다. 지시서는 "실패하면 원인을 볼 수 있게 남긴다(OOMKilled 확인용)"다.
- 원인: provider 4.5.9 `_run_image_with_mounts`는 `auto_remove == "success"`일 때 `remove_container`를 `try ... finally`의 `finally`에서 부른다(설치된 `operators/docker.py` 460행). 종료 코드가 0이 아니어서 `DockerContainerFailedException`을 올려도, 중단으로 `wait`가 끝나도 지운다. 이름과 달리 실패 보존을 보장하지 않는다.
- 처방: `auto_remove="never"`로 두고 `JobOperator`가 성공 때만 직접 지운다. 실패하면 컨테이너 id, `OOMKilled`, `ExitCode`를 태스크 로그에 남기고 컨테이너는 그대로 둔다. `exit 3` 시험으로 확인(e91841dc975d 남음, 로그에 id와 ExitCode 3).
- 재발 방지: 남은 컨테이너는 지시서 3절대로 목록만 남기고 지우는 것은 사용자가 정한다.
- 상태: **처방함.**

## F-29. 생활이동 계산은 도는데 DockerOperator 태스크가 20분 뒤 `ChunkedEncodingError`로 실패: socket-proxy가 유휴 연결을 10분에 끊음 (결정 109 H4 재측정, 2026-11-14 22:06)
- 증상: 재부팅 뒤 다시 돈 compute(시도 2)가 실행기 시작(21:46:47, 마지막 로그 줄) 정확히 20분 뒤 `ChunkedEncodingError: Response ended prematurely`로 실패했다. 계산 컨테이너 `stoic_bell`은 그대로 돌고 있었다(Up 26분, OOM 아님). 내가 멈춤(137, 남김).
- 원인: `tecnativa/docker-socket-proxy` v0.5.0의 haproxy 설정은 `timeout client 10m`, `timeout server 10m`가 고정이고 환경 변수로 바꿀 수 없다(이벤트 API만 `timeout server 0`). provider는 컨테이너 출력을 붙어서 읽는 연결(attach)을 열어 두고, 출력이 끝나면 `wait`로 종료를 기다린다. 실행기는 Spark 경고 뒤 20분 넘게 아무것도 찍지 않아 attach가 10분에, 이어서 연 `wait`가 다시 10분에 끊겼다(10 + 10 = 20분, 시각이 맞음). H4 첫 시도(재부팅 전)는 실행기 13분째에 멈춰 이 문턱에 닿지 않았고, 짧은 시험(`exit 3`, 중단 시험)은 10분 안이었다.
- 처방 선택지: (A) 계산 명령에 1분마다 한 줄 heartbeat (B) haproxy 설정 파일을 덮어써 attach·wait 경로에 긴 timeout. A를 골랐다(판단 111): 한 줄이고, 사람이 태스크 로그에서 "아직 도는 중"을 볼 수 있다. 한계는 `dags/jobs.py` 주석에 적었다(다른 긴 무출력 호출이 생기면 B).
- 확인 (2026-11-14 22:10~22:21): heartbeat를 넣은 같은 경로(DockerOperator → socket-proxy)로 11분 무출력 작업(`sleep 660`) → 성공 663초, `[alive]` 10줄, 컨테이너 지워짐. `exit 3` → `DockerContainerFailedException`, 컨테이너 c8a7cba4f11d 남음(종료 코드 보존). 생활이동 재실행(시도 3)은 측정 46.
- 재발 방지: 문을 하나 더 두면(프록시) 그 문의 시간 제한이 긴 작업의 상한이 된다. 새 중계 계층을 넣을 때는 가장 긴 작업 시간보다 긴 무출력 시험을 한 번 돌린다.
- 상태: **처방함.**

## F-30. DuckDB 배치 단위 처리(D2)가 4번째 배치에서 넘침: 누적 임시 표가 해시 집계와 같은 16GiB를 나눠 씀, B로 다시 하자 컨테이너 상한 137 (결정 110, 측정 50, 2026-11-15 00:00~00:05)
- 태그: 메모리, 설계 가정(배치마다 메모리가 새로 시작한다고 봄)
- 증상: 배치 1~3은 각 약 69초에 성공. 배치 4(202601)의 `clean`(키 9개 해시 집계)에서 `OutOfMemoryException (16.0/16.0GiB)`. 넘친 배치만 선택지 B로 다시 하자 컨테이너 메모리 17.87GiB / 18GiB 뒤 종료 코드 137(cgroup OOM). 두 시도 모두 배치 1~3의 시간·메모리가 거의 같았다(재현 2/2).
- 가설:
  - H1 배치 4가 특별히 크다 → 202601은 1억 3,345만 행으로 10배치 중 두 번째로 작다 → **지움.**
  - H2 누적 표(`clean_all`, `quarantine_all`)가 버퍼 풀을 차지해 해시 집계 몫이 배치마다 준다 → memwatch에서 `IN_MEMORY_TABLE`이 매 배치 16GiB까지 오르고 임시가 배치마다 약 9GB씩 늘었다(9.1 → 18.5 → 27.2GiB). 배치 4 실패 직전 `HASH_TABLE` 13.8GiB와 `IN_MEMORY_TABLE`이 함께 16GiB → **남김(가장 유력).**
  - H3 측정 49의 흘리지 않는 `ALLOCATOR` 누적 → 배치마다 1.1GiB 안팎에서 늘지 않음 → **지움**(배치 처리로 이 문제는 사라짐).
- 원인: 결정 110은 "배치마다 계산하면 한 번에 잡는 메모리가 배치 크기로 준다"를 가정했다. 계산 하나는 그렇게 됐지만, 결과를 같은 DuckDB 인스턴스의 임시 표에 쌓으면 임시 표도 같은 `memory_limit` 안에서 관리되어 배치가 지날수록 계산 몫이 준다. B는 `checked` 임시 표를 더 만들어 같은 풀을 더 썼고, DuckDB가 세지 않는 할당(약 1.9GiB)까지 더해져 18GiB 컨테이너 상한(H5 규칙으로 정한 값, `memory_limit` 16GiB와 여유 2GiB)을 넘었다.
- 처방과 결과: 지시서 4절에 따라 B까지 쓰고 "완주 못 함"으로 결론. 예산을 늘리지 않았다(J-15). 다음 후보(재지 않음, 사용자 결정 필요): (1) 배치 결과를 Parquet 파일로 내보내고 마지막에 파일로 읽기(버퍼 풀에서 누적을 뺌, 처리 단위마다 다시 돌리기도 쉬워짐) (2) 배치 안에서 `30_commute`에 필요한 만큼 줄인 뒤 누적 (3) DuckDB 인스턴스를 배치마다 새로 띄우기.
- 재발 방지: 배치 처리를 설계할 때 "계산의 메모리"와 "누적 결과가 놓이는 곳의 메모리"를 따로 센다.
- 상태: **처방함(결정 111, 측정 51): 배치마다 새 프로세스 + Parquet 넘김으로 완주, Spark와 세 표 모두 0행 차이.** 원인 보정: 202601은 누적 없이도 threads 12에서 넘친다(F-31). 측정 50의 실패는 누적 표와 F-31이 겹친 것으로 본다.

## F-31. DuckDB 202601 배치만 threads 12에서 흘리기 전에 8초 만에 메모리 오류 (결정 111 D4, 측정 51, 2026-11-15 00:59, 01:07)
- 태그: 메모리, 엔진 동작(흘리기 시작 시점)
- 증상: 배치마다 새 프로세스로 돈 D4에서 다른 9개 배치는 threads 12로 48.9~61.5초에 성공했는데, 202601만 7.9초에 `OutOfMemoryException (16.0/16.0GiB)`. 같은 배치를 혼자 다시 돌려도 8.3초에 같은 오류. threads 4로는 83.8초·80.9초에 성공.
- 관찰(1초 memwatch, `logs/duckdb_memwatch/duckdb-12185ac51ffb-b00-0.csv`): `HASH_TABLE`이 1초에 약 3.5GiB씩 늘어 5초 만에 15.08GiB, 그동안 **임시 사용 0**. 다른 배치는 같은 최대(약 14.9GiB)까지 가면서 임시로 6.8~8.9GiB를 흘렸다. 202601은 흘리기 시작하기 전에 상한에 닿는다.
- 가설:
  - H1 202601이 크다 → 원본 1억 3,345만 행, 통과 1억 3,058만 행으로 10배치 중 두 번째로 작다 → **지움**(F-30 H1과 같은 결론).
  - H2 Iceberg 파일 배치가 다르다 → 파일 11개, 파일당 최대 1,700만 행으로 다른 달과 같은 범위(202602는 파일 10개로 성공) → 이것만으로는 설명 안 됨.
  - H3 F-30의 누적 표 → 새 프로세스에서도 재현 → **F-30의 원인 설명을 보정한다.** 측정 50에서 배치 4가 넘친 데에는 누적 표 몫도 있었겠지만, 누적 없이도 202601은 threads 12에서 넘친다.
  - H4 스레드별 해시 표가 동시에 커지는 속도가 흘리기 판단보다 빠르다(스레드 수에 비례). threads 4에서 성공한 것과 맞지만, 왜 202601에서만인지는 설명하지 못한다 [확인 필요].
- 처방과 결과: 결정 111의 넘침 규칙(그 배치만 threads 4로 한 번 더)이 이 경우를 잡아 D4가 완주했다. 근본 원인은 확정하지 못했다. 다음 후보(재지 않음): 202601의 키 분포(그룹 수, 키 순서)를 다른 달과 비교, threads 8에서의 경계.
- 재발 방지: 배치 처리의 넘침 규칙은 "같은 배치를 더 적은 스레드로"를 기본으로 둔다(결정 111). 새 달이 들어올 때 같은 일이 나면 rc 75 다음 시도가 로그에 남는다.
- 상태: **우회함(넘침 규칙), 원인 [확인 필요].**

## F-32. 스케줄러가 갑자기 죽으면 계산 컨테이너가 주인 없이 끝까지 돌고, 약 5분 뒤 Airflow가 같은 작업을 새 시도로 다시 돌린다 (결정 109 S2, 2026-11-15 02:08~02:30)
- 태그: 오케스트레이션, 중복 실행, 자원(잠금)
- 증상 [유도]: 시험 DAG `test_scheduler_kill`(spark-job 컨테이너에서 heavy 잠금을 잡고 300초 잠)을 돌리는 중 `docker kill new_proj-airflow-scheduler-1`(02:08:56), 14초 뒤 `docker start`.
  - 옛 계산 컨테이너(`admiring_williams`, 라벨 try=1)는 계속 돌아 300초를 다 자고 종료 코드 0으로 끝났다(02:13:46). 그동안 잠금을 쥐고 있었다.
  - 태스크는 계속 running으로 보였고, 02:14:16에 스케줄러가 "Reset the following 1 orphaned TaskInstances"를 찍은 뒤 시도 2(`fervent_satoshi`)가 처음부터 다시 돌아 02:19:17에 success. **같은 작업이 두 번 돌았다.** 옛 컨테이너는 Exited로 남았다(지우는 주인이 없음).
  - 기록: `logs/s2/timeline1.txt`(15초 간격 컨테이너·태스크·잠금), 스케줄러 로그.
- 원인(Airflow 3.3.2, LocalExecutor):
  - 태스크 프로세스(DockerOperator가 로그를 읽고 끝나기를 기다리는 쪽)는 스케줄러 컨테이너 안에 있어 스케줄러와 함께 죽는다. `on_kill`이 불리지 않아 계산 컨테이너를 멈출 사람이 없다. 계산 컨테이너는 호스트 Docker의 독립 컨테이너라 계속 돈다.
  - 새 스케줄러는 "고아 태스크 입양 또는 재설정"을 시작할 때와 그 뒤 5분마다 한다(로그 17:09:16, 17:14:16). 시작 때(죽은 지 20초)에는 옛 스케줄러 작업이 아직 죽은 것으로 판정되지 않아 넘어갔고, 다음 주기에 재설정했다. 재설정은 재시도 횟수를 쓰지 않으므로 `retries=0`이어도 시도 2가 생긴다.
  - 이번에는 옛 작업(300초)이 재설정(약 320초 뒤)보다 먼저 끝나 잠금이 풀려 있었고, 그래서 두 번째가 잠금을 잡고 다시 돌았다. 작업이 5분보다 길면(생활이동 compute 약 50분) 시도 2는 옛 컨테이너가 쥔 잠금에 막혀 바로 실패하고(재시도 0이라 태스크 failed), 계산은 주인 없는 컨테이너가 끝까지 하지만 Airflow는 실패로 기록하고 하류(asset → wh_load)가 돌지 않는다 [이 경우는 재현하지 않음, 위 동작에서 추론].
- 처방 선택지:
  - A. 스케줄러 시작 때 라벨 `seoul.job=1`인 실행 중 계산 컨테이너를 모두 멈춘다(지우지 않음). LocalExecutor라 스케줄러가 새로 뜨는 순간 도는 계산 컨테이너는 모두 주인이 없다. 잠금이 풀려 시도 2가 처음부터 정상으로 돈다(계산은 파티션 덮어쓰기라 멱등). 대가: 옛 계산의 진행분을 버린다.
  - B. 옛 컨테이너에 다시 붙기(입양). provider에 그런 기능이 없고 코드가 커진다.
  - C. 그대로 두고 사람이 정리. 생활이동처럼 긴 작업에서 "계산은 됐는데 실패" 상태가 된다.
- 처방: A. `JobOperator.execute`가 계산 컨테이너에 `seoul.job`, `seoul.dag_id`, `seoul.run_id`, `seoul.task_id`, `seoul.try` 라벨을 단다. `common.stop_orphan_jobs()`(socket-proxy 경유)가 라벨 컨테이너를 멈추고 목록을 찍는다. `docker-compose.yml` airflow-scheduler의 command가 이것을 먼저 부르고 `exec airflow scheduler`(정리 실패해도 스케줄러는 뜨고 실패를 로그에 남김).
- 처방 뒤 같은 시험(02:20:04 kill, 1초 뒤 start, `logs/s2/timeline2.txt`, `logs/s2/scheduler2.txt`): 스케줄러 시작 로그 "주인 없는 계산 컨테이너 1개 멈춤: [('e30c6145e24c', 'test_scheduler_kill', ..., 'hold_lock', '1')]", 옛 컨테이너 `flamboyant_northcutt`는 종료 코드 143(SIGTERM)으로 멈춤, 02:20:11 기록에 잠금 0. 17:25:10 고아 재설정 뒤 시도 2(`wizardly_jepsen`)가 잠금을 잡고 처음부터 돌아 success(02:30:11). **동시에 두 개가 돈 때가 없고, 계산을 끝까지 한 것은 시도 2 하나다.**
- 재발 방지와 한계:
  - 멈춘 컨테이너는 원인 조사용으로 남는다(지우지 않음). 쌓이면 사람이 지운다.
  - 시도 2는 재설정 주기(5분)만큼 늦게 시작한다. 그동안 태스크는 running으로 보인다.
  - 계산 컨테이너에 compose 라벨(`com.docker.compose.service=spark-job`)이 함께 붙어 있다(이미지 라벨이 상속됨). `docker compose down --remove-orphans` 같은 명령이 이 컨테이너를 compose 것으로 다룰 수 있다 [확인 필요].
  - CeleryExecutor나 k8s로 가면 태스크 프로세스가 스케줄러 밖에 있어 이 처방(시작 때 전부 멈춤)은 틀린 처방이 된다. 그때는 TI 상태와 라벨을 대조해야 한다.
- 상태: **처방함, 처방 뒤 1회 확인.**

## F-33. 에이전트 컨테이너에서 읽기 전용 레포 복제가 git 소유자 검사에 걸림 (결정 112 A1.5, 2026-11-15 02:31)
- 태그: 컨테이너 권한, git
- 증상: `docker compose run --rm agent-job python -m agent.run --data S1` 첫 실행이 0초 만에 `git clone ... /opt/project ...` 실패: "detected dubious ownership in repository at '/opt/project/.git'". `agent/run.py`의 `git()`은 이미 `git -c safe.directory=* ...`로 불렀다.
- 원인: 붙인 레포는 컨테이너 안에서 root 소유로 보이고 실행 사용자는 uid 50000(job)이다. 로컬 경로 clone은 원본 쪽에서 하위 git(upload-pack)을 따로 띄우는데, 명령줄 `-c`도 `GIT_CONFIG_COUNT` 환경 변수 설정도 그 하위 프로세스의 소유자 검사에는 쓰이지 않았다(둘 다 같은 오류로 재현). `git config --global`로 넣으면 통과했다(같은 컨테이너에서 확인). 앞선 worktree 방식(측정 45)은 clone을 하지 않아 `-c`로 충분했다.
- 처방: `Dockerfile` agent-job에 `git config --system --add safe.directory '*'`. 이 이미지는 읽기 전용 레포와 `/scratch` 복제본만 다룬다. `git()`의 `-c`는 지움.
- 확인: 다시 빌드 뒤 같은 clone이 main `daa0ac6`를 받음. 첫 실패 실행도 `ops.agent_run`에 error로 남았다(`report.json`과 같은 내용).
- 상태: **처방함.**

## F-34. 판정이 dbt가 skip한 표를 읽다 오류로 끝남: 기준선(M7)과 후보(S3) (결정 112 A6, 2026-11-15 04:15~05:39)
- 태그: 판정 코드, dbt skip
- 증상: (1) M7 수정 3회가 에이전트 수정·후보 빌드까지 끝난 뒤 G3에서 `UndefinedTable: relation "mart_sbx_base.dong_profile" does not exist`로 끝남(rc=1, 판정 없음). (2) S3 판정이 G1에서 `stg_dbt_sbx_cand.batch_status does not exist`로 끝남. 운영 표·상품은 그대로였다(실행마다 `ops_snapshot` 비교).
- 원인: `dbt build`는 테스트가 실패하면 그 하류 모델을 skip한다. (1) M7 기준선은 전월세 1,320단위가 거절돼 `rent_listing`이 0행이 되고 `rent_listing_not_empty`가 실패해 `dong_profile` 등 하류가 만들어지지 않았다. (2) S3 후보는 단위 테스트 `rent_rows_rules`(산 지번은 대지구분 2)가 실패해 `rent_rows`부터 `batch_status`까지 skip됐다. 판정 코드는 두 쪽 표가 다 있다고 가정했다. S1·M1은 혼잡도 거절이 상품 표를 비우지 않아 드러나지 않았다.
- 처방(판단 121): `sandbox.diff`는 기준선 상품 표가 없으면 운영 `mart`(고객이 지금 받는 마지막 정상본)와 비교한다. `sandbox.judge`는 후보 빌드에 실패 노드가 있으면 G1만으로 바로 fail을 낸다. `agent/run.py`는 fail이면 평가용 상품 비교를 하지 않는다.
- 확인: M7 patch 3개를 판정만 다시(판단 120): 3회 모두 needs_decision(G1~G4 통과, G5 전월세 승인 단위 정제 행 187 차이), 정답표와 같다. S3: fail(G1, `rent_rows_rules` 실패). 운영 변화 0.
- 상태: **처방함.**
## F-35. GitHub CI `check`가 origin/main(5d05f47)에서 실패, 로컬 HEAD도 다른 이유로 실패 (handoff_agent_finish.md G0, 2026-11-15 19:35~19:45)
- 태그: CI, 시험 환경
- 증상: origin/main 5d05f47의 마지막 GitHub Actions `check`가 `정제 규칙 단위 테스트` 단계에서 exit 1, dbt parse는 건너뜀. Actions 로그는 보지 못했다(토큰에 Actions 읽기 없음).
- 재현: CI와 같은 조건(`python:3.12-slim-bookworm`, `pip install -r requirements.txt`, ci.yml `check`의 명령 그대로)을 컨테이너에서 커밋마다 돌림(`logs/g0/ci_by_commit.txt`, `ci_5d05f47.log`, `ci_85faabc.log`).
  - 5d05f47, 3efb02f, 0eea892: `tests/test_runner_swap.py`가 `common.read_env("DATABASE_URL")`로 `.env`를 읽다 `FileNotFoundError`. DB가 있어야 도는 시험이 `tests/test_*.py` 이름이라 CI가 단위 테스트로 돌렸다. 294ee65(결정 105 옛 경로 은퇴)에서 이 파일이 지워져 그 뒤 커밋은 통과(294ee65, 95df778, d5664d5, fae014f, 51211d6, 50924ed, ecac268 OK. ui 갈래 3392122는 지우기 전에 갈라져 실패, 병합 ecac268에서 풀림).
  - daa0ac6부터 HEAD 85faabc까지: `tests/test_ui_pipelines.py`가 실패. 이 시험은 DAG 파일을 가짜 Airflow 모듈로 실행해 일정만 모으는데, 결정 109로 DAG가 `dags/jobs.py`(DockerOperator, `docker.types.Mount`, compose 환경 변수 `HOST_PROJECT_DIR`·`DATABASE_URL`)를 import하게 됐다. 가짜 모듈 목록에 docker provider가 없어 `ModuleNotFoundError`. 그 뒤 결정 112의 `agent_triage`(DAG를 읽을 때 `.env`를 읽음)와 시험 DAG `test_scheduler_kill`로 DAG 수도 8개가 아니게 됐다.
- 원인: 로컬에서는 이 시험을 돌리는 Python이 3.10이라(tomllib 없음) 결정 109 뒤 아무도 돌리지 않았고, origin에 push하지 않아 GitHub CI도 돌지 않았다. CI 실패는 push를 멈춘 5d05f47 한 번만 보였다.
- 처방(시험만 고침, 운영 DAG 코드는 그대로):
  - `tests/test_ui_pipelines.py`: 가짜 모듈에 `airflow.providers.docker.*`, `docker`, `docker.types`, `common`을 더함. `JobOperator`가 상속하므로 `DockerOperator`는 빈 클래스. compose 환경 변수 세 개를 시험 값으로. 손으로만 도는 `test_*.py` DAG는 뺌. 기대 DAG 수 8 → 9.
  - `ui/pipelines.toml`: `agent_triage = "*/30 * * * *"`를 wh_load 파이프라인의 dags에 더함(관문 거절·dbt 실패 신호를 받는 1차 대응).
- 확인: 고친 작업 트리 사본에서 CI 두 단계 모두 통과(`logs/g0/ci_worktree.txt`: 단위 테스트, `rent_fingerprint --selftest`, `dbt parse --no-partial-parse`). GitHub에서의 통과는 A5 동기화 PR에서 확인한다 [확인 필요].
- 남은 것: DB가 필요한 시험은 `check_*.py`, CI 단위 테스트는 `test_*.py`라는 이름 규칙이 문서에 없다. 로컬에 Python 3.12가 없어 CI와 같은 시험을 손으로 돌리려면 위 컨테이너 명령을 쓴다.
- 상태: **처방함(로컬 재현 통과).**
