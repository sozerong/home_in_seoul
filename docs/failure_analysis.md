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

분류(태그, 여러 개 가능): `메모리`(OOM, GC, spill) / `분포`(스큐, 병렬도, 파티션) / `저장`(작은 파일, 커밋, 스냅샷) / `연결`(JDBC, 타임아웃, 재시도) / `타입·스키마`(형변환, 방언, 새 필드) / `정확성`(값 어긋남, 중복, 누락) / `운영`(멱등, 재실행, 순서)

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

## F-4. 상가정보를 서울 전체로 넓히자 `fact_store`가 140초: 행정동 경계에 공간 인덱스가 없어 모든 업소를 모든 동과 비교
- 태그: 분포, 메모리 아님(계산량)
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
- 재발 방지: 없음(처방 전).
- 남은 것: `ST_Transform` 몫, 인덱스를 넣었을 때 시간은 재지 않았다.


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
- 처방과 결과: 사용자 지시("오류는 기록하고 해결하지 말 것", 2026-11-13)에 따라 고치지 않았다.
- 재발 방지: 없음(처방 전).
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
- 처방과 결과: 사용자 지시("오류는 기록하고 해결하지 말 것", 2026-11-13)에 따라 고치지 않았다. 8절에서 옛 표를 지우면 내보내기가 실패하므로 그 전에 처방이 필요하다.
- 재발 방지: 없음(처방 전).
- 남은 것: 같은 문제가 `measure/`·`warehouse/sql/ops/`에 더 있는지는 8절 목록에 함께 적었다.

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
- 처방과 결과: 사용자 지시("오류는 기록하고 해결하지 말 것", 2026-11-13)에 따라 고치지 않았다.
- 재발 방지: 없음(처방 전).
- 남은 것: 관문 이전(측정 22)의 M1·M2·M4·M6은 stg 변이라 정밀도를 바로 비교할 수 없다.
