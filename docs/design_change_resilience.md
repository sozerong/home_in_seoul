# 상세 설계: 컬럼 리니지(P4)와 혼잡도 정제 SQL 이전

이 문서는 Claude Code가 구현할 때 따르는 설계서다. `CLAUDE.md`(작업 규칙)와 `change_resilience_plan.md`(왜, 무엇을, 어떤 순서로)를 먼저 읽고, 이 문서에서 **어떻게**를 읽는다. 설계 결정과 근거는 `docs/decisions.md` 결정 40~65에 있고, 여기에는 결정 번호만 단다. 이 문서와 결정 문서가 다르면 결정 문서를 따르고, 다른 곳을 사용자에게 알린다.

작성: 2026-11-13. 사전 시험 근거는 `docs/measurements.md` 측정 16.

## 0. 범위와 작업 순서

| 순서 | 작업 | 근거 결정 | 끝나면 |
|---|---|---|---|
| 1 | P0 마무리: S6a, S6b 기준값 측정 | 52, 65 | 측정 17, 18 |
| 2 | P4-1: 정답 모델 6개의 정답 작성, 사용자 검토 뒤 커밋 | 49, 59 | `tests/lineage_truth/*.csv` |
| 3 | P4-2: 추출기(스키마 추론, 간선 추출, 상품 층) | 41, 44, 46, 53, 54, 55 | `lineage/` 패키지와 시험 |
| 4 | P4-3: `lineage` 스키마 DDL(표 2개, 뷰 4개)과 Spark 실행·적재 | 47, 48, 56 | `warehouse/sql/08_lineage.sql`, 첫 회차 |
| 5 | P4-4: lineage DAG와 wh_load Asset 연결 | 50, 57 | `dags/lineage.py` |
| 6 | P4-5: MCP 서버 | 51, 58 | `lineage_mcp/`, `.mcp.json` |
| 7 | P4-6: 정확성 측정(정답 대조, 변이 시험), 엔진 비교 측정, S4-1 재측정 | 40, 44, 49, 59 | 측정 기록 |
| 8 | 혼잡도 정제 SQL 이전 | 42, 43, 60~64 | 이전 전후 리니지 비교, S1 재측정 |

각 순서 안에서도 `CLAUDE.md` 1절대로 파일 하나, 함수 하나 크기로 끊고, 만들기 전에 설명하고, 만든 뒤 멈춘다. 순서 2는 반드시 순서 3보다 먼저 커밋한다(정답이 파서 결과에 끌려가지 않게, 결정 49).

---

## 1. 컬럼 리니지 (P4)

### 1.1 한 장 요약

```
target/compiled/seoul_dong/models/**/*.sql (dbt build가 만듦, 모델 17개)
  + warehouse/sql/01~06 (stg DDL) + warehouse/dbt/seeds/*.csv (헤더)
  + warehouse/export_product.py의 TABLES (상품 파일 목록)
        │
        ▼  1단계 (드라이버, 순서대로): 스키마 추론. 모델을 의존 순서로 qualify해 출력 컬럼을 얻는다 (결정 53)
        ▼  2단계 (Spark, 파일마다 병렬): 간선 추출 함수 extract_edges(sql_text, schema) (결정 44, 54)
        ▼  3단계 (드라이버): 상품 층 간선 더하기 (결정 55)
        ▼  4단계: Spark JDBC로 lineage.edge_raw에 회차와 함께 덧붙임 (결정 47, 48, 56)
lineage.run, lineage.edge_raw  ──>  뷰 4개  ──>  Metabase, export_product(P5), MCP 서버(결정 51, 58)
```

### 1.2 파일 구조

```
lineage/                     새 패키지 (레포 루트, Python 3.12)
  __init__.py
  schema.py                  1단계: 스키마 추론 (stg DDL, seed CSV 헤더, 모델 qualify)
  edges.py                   2단계: extract_edges(sql_text, model, schema) -> list[dict]. 순수 함수
  product.py                 3단계: 상품 층 간선
  extract.py                 진입점. --engine spark|python. 1~4단계를 묶는다
tests/test_lineage_edges.py  추출 함수 시험 (DB, Spark 없이 돈다. CI에 붙는다)
tests/lineage_truth/*.csv    정답 (순서 2에서 먼저 커밋)
measure/lineage_score.py     정답 대조 점수 (맞음, 놓침, 틀림, 종류별)
measure/lineage_mutation.py  변이 시험
warehouse/sql/08_lineage.sql lineage 스키마, 표 2개, 뷰 4개, 읽기 전용 역할
dags/lineage.py              lineage DAG
lineage_mcp/server.py        MCP 서버 (stdio)
.mcp.json                    Claude Code가 MCP 서버를 띄우는 설정
```

`extract_edges`는 Spark를 import하지 않는 순수 함수다. 엔진(Python 단독 / Spark)은 `extract.py`에서만 고른다. 이것이 결정 44의 "엔진은 바뀌는 부분, 흐름은 유지하는 부분"을 코드 경계로 만든 것이다.

### 1.3 1단계: 스키마 추론 (결정 53)

sqlglot이 `SELECT *`를 펼치고 테이블 이름 없이 쓴 컬럼을 풀려면 테이블마다 컬럼 목록이 필요하다. DB에 묻지 않고 레포 안의 것만으로 만든다.

1. **stg 소스**: `warehouse/sql/01~06`의 `CREATE TABLE`을 sqlglot으로 파싱해 컬럼 목록을 얻는다(격리 표 포함 14개).
2. **seed**: `warehouse/dbt/seeds/*.csv` 첫 줄(헤더). manifest의 `column_types`는 일부만 적을 수 있어 쓰지 않는다.
3. **모델**: manifest의 `depends_on`으로 의존 순서를 정하고, 그 순서대로 컴파일된 SQL을 `qualify(schema=지금까지 쌓은 스키마, validate_qualify_columns=False)`해 `selects`의 이름을 그 모델의 컬럼으로 쌓는다.

이름 규칙: 스키마 사전의 키는 컴파일된 SQL 그대로(`seoul.wh_build.dim_dong`)로 쌓고, 간선을 만들 때 4.2의 정규화를 한다.

사전 시험(측정 16): 모델 17개 모두 파싱·qualify 성공, `dong_profile` 출력 28개가 상품 파일 헤더 28개와 같다. 이 단계는 앞 모델의 결과가 뒤 모델의 입력이라 **순서대로만** 돌 수 있다. Spark로 나누지 않고 드라이버에서 돈다(결정 56).

### 1.4 2단계: 간선 추출 (결정 46, 54)

함수 계약:

```
extract_edges(sql_text: str, model: str, schema: dict) -> list[dict]
  dict 키: target_table, target_column, source_table, source_column,
           kind ('DIRECT'|'INDIRECT'), subkind, status ('ok'|'unresolved'|'parse_error'), detail
```

**DIRECT (값 관계)**: 출력 컬럼마다 `sqlglot.lineage.lineage(col, sql, schema, dialect='postgres')`의 잎 노드를 모은다. 잎의 `source`가 `exp.Table`이면 그 물리 테이블 이름으로 바꾼다(별칭 `st.am_peak_congestion` → `mart_build.dong_station.am_peak_congestion`). 세부 종류:

| subkind | 판정 |
|---|---|
| IDENTITY | 출력 식이 컬럼 하나 그대로 |
| AGGREGATION | 출력 식에 집계 함수(sum, count, max, percentile_cont 등)가 있음 |
| TRANSFORMATION | 그 밖의 계산(round, ST_Distance, 캐스팅, CASE의 결과 쪽) |

보정 두 가지(측정 16에서 본 것):
- `count(*)`처럼 컬럼이 없는 집계는 잎이 출력 이름 자신으로 나온다. 이때 FROM의 테이블을 출처로, `source_column = '*'`(테이블의 행), subkind AGGREGATION으로 둔다.
- jsonb 키 읽기 `raw->>'DOW_SE'`는 sqlglot이 `JSONExtractScalar`로 파싱하고, lineage는 `raw` 컬럼 하나로 돌려준다. 출력 식 안의 `JSONExtractScalar(Column raw, JSONPathKey X)`를 찾아 `source_column = 'raw.X'`로 바꾼다(혼잡도 이전 뒤에 쓰인다, 결정 60).

**INDIRECT (행 관계)**: lineage는 이것을 주지 않는다(측정 16: `am_peak_congestion`의 잎은 `f.congestion` 하나). `sqlglot.optimizer.scope.build_scope`로 쿼리를 scope(본문, CTE, 서브쿼리, LATERAL)로 나누고, scope마다 아래 절의 컬럼을 모은다.

| 절 | subkind |
|---|---|
| WHERE, 서브쿼리 조건(`in (select ...)`) | FILTER |
| JOIN ON, USING | JOIN |
| GROUP BY | GROUP_BY |
| HAVING | FILTER |
| ORDER BY (LIMIT과 함께면 고르기, 없으면 순서) | SORT |
| 윈도우 PARTITION BY, ORDER BY | WINDOW |
| 집계의 FILTER (where ...), CASE WHEN 조건 | CONDITIONAL |

컬럼이 CTE나 서브쿼리 scope를 가리키면(`<covered:scope>.bldn_id`) 그 scope 안으로 따라 들어가 물리 테이블 컬럼까지 풀어 간다.

**INDIRECT 간선을 어느 출력 컬럼에 붙이나** (붙이는 규칙):
1. WHERE, INNER JOIN, GROUP BY, HAVING, SORT: 그 scope를 거쳐 나가는 **모든** 출력 컬럼. 행 자체를 거르거나 묶기 때문이다.
2. LEFT JOIN의 ON: 그 조인의 **오른쪽 소스에서 온** 출력 컬럼에만. 왼쪽 행은 빠지지 않기 때문이다. 예: `dong_profile`의 `mr.lease_type = '월세'`는 `monthly_*`에만 붙고 `jeonse_*`, `dong_id`에는 붙지 않는다.
3. CONDITIONAL(집계 FILTER, CASE 조건): 그 식이 들어 있는 **그 출력 컬럼에만**. 예: `commute_od`의 `filter (where age in (...))`는 `avg_minutes_age20_34`, `population_age20_34`에만.
4. "그 scope를 거쳐 나가는 출력 컬럼"은 출력 컬럼별 lineage 트리의 노드가 어느 scope에 속하는지로 판정한다.

**알려진 한계 (고치기 전에 측정에서 센다)**:
- LATERAL 안 `select s.*`에서 `a.bldn_id` 하나를 고를 때 lineage가 s의 컬럼 7개를 모두 출처로 돌려준다(측정 16). 값 관계 과다 귀속이다. 정답 대조에서 "틀림"으로 세고, 고칠지는 그 뒤 정한다.
- 파싱에 실패한 파일은 버리지 않고 `status='parse_error'` 행 하나로 남긴다(`CLAUDE.md` 4절 4번). 출처를 풀지 못한 컬럼은 `status='unresolved'`로 남긴다.

### 1.5 3단계: 상품 층 (결정 55)

`warehouse/export_product.py`의 `TABLES` 키(상품 파일 이름)마다, 마트 표의 모든 컬럼에 대해 `product.<파일>.<컬럼> ← mart.<표>.<컬럼>` IDENTITY 간선을 더한다. 내보내기가 `SELECT * FROM mart.{name}`이라 1:1이다. `TABLES`는 파일을 import하지 않고 sqlglot이 아니라 `ast` 모듈로 읽는다(실행하면 DB에 붙으므로).

### 1.6 4단계: 저장 (결정 47, 48, 56)

`warehouse/sql/08_lineage.sql` (Claude가 직접 작성하고 실행, `CLAUDE.md` 1절 예외):

```sql
CREATE SCHEMA IF NOT EXISTS lineage;

CREATE TABLE IF NOT EXISTS lineage.run (
    run_id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at     timestamptz NOT NULL DEFAULT now(),
    finished_at    timestamptz,
    status         text NOT NULL,          -- running / success / failed
    trigger        text NOT NULL,          -- asset(데이터 도착 뒤 wh_load) / manual(코드 작업)
    git_commit     text,
    sqlglot_version text NOT NULL,
    engine         text NOT NULL,          -- spark / python
    manifest_generated_at timestamptz,     -- 입력이 언제 컴파일된 것인지
    sql_files      integer,
    edges          integer
);

CREATE TABLE IF NOT EXISTS lineage.edge_raw (
    run_id         bigint NOT NULL REFERENCES lineage.run,
    sql_file       text NOT NULL,          -- target/compiled 기준 경로. 상품 층은 'export_product.py'
    file_kind      text NOT NULL,          -- model / test / product
    target_table   text,                   -- 원문 그대로 (wh_build.dim_dong 등)
    target_column  text,
    source_table   text,
    source_column  text,                   -- '*'는 테이블의 행, 'raw.X'는 jsonb 키
    kind           text,                   -- DIRECT / INDIRECT
    subkind        text,
    status         text NOT NULL,          -- ok / unresolved / parse_error
    detail         text                    -- 오류 메시지, 식 일부
);
CREATE INDEX IF NOT EXISTS edge_raw_run ON lineage.edge_raw (run_id);
```

원본 표에는 정규화하지 않은 값을 그대로 둔다(빌드 스키마 이름, 테스트 SQL에서 나온 간선 포함). 거르고 바꾸는 일은 뷰가 한다(결정 48). 회차 한 번의 적재는 `run` 행을 먼저 `running`으로 넣고, 간선을 쓰고, 끝에 `success`로 바꾼다. 중간에 실패하면 `failed`로 남고 뷰는 그 회차를 읽지 않는다.

입력 범위: 처음에는 `file_kind = model`만 추출한다. 테스트 SQL 65개는 결정 48대로 원본에 넣을 수 있게 열어 두지만, 첫 구현에서는 뺀다. 쓰는 곳이 없기 때문이다(`CLAUDE.md` 4절 6번). 넣기로 하면 이 문서를 고친다.

### 1.7 뷰 4개 (결정 48)

공통 정규화 뷰 `lineage.edge`(나머지 뷰가 읽는 바탕):
- 최신 회차: `status = 'success'`인 `run_id` 중 가장 큰 것.
- `file_kind <> 'test'`, `status = 'ok'`.
- 이름 정규화: `seoul.` 접두어를 빼고 `wh_build` → `wh`, `mart_build` → `mart`(앞으로 `stg_dbt_build` → `stg_dbt`).

| 뷰 | 모양 | 쓰는 곳 |
|---|---|---|
| `lineage.table_edge` | (source_table, target_table, kinds) 표 단위로 묶은 간선 | 그림, Metabase |
| `lineage.downstream` | (from_table, from_column, to_table, to_column, depth, path_kinds) 재귀로 닫은 하류 쌍 | S1, S4, S6, MCP |
| `lineage.upstream` | downstream을 뒤집은 것. 상품 컬럼에서 시작해 stg(이전 뒤에는 raw)까지 | P5, MCP |
| `lineage.run_diff` | 최신 성공 회차와 그 직전 성공 회차의 간선 차이 (added / removed) | 혼잡도 이전 전후(결정 45), 데이터 도착 뒤 |

재귀는 `WITH RECURSIVE`로 하고, 같은 경로를 두 번 밟지 않게 지나온 노드 배열로 순환을 막는다. `path_kinds`는 경로 위 간선 종류의 집합이다. 하나라도 INDIRECT가 있으면 "행이 빠지는 영향"으로 읽는다.

권한: `lineage_reader` 역할을 만들어 `lineage` 스키마에 SELECT만 준다. MCP 서버가 이 역할로 붙는다(결정 58). 비밀번호는 `.env`에 둔다.

### 1.8 Spark 실행 (결정 44, 56)

`python -m lineage.extract --engine spark --trigger manual|asset`:
1. 드라이버: 1단계 스키마 추론. 결과 사전을 `broadcast`로 넘긴다.
2. `sc.parallelize([(파일 경로, SQL 문자열), ...])` → `flatMap(lambda x: extract_edges(...))`. 파일 하나가 일 하나다.
3. 드라이버: 상품 층 간선을 더해 DataFrame으로 만든다.
4. `df.write.jdbc(url, "lineage.edge_raw", mode="append")`. 드라이버 jar는 이미 있다(`/opt/spark-jars/postgresql-42.7.14.jar`).

`--engine python`은 2단계를 `itertools.chain.from_iterable(map(...))`로 돌리고 psycopg로 쓴다. 두 엔진의 간선 집합이 같아야 한다(결정 44의 검증).

설치: `sqlglot`을 `requirements.txt`(CI와 Windows .venv)와 spark-venv(Dockerfile)에 같은 버전으로 고정한다. 사전 시험은 30.31.0으로 했다. 새 의존성이므로 결정 문서에 이유가 있다(결정 54).

### 1.9 lineage DAG (결정 50, 57)

- `dags/assets.py`에 `WH_BUILT = Asset(name="wh.built", ...)`를 더한다.
- `dags/wh_load.py`의 `export_product` 작업에 `outlets=[WH_BUILT]`를 단다. 상품까지 나간 뒤에 알린다(상품 층 간선이 export_product.py를 읽으므로 같은 코드 기준이 된다).
- `dags/lineage.py`: `schedule=WH_BUILT`, 작업 하나 `extract_lineage`(BashOperator, spark-venv python으로 `lineage.extract --engine spark --trigger asset`). `on_failure_callback=notify_failure`. 실패해도 wh_load와 상품은 영향이 없다.
- 코드 작업 뒤에는 Airflow 화면에서 lineage DAG를 손으로 돌린다(trigger=manual로 남기려면 DAG params로 넘긴다).

### 1.10 MCP 서버 (결정 51, 58)

- 위치와 실행: `lineage_mcp/server.py`, Python MCP SDK의 FastMCP, stdio. Claude Code가 Windows 호스트에서 레포 루트의 `.mcp.json`으로 띄운다. DB는 `localhost:5433`에 `lineage_reader`로 붙는다.
- 도구 5개 (모두 뷰를 읽기만 한다. 자유 SQL 도구는 두지 않는다):

| 도구 | 입력 | 돌려주는 것 |
|---|---|---|
| `downstream` | table, column, include_indirect=True | 영향받는 컬럼 목록(깊이, 경로 종류). 상품 컬럼은 따로 표시 |
| `upstream` | table, column | 출처 컬럼 목록 |
| `table_edges` | 없음 또는 table | 표 단위 간선 |
| `run_diff` | 없음 | 최신 두 회차 차이 |
| `find_columns` | 이름 일부 | 지도에 있는 (표, 컬럼) 후보. 정확한 이름을 모를 때 |

- 각 응답 맨 위에 회차 정보(run_id, 시각, git 커밋)를 붙인다. 지도가 코드보다 늦었는지 LLM이 판단할 수 있게.

### 1.11 정확성: 정답과 변이 시험 (결정 49, 59)

**정답 모델 6개** (17개 중 어려운 패턴이 있는 것):

| 모델 | 들어 있는 패턴 |
|---|---|
| `mart.dong_station` | CTE 3개, LATERAL + 거리 정렬 + LIMIT 1, 필터(dow, slot), PostGIS, 스칼라 서브쿼리 |
| `mart.commute_od` | CTE 3개, 정렬 + LIMIT 6, `in (select)` 필터, 집계 FILTER(CONDITIONAL), CASE, PostGIS |
| `mart.dong_rent_summary` | `percentile_cont ... within group`, `count(*)`, GROUP BY |
| `mart.dong_profile` | LEFT JOIN 4개, ON 안의 상수 조건(lease_type) |
| `wh.map_congestion_station` | UNION ALL, seed, DISTINCT, `lpad` 조인 조건 |
| `wh.map_dong_mois` | 윈도우 함수 |

정답 파일 `tests/lineage_truth/<모델>.csv`: `target_column,source_table,source_column,kind,subkind,note`. 순서 2에서 Claude Code가 SQL을 읽고 쓰되, **파서 코드가 레포에 생기기 전에** 사용자가 검토하고 커밋한다. 측정 15의 판단(`dow` → `am_peak_congestion` FILTER 등)은 그대로 정답에 들어간다.

점수(`measure/lineage_score.py`): 종류(DIRECT/INDIRECT)별로 맞음, 놓침, 틀림 수. 놓친 것과 틀린 것은 고치기 전에 패턴별로 분류해 측정 기록에 적는다.

**변이 시험** (`measure/lineage_mutation.py`): 정답 간선 중 6개를 실행으로 확인한다.

| # | 바꿀 stg 컬럼 | 바꾸는 방법 | 영향이 있어야 할 상품 컬럼 (정답 기준) |
|---|---|---|---|
| M1 | `stg.congestion.dow` | '평일' → '주중' | `dong_profile.am_peak_congestion` (FILTER) |
| M2 | `stg.congestion.congestion` | 모두 +1 | `dong_profile.am_peak_congestion` (DIRECT) |
| M3 | `stg.station_master.lat`, `lot` | 한 역의 좌표를 옮김 | `dong_profile.nearest_station_*` (SORT, DIRECT) |
| M4 | `stg.rent.monthly_rent` | 월세 일부를 0으로(0이면 전세, 01_stg_rent.sql) | `dong_profile.monthly_*`, `jeonse_*` (JOIN, GROUP_BY) |
| M5 | `stg.living_migration.age` | '25' → '26' | `commute_od.avg_minutes_age20_34`, `population_age20_34` (CONDITIONAL), `basis`, `avg_minutes` |
| M6 | `stg.sbiz_store.sub_cd` | 편의점 코드(G20405) 일부를 다른 코드로 | `dong_profile.convenience_store*` (CONDITIONAL) |

방법: 실제 `stg`, `wh`, `mart`는 건드리지 않는다.
1. `stg`를 `stg_mut` 스키마로 복사하고 위 변이 하나를 가한다.
2. dbt를 `--vars '{source_schema: stg_mut, build_suffix: _mut}'`로 돌려 `wh_mut`, `mart_mut`에 만든다. 이를 위해 `sources.yml`의 `schema`와 `generate_schema_name` 매크로가 var를 읽게 고친다(기본값은 지금과 같아야 하고, 기본값으로 돌린 결과가 바뀌지 않음을 두 방향 `EXCEPT ALL`로 확인한다).
3. 상품 표(`mart_mut.*`)를 지금 `mart.*`와 컬럼마다 비교해 값이 바뀐 컬럼과 행 수가 바뀐 표를 적는다. 이것이 실행으로 얻은 정답이다.
4. 끝나면 `stg_mut`, `wh_mut`, `mart_mut`를 지운다.

변이 하나에 dbt build 한 번(약 1분, 측정 8)이 든다.

### 1.12 측정 (P4-6)

- **정확성**: 정답 6개 모델 대조 결과와 변이 시험 6개 결과.
- **엔진 비교**: 같은 입력(모델 17개)으로 `--engine python`과 `--engine spark`를 각 3회. 프로세스 시작부터 끝까지 시간, 최대 메모리, 간선 집합이 같은지. 이 규모에서 Spark가 느리면 느리다고 쓴다(결정 44).
- **S4-1 재측정**: 측정 15와 같은 질문(`stg.congestion.dow`가 바뀌면)을 `lineage.downstream`(또는 MCP `downstream`)으로 풀어 후보 수, 결과, 단계를 측정 15와 나란히 적는다.

---

## 2. 혼잡도 정제 SQL 이전 (결정 42, 43, 60~64)

### 2.1 바뀌는 흐름

```
지금:   레이크 XML ──(transform/congestion.py: 파싱 + 이름 대응 + 정제 + 검증 + 격리)──> stg.congestion ──> wh.fact_congestion ...
바꾼 뒤: 레이크 XML ──(transform/congestion_raw.py: 파싱만)──> raw.congestion (jsonb)
                    ──(dbt: stg_dbt.congestion, stg_dbt.congestion_quarantine + 테스트)──> wh.fact_congestion ...
```

받는 부분(XML 파싱)은 Python, 꺼내는 부분(이름 대응, 정제, 검증, 격리)은 SQL이다.

### 2.2 원본 표 (결정 60)

`warehouse/sql/09_raw_congestion.sql`:

```sql
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.congestion (
    collected_date date        NOT NULL,
    source_file    text        NOT NULL,
    row_no         integer     NOT NULL,   -- 파일 안에서 몇 번째 row
    raw            jsonb       NOT NULL,   -- row의 자식 태그 전부, 값은 문자열 그대로(공백 포함)
    loaded_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (collected_date, source_file, row_no)
);
```

`transform/congestion_raw.py`: 지금 `congestion.py`의 파일 읽기 부분과 같다(`subwConfusion_p*.xml`, `row` 요소, `child.tag: child.text or ""`). 수집일 하나를 지우고 다시 넣는다(멱등). 검사는 두 가지만 한다: 파일이 없으면 실패, row가 0개면 실패. 필드 검사(CF1)는 하지 않는다. 그건 SQL 쪽 일이다(2.5).

### 2.3 dbt 정제 모델 (결정 61, 62, 64)

- 위치: `warehouse/dbt/models/stg_dbt/`. `dbt_project.yml`에 `stg_dbt: +schema: stg_dbt`. 빌드 때는 `stg_dbt_build`에 만들어진다(지금 `generate_schema_name` 그대로).
- blue/green 범위: `swap_schemas`와 `drop_build_schemas`의 기본 목록을 `['stg_dbt', 'wh', 'mart']`로 늘린다. 매크로가 이미 목록을 받으므로 기본값만 바꾼다.
- 소스: `sources.yml`에 `raw` 소스(`raw.congestion`)를 더한다.
- 재빌드 방식: `table`로 매번 전체를 다시 만든다(결정 62). `raw.congestion`에 모든 수집일이 있으므로 처리 단위 멱등은 원본 쪽(수집일 지우고 넣기)이 맡고, 정제는 전체 재계산으로 늘 같은 결과를 낸다.
- 이름 대응: 원천 키를 SQL에서 직접 읽는다. 원천 이름이 바뀌면 `coalesce(raw->>'새이름', raw->>'옛이름')`처럼 별칭을 더한다(결정 64). 원천 키 목록은 매크로 `congestion_keys()` 한 곳에 두고 모델과 테스트가 함께 쓴다. 매크로는 컴파일 때 풀려 컴파일된 SQL에는 글자로 남으므로 리니지 파서가 읽는다.

`stg_dbt.congestion` (지금 `stg.congestion`과 컬럼 이름, 순서, 타입이 같아야 한다):

| 컬럼 | 만드는 법 | 지금 규칙 |
|---|---|---|
| collected_date, source_file, row_no | raw 그대로 | |
| dow, line, sttn_no, sttn_nm, direction | `btrim(raw->>'DOW_SE')` 등 | Python의 `strip()` |
| sttn_no4 | `lpad(sttn_no, 4, '0')` | CC2 |
| slot | `jsonb_each_text(raw)`로 풀고 키 `TIMEhhmm` → `make_time(hh, mm, 0)` | CC3, CC4 |
| congestion | 값을 `numeric(5,1)`로. 숫자가 아니거나 0 미만, 300 초과면 격리 | CC5, CV5 |

행 전체 격리(CV1~CV4): 신원 필드 5개 중 빈 값(CV1), 요일·방향 범위 밖(CV2), 호선 범위 밖(CV3), 역번호가 숫자 3~4자리가 아님(CV4). 해당하면 그 행의 39칸 모두 정제 결과에서 빠진다.

`stg_dbt.congestion_quarantine` (지금 `stg.congestion_quarantine`과 같은 모양, `quarantined_at`은 빌드 시각):
- CV1~CV4: 행 하나에 한 줄, `reasons`는 걸린 규칙을 정렬한 배열, `raw`는 원본 행 전체(공백 그대로).
- CV5: 칸 하나에 한 줄, `reasons = {CV5}`, `raw`는 신원 필드 5개 + 그 시각 한 칸(`jsonb_build_object`).

### 2.4 하류 연결

- `wh/fact_congestion.sql`의 `source('stg', 'congestion')` → `ref('congestion')`(stg_dbt).
- `dags/assets.py`: `RAW_CONGESTION`을 더하고 `WH_INPUTS`의 `STG_CONGESTION`을 바꾼다.
- `dags/seoul_api.py`: 혼잡도 작업이 `transform.congestion` 대신 `transform.congestion_raw`를 부른다. `stg_updated` 작업의 outlets도 바꾼다.

### 2.5 검사 위치 (결정 63)

| 지금 | 바꾼 뒤 | 심각도 |
|---|---|---|
| CF1 필드 집합이 정확히 같은가 | (1) 쓰는 키(신원 5 + 시간 39)가 하나라도 없는 행이 있으면 실패. (2) 모르는 키가 있으면 경고 | (1) error, (2) warn |
| CF2 (요일, 호선, 역번호, 방향, 시각) 중복 | `stg_dbt.congestion`의 (collected_date, dow, line, sttn_no, direction, slot) unique | error |
| CF3 격리 비율 5% 초과 | 수집일별 격리 칸 / 전체 칸 > 0.05인 수집일이 있으면 실패 | error |
| CF4 직전 수집일 대비 원본 행 수 10% 넘게 감소 | raw의 수집일별 행 수를 직전 수집일과 비교 | error |
| 변경 알림(직전 수집일과 값 비교) | `warehouse/sql/ops/`에 운영 뷰 하나 | 실패 아님 |

검사는 모두 dbt 테스트라 실패하면 blue/green으로 교체가 막히고 실제 `stg_dbt`, `wh`, `mart`, 상품은 그대로다. 대가: 혼잡도 검사 하나가 실패하면 그 회차의 **모든** 출처 갱신이 멈춘다(지금은 혼잡도만 멈춤). 결정 63에 적은 대로 받아들이고, 경고와 실패를 나눈 것으로 실패할 일을 "쓰는 키가 사라짐" 같은 진짜 위험으로 줄인다.

### 2.6 바꾸기 전과 같음 증명

1. 바꾸기 전에 `stg.congestion`(260,676행, 측정 13 기준)과 `stg.congestion_quarantine`을 그대로 둔다(Python 경로는 증명 끝까지 지우지 않는다).
2. 레이크의 혼잡도 수집일 4개(10-21, 10-26, 11-02, 11-03)를 `congestion_raw`로 원본 표에 넣고 dbt를 빌드 스키마까지만 돌린다.
3. `stg.congestion`과 `stg_dbt_build.congestion` 두 방향 `EXCEPT ALL` 차이 0. 격리 표는 `quarantined_at`을 빼고 같은 방법.
4. 교체 뒤 wh, mart 전체와 상품 파일 sha256이 바꾸기 전과 같은지(측정 8과 같은 방법).
5. 같으면 `transform/congestion.py`와 `tests/test_congestion.py`의 규칙 시험을 dbt 쪽 시험으로 옮기고 Python 정제를 은퇴시킨다. 옛 `stg.congestion` 표를 지울지는 사용자가 정한다.
6. 리니지를 다시 추출해 `lineage.run_diff`로 이전 전후 지도를 비교한다(결정 45): raw → stg_dbt 간선이 생기고, wh 이하 간선은 그대로여야 한다.
7. S1을 측정 13과 같은 방법으로 다시 잰다.

---

## 3. S6 기준값 측정 (결정 52, 65)

둘 다 측정 12~14처럼 실제 레이크와 DB에 쓰지 않는 방법으로 잰다. "지금 구조에서 이 요구가 오면"을 재는 것이므로 코드는 레포를 고치지 않고 임시 사본으로 바꾼다.

**S6a 편의 업종 추가**
- 요구: 생활 편의에 업종 하나를 더한다. 업종은 레이크 상가정보(서울 202606)에서 지금 6코드 밖이면서 서울 전체에 고르게 있는 것을 하나 골라 기록한다(이름과 코드).
- 잴 것: 고쳐야 하는 파일과 곳(`transform/sbiz_store.py`의 `ESSENTIAL`과 MF3, `mart/dong_amenity.sql`, `mart/dong_profile.sql`, `mart.yml`, `docs/data_contract.md`, `docs/rules.md`, 시험), 다시 돌려야 하는 범위(레이크 zip부터 stg를 다시 만드는지), 정제 실행 시간(DB 막고 2회), 상품 파일 컬럼이 늘어나는지.

**S6b 저녁 귀가 18~20시**
- 요구: 직장에서 집으로 오는 저녁 시간도 보고 싶다.
- 먼저 확인: 생활이동에서 "주간상주지 → 야간상주지"에 해당하는 이동유형 값이 무엇인지 `docs/data_sources.md`와 매뉴얼(`docs/seoul_living_mobility_manual.md`)에서 찾는다 [확인 필요]. 지금 범위는 HW, 월~금, 도착 07·08·09시다(`transform/living_migration.py` `HOURS`).
- 잴 것: 고쳐야 하는 파일과 곳(`HOURS`와 이동유형 조건, Spark 미리 거르기 조건, `wh/fact_commute.sql`, `mart/commute_od.sql` 또는 새 마트, 문서, 시험), 어디부터 다시 만드는지(레이크를 다시 풀지 않고 Iceberg부터 되는지), 한 달 Iceberg → 정제 실행 시간(DB 막고 1회).

---

## 4. 확인 필요 목록 (구현 중 실행으로 닫는다)

| # | 무엇 | 어디서 |
|---|---|---|
| U1 | MCP Python SDK 버전과 FastMCP stdio가 Windows에서 Claude Code와 붙는지 | P4-5 |
| U2 | `dbt --vars`로 source schema와 빌드 접미사를 바꿔도 기본 실행 결과가 같은지 | P4-6 변이 시험 전 |
| U3 | sqlglot이 `percentile_cont ... within group`, 집계 FILTER, `make_time`을 postgres 방언으로 문제없이 파싱하는지 (dong_rent_summary, commute_od는 사전 시험에서 qualify까지 통과) | P4-2 |
| U4 | Metabase가 새 스키마 `lineage`를 읽는지 | P4-3 |
| U5 | 생활이동의 귀가 방향 이동유형 값 | S6b |
| U6 | `stg_dbt.congestion` 전체 재빌드 시간이 수집일이 쌓여도 감당할 만한지 (1년이면 약 340만 행 추정) | 이전 뒤 측정 |

## 5. 하지 않는 것

- 테스트 SQL의 리니지 추출(1.6), 테이블별 사용자 정보(결정 48), 주기 실행(결정 44), 클라우드·k8s(결정 50의 남은 것), MCP의 자유 SQL 도구(1.10).
- P6, P3, P5, P7의 설계. 정제가 SQL로 옮겨진 뒤 다시 설계한다(결정 52).
