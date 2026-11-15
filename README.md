# 서울 동네 찾기 데이터 파이프라인

> 서울에서 일하게 된 사회초년생이 직장 위치, 예산(보증금·월세 상한), 출퇴근 시간 한도를 넣으면, 어느 동네(행정동)가 주거비·출퇴근·출근길 혼잡·생활 편의 면에서 맞는가?

**요약.** 이 프로젝트에서 한 일 네 가지와 결과다(수치의 조건은 링크한 측정 기록에).
- **원천 필드 변경 대응:** 정제를 Python에서 dbt SQL로 옮겼다. 쓰는 필드 이름 하나가 바뀌면 코드·시험·문서 5개 파일을 고치고 옛 수집일은 다시 못 돌리던 것([측정 13](docs/measurements.md#측정-13-s1-쓰는-필드-이름-변경혼잡도-기준값-변경에-강한-파이프라인-p0-2026-11-12))이, 매크로 별칭 1줄 + 정제 모델 1줄로 새·옛 수집일을 함께 처리하게 됐다([측정 23](docs/measurements.md#측정-23-혼잡도-정제-sql-이전-같음-증명-지도-변화-s1-재측정-순서-8-결정-42-6064-71-2026-11-13)).
- **컬럼 리니지:** dbt 컴파일 SQL을 sqlglot으로 파싱해 값 경로와 필터 경로를 나눈 컬럼 단위 리니지를 만들었다. 상류 컬럼 하나의 영향 후보가 상품 컬럼 28개(테이블 의존)에서 1개로, 조회 1회 0.24초([측정 20](docs/measurements.md#측정-20-s4-1-재측정과-s6-컬럼의-영향-조회-컬럼-리니지로-p4-3-2026-11-13)). 원천 값을 일부러 바꾼 변이 시험 7건에서 놓침 0([측정 31](docs/measurements.md#측정-31-변이-시험-다시-쓰기-raw-키-값-변이-design_remaining_workmd-7절-25-4-결정-84-2026-11-13)).
- **Airflow 지휘·계산 분리:** 무거운 계산을 DockerOperator로 작업마다 메모리 상한을 건 계산 컨테이너에서 돌린다([결정 109](docs/decisions.md#결정-109-무거운-계산을-airflow-컨테이너-밖-작업-컨테이너로-뺀다-standalone-해체--dockeroperator-결정-101-b를-docker-단계로-앞당김)). OOM 3회 유도에서 죽은 것은 그 계산 컨테이너뿐이고 스케줄러 하트비트 간격은 최대 6.4초([측정 48](docs/measurements.md#측정-48-계산-컨테이너-oom-유도와-지휘-영향-결정-109-h6-2026-11-14-23342337)). 스케줄러가 죽으면 주인 없는 계산 컨테이너가 같은 일을 두 번 돌던 것을, 스케줄러 시작 때 멈추게 처방했다([F-32](docs/failure_analysis.md#f-32-스케줄러가-갑자기-죽으면-계산-컨테이너가-주인-없이-끝까지-돌고-약-5분-뒤-airflow가-같은-작업을-새-시도로-다시-돌린다-결정-109-s2-2026-11-15-02080230)).
- **DuckDB 14억 행 정제 OOM:** 같은 SQL을 한 번에 돌리면 16GiB에서 656.0초에 메모리 오류([측정 49](docs/measurements.md#측정-49-duckdb-메모리-진단-무엇이-16gib를-잡는가-결정-110-d1-2026-11-14-23442355)). 배치마다 새 프로세스를 띄우고 결과를 Parquet로 넘겨 658.8초에 완주했고, Spark 결과(2,419초, [측정 42](docs/measurements.md#측정-42-f-9-처방-a--c--d-뒤-생활이동-전체-실행-1회-airflow-file_living_migration-결정-103-handoff_judgmentsmd-j-12j-14-2026-11-14-13001404))와 세 표 모두 0행 차이([결정 111](docs/decisions.md#결정-111-duckdb는-배치마다-새-프로세스에서-돌리고-배치-결과는-parquet-파일로-넘긴다-f-30-결정-110-후속), [측정 51](docs/measurements.md#측정-51-duckdb-배치마다-새-프로세스--parquet-넘김-생활이동-전체-완주-결정-111-d3d4-2026-11-15-00470109)).

이 질문에 답할 수 있도록, 위치 기준과 갱신 주기가 서로 다른 공공데이터 여러 종을 **2025년 행정동 하나의 기준**으로 맞추고, 부동산 플랫폼이 가져다 쓸 수 있는 **데이터 상품**(행정동 지표 파일 + 데이터 계약)으로 내보낸다. 질문 정의는 [docs/question.md](docs/question.md), 설계 결정은 [docs/decisions.md](docs/decisions.md)에 있다.

## 구조

```mermaid
flowchart LR
    subgraph 수집
        A1[국토교통부 전월세 API] --> L
        A2[서울 Open API<br>역사마스터, 혼잡도] --> L
        F1[손으로 받는 파일<br>생활이동, 상가정보,<br>SGIS 경계, 연속지적도] -->|lake/_inbox + 센서| L
    end
    L[(레이크<br>lake/출처/collected_date=YYYY-MM-DD/<br>받은 그대로)] --> S
    L -->|생활이동만, Spark| I[(레이크하우스 Iceberg<br>lh.living.migration_rows<br>24시간대 전체 행, 대상연월 파티션)]
    I -->|Spark SQL 실행기<br>spark_sql/living| S
    S[(스테이징 stg / stg_dbt<br>정제·검증, 배치 관문, 격리 테이블)] --> W
    W[(웨어하우스 wh<br>행정동 차원 + 공간 조인<br>EPSG:5179)] --> M
    M[(마트 mart)] --> P[데이터 상품<br>product/vYYYY.MM.DD/<br>Parquet, CSV, manifest]
    S -.-> O[운영 대시보드<br>Metabase, ops 뷰]
    P -.-> LN[컬럼 리니지<br>lineage 스키마, MCP]
    AF{{Airflow 3<br>출처별 DAG, 센서, Asset}} -.-> 수집
    AF -.-> S
    AF -.-> W
    AF -.-> M
    AF -.-> P
    AF -->|DockerOperator| JOB[[계산 컨테이너<br>seoul-spark-job / seoul-duckdb-job<br>작업마다 하나, 18GiB]]
    JOB -.-> I
    AF -->|agent_triage, 신호가 있을 때| AG[[SQL 에이전트 컨테이너<br>seoul-agent-job<br>레포 읽기 전용 + /scratch 복제본]]
    AG -.->|격리 스키마 *_sbx_*에서만 빌드| S
    AG -.->|수정안 bundle·patch, PR 본문| H[사람 검토]
```

| 층 | 무엇 | 어디 |
|---|---|---|
| 레이크 | 받은 그대로의 원본. 출처별·수집일별 폴더. 수정하지 않는다 | `lake/` (git 제외) |
| 레이크하우스 | 가장 큰 출처(생활이동, 한 달 1억 4천만 행)만 Iceberg 표로. 과거 스냅샷 조회, 대상연월 파티션 덮어쓰기. 카탈로그는 PostgreSQL `iceberg` DB, 파일은 `lakehouse/` (git 제외) | `transform/living_iceberg.py`, `transform/iceberg.py` (결정 35) |
| 스테이징 | 출처마다 정제·검증. 어긋난 행은 사유와 함께 격리하고, 배치(처리 단위)마다 관문이 승인·거절을 판정한다(결정 75, 100). 전월세·혼잡도·상가정보는 dbt(`stg_dbt`), 생활이동은 Spark SQL 실행기가 Iceberg 전체에서 경계 표 `stg.living_commute`, 판정 `stg.living_batch_status`, 격리 `stg.living_quarantine`을 한 트랜잭션으로 바꾼다(결정 88~90, 96) | `transform/`, `spark_sql/living/`, `stg`·`stg_dbt` 스키마, 규칙은 [docs/rules.md](docs/rules.md) |
| 웨어하우스 | 2025 SGIS 행정동 차원을 중심으로, 필지·역·업소를 공간 조인으로 행정동에 붙임 | dbt `warehouse/dbt/models/wh/`, 손 대응표 `warehouse/dbt/seeds/`, [docs/erd.md](docs/erd.md) |
| 마트 | 동네 요약, 출발 × 직장 출퇴근 표(생활이동 최근 6개 대상연월, 결정 36), 최근 6개월 신규 계약 | dbt `warehouse/dbt/models/mart/`. 검사는 dbt 테스트(`*.yml`, `warehouse/dbt/tests/`) |
| 데이터 상품 | 버전 폴더에 파일로 내보냄 | `warehouse/export_product.py`, [docs/data_contract.md](docs/data_contract.md) |
| 운영 대시보드 | 지금 정상인지, 어느 출처와 DAG를 고칠지 색으로 판정. 읽기 전용 계정으로 봄 | Metabase, 카드 `warehouse/sql/ops/`, 생성 `warehouse/metabase_setup.py` (결정 38, 39) |
| 파이프라인 UI | 파이프라인 구조, 실행·관문·품질, 컬럼 리니지 지도, 변경 시나리오를 DB와 Airflow API에서 읽어 보여 주는 읽기 전용 화면 | `ui/`, [docs/ui.md](docs/ui.md) (결정 106) |
| 일정 | 출처별 DAG, 손으로 받는 파일은 센서로 기다림, 스테이징이 바뀌면 웨어하우스 → 마트 → 데이터 상품까지 자동으로 다시 만듦 | `dags/` |
| SQL 에이전트 | 관문 거절·dbt 실패가 신호(`ops.agent_signal`)로 남으면, 에이전트가 자기 컨테이너에서 레포 복제본의 정제 SQL만 고치고, 파이프라인 코드가 격리 스키마에서 G1~G5로 판정한다. 운영 스키마와 호스트 `.git`은 바꾸지 않고, 병합은 사람이 한다 | `agent/`, `dags/agent_triage.py`, [docs/design_agent.md](docs/design_agent.md) (결정 108, 112) |
| 실행 구조 | Airflow는 구성 요소별 컨테이너(API 서버, 스케줄러, DAG 처리기, 트리거러)로 지휘만 하고, Spark 계산은 작업마다 따로 뜨는 계산 컨테이너에서 돈다. 계산이 메모리를 넘쳐도 그 컨테이너만 끝나고 스케줄러는 산다(측정 48). 무거운 작업은 PostgreSQL advisory lock으로 컨테이너를 넘어 한 번에 하나 | `docker-compose.yml`, `dags/jobs.py`, `common.heavy_lock` (결정 109) |

## 데이터 소스
| 영역 | 데이터 | 받는 곳 | 갱신 | 받는 방법 |
|---|---|---|---|---|
| 주거비 | 오피스텔·연립다세대 전월세 실거래가 (국토교통부) | 공공데이터포털 API | 신고 지연(계약 후 30일 내) | `ingestion/molit_rent.py` (DAG `molit_rent`, 매주) |
| 출퇴근 | 서울 생활이동 (행정동) + 코드표 | 서울 열린데이터광장 파일 | 월 1회 | inbox (DAG `file_living_migration`, 매월 21일) |
| 혼잡 | 서울교통공사 지하철 혼잡도 | 서울 Open API | 분기 (API는 2분기 늦음, 결정 7) | `ingestion/seoul_api.py` (DAG `seoul_api`, 매주) |
| 역 | 서울시 역사마스터 | 서울 Open API | 수시 | 위와 같음 |
| 편의 | 소상공인 상가(상권)정보 | 공공데이터포털 파일 | 분기 | inbox (DAG `file_sbiz_store`, 수동) |
| 지역 | 통계청 SGIS 센서스용 행정구역경계(읍면동) | SGIS | 연 1회 | inbox (DAG `file_sgis_boundary`, 수동) |
| 지역 | 연속지적도 (필지, PNU) | 브이월드 | 수시 | inbox (DAG `file_parcel_boundary`, 수동) |

탐색 기록과 출처별 특이사항은 [docs/data_sources.md](docs/data_sources.md).

## 처음부터 실행하기

### 1. 준비
- Docker Desktop, Python 3.12
- 인증키: 공공데이터포털(전월세 2종 활용신청), 서울 열린데이터광장 인증키
- 프로젝트 루트에 `.env`를 만든다 (git에 올리지 않는다).
```
POSTGRES_PASSWORD=영문과숫자만
# DB 비밀번호를 바꿀 땐 위 POSTGRES_PASSWORD와 이 줄을 같이 바꾼다
DATABASE_URL=postgresql://seoul:영문과숫자만@localhost:5433/seoul
DATA_GO_KR_SERVICE_KEY=공공데이터포털_Decoding_키
SEOUL_OPEN_API_KEY=서울_열린데이터광장_키
# 운영 대시보드용. 아무 긴 값이나 넣는다
METABASE_DB_PASSWORD=영문과숫자만
METABASE_ADMIN_PASSWORD=영문과숫자만
# 파이프라인 UI의 읽기 전용 DB 계정(ui_reader)
UI_READER_PASSWORD=영문과숫자만
```

### 2. DB와 Python 환경
```
docker compose up -d db
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```
DB 구조 만들기 (00은 Airflow용 `airflow`, Iceberg 카탈로그용 `iceberg`, 대시보드용 `metabase` DB를 만든다. psql 전용 명령이 있어 컨테이너 안 psql로, 스테이징 01~06, 운영 07, 리니지 08, 원본 09~11, 생활이동 경계·판정·격리 12는 순서대로 실행기로. 혼잡도 정제(stg_dbt)와 wh·mart는 `wh_load`가 dbt로 만든다):
```
docker compose exec -T db psql -U seoul -d seoul -v ON_ERROR_STOP=1 < warehouse\sql\00_airflow_db.sql
for %f in (01_stg_rent 02_stg_station_master 03_stg_congestion 05_stg_sbiz_store 06_stg_boundary 07_ops 08_lineage 09_raw_congestion 10_raw_rent 11_raw_sbiz_store 12_stg_living_commute) do python -m warehouse.run_sql warehouse/sql/%f.sql
```
모든 DDL은 `IF NOT EXISTS`라 여러 번 실행해도 된다.

### 3. Airflow
`.env`에 Airflow 구성 요소가 함께 쓰는 값 세 줄을 더한다(비밀 둘은 아무 긴 값, fernet 키는 `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`):
```
AIRFLOW_FERNET_KEY=...
AIRFLOW_JWT_SECRET=...
# DockerOperator가 계산 컨테이너에 붙일 레포의 호스트 경로(비밀 아님)
HOST_PROJECT_DIR=C:/Users/me/new_proj
```
```
docker compose build
docker compose --profile jobs build spark-job duckdb-job agent-job
docker compose up -d
```
Airflow는 구성 요소별 컨테이너로 뜬다(결정 109): `airflow-init`(DB 준비 후 끝남), `airflow-apiserver`(화면·API), `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`, 그리고 계산 컨테이너를 만들 때만 쓰는 `socket-proxy`. Spark와 DuckDB는 Airflow 안에서 돌지 않고, 작업마다 `seoul-spark-job`·`seoul-duckdb-job` 컨테이너가 따로 뜬다(DAG는 DockerOperator, 손으로는 `docker compose run --rm spark-job ...`).
http://localhost:8080 (개발용, 로그인 없음, 내 PC에서만 열림). DAG는 처음에 일시정지 상태다.
무거운 작업(생활이동 정제, dbt 전체 빌드, 리니지 추출)은 pool `heavy`(크기 1)로 한 번에 하나만 돈다(`docs/resource_guard.md` L5). 처음 한 번 만든다:
```
docker compose exec airflow-scheduler airflow pools set heavy 1 "무거운 작업 한 번에 하나"
```
Airflow 이미지에는 dbt(`/opt/airflow/dbt-venv`)가, `seoul-spark-job` 이미지에는 Spark 3.5.9 + Iceberg 1.12.0(Java 17)이, `seoul-duckdb-job` 이미지에는 DuckDB 2.0.0이, `seoul-agent-job` 이미지에는 Claude Agent SDK 0.2.182와 같은 버전의 dbt, git이 들어 있다. Iceberg 연결 확인:
```
docker compose run --rm spark-job python -m common lock -- python -m transform.iceberg --selftest
```
SQL 에이전트(선택, 결정 108, 112): `.env`에 Max 구독 토큰(`claude setup-token`으로 만든 `CLAUDE_CODE_OAUTH_TOKEN`)과 리니지 읽기 계정 비밀번호(`LINEAGE_READER_PASSWORD`)가 있어야 돈다. `ANTHROPIC_API_KEY`가 환경에 있으면 API 과금이 되므로 에이전트가 멈춘다. 변이 사례로 평가하거나(`--data S1`, 정답표 `docs/agent_answer_key.json`), 사람이 고친 patch를 판정만 한다:
```
docker compose run --rm agent-job python -m agent.run --data S1
docker compose run --rm agent-job python -m agent.run --data S1 --diagnose
docker compose run --rm agent-job python -m agent.run --data S3 --cand-patch agent/s3_rule.patch
```
결과는 `logs\agent\<실행>\report.json`(G1~G5), 고친 브랜치는 같은 폴더의 `branch.bundle`·`branch.patch`, 보낼 PR 본문과 Slack 메시지는 `pr.json`·`slack.json`(GitHub 토큰·Slack 주소가 없으면 파일로만). Airflow에서는 `agent_triage` DAG(30분마다, 처음엔 일시정지)가 `ops.agent_signal`의 새 신호 하나를 집어 같은 컨테이너를 부른다.

DB를 화면에서 보며 SQL을 던지려면 http://localhost:5050 (pgAdmin, 개발용, 로그인 없음, 내 PC에서만 열림). 왼쪽 `Servers → seoul (PostGIS)`를 누르고 `.env`의 `POSTGRES_PASSWORD`를 한 번 입력한다. 쿼리는 `Tools → Query Tool`. 도형 칸(geom, point)은 결과 표의 지도 아이콘으로 지도에 그려 볼 수 있다.

### 4. 데이터 넣기
손으로 받는 파일은 받은 zip 그대로 `lake\_inbox\{출처}\`에 넣고 해당 `file_*` DAG를 실행한다. 스테이징이 바뀔 때마다 `wh_load`가 자동으로 돌기 때문에 **아래 순서대로** 넣는다(순서를 어기면 `wh_load`의 dbt 테스트 `not_null_dim_dong_mois_dong_cd`(행정안전부 코드를 정하지 못한 행정동)가 실패하고, 전월세는 dbt unit test는 통과하지만 법정동 코드표 `stg.bjd_code`가 비어 모든 행이 V8로 격리되고 배치 관문(결정 75)이 전월세 처리 단위를 모두 거절해 `rent_listing_not_empty`가 실패한다 [확인 필요: 관문 도입 뒤 다시 실행하지 않음]).

| 순서 | inbox 폴더 | 파일 이름 | DAG |
|---|---|---|---|
| 1 | `sbiz_store` | `소상공인시장진흥공단_상가(상권)정보_*.zip` | `file_sbiz_store` |
| 2 | `sgis_boundary` | `bnd_dong_*.zip` | `file_sgis_boundary` |
| 3 | `parcel_boundary` | `LSMD_CONT_LDREG_*.zip` (서울) | `file_parcel_boundary` |
| 4 | `living_migration` | `생활이동_행정동_*.zip` | `file_living_migration` (zip → Iceberg 한 달 약 4분, 그 뒤 Spark SQL 실행기가 10달 전체를 다시 계산해 약 53분([측정 38](docs/measurements.md)). 셔플·흘림 파일을 작업 공간 볼륨 /scratch에 최대 약 260GB 쓰고 호스트 C:를 328GB 줄였으므로(측정 38), C: 여유에서 남길 몫 50GiB를 뺀 값이 394GB(그 x 1.2)보다 작으면 실행기가 시작하지 않는다. 호스트 C: 여유로 약 448GB다. 모자라면 공간 회수(측정 36)부터 한다) |
| 5 | (API) | | `seoul_api`, `molit_rent` 일시정지 풀기. 켜는 순간 가장 최근 예정분이 한 번 돈다 |

과거 기간이 필요하면 백필한다(예: 전월세 4주):
```
docker compose exec airflow-scheduler airflow backfill create --dag-id molit_rent --from-date 2026-09-20 --to-date 2026-10-18 --reprocess-behavior none --max-active-runs 1
```

### 5. 마트와 데이터 상품
스테이징이 바뀔 때마다 `wh_load` DAG가 dbt 빌드(빌드용 스키마 `wh_build`·`mart_build`에 모델과 테스트), 스키마 교체(테스트를 모두 통과하면 `wh`·`mart`로), 내보내기를 차례로 돌린다(결정 30~32). 테스트가 하나라도 실패하면 실제 `wh`·`mart`와 상품은 그대로다. 손으로 다시 만들 때:
```
docker compose exec -w /opt/project/warehouse/dbt airflow-scheduler /opt/airflow/dbt-venv/bin/dbt run-operation drop_build_schemas
docker compose exec -w /opt/project/warehouse/dbt airflow-scheduler /opt/airflow/dbt-venv/bin/dbt build
docker compose exec -w /opt/project/warehouse/dbt airflow-scheduler /opt/airflow/dbt-venv/bin/dbt run-operation swap_schemas
python -m warehouse.export_product
```
모델 사이의 관계 그림은 `dbt docs generate` 후 `dbt docs serve`로 볼 수 있다.
결과: `product\vYYYY.MM.DD\` (파일 설명은 [docs/data_contract.md](docs/data_contract.md)).

### 6. 운영 대시보드
```
docker compose up -d metabase
python -m warehouse.metabase_setup
```
출력된 주소(http://localhost:3000/dashboard/번호)를 연다. 로그인은 `dev@example.com`과 `.env`의 `METABASE_ADMIN_PASSWORD`. 내 PC에서만 열린다. 카드를 고치려면 `warehouse/sql/ops/`의 SQL을 고치고 위 스크립트를 다시 실행한다(화면에서 손으로 고친 내용은 덮인다).

파이프라인 UI(읽기 전용, [docs/ui.md](docs/ui.md)):
```
docker compose exec -T db psql -U seoul -d seoul -v ON_ERROR_STOP=1 -v ui_pw=<.env의 UI_READER_PASSWORD> < warehouse\sql\13_ui_reader.sql
docker compose up -d ui
```
http://127.0.0.1:8090 을 연다. 내 PC에서만 열린다.

### 7. 확인
```
for %f in (tests\test_*.py) do python -m tests.%~nf
docker compose run --rm spark-job python -m tests.check_runner_swap
docker compose run --rm spark-job python -m tests.check_scratch
docker compose run --rm duckdb-job python -m tests.check_scratch
docker compose run --rm spark-job python -m tests.check_living_conflict
python -X utf8 tests\check_heavy_lock.py
docker compose run --rm agent-job python -m agent.run --selftest
docker compose run --rm agent-job python -m tests.check_agent_signal
docker compose exec -T db psql -U seoul -d seoul < warehouse\sql\demo.sql
```
`tests\test_*.py`는 DB 없이 돌아 CI(`.github/workflows/ci.yml`)도 같은 것을 돌린다. DB가 필요한 확인은 `tests\check_*.py`다(CI 밖). `demo.sql` 맨 위 `params`(직장 행정동, 예산, 면적, 출퇴근 한도)를 바꿔 가며 결과를 본다.

## 결과 예시
`demo.sql` 기본값: 직장 역삼1동(강남구), 월세, 보증금 3,000만 원·월세 80만 원 이하, 전용 40㎡ 이하, 출퇴근 50분 이하 **또는** 직선거리 3km 이내, 예산 안 신규 계약 3건 이상. 2026-11-14 00시 마트 기준(상품 v2026.11.14: 계약월 202606~202610, 생활이동 202602~202607), 직선거리 순 앞 8개 (매주 수집되면 숫자가 조금씩 바뀐다):

| 동네 | 이동 시간(분) | 직선거리(km) | 예산 안 계약 | 월세 중앙값(만 원) | 가까운 역 | 아침 최대 혼잡도(%) |
|---|---|---|---|---|---|---|
| 역삼1동 | 156.9 | 0.00 | 56 | 70 | 역삼 400m | 76.5 |
| 역삼2동 | 99.9 | 1.15 | 30 | 64 | 선릉 706m | 88.3 |
| 서초2동 | 85.2 | 1.25 | 10 | 72 | 양재(서초구청) 742m | 50.8 |
| 도곡1동 | 67.0 | 1.38 | 8 | 65.5 | 매봉 613m | 58.4 |
| 반포1동 | 82.6 | 1.45 | 26 | 63 | 사평 204m | 93.2 |
| 논현1동 | 103.8 | 1.45 | 43 | 70 | 논현 444m | 80.7 |
| 논현2동 | 78.9 | 1.74 | 15 | 69 | 학동 411m | 71.0 |
| 서초1동 | 61.7 | 1.81 | 64 | 65 | 남부터미널(예술의전당) 386m | 63.2 |

이동 시간은 생활이동의 평균 이동 시간이라 가까운 동네일수록 길게 나온다(근거리 직행 제외, 경유 포함). 그래서 직선거리를 함께 쓴다([결정 23](docs/decisions.md), [데이터 계약 6절](docs/data_contract.md)).

## 주장과 증거
이 파이프라인이 지킨다고 말하는 여덟 가지와, 그것을 실제로 돌려 확인한 방법이다. 강도와 한계는 [docs/guarantees.md](docs/guarantees.md), 수치의 조건은 [docs/measurements.md](docs/measurements.md)에 있다. 명령은 레포 루트에서 실행한다.

| 주장 | 증명 방법 | 수치 | 다시 돌리는 명령 |
|---|---|---|---|
| 원천이 바뀌어도 SQL만 고친다 | 같은 가정(쓰는 필드 이름 변경 S1, 요구 추가 S6b)을 정제를 SQL로 옮기기 전과 후에 시험 | S1: 옮기기 전에는 코드·시험·문서 5개 파일을 고쳐야 했고 고친 뒤에는 옛 수집일을 다시 못 돌렸다(측정 13). 옮긴 뒤에는 2줄(매크로 별칭, `coalesce`)로 10-21·11-03 두 수집일 모두 65,169행(측정 23). S6b: 정제 Python 4곳 이상과 DDL(측정 18) → 마트 파일 1개, 기존 상품 4표 차이 0(측정 34) | `docker compose exec -w /opt/project airflow-scheduler python -m measure.s1_sql`<br>`docker compose exec -w /opt/project airflow-scheduler python -m measure.s6b_sql` |
| 옮겨도 결과가 같다 | 옛 결과를 보관하고 새 경로로 다시 만든 뒤 두 방향 `EXCEPT ALL`(+ 열 타입, 상품 sha256) | 혼잡도 325,845행, 전월세 450,957행 + 격리 11행, 생활이동 Python → Spark SQL 차이 0(측정 23, 24, 33). 생활이동 실행기 처방(F-9) 전후 경계 13,782,826행·격리 31,935,758행·판정 10행 차이 0(측정 42). Airflow 지휘·계산 분리(결정 109) 뒤 계산 컨테이너에서 같은 실행: 같은 행 수, 상품 데이터 파일 8개 sha256이 분리 전과 같음(측정 46), 2회째도 같음(측정 52). 같은 생활이동 SQL을 엔진만 DuckDB로 바꿔 Spark 결과 세 표와 차이 0(측정 51) | `docker compose exec -T db psql -U seoul -d seoul < measure\living_keep_compare.sql` (보관본이 있을 때, guarantees 2.2) |
| 다시 돌려도 같다 | 같은 입력을 다시 처리하고 결과 비교 | 같은 생활이동 zip을 Airflow DAG로 다시 넣어 세 표 차이 0, 상품 파일 8개 sha256 같음(측정 42). Iceberg 같은 파티션 두 번 덮어쓰기 차이 0(측정 9). 전월세 처리 단위 백필 6회(측정 6) | 레이크의 zip을 `lake\_inbox\living_migration\`에 다시 넣고 Airflow에서 `file_living_migration` 실행 뒤 위 비교. 처리 단위 하나는 Airflow 화면에서 Clear |
| 전부 되거나 전부 안 된다 | 교체 도중 실패를 일부러 넣는다 | 스키마 교체: 둘째 스키마 실패 → 첫째도 취소(결정 32), 실제 wh_load에서도 1회(결정 61). 실행기 교체: 첫 표를 0행으로 두면 둘째 표도 이전 그대로(F-13). 실제 실행에서 세 표 교체가 한 트랜잭션 448.6초(측정 42) | `docker compose run --rm spark-job python -m tests.check_runner_swap` |
| 실패는 드러난다 | 나쁜 입력과 자원 고갈을 일부러 만든다 | 쓰는 키 이름 변경 → 관문 거절(측정 23, 30). 한 배치의 9.95%를 깨뜨림 → 그 배치만 거절, 다른 출처는 갱신(측정 25). 메모리 초과 → 그 계산 컨테이너만 종료 코드 137, 스케줄러 하트비트 간격 최대 6.4초(평소 약 5초), 같이 돈 DAG·dbt 성공(3회, 측정 48). 실제 DuckDB 측정이 컨테이너 상한에 닿았을 때도 Airflow는 그대로(측정 50). 임시 공간 상한 → DuckDB가 오류로 끝나고 VM은 멈추지 않음(측정 39, 48). 계산 중 스케줄러를 `docker kill`: 처방 전에는 같은 작업이 두 번 돎(F-32), 처방(시작 때 주인 없는 계산 컨테이너 멈춤) 뒤 한 시도만 끝까지(1회, guarantees 2.9) | `python -X utf8 -u -m measure.induce_guard mem 3`<br>`python -X utf8 -u -m measure.induce_guard disk` |
| 컬럼 리니지가 영향을 놓치지 않는다 | 원천 값을 일부러 바꾼 변이(M1~M7)를 격리 스키마에서 다시 빌드해, 실제로 바뀐 상품 컬럼이 지도의 예측 안에 드는지 본다. 관문을 출처별로 나눈 뒤 다시 잼 | 변이 7건 놓침 0([측정 31](docs/measurements.md#측정-31-변이-시험-다시-쓰기-raw-키-값-변이-design_remaining_workmd-7절-25-4-결정-84-2026-11-13)). 출처별 관문 뒤 같음 증명 120개 통과, 상품 sha256 같음([측정 35](docs/measurements.md#측정-35-f-7-처방-a출처별-관문-모델-같음-증명과-지도-정밀도-결정-97-failure_analysis-f-7-2026-11-13)). 영향 후보 28개 → 1개, 조회 0.24초([측정 20](docs/measurements.md#측정-20-s4-1-재측정과-s6-컬럼의-영향-조회-컬럼-리니지로-p4-3-2026-11-13)) | `docker compose exec -w /opt/project airflow-scheduler python -m measure.lineage_mutation` |
| DuckDB도 같은 SQL로 같은 결과를 낸다 | 생활이동 정제 SQL을 엔진만 DuckDB로 바꿔 배치마다 새 프로세스로 돌리고, Spark 결과 세 표와 두 방향 `EXCEPT ALL` | 658.8초 완주, `living_commute` 13,782,826행·`living_batch_status` 10행·`living_quarantine` 31,935,758행 모두 0행 차이. 배치 하나(202601)는 threads 12에서 넘쳐 threads 4로 다시 해서 성공([F-31](docs/failure_analysis.md#f-31-duckdb-202601-배치만-threads-12에서-흘리기-전에-8초-만에-메모리-오류-결정-111-d4-측정-51-2026-11-15-0059-0107), 원인 미확정). 1회([측정 51](docs/measurements.md#측정-51-duckdb-배치마다-새-프로세스--parquet-넘김-생활이동-전체-완주-결정-111-d3d4-2026-11-15-00470109)) | `docker compose run --rm duckdb-job python -m measure.duckdb_sql spark_sql/living --show batch_status --compare` |
| SQL 에이전트는 운영을 바꾸지 않고, 해석 변경을 통과시키지 않는다 | 정답을 먼저 정한 사례(원천 변경 S1·M1·M7, 대조군 4, 사람이 고친 규칙 S3)로 에이전트를 돌리고, 실행마다 운영 표 49개 행 수·상품 sha256을 앞뒤로 비교 | 거짓 통과 0, 원인 적중 12/12, 판정 일치 16/17(S3는 단위 테스트가 먼저 잡아 fail). 해석이 바뀌는 수정은 G5가 needs_decision으로 멈춤(M7 3/3 + 끝까지 1회, S3b). 허용 밖 다섯 경로를 고치라고 시킨 실행(O1)에서 Edit 거절 4건, Write·Bash는 도구 없음, 허용 밖 바뀐 파일 0. 26번 실행(A4 DAG 포함) 모두 운영 변화 0, 에이전트 컨테이너 최대 0.29GiB(측정 53, 54, 55) | `docker compose run --rm --no-deps agent-job python -m agent.run --data M1`<br>`python measure/ops_snapshot.py --compare <앞 스냅숏>` |

한계: 대부분 1회 증명이다(guarantees 0절 강도 ①). 매 실행 강제(③)는 관문과 계약 검사뿐이다. 생활이동 처방(F-9)은 결과가 같고 셔플을 절반으로 줄였지만 실행 시간은 줄지 않았다(측정 42). 계산을 작업 컨테이너로 뺀 뒤 1회째는 11.3% 느렸고(측정 46) 2회째는 18.3% 빨라(측정 52), 같은 일인데 세 실행이 36% 벌어져 분리가 속도에 주는 영향은 말할 수 없다. 같은 SQL을 DuckDB로 돌리면 배치마다 새 프로세스로 16GiB 안에서 658.8초에 끝나 Spark(1,971~2,419초)보다 약 3.0~3.7배 빠르다(측정 51, 1회). 다만 배치 하나(202601)가 threads 12에서만 넘치는 원인은 확정하지 못했다(F-31). SQL 에이전트의 허용 밖 편집 차단은 실제 실행 1회(O1, 측정 55)로 봤고, 금지 도구(Write, Bash)는 "불렀는데 막힘"이 아니라 "도구가 주어지지 않음"으로만 확인됐다. 에이전트의 실제 PR·Slack(A5)은 로컬 main이 GitHub main보다 앞서 있어 동기화를 기다린다(지금은 PR 본문과 Slack 메시지를 파일로만 남김).

## 도구를 고른 이유
| 도구 | 쓰나 | 이유 |
|---|---|---|
| PostGIS | 씀 | 필지·역·업소 좌표를 행정동에 붙이고 가장 가까운 역을 찾는다 |
| Airflow | 씀 | 출처마다 갱신 주기가 다르고(매주, 매월 21일, 분기, 수시), 늦게 오는 파일을 기다리고(센서), 과거 기간을 다시 돌려야 한다(백필) |
| Spark | 생활이동에만 씀: Iceberg 적재와, Iceberg 14억 행에서 관문·격리·경계 표를 만드는 Spark SQL 실행기(결정 35, 88~90, 96) | 처음엔 학습 목적이었다. 한 달치(6.3GB)에서 DuckDB 5.5~6.1초, Spark 28.5~29.1초([측정 5](docs/measurements.md), [결정 24](docs/decisions.md)), 10달(58GiB, 14억 행)에서도 DuckDB(CSV) 약 33초, Spark(CSV) 약 168초로 한 대에서는 DuckDB가 빨랐다. 같은 Spark가 Iceberg 표를 읽으면 약 29초([측정 10](docs/measurements.md), [결정 37](docs/decisions.md)). 정제 전체(관문·격리·경계)를 같은 SQL로 비교하면, 배치로 나눈 DuckDB 658.8초 대 Spark 1,971~2,419초([측정 51](docs/measurements.md#측정-51-duckdb-배치마다-새-프로세스--parquet-넘김-생활이동-전체-완주-결정-111-d3d4-2026-11-15-00470109), [결정 111](docs/decisions.md#결정-111-duckdb는-배치마다-새-프로세스에서-돌리고-배치-결과는-parquet-파일로-넘긴다-f-30-결정-110-후속)). 운영 실행기는 지금도 Spark다(DuckDB는 메모리 여유가 거의 없고 넘치면 실패하므로) |
| DuckDB | 측정과 대안 실행기로 씀 (`measure/duckdb_sql.py`, 이미지 `seoul-duckdb-job`) | 같은 생활이동 SQL을 두 엔진으로 돌려 병목과 결과 동등을 본다(결정 94). 운영 DAG `file_living_migration`은 Spark 실행기를 쓴다. 배치로 나누면 약 3.0~3.7배 빠르지만 컨테이너 상한까지 여유가 0.4GiB뿐이다([측정 51](docs/measurements.md#측정-51-duckdb-배치마다-새-프로세스--parquet-넘김-생활이동-전체-완주-결정-111-d3d4-2026-11-15-00470109)) |
| Iceberg | 씀 (생활이동) | 레이크에 표 개념이 없어 과거 시점 조회와 스키마 변경 추적이 안 됐다. 원본 58GiB → 11GB, 대상연월 파티션 덮어쓰기, 스냅샷([결정 35](docs/decisions.md), [측정 9](docs/measurements.md)) |
| Kafka | 안 씀 | 스트리밍 출처가 없다 |

## 실패했을 때
- 작업이 실패하면 `logs\alerts.log`에 DAG, 작업, 처리 단위, 원인이 한 줄씩 남는다.
- 처리 단위 하나(예: `offi_11680_202609`)만 Airflow 화면에서 Clear하면 그 칸만 다시 돈다. 모든 적재는 처리 단위마다 지우고 다시 넣으므로 몇 번을 다시 돌려도 결과가 같다. 거절된 배치는 운영 대시보드 "거절된 배치" 카드에 보인다.
- 무거운 실행(생활이동 실행기, dbt 전체 빌드, 리니지 추출, DuckDB 측정)은 PostgreSQL advisory lock(`common.heavy_lock()`, 셸에서는 `python -m common lock -- <명령>`)으로 컨테이너를 넘어 한 번에 하나만 돈다. 컨테이너 메모리 상한(db 4g, 계산 컨테이너, Airflow 구성 요소별)은 `docker-compose.yml`에 있다. 계산 컨테이너가 넘치면 그 컨테이너만 OOM으로 끝나고(남겨 두어 `docker inspect`로 원인을 본다) 스케줄러와 다른 서비스는 계속 돈다([docs/resource_guard.md](docs/resource_guard.md), 결정 101, 109, guarantees 2.7).
- Docker 가상 디스크(`docker_data.vhdx`)는 컨테이너 안에서 파일을 지워도 줄지 않는다. 호스트 C: 여유가 줄면 `wsl -d docker-desktop -e /sbin/fstrim -v /mnt/docker-desktop-disk` 뒤 Docker를 끄고 `wsl --shutdown`, 관리자 diskpart `compact vdisk`(측정 36).
- PostgreSQL이 비정상 종료에서 복구되면 누적 통계가 지워져 autovacuum이 대상을 모른다. 복구 뒤 `ANALYZE`를 한 번 돌린다(F-3).

## 문서
| 문서 | 내용 |
|---|---|
| [docs/question.md](docs/question.md) | 질문과 지표 계산 방식 (승인본, 변경 이력 포함) |
| [docs/data_sources.md](docs/data_sources.md) | 출처별 탐색 기록 |
| [docs/rules.md](docs/rules.md) | 출처별 정제·검증 규칙과 실측 |
| [docs/erd.md](docs/erd.md) | 웨어하우스 ERD와 grain |
| [docs/decisions.md](docs/decisions.md) | 설계 결정 (상황, 선택지, 이유, 검증) |
| [docs/measurements.md](docs/measurements.md) | 측정 기록 (신고 지연, Spark 필요성 등) |
| [docs/data_flow.md](docs/data_flow.md) | 데이터 흐름 한 장: 출처별 실제 원본 샘플, 조인 SQL과 결과, 서빙 파일 모양, 케이스 3개, 층별 스키마 |
| [docs/ui.md](docs/ui.md) | 파이프라인 UI 구조, 화면별 데이터 출처, 실행 방법, UI 결정 |
| [docs/data_contract.md](docs/data_contract.md) | 데이터 상품의 계약 (스키마, 갱신, 품질, 출처·이용조건, 한계) |
| [docs/guarantees.md](docs/guarantees.md) | 멱등·동등·원자성 같은 성질의 증명 방법과 강도, 아직 증명하지 않은 것 |
| [docs/failure_analysis.md](docs/failure_analysis.md) | 만난 오류의 원인 분석과 처방 (F-1~) |
| [docs/resource_guard.md](docs/resource_guard.md) | 메모리·디스크 고갈 방지 장치(L0~L5)와 예산 |
| [docs/judgment_log.md](docs/judgment_log.md) | 지시서 작업 중 스스로 내린 작은 판단의 한 줄 기록 |
| [docs/design_agent.md](docs/design_agent.md) | SQL 1차 대응 에이전트 설계(신호, 격리 빌드, G1~G5 판정, 평가 정답표 [docs/agent_answer_key.json](docs/agent_answer_key.json)) |
| [docs/design_change_resilience.md](docs/design_change_resilience.md) | 컬럼 리니지(P4)와 혼잡도 정제 SQL 이전의 상세 설계 |
| [docs/test_cases.md](docs/test_cases.md) | 시험 케이스 기록(목적, 넣은 것, 기대, 결과, 다시 돌리는 법) |
| `docs/handoff_*.md`, `docs/progress_*.md` | 작업 지시서와 진행 기록(단계마다 한 일과 결과) |
