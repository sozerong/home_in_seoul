# 서울 동네 찾기 데이터 파이프라인

> 서울에서 일하게 된 사회초년생이 직장 위치, 예산(보증금·월세 상한), 출퇴근 시간 한도를 넣으면, 어느 동네(행정동)가 주거비·출퇴근·출근길 혼잡·생활 편의 면에서 맞는가?

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
    S[(스테이징 stg<br>정제·검증, 격리 테이블)] --> W
    W[(웨어하우스 wh<br>행정동 차원 + 공간 조인<br>EPSG:5179)] --> M
    M[(마트 mart)] --> P[데이터 상품<br>product/vYYYY.MM.DD/<br>Parquet, CSV, GeoJSON, manifest]
    AF{{Airflow 3<br>출처별 DAG, 센서, Asset}} -.-> 수집
    AF -.-> S
    AF -.-> W
```

| 층 | 무엇 | 어디 |
|---|---|---|
| 레이크 | 받은 그대로의 원본. 출처별·수집일별 폴더. 수정하지 않는다 | `lake/` (git 제외) |
| 스테이징 | 출처마다 정제·검증. 어긋난 행은 사유와 함께 격리 | `transform/`, `stg` 스키마, 규칙은 [docs/rules.md](docs/rules.md) |
| 웨어하우스 | 2025 SGIS 행정동 차원을 중심으로, 필지·역·업소를 공간 조인으로 행정동에 붙임 | `warehouse/sql/07_wh.sql`, `08_wh_load.sql`, [docs/erd.md](docs/erd.md) |
| 마트 | 동네 요약, 출발 × 직장 출퇴근 표, 최근 6개월 신규 계약 | `warehouse/sql/09_mart.sql` |
| 데이터 상품 | 버전 폴더에 파일로 내보냄 | `warehouse/export_product.py`, [docs/data_contract.md](docs/data_contract.md) |
| 일정 | 출처별 DAG, 손으로 받는 파일은 센서로 기다림, 스테이징이 바뀌면 웨어하우스 자동 재구성 | `dags/` |

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
```

### 2. DB와 Python 환경
```
docker compose up -d db
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```
DB 구조 만들기 (00은 psql 전용 명령이 있어 컨테이너 안 psql로, 01~07은 순서대로 실행기로):
```
docker compose exec -T db psql -U seoul -d seoul -v ON_ERROR_STOP=1 < warehouse\sql\00_airflow_db.sql
for %f in (01_stg_rent 02_stg_station_master 03_stg_congestion 04_stg_living_migration 05_stg_sbiz_store 06_stg_boundary 07_wh) do python -m warehouse.run_sql warehouse/sql/%f.sql
```
모든 DDL은 `IF NOT EXISTS`라 여러 번 실행해도 된다.

### 3. Airflow
```
docker build -t seoul-airflow:3.3.2 .
docker compose up -d
```
http://localhost:8080 (개발용, 로그인 없음, 내 PC에서만 열림). DAG는 처음에 일시정지 상태다.

### 4. 데이터 넣기
손으로 받는 파일은 받은 zip 그대로 `lake\_inbox\{출처}\`에 넣고 해당 `file_*` DAG를 실행한다. 스테이징이 바뀔 때마다 `wh_load`가 자동으로 돌기 때문에 **아래 순서대로** 넣는다(순서를 어기면 `wh_load`가 "행정안전부 코드를 정하지 못한 행정동"으로 실패하고, 전월세 정제는 "법정동 코드표가 없습니다"로 실패한다).

| 순서 | inbox 폴더 | 파일 이름 | DAG |
|---|---|---|---|
| 1 | `sbiz_store` | `소상공인시장진흥공단_상가(상권)정보_*.zip` | `file_sbiz_store` |
| 2 | `sgis_boundary` | `bnd_dong_*.zip` | `file_sgis_boundary` |
| 3 | `parcel_boundary` | `LSMD_CONT_LDREG_*.zip` (서울) | `file_parcel_boundary` |
| 4 | `living_migration` | `생활이동_행정동_*.zip` | `file_living_migration` |
| 5 | (API) | | `seoul_api`, `molit_rent` 일시정지 풀기. 켜는 순간 가장 최근 예정분이 한 번 돈다 |

과거 기간이 필요하면 백필한다(예: 전월세 4주):
```
docker compose exec airflow airflow backfill create --dag-id molit_rent --from-date 2026-09-20 --to-date 2026-10-18 --reprocess-behavior none --max-active-runs 1
```

### 5. 마트와 데이터 상품
```
python -m warehouse.run_sql warehouse/sql/09_mart.sql
python -m warehouse.export_product
```
결과: `product\vYYYY.MM.DD\` (파일 설명은 [docs/data_contract.md](docs/data_contract.md)).

### 6. 확인
```
for %t in (test_rent test_station_master test_congestion test_living_migration test_sbiz_store test_boundary) do python -m tests.%t
docker compose exec -T db psql -U seoul -d seoul < warehouse\sql\demo.sql
```
`demo.sql` 맨 위 `params`(직장 행정동, 예산, 면적, 출퇴근 한도)를 바꿔 가며 결과를 본다.

## 결과 예시
`demo.sql` 기본값: 직장 역삼1동(강남구), 월세, 보증금 3,000만 원·월세 80만 원 이하, 전용 40㎡ 이하, 출퇴근 50분 이하 **또는** 직선거리 3km 이내, 예산 안 신규 계약(202606~202609) 3건 이상. 데이터 상품 v2026.10.22 기준 직선거리 순 앞 8개:

| 동네 | 이동 시간(분) | 직선거리(km) | 예산 안 계약 | 월세 중앙값(만 원) | 가까운 역 | 아침 최대 혼잡도(%) |
|---|---|---|---|---|---|---|
| 역삼1동 | 148.7 | 0.00 | 42 | 70 | 역삼 400m | 76.5 |
| 역삼2동 | 100.1 | 1.15 | 21 | 66 | 선릉 706m | 88.3 |
| 서초2동 | 88.7 | 1.25 | 9 | 72 | 양재(서초구청) 742m | 50.8 |
| 도곡1동 | 68.3 | 1.38 | 7 | 66 | 매봉 613m | 58.4 |
| 반포1동 | 79.2 | 1.45 | 21 | 63 | 사평 204m | 93.2 |
| 논현1동 | 105.3 | 1.45 | 29 | 70 | 논현 444m | 80.7 |
| 논현2동 | 76.0 | 1.74 | 12 | 65 | 학동 411m | 71.0 |
| 서초1동 | 63.1 | 1.81 | 21 | 64 | 남부터미널(예술의전당) 386m | 63.2 |

이동 시간은 생활이동의 평균 이동 시간이라 가까운 동네일수록 길게 나온다(근거리 직행 제외, 경유 포함). 그래서 직선거리를 함께 쓴다([결정 23](docs/decisions.md), [데이터 계약 6절](docs/data_contract.md)).

## 도구를 고른 이유
| 도구 | 쓰나 | 이유 |
|---|---|---|
| PostGIS | 씀 | 필지·역·업소 좌표를 행정동에 붙이고 가장 가까운 역을 찾는다 |
| Airflow | 씀 | 출처마다 갱신 주기가 다르고(매주, 매월 21일, 분기, 수시), 늦게 오는 파일을 기다리고(센서), 과거 기간을 다시 돌려야 한다(백필) |
| Spark | 안 씀 | 가장 큰 입력(생활이동 한 달치 6.3GB, 1억 4,118만 행)을 재 보니 PC 한 대에서 DuckDB 5.5~6.1초, Spark 28.5~29.1초, 지금 방식 약 2분 20초. 월 1회라 병목이 아니다([측정 5](docs/measurements.md), [결정 24](docs/decisions.md)) |
| Kafka | 안 씀 | 스트리밍 출처가 없다 |

## 실패했을 때
- 작업이 실패하면 `logs\alerts.log`에 DAG, 작업, 처리 단위, 원인이 한 줄씩 남는다.
- 처리 단위 하나(예: `offi_11680_202609`)만 Airflow 화면에서 Clear하면 그 칸만 다시 돈다. 모든 적재는 처리 단위마다 지우고 다시 넣으므로 몇 번을 다시 돌려도 결과가 같다.

## 문서
| 문서 | 내용 |
|---|---|
| [docs/question.md](docs/question.md) | 질문과 지표 계산 방식 (승인본, 변경 이력 포함) |
| [docs/data_sources.md](docs/data_sources.md) | 출처별 탐색 기록 |
| [docs/rules.md](docs/rules.md) | 출처별 정제·검증 규칙과 실측 |
| [docs/erd.md](docs/erd.md) | 웨어하우스 ERD와 grain |
| [docs/decisions.md](docs/decisions.md) | 설계 결정 (상황, 선택지, 이유, 검증) |
| [docs/measurements.md](docs/measurements.md) | 측정 기록 (신고 지연, Spark 필요성 등) |
| [docs/data_flow.md](docs/data_flow.md) | 원본 모양에서 데이터 상품까지 출처별 흐름, 거래 한 건 따라가기 |
| [docs/data_contract.md](docs/data_contract.md) | 데이터 상품의 계약 (스키마, 갱신, 품질, 출처·이용조건, 한계) |
