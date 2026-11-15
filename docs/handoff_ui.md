# UI 구현 지시서 (별도 세션, 2026-11-14)

파이프라인 플랫폼 UI를 레포 안의 읽기 전용 웹 앱으로 만든다(결정 106, 사용자 2026-11-14).

**판단이 필요한 지점은 `docs/handoff_judgments.md`(J-1~J-18)의 기준대로 하고, 근거와 함께 `docs/judgment_log.md`에 남긴다. 기준에 없거나 실측과 어긋나면 멈추고 선택지 표를 보인다.**

- 메인 세션(`docs/handoff_decisions.md`)과 동시에 돈다. 그래서 아래 1절의 격리를 먼저 지킨다.
- 공통 규칙은 CLAUDE.md와 handoff_finish.md 0절을 따른다. 한 번에 한 단계씩 진행하고, 단계마다 보고한 뒤 "다음"을 기다린다.

## 0. 목적

- 사람이 파이프라인을 보는 화면이다. 각 파이프라인의 구조(무엇을 받아 어디에 담고, 어떤 관문을 지나 무엇을 만드는지), 실행 상태, 관문 판정, 오류 기록, 컬럼 리니지를 **DB와 Airflow에서 실시간으로 읽어** 보여 준다.
- 나중에 만들 1차 대응 에이전트가 쓸 읽기 API의 바탕이 된다. 다만 에이전트 기능은 이번 범위가 아니다.
- **시각 설계는 이미 있다.**
  - 디자인 캔버스 3화면(파이프라인, 데이터 지도, 변경 시나리오)이다.
  - 원본 HTML은 `Claude outputs/ui_reference/`에 있다: `Pipelines.dc.html`, `Main.dc.html`, `Scenarios.dc.html`.
  - 화면 구성과 색, 리니지 그래프 그리기 로직(층 배치, 곡선, `walk` 너비 우선 탐색)은 거기서 옮긴다.
  - 그 파일들 안의 숫자는 날짜를 박은 스냅샷이다. 이번에는 전부 쿼리 결과로 바꾼다.
  - `<x-dc>`, `sc-for` 같은 캔버스 전용 문법은 쓰지 않는다. 일반 HTML과 JS로 옮긴다.

## 1. 격리 (먼저)

- 메인 세션의 handoff_decisions.md 1절 커밋이 끝난 뒤 시작한다. 시작은 이렇게 한다.
  `git worktree add ../new_proj_ui -b ui`
  그 뒤 모든 작업은 `../new_proj_ui`에서 한다.
- 메인 작업 트리의 파일은 고치지 않는다.
- 이 세션은 DB를 **읽기만** 한다.
  - stg, wh, mart에 쓰지 않는다. DDL은 9절의 역할 파일 하나만 쓴다.
  - Spark, dbt 빌드, DuckDB 같은 무거운 작업을 돌리지 않는다. 메인 세션이 heavy 잠금을 쓴다.
- 결정 기록은 `decisions.md`를 직접 고치지 않는다(번호 충돌 방지). `docs/ui.md`의 "UI 결정" 절에 쌓고, 병합할 때 decisions.md 다음 번호로 옮긴다.

## 2. 구조 (새 의존성 없이)

| 부분 | 선택 | 이유 |
|---|---|---|
| 서버 | FastAPI + uvicorn | Airflow 3 이미지(`seoul-airflow:3.3.2`)에 이미 있다(Airflow API 서버가 FastAPI). U0에서 확인한다. 없으면 멈추고 보고한다 |
| 실행 | `docker-compose.yml`에 서비스 `ui` 하나. 이미지는 `seoul-airflow:3.3.2`, 명령은 `uvicorn ui.app:app --host 0.0.0.0 --port 8000`, 포트는 `127.0.0.1:8090:8000`. 레포는 읽기 전용으로 마운트 | 새 이미지 없음. 바깥에 열지 않음 |
| 자원 | `mem_limit`와 `memswap_limit` 512m, `cpus: 1` | 결정 101(L2). 측정 37 예산에서 Airflow 몫을 줄이지 않는 크기 |
| 화면 | `ui/static/index.html` 한 파일. 일반 JS와 SVG, 해시 라우팅으로 3화면 | 빌드 단계, npm, 차트 라이브러리 없음 |
| DB 접속 | 새 역할 `ui_reader`(LOGIN, SELECT만). 비밀번호는 `.env` | 소비자마다 역할을 나눈다(`metabase_ro`, `lineage_reader`와 같은 방식) |
| Airflow 실행 기록 | **Airflow REST API v2**(`/api/v2/dags`, `/dagRuns`)로 읽는다. 메타데이터 DB 표를 직접 읽지 않는다 | REST API는 공개 계약이고, 메타 DB 표는 버전마다 바뀌는 내부 구조다. 인증(simple auth manager의 토큰)은 U0에서 확인한다 [확인 필요]. 막히면 선택지 표를 들고 멈춘다 |
| 파이프라인 정의 | `ui/pipelines.yaml`. 흐름 레인, 정의 폼, 관련 측정, 시나리오를 손으로 쓴 메타데이터 | 레인 구성은 코드에서 뽑기 어렵다. **대신 어긋남 검사를 둔다:** yaml의 dag_id와 일정이 실제 DAG와 같은지 확인하는 시험 하나. 이 yaml로 DAG를 만들지는 않는다 |
| 오류 상태 | `docs/failure_analysis.md`에서 `## F-N. 제목`과 `- 상태:` 줄을 읽는다 | 문서가 원본이다. 지금 파일로 읽기 시험 하나 |

## 3. 화면별 데이터 출처

| 화면 요소 | 출처 |
|---|---|
| 개요 카드: DAG 수, 마지막 실행, 실패 | Airflow API |
| 개요 카드: 선택·처방 대기 오류 수 | failure_analysis.md |
| 개요 카드: 리니지 간선 수와 회차, stale 여부 | `lineage.latest_build`, `lineage.edge` |
| 개요 카드: 거절된 배치 수 | `stg_dbt.batch_status` |
| 파이프라인 목록: 이름, 원천→엔진→목적지, 일정, 트리거 | pipelines.yaml + Airflow API(일정, 일시정지 여부) |
| 상세: 최근 실행 막대(성공·실패·실행 중, 시간) | Airflow API dagRuns 최근 30개 |
| 상세: 관문 판정(배치별 승인·거절, 사유) | `stg_dbt.batch_status` (출처로 거름) |
| 상세: 품질과 신선도 | `ops.quality`, `ops.freshness` |
| 상세: 빌드 기록(wh_load) | `ops.build_run`, `ops.build_diff` |
| 상세: 엔진 지표(생활이동, 리니지) | `ops.engine_run` |
| 상세: 흐름 레인, 정의 폼과 JSON 보기 | pipelines.yaml |
| 상세: 기록된 오류 | failure_analysis.md (yaml에 파이프라인별 F 번호 목록) |
| 데이터 지도: 표·컬럼 노드, 값·행·행 집합 간선 | `lineage.edge` (최신 회차) |
| 데이터 지도: 고른 컬럼의 영향 범위 | `lineage.downstream` / `lineage.upstream` |
| 변경 시나리오 | pipelines.yaml의 시나리오 절(측정 번호와 전후 수치). 정적 |

- 각 화면 아래에 "이 숫자는 어디서 왔나"(쿼리 이름과 읽은 시각)를 작게 보인다.

## 4. API (읽기 전용 GET만)

- `/api/overview`
- `/api/pipelines`, `/api/pipelines/{id}`
- `/api/lineage/graph`, `/api/lineage/impact?column=&direction=down|up`
- `/api/failures`

하나의 엔드포인트는 쿼리 한두 개로 끝낸다. 쿼리는 `ui/queries.sql`처럼 한곳에 모아 화면의 출처 표기와 같은 이름을 쓴다.

## 5. 단계 (단계마다 멈춤)

| 단계 | 할 일 | 사용자가 확인하는 법 |
|---|---|---|
| U0 | 이미지에 fastapi·uvicorn이 있는지, Airflow API 토큰 발급과 dagRuns 조회 curl 한 번, `ui_reader` 권한 목록 | 보고 표 |
| U1 | `warehouse/sql/09_ui_reader.sql` 역할 생성, compose `ui` 서비스, `/api/overview` | `curl 127.0.0.1:8090/api/overview` 값이 DB와 Airflow 화면과 같은지 |
| U2 | pipelines.yaml(DAG 8개), `/api/pipelines`, 어긋남 시험 | yaml의 일정 하나를 일부러 틀리게 하면 시험이 실패하는지 |
| U3 | 파이프라인 화면(개요, 목록, 상세) | 브라우저에서 캔버스 Pipelines와 나란히 비교 |
| U4 | 데이터 지도(Main.dc.html 로직 이식, 실시간 리니지) | `stg_dbt.congestion.dow`를 골랐을 때 리니지 MCP 답(`am_peak_congestion` 경로)과 같은 컬럼이 강조되는지 |
| U5 | 변경 시나리오 화면 | 캔버스 Scenarios와 비교 |
| U6 | 실패 드러내기 | DB를 멈추거나 Airflow API 토큰이 틀리면, 빈 화면이 아니라 어느 출처가 실패했는지 보이는지(설계 원칙 4) |
| U7 | `docs/ui.md`(구조 그림, 출처 표, 실행 방법, UI 결정), README 한 줄, 스크린샷 3장 | 사용자가 README대로 `docker compose up ui`로 띄워 본다 |

## 6. 하지 않는 것

- 쓰기 API, DAG 실행 버튼, 재실행 버튼. 실행은 Airflow 화면에서 한다.
- 로그인과 권한 기능. 127.0.0.1에만 열기 때문이다.
- 새 파이썬 패키지와 npm. 필요하면 멈추고 선택지를 보인다.
- 에이전트 기능(SQL 수정안, 격리 검증). 다음 단계다.
- 실시간 푸시. 새로고침으로 충분하다.

## 7. 완료 기준

| 기준 | 확인 |
|---|---|
| 화면 3개의 모든 숫자가 쿼리나 문서에서 온다(하드코딩 0) | `grep`으로 index.html에 측정 숫자가 없는지 + 출처 표 |
| 데이터 지도의 영향 범위가 리니지 MCP와 같다 | U4 대조 |
| 출처 하나가 실패해도 화면이 실패를 보인다 | U6 |
| 어긋남 시험이 CI 이름 규칙(`tests/test_*.py`) 안에 있고 DB 없이 돈다 | DB가 필요한 시험은 `check_` 이름(F-18) |
| main에 병합하고 decisions.md로 결정을 옮김 | 메인 세션 커밋과 충돌 없이 |
