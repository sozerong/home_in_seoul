# 지시서: Airflow 지휘·계산 분리(결정 109)와 DuckDB OOM 배치 처리(결정 110)

작성 2026-11-14. **이 파일 하나로 끝까지 진행한다.** 사용자 지시(2026-11-14): 이 작업에서는 단계마다 멈추지 않는다. 각 단계의 결과를 아래 "진행 기록"에 남기고 다음 단계로 간다. 멈추는 것은 3절의 조건에 해당할 때뿐이다. 사소한 판단은 스스로 하고 한 줄로 기록한다(handoff_judgments.md 규칙 3). 커밋은 사용자가 한다(규칙 4). k8s 이식은 대기 중이다.

## 1. 완료의 정의 (모두 참이어야 끝)
1. compose에 `airflow`(standalone) 서비스가 없다. `airflow-init`, `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`, `socket-proxy`가 뜨고, 화면(127.0.0.1:8080)이 열린다. ui 서비스가 병합돼 있으면 새 API 주소로 동작한다.
2. Spark와 DuckDB는 `seoul-spark-job`, `seoul-duckdb-job` 컨테이너에서만 돈다. Airflow 이미지에는 JDK, `spark-venv`, Spark jar가 없다. 코드와 문서의 실행 안내에 `/opt/airflow/spark-venv`와 `/tmp/duckdb_pkg`가 남아 있지 않다(기록 문서의 과거 서술은 그대로 둔다).
3. 무거운 작업 잠금이 컨테이너를 넘어 동작한다(PostgreSQL advisory lock). 이를 확인하는 시험이 있고 통과한다.
4. 생활이동 DAG가 DockerOperator로 1회 전체 성공하고, 상품 sha256이 H0 기준선과 같다. wh_load 1회 성공.
5. 계산 컨테이너를 일부러 OOM 내는 시험(H6)의 결과가 `docs/guarantees.md`와 `docs/measurements.md`에 있다.
6. DuckDB: 진단(D1)과 배치 처리(D2)의 결과가 측정 번호로 있다. D2가 16GiB 안에서 완주하고 Spark 결과와 두 방향 `EXCEPT ALL` 0행이다. 완주하지 못했으면 원인 분석이 F 번호로 있다.
7. 결정 109·110의 검증·결과 칸, F-19 처방 결과, resource_guard.md(L2, L5, 7절), README(서비스 이름, 실행 명령, 구조), test_cases.md의 실행 명령이 새 구조와 맞다.
8. 마지막 보고(6절)를 했다.

## 2. 시작 전 (사용자가 끝내 두는 것. 안 돼 있으면 H0에서 멈춘다)
- A1 작업이 커밋돼 있다(`git status`에 `agent/`, `Dockerfile` 변경이 없다).
- ui 브랜치가 main에 병합돼 있다(이미 준 명령: `git merge ui`, worktree 제거, 브랜치 삭제). 병합 전이면 compose 충돌이 난다.
- 무거운 실행 세 번(H4 Spark 전체 1회, D1·D2 DuckDB 각 1회)이 들어갈 디스크가 있다. J-7 사전 점검을 통과하는지는 H0과 각 무거운 실행 직전에 본다.

## 3. 멈추는 조건 (이때만 멈추고 보고한다)
- 결과가 달라졌다: 상품 sha256이 H0과 다르거나, `EXCEPT ALL`이 0이 아니다.
- J-7 사전 점검(디스크)을 통과하지 못한다. 공간 회수는 사용자 일이다.
- 지워야만 진행되는 것이 생겼다(파일, 볼륨, 이미지, 컨테이너). 단, 이 작업에서 만든 시험용 컨테이너는 지우지 않고 목록만 남긴 채 계속한다.
- 되돌릴 수 없는 일이 필요하다(DB 스키마 삭제, 레이크 수정 등).
- 2절 조건이 안 돼 있다.

그 밖의 실패(빌드 오류, 설정 오류, 예상과 다른 측정값)는 원인을 분석해 `failure_analysis.md`에 F 번호로 남기고, 고쳐서 계속한다. H6이나 D2의 결과가 기대와 다르면 있는 그대로 기록하고 계속한다(결과를 맞추려고 조건을 바꾸지 않는다).

## 4. 미리 정한 값과 규칙 (사용자 결정, 2026-11-14)
- 실행 방식: LocalExecutor 유지 + DockerOperator(결정 109 C). pool `heavy`(1) 유지. DAG 기본 `retries: 0`도 그대로라 종료 코드 137은 재시도되지 않는다. 계산 태스크에 재시도를 새로 넣지 않는다.
- dbt: **Airflow 이미지에 남긴다.** wh_load, `agent/sandbox.py`, `measure/*` 여러 개가 `/opt/airflow/dbt-venv`를 쓰고, 에이전트는 이번 범위 밖이라 옮기면 A 작업과 섞인다. 계산은 주로 PostgreSQL 쪽이다. H5에서 dbt 빌드 중 프로세스 RSS를 재 스케줄러 상한 근거로만 쓴다.
- SQL 에이전트: Airflow 이미지와 스케줄러 쪽에 그대로 둔다. `common.heavy_lock()`의 이름과 쓰는 법을 바꾸지 않아 `agent/run.py`가 그대로 동작하게 한다.
- Airflow 구성 요소 상한: H1에서 잰 구성 요소별 최대 RSS × 1.5를 256MiB 단위로 올림, 최소 512MiB. 스케줄러는 H4까지 지금 값(22g, 스왑 금지)을 유지하고, H5에서 "wh_load 중 최대 RSS × 1.5"로 낮춘다(LocalExecutor 워커가 dbt를 돌리므로 그 몫 포함).
- 계산 컨테이너 상한: 지금 compose 주석의 계산(VM 30.91GiB − db 4 − metabase 2 − pgadmin 0.5 − OS 2 − ui 0.5) − H5 뒤 Airflow 구성 요소 상한 합 − socket-proxy 상한. 1GiB 단위로 내림, 스왑 금지. pool `heavy`가 한 번에 하나라 spark-job과 duckdb-job은 같은 값을 쓴다.
- 엔진 설정은 비교 조건을 지키려고 그대로 둔다: Spark driver 8g, local[*], 셔플 파티션 200, AQE 켬. DuckDB `memory_limit` 16GiB, threads 12, 임시 상한은 `temp_budget()`. J-15(예산을 넘겨 재지 않음)를 지킨다.
- D2가 어떤 배치에서 16GiB를 넘으면: 그 배치 안에서만 결정 110 선택지 B(`checked`를 임시 표로 먼저 만든 뒤 나머지)를 쓴다. 그래도 넘으면 F 번호로 기록하고 D2를 "완주 못 함"으로 결론 낸다(멈추지 않고 7번 문서 마무리로 간다).
- 새 의존성은 J-17 규칙을 따른다: `apache-airflow-providers-docker`, `tecnativa/docker-socket-proxy`만 이 작업에서 허용한다. 버전은 Airflow 3.3.2 constraints와 맞는 것으로 고정한다.

## 5. 단계

### H0. 기준선과 사전 점검
- 2절 조건 확인. `git status`, `docker compose ps`, `docker image ls seoul-airflow`(크기)를 진행 기록에 남긴다.
- 지금 상품 폴더(가장 최근 버전)의 파일별 sha256 목록을 `logs/`에 남긴다(비교 기준).
- `pg_dump`로 `seoul`, `airflow` 두 DB를 떠 둔다(컨테이너를 다시 만들기 전 규칙). 위치와 크기를 기록한다.
- J-7 사전 점검을 돌려 통과하는지 본다.

### H1. standalone 해체
- `airflow` 서비스를 `airflow-init`(db migrate, 한 번 돌고 끝), `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`로 나눈다. 공통 설정은 `x-airflow-common` 앵커로 묶는다.
- Airflow 3.3.2 공식 `docker-compose.yaml`을 내려받아 참고한다. 구성 요소 사이에 맞춰야 하는 설정(Execution API 주소, JWT 비밀 등)은 그 파일에서 이름을 확인해 쓴다. 추측하지 않는다. 비밀값은 `.env`에 둔다.
- 포트 `127.0.0.1:8080`은 apiserver에만. `SIMPLE_AUTH_MANAGER_ALL_ADMINS`, `TZ`, `DATABASE_URL`, 볼륨(`./`, `./dags`, `scratch`)은 지금과 같게 둔다.
- ui 서비스의 `AIRFLOW_API`를 `http://airflow-apiserver:8080`으로 바꾼다.
- `docker compose exec airflow ...`를 쓰던 곳을 새 서비스 이름으로 고친다: README 94, 116행. `measure/induce_guard.py`의 컨테이너 이름은 H6에서 고친다.
- 확인: 화면 접속, DAG 목록, 가벼운 DAG(예: `seoul_api` 수동 1회) 성공, 메타데이터 DB의 스케줄러·DAG 처리기·트리거러 하트비트가 이어진다.
- 그동안 `measure/vm_watch.py`(Airflow 이미지의 기본 python으로 실행)를 각 구성 요소 컨테이너에서 돌려 최대 RSS를 잰다. 4절 규칙으로 상한을 정해 compose에 넣고, 다시 띄워 같은 확인을 한 번 더 한다.

### H2. 계산 이미지
- Dockerfile을 여러 target으로 나눈다(또는 파일을 나눈다). 판단해서 기록한다.
  - `seoul-spark-job`: Python 3.12, JDK 17, `pyspark==3.5.9`, Iceberg 런타임과 PostgreSQL JDBC jar(지금과 같은 버전), `requirements.txt`.
  - `seoul-duckdb-job`: Python 3.12, `duckdb==2.0.0`, `requirements.txt`. 쓰는 확장(iceberg, postgres)을 빌드 때 설치한다.
- compose에 두 서비스를 `profiles: [jobs]`로 둔다. 평소에는 뜨지 않고 `docker compose run --rm`으로 쓴다. 상한은 일단 H4 전 값(20g, 스왑 금지)으로 두고 H5에서 4절 규칙으로 다시 정한다.
- 마운트: 코드 `./`는 `/opt/project:ro`, `lake/`도 읽기 전용, `lakehouse/`와 `scratch` 볼륨만 쓰기. 바인드 마운트 안에서 하위 경로를 쓰기로 다시 여는 방식은 `lakehouse/`에만 쓴다. `PYTHONDONTWRITEBYTECODE=1`.
- 확인: 두 이미지에서 `tests/check_scratch.py`가 통과한다. spark-job에서 실행기 `--help`와 Iceberg 한 달 `count(*)`가 된다. duckdb-job에서 `measure.duckdb_sql --help`가 된다. 이미지 크기를 기록한다.

### H3. 무거운 작업 잠금 이전
- `common.heavy_lock()`을 PostgreSQL advisory lock으로 바꾼다.
  - 고정 키 하나로 `pg_try_advisory_lock`. 잠금 동안 그 연결을 프로세스가 끝날 때까지 들고 있는다.
  - 못 잡으면 지금처럼 바로 실패한다. 메시지에 "다른 무거운 작업이 잠금을 잡고 있음"과 잡은 쪽을 찾는 질의(`pg_locks`)를 넣는다.
  - 잡은 뒤 `clear_scratch`를 부르는 순서와, 끝날 때 다시 비우는 동작은 그대로 둔다.
  - 이름과 쓰는 법이 같아 `agent/run.py`, `transform/*`, `measure/*`는 고치지 않는다.
- DAG의 `flock -n /tmp/heavy.lock`(`wh_load.py` 34행, `column_lineage.py` 29행)을 같은 잠금으로 바꾼다. 작은 래퍼(`python -m common lock -- <명령>` 같은 형태)를 둘지는 판단해서 기록한다.
- `tests/check_scratch.py`의 `flock` 직접 사용을 새 잠금으로 고친다.
- 새 시험: 서로 다른 두 컨테이너(스케줄러와 duckdb-job)에서 동시에 잠금을 잡으면 두 번째가 바로 실패하는지, 첫 번째가 끝나면 풀리는지 본다. `tests/`에 남긴다.

### H4. DockerOperator로 교체
- Airflow 이미지에 `apache-airflow-providers-docker`를 넣는다.
- `socket-proxy` 서비스(`tecnativa/docker-socket-proxy`)를 둔다.
  - 호스트 소켓은 이 서비스에만 읽기 전용으로 마운트한다.
  - Airflow 구성 요소는 `tcp://socket-proxy:2375`로만 접근한다.
  - 여는 API 묶음은 DockerOperator가 실제로 쓰는 최소한으로, 시험하며 찾고 기록한다.
  - 포트는 호스트에 열지 않는다.
- 바꿀 태스크:
  - `file_sources.py`의 생활이동 적재·실행기 태스크
  - `column_lineage.py`의 Spark 추출
- DockerOperator에서 엔진 수준으로 챙길 것 (각각 판단 결과를 기록):
  - **바인드 마운트 경로는 호스트 기준이다.** DockerOperator는 호스트의 Docker에 컨테이너를 만들게 하므로 `/opt/project` 같은 컨테이너 안 경로가 아니라 호스트 경로를 넘겨야 한다. 호스트 레포 경로를 비밀 아닌 설정값으로 `.env`에 두고, `docker inspect`로 실제 마운트가 맞는지 확인한다. `scratch`는 이름 볼륨으로 넘긴다.
  - `mount_tmp_dir=False`. 기본값은 Airflow 컨테이너 안 임시 폴더를 호스트 경로처럼 붙이려다 실패한다.
  - 네트워크는 compose 기본 네트워크 이름을 쓴다.
  - 비밀값(`DATABASE_URL`, `POSTGRES_PASSWORD`)은 `private_environment`로 넘겨 로그와 렌더된 템플릿에 찍히지 않게 한다.
  - 계산 컨테이너의 상한(`mem_limit`, 스왑 금지, `cpus` 12)을 태스크에 준다.
  - 컨테이너 정리: 성공하면 지운다. 실패하면 원인을 볼 수 있게 남긴다(OOMKilled 확인용). 남은 것은 목록으로 기록한다(3절).
  - 중단 시험: 실행 중인 생활이동 태스크를 화면에서 실패로 표시했을 때 계산 컨테이너도 멈추는지 본다. 안 멈추면 F 번호로 기록하고 `on_kill` 동작을 확인해 고친다.
- 확인: 생활이동 DAG 1회 전체 성공(J-14 방식으로 같은 파일 재투입). `ops.engine_run` 기록이 남는다. 상품 sha256이 H0과 같다. 시간과 셔플·흘림을 측정 42와 같은 표로 남긴다(측정 번호). 결과가 달라지면 3절에 따라 멈춘다.

### H5. Airflow 이미지 슬림화와 상한 재설정
- 이 단계 전에 코드와 실행 안내에서 `/opt/airflow/spark-venv`, `/tmp/duckdb_pkg` 참조를 모두 새 방식(`docker compose run --rm spark-job ...` / `duckdb-job ...`)으로 바꾼다. 확인된 곳:
  - `measure/`: `change_cost`, `duckdb_sql`, `induce_guard`, `living_boundary_gap`, `living_g3`, `vm_watch`
  - `tests/`: `check_living_conflict`, `check_runner_swap`, `check_scratch`
  - `lineage/extract.py`, `transform/iceberg.py`(안내 문구), `docs/test_cases.md`
  - `grep`이 0이 될 때까지 고친다.
- Airflow 이미지에서 JDK, `spark-venv`, jar를 뺀다. dbt는 남긴다(4절).
- wh_load 1회를 돌리며 스케줄러 컨테이너 최대 RSS(dbt 프로세스 RSS 따로)를 잰다. 4절 규칙으로 스케줄러 상한을 낮추고, 계산 컨테이너 상한을 다시 계산해 넣는다.
- 확인: 이미지 크기와 빌드 시간 전후(측정 번호). wh_load 1회 성공. 가벼운 DAG 1회 성공.

### H6. 증명: 장애가 그 컨테이너에서 끝나는가
- `measure/induce_guard.py`를 새 구조로 고친다: 컨테이너 이름, `seoul-duckdb-job` 이미지, `duckdb_pkg` 제거.
- 시험 순서:
  1. 가벼운 DAG를 하나 돌려 둔다.
  2. 그동안 `seoul-duckdb-job` 컨테이너를 작은 상한(예: 2g, F-17과 같은 질의)으로 띄워 일부러 OOM을 낸다. `--rm` 없이 띄워 `docker inspect`로 `OOMKilled`와 종료 코드를 본다.
  3. 같은 시간 동안 메타데이터 DB의 스케줄러 하트비트 간격, 그 DAG의 성공 여부, 스케줄러 컨테이너의 vm_watch를 기록한다.
- 3회 반복한다. 결과를 `docs/guarantees.md`에 증명 방법, 조건, 강도(재현 가능), 한계(같은 VM이라 VM 전체 메모리와 디스크는 여전히 공유)와 함께 남기고, 결정 109 검증 칸을 채운다.

## 2부. DuckDB OOM (결정 110)

### D1. 진단: 무엇이 메모리를 잡는가
- `measure/duckdb_sql.py`에 `--memwatch 초` 옵션을 넣는다. 옆 연결(같은 DB 인스턴스의 두 번째 커서)에서 `FROM duckdb_memory()`를 그 간격으로 읽어, 태그별 `memory_usage_bytes`, `temporary_storage_bytes`를 `logs/`에 CSV로 쓴다. 실패해도 마지막 줄까지 남게 줄마다 flush한다.
- 측정 43과 같은 명령과 예산(16GiB, threads 12)으로 duckdb-job에서 1회 실행한다(실패 예상). 실패 직전에 커진 태그와 그 크기를 측정 번호로 남기고, 측정 43과 F-19의 [확인 필요](어느 연산자)를 채운다.

### D2. 배치 단위 처리
- `spark_sql/living/*.sql`은 고치지 않는다(결정 94의 같은 SQL 조건).
- `measure/duckdb_sql.py`에 `--per-batch`를 넣는다.
  1. 배치 목록(`target_ym, collected_date`)을 Iceberg 메타데이터나 작은 질의로 얻는다.
  2. 배치마다 원천 뷰 `lh.living.migration_rows`를 그 배치로 걸러 다시 만든다. `checked` → `clean`, `quarantine`을 계산해 누적 표(`clean_all`, `quarantine_all`)에 더한다.
  3. 모든 배치가 끝나면 `clean`, `quarantine` 이름이 누적 표를 가리키게 한 뒤 `22_batch_status`(배치를 넘는 `lag` 포함)와 `30_commute`를 그대로 실행한다.
  4. 배치마다 시간, `--memwatch` 최대값, 임시 사용량을 기록한다.
- 걸러낸 뷰에서 DuckDB가 Iceberg 파티션만 읽는지 EXPLAIN과 읽은 양으로 확인한다. 안 되면 그 사실을 기록하고 그대로 진행한다(시간 문제이지 메모리 문제가 아니다).
- 16GiB, threads 12로 1회 실행한다. 4절의 넘침 규칙을 따른다.
- `--compare`로 Spark 결과 표(`stg.living_commute`, `stg.living_batch_status`, `stg.living_quarantine`)와 두 방향 `EXCEPT ALL`을 낸다. 0이 아니면 3절에 따라 멈춘다.
- 측정 번호로 남긴다: 전체 시간, 배치별 시간과 메모리 최대, 임시 최대, Spark(측정 42)와 같은 표로 비교. 결정 110 검증 칸과 F-19 "처방과 결과"를 채운다.

## 6. 문서 마무리와 마지막 보고
- 고칠 문서: 결정 109·110의 검증·결과, `failure_analysis.md`(F-19 결과 + 이 작업 중 생긴 F), `measurements.md`, `guarantees.md`, `resource_guard.md`(L2 상한 표, L5 잠금 방식, 7절 저장소 표의 컨테이너 구성), `README.md`(서비스 이름, 실행 순서, 아키텍처 설명, 증거 표에 H4·H6·D2 결과), `docs/test_cases.md`의 실행 명령.
- 마지막 보고(사용자에게, 짧게):
  1. 1절 완료 정의 8개 각각 참/거짓과 근거 위치(측정·F·결정 번호)
  2. 핵심 수치: H6 하트비트 공백과 137이 찍힌 컨테이너, H4 sha256 동등, D1 커진 태그, D2 완주 여부와 시간·메모리 최대, 이미지 크기 전후
  3. 남긴 시험용 컨테이너와 이미지 목록(지울지는 사용자)
  4. 커밋 명령: 이 작업 파일만 담는 `git add` 목록과 커밋 메시지 초안(Co-Authored-By 줄 포함)

## 진행 기록
(Claude Code가 단계마다 시각, 한 일, 결과, 판단 한 줄을 여기에 덧붙인다.)

- 2026-11-14 H0 시작 전 점검: A1 커밋됨(51211d6, 지시서 50924ed). `git status`는 `cd`(ps 출력이 잘못 저장된 파일)만 남음. **ui 브랜치 미병합**(`git merge-base --is-ancestor ui main` 거짓, 3392122 한 커밋, `git merge-tree`로 충돌 없음 확인). 실행 중인 ui 컨테이너(`new_proj-ui-1`)는 `new_proj_ui` worktree를 `/opt/project`로 붙이고 있다. 2절 조건 미충족이라 3절에 따라 멈춤.
- 2026-11-14 20:29~20:35 H0. 사용자가 ui 병합(ecac268), worktree 제거, ui를 main 폴더 기준으로 다시 띄움.
  - 상태: `git status`는 `cd`만(logs/h0/git_status.txt). compose: airflow(standalone, Up 9h), db, metabase, pgadmin, ui. `seoul-airflow:3.3.2` 5.47GB(내용 1.41GB).
  - 상품 기준선: v2026.11.14 데이터 파일 8개 sha256 → `logs/h0/product_v2026.11.14.sha256`(manifest.json은 created_at이 있어 비교에서 뺀다, 측정 42와 같음).
  - 덤프: `pg_dump -Fc` seoul 886,151,938B, airflow 598,970B, 133초. `pg_restore --list` TABLE DATA seoul 64, airflow 71. 위치 `C:\Users\dudfo\pg_backup_20261114_h0\`(SHA256SUMS). 처음에 이미 있던 `pg_backup_20261114\`에 써서 10:55 덤프 두 개를 덮어썼다(F-24).
  - J-7: `temp_budget()` 578.1GB ≥ 실행기 문턱 394GB(호스트 C: 여유 589G). 통과.
  - 판단: ui worktree의 `.env`가 worktree와 함께 지워져 `UI_READER_PASSWORD`가 없었다(ui가 빈 비밀번호로 뜸). 새 값을 만들어 `.env`에 넣고 `13_ui_reader.sql`을 다시 실행, ui 재생성. `/api/overview`가 DB와 Airflow 값을 돌려줌.
  - 판단: standalone이 만든 fernet 키를 `.env` `AIRFLOW_FERNET_KEY`로 옮겼다(Variable `rent_recent_months`가 암호화돼 있음). JWT 비밀은 새로 만들어 `AIRFLOW_JWT_SECRET`. standalone 컨테이너 안 작업 로그(2.7MB)는 `logs/h0/airflow_task_logs.tgz`로 보관.
  - 실행 중 DAG: `file_living_migration` manual 실행 하나가 `wait_for_file`에서 reschedule 대기 중(inbox 비어 있음). H4에서 이 실행이나 새 실행에 파일을 넣는다.
- 2026-11-14 20:35~20:50 H1. standalone을 `airflow-init`(db migrate), `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`로 나눔(`x-airflow-common` 앵커). 공식 3.3.2 compose에서 이름을 확인한 설정: `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`(http://airflow-apiserver:8080/execution/), `AIRFLOW__API_AUTH__JWT_SECRET`, `AIRFLOW__CORE__FERNET_KEY`(비밀은 `.env`).
  - 확인: 화면 200, `/api/v2/monitor/health` 네 구성 요소 healthy, `seoul_api` 수동 1회 성공, wh_load와 lineage 성공 뒤 상품 sha256 H0과 같음(`logs/h0/after_h1_product.sha256`). ui `/api/overview` 정상.
  - vm_watch 최대 cg_rss(H1 + 상한 뒤 H1b + H4 대기 중 중 최대): apiserver 287MB, dag-processor 337MB, triggerer 411MB, scheduler 1,315MB(wh_load 중). 4절 규칙(×1.5, 256MiB 올림, 최소 512MiB) → apiserver 512m, dag-processor 512m, triggerer 768m. 스케줄러는 22g 유지(H5에서). 상한 뒤 같은 확인 한 번 더 통과.
  - 판단: compose up이 db 컨테이너를 다시 만들었다(pgdata 볼륨이라 데이터는 그대로, 행 수 확인). 원인 [확인 필요], F-25로 기록.
- 2026-11-14 20:36~20:42 H2. Dockerfile 하나에 target 세 개(job-base 공통, spark-job, duckdb-job, airflow). 판단: 파일 하나에 두면 requirements.txt와 jar 버전을 한곳에서 본다.
  - 빌드 109초. `seoul-spark-job:3.5.9` 1.8GB, `seoul-duckdb-job:2.0.0` 768MB. compose `profiles: [jobs]`, 20g 스왑 금지, cpus 12. 코드 `/opt/project:ro`, `lakehouse/`와 `scratch`만 쓰기.
  - 확인: 두 이미지에서 `tests.check_scratch` 통과, spark-job에서 Iceberg 202607 `count(*)` 141,178,148행(8초), duckdb-job `measure.duckdb_sql --help`.
  - 판단: 잠금 없이 돌린 첫 count가 lineage DAG(잠금을 잡고 `/scratch`를 비움)와 겹쳐 셔플 파일이 사라져 실패했다. 잠금 밖에서 `/scratch`를 쓴 내 실수. 잠금 안에서 다시 돌려 통과(F-26).
- 2026-11-14 20:42~20:48 H3. `common.heavy_lock()`을 advisory lock(키 20261114, `pg_try_advisory_lock`, 연결을 프로세스 끝까지 보유)으로. 이름·쓰는 법 그대로. 래퍼 `python -m common lock -- <명령>`을 둠(판단: DAG의 bash 태스크와 계산 컨테이너가 같은 잠금을 쓰려면 셸에서 부를 문이 필요하다). wh_load·lineage의 `flock`을 래퍼로 교체, `check_scratch`도 교체.
  - 새 시험 `tests/check_heavy_lock.py`(호스트): 스케줄러가 잡은 동안 duckdb-job 1.8초 만에 실패(메시지에 잡은 쪽), 풀린 뒤 잡힘, 잡은 프로세스 kill -9 뒤 0.2초 안에 풀림. 통과.
- 2026-11-14 20:50~21:09 H4. provider-docker 4.5.9, docker 7.2.0(3.3.2 constraints), `tecnativa/docker-socket-proxy:v0.5.0`(128m, 소켓 ro, 호스트 포트 없음). 이미지 빌드 161초.
  - 연 API 묶음: `CONTAINERS`, `POST`, `IMAGES`(이미지 있는지 확인하는 `GET /images/json`이 403이라 추가, F-27). `_ping`, `version`은 기본 허용. `info`, `volumes`, `networks`는 403 그대로.
  - `dags/jobs.py` `spark_job()`: 호스트 경로 `HOST_PROJECT_DIR`(.env, 비밀 아님) 바인드, `lakehouse/`만 쓰기, `scratch` 이름 볼륨, `mount_tmp_dir=False`, 네트워크 `new_proj_default`, 비밀은 `private_environment`. `docker inspect`로 마운트·상한 확인(Memory=MemorySwap=21474836480, NanoCpus=12000000000).
  - 판단: provider의 `cpus`는 `cpu_shares`만 넣어 NanoCpus가 0이었다. `JobOperator`가 `nano_cpus`와 `memswap_limit`을 더한다.
  - 판단: `auto_remove="success"`가 실패한 컨테이너도 지웠다(중단 시험 컨테이너 ba40ad2b46bf가 사라짐, F-28). `auto_remove="never"` + 성공 때만 직접 지움, 실패 때는 id·OOMKilled·ExitCode를 로그에 남김. `exit 3` 시험으로 확인(e91841dc975d `musing_hawking` 남김).
  - 중단 시험: 실행 중 compute를 API로 failed 표시 → 계산 컨테이너가 7.7초 안에 멈추고 잠금 풀림(`logs/h4_stop_test.log`). provider `on_kill`이 컨테이너를 stop한다. F 없음.
  - 비밀: 작업 로그와 렌더된 템플릿 필드에 `DATABASE_URL`, `POSTGRES_PASSWORD` 값이 없음을 grep으로 확인.
  - 21:08 J-7 통과(`temp_budget()` 576.6GB, C: 여유 588G) 뒤 J-14 방식으로 생활이동 zip 재투입, `manual__2026-11-14T12:08:43` 실행 시작.
- 2026-11-14 21:27~21:41 재부팅(사용자). 21:27:22 Docker Desktop이 compose 컨테이너를 모두 멈추고(종료 코드 143, 스케줄러·metabase·pgadmin 137), 생활이동 계산 컨테이너 `crazy_kilby`는 21:38 Docker 재시작 때 255로 끝났다(Iceberg 적재 315.5초는 끝났고 실행기 도중). 실행기의 교체 트랜잭션 전이라 `stg` 표는 그대로, Iceberg 202607은 같은 파일로 새 스냅샷만 생김. inbox zip은 남아 있었다. 중단된 H4 수치는 쓰지 않는다.
  - 다시 띄움: metabase·pgadmin은 바뀐 것이 없는데 `up --dry-run`이 Recreate로 보여(F-25와 같은 현상) `docker start`로만 켰다. Airflow 구성 요소와 ui는 이미 빌드해 둔 슬림 이미지(519f4320c5bb, JDK·spark-venv·jar 없음 확인)로 다시 만들어졌다. 판단: H5의 이미지 교체가 H4 재측정 앞으로 왔다. 계산은 spark-job 이미지에서 돌아 H4가 보는 경로는 같다. 스케줄러 상한은 22g 그대로.
  - 스케줄러가 뜨자 주인 없는 `compute`(시도 1)를 되돌려 시도 2로 다시 실행했다(21:41:37, 새 컨테이너 `stoic_bell`). `retries: 0`인데도 다시 돈 것은 Airflow가 죽은 스케줄러의 태스크를 실패가 아니라 "고아"로 되돌리기 때문이다. 종료 코드 137 같은 태스크 실패는 여전히 재시도되지 않는다. 지난 실행의 `/scratch` 찌꺼기는 새 실행이 잠금을 잡으며 비웠다(S5가 실제 비정상 종료 뒤에 동작). 이 실행을 H4 재측정으로 쓴다. vm_watch `h4r_*`, job_stats `h4r_living`.
- 2026-11-14 22:06 H4 재측정(시도 2)이 실행기 무출력 20분 뒤 `ChunkedEncodingError`로 실패(F-29, socket-proxy 유휴 10분 제한). 계산 컨테이너 `stoic_bell`을 멈춤(137, 남김, 교체 전이라 `stg` 그대로). `dags/jobs.py`에 1분 heartbeat를 넣고 11분 무출력 시험 통과 뒤, compute와 하위를 API로 Clear해 시도 3(`stoic_wiles`, 22:10:10)으로 다시 돌림.
- 2026-11-14 23:16~23:22 H4 완료(측정 46). compute 시도 3 성공(Iceberg 286.1초, 실행기 3,705.4초, 측정 42보다 11.3% 느림, 셔플·흘림 같음), `cleanup_inbox`(inbox 비움), wh_load·lineage 성공. **상품 데이터 파일 8개 sha256이 H0과 같음**(`logs/h0/after_h4_product.sha256`). 계산 컨테이너 최대 10.25GiB.
  - 판단: 계산 컨테이너의 Spark 이벤트 로그가 성공한 컨테이너와 함께 지워져 작업별 경과 표는 없음(`ops.engine_run`은 남음). 남은 것으로 기록.
- 2026-11-14 23:16~23:34 H5(측정 47). 슬림 이미지는 재부팅 뒤 이미 적용(4.49GB·68초, 전 6.01GB·161초). wh_load(슬림, 22g 상한) 중 스케줄러 RSS 최대 1,040MiB → 1792m. dag-processor가 H4 재측정 중 442MiB까지 올라 512m → 768m(판단 112). 계산 상한 18.04 → 18g(compose spark-job·duckdb-job, `dags/jobs.py` MEM). 다시 띄운 뒤 import 오류 0, wh_load 2회·seoul_api·lineage 2회 성공, 스케줄러 최대 982MiB, 상품 sha256 그대로.
- 2026-11-14 23:34~23:37 H6(측정 48, guarantees 2.7). OOM 유도 3회 모두 통과: `h6-oom-1~3` OOMKilled=true·137(22~24초), 하트비트 간격 최대 5.8·6.0·6.4초, seoul_api 3회 성공, 멈춘 서비스 없음. 그동안 seoul_api가 부른 wh_load가 스케줄러에서 dbt를 돌렸고 성공(스케줄러 RSS 최대 1,172MiB). 디스크 유도 8.7초 오류·잔여 0B.
- 2026-11-14 23:44~23:55 D1(측정 49). J-7 통과(DuckDB 문턱 151.2GB x 1.2 + 50GiB ≈ 235GB, C: 여유 567G, `temp_budget()` 554.6GB). 656.0초에 16GiB 오류(측정 43 재현). 커진 태그: `HASH_TABLE` 최대 14.01GiB(임시 125GiB), 흘리지 않는 `ALLOCATOR` 10.36GiB. 키 9개 해시 집계가 두 물결.
- 2026-11-14 23:55 ~ 11-15 00:05 D2(측정 50, F-30). 배치 필터가 Iceberg 파티션까지 내려감 확인. 배치 1~3 각 약 69초 성공, 배치 4에서 16GiB 오류(시도 1). 넘친 배치만 B로 다시 하도록 고쳐(`measure/duckdb_sql.py`, 판단 113) 시도 2 → 배치 4를 B로 다시 하던 중 컨테이너 17.87/18GiB 뒤 137. 지시서 4절대로 **"완주 못 함"** 결론. `EXCEPT ALL`은 계산이 끝나지 않아 없음(3절 멈춤 조건 아님). 같은 시간 Airflow·db 그대로, 잠금 풀림.
