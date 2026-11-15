# 지시서: DuckDB OOM 마무리(결정 111), 분리 작업 남은 것, SQL 에이전트 A1.5~A7(결정 112)

작성 2026-11-15. 앞 지시서 `docs/handoff_airflow_split.md`의 결과(측정 46~50, F-24~F-30, 결정 109 적용, 결정 110 완주 못 함)에서 이어진다. **이 파일 하나로 끝까지 진행한다.** 단계마다 멈추지 않고 아래 "진행 기록"에 남긴 뒤 계속한다. 멈추는 것은 3절의 조건뿐이다. 사소한 판단은 스스로 하고 `judgment_log.md`에 한 줄로 남긴다(다음 번호 115부터). 커밋은 사용자가 한다. k8s 이식은 대기 중이다.

## 1. 완료의 정의
**1부. DuckDB OOM (결정 111)**
1. DuckDB가 생활이동 관문·정제·통근 SQL 전체를 16GiB(컨테이너 18g) 안에서 완주한다. 완주하지 못했으면 원인이 F 번호로 있고, 결정 111 결과 칸에 "완주 못 함"과 다음 후보가 있다.
2. 완주했으면 Spark 결과 세 표와 두 방향 `EXCEPT ALL`이 0행이다.
3. 측정 번호에 전체 시간, 배치별 시간·메모리 최대·임시 최대가 있고, Spark(측정 42·46)와 같은 표로 나란히 놓은 비교가 있다. 결정 94의 질문("한 대에서 DuckDB가 Spark보다 나은가")에 대한 답이 갱신된다.

**2부. 분리 작업(결정 109) 남은 것**
4. Spark 이벤트 로그가 계산 컨테이너가 지워져도 남는다. 생활이동 전체 실행 1회로 확인한다. 같은 실행을 측정 46의 속도 차이에 대한 두 번째 표본으로 쓴다.
5. 스케줄러 컨테이너가 갑자기 죽을 때(kill) 계산 컨테이너와 잠금이 어떻게 되는지 시험 결과가 있다. 중복 실행이나 주인 없는 컨테이너가 생기면 처방이 있다.
6. F-25(compose up이 바뀐 것 없는 db를 다시 만듦)의 원인이 확정되거나, 확인한 범위와 남은 것이 기록돼 있다.

**3부. SQL 에이전트 (결정 108, 112, design_agent.md 9절)**
7. A1.5: 에이전트가 스케줄러 컨테이너가 아니라 자기 계산 컨테이너(`seoul-agent-job`)에서 돈다. 레포는 읽기 전용으로 붙이고 `/scratch` 안 복제본에서 작업한다. 측정 45의 S1 사례가 같은 판정(pass)으로 재현된다.
8. A2~A4, A6, A7의 완료 기준(design_agent.md 9절 표)을 만족한다. A5는 4절의 사용자 준비물이 있으면 실제 PR 1건과 Slack 메시지 1건까지 간다. 없으면 PR·Slack 코드를 dry-run 시험까지 끝내고 "실제 PR 대기"로 보고한다.

**공통**
9. 문서가 새 구조와 맞다.
   - 결정 111·112 기록
   - 측정, F, guarantees(새 성질 "에이전트는 운영을 바꾸지 않는다")
   - resource_guard.md(에이전트 컨테이너 상한)
   - README(실행 명령, 아키텍처, 증거 표)
   - design_agent.md(D3 변경: 복제본)
10. 마지막 보고(6절)를 했다.

## 2. 시작 전 (사용자)
- 이 지시서를 넘기는 것으로 결정 111(4절 1부)과 결정 112(4절 3부)를 승인한 것으로 본다. Claude Code가 P0에서 decisions.md에 기록한다.
- A5용(없어도 진행은 한다):
  - `.env`에 `SLACK_WEBHOOK_URL`(시험 채널)
  - 이 레포만 고른 GitHub fine-grained 토큰(contents, pull requests 쓰기)을 `GITHUB_TOKEN`으로
  - GitHub에서 main 보호 규칙(PR 필수, CI 통과 필수)
- 지울지 정할 것(지시서는 지우지 않는다): 레포 루트의 `cd` 파일, 앞 작업에서 남긴 실패 컨테이너(측정 46·48·50 기록의 이름들).

## 3. 멈추는 조건 (이때만 멈춘다)
- 결과가 달라졌다:
  - 상품 sha256이 직전 기준선과 다르다.
  - 완주한 DuckDB와 Spark의 `EXCEPT ALL`이 0이 아니다.
  - 에이전트 실행 전후 운영 표 행 수나 상품 sha256이 달라졌다(에이전트가 운영을 바꿈).
- J-7 사전 점검(디스크) 실패. 공간 회수는 사용자 일이다.
- 지워야만 진행되는 것, 되돌릴 수 없는 일이 필요하다. 이 작업에서 만든 시험용 컨테이너는 지우지 않고 목록만 남긴다.
- 에이전트가 허용 경로(`models/stg_dbt/**`, `macros/**`의 `.sql`) 밖을 바꾸려 했고 막히지 않았다(안전장치 실패).
- 사용자 준비물이 필요한 단계(A5 실제 PR·Slack)는 멈추지 않고 건너뛰고 보고한다.

그 밖의 실패는 F 번호로 원인을 남기고 고쳐서 계속한다. 측정 결과가 기대와 달라도 있는 그대로 기록하고 계속한다.

## 4. 미리 정한 값과 규칙
**1부 (결정 111): DuckDB는 배치마다 새 프로세스에서 돌리고, 배치 결과는 Parquet 파일로 넘긴다.**
- 근거: F-30. 배치 하나는 16GiB 안에서 약 69초에 끝났다(측정 50). 넘친 것은 같은 인스턴스에 쌓은 누적 임시 표가 해시 집계와 버퍼 풀을 나눠 쓴 탓이다. 같은 프로세스에서 연결만 새로 열면 DuckDB가 세지 않는 할당(측정 50의 `ALLOCATOR` 약 1.1GiB, 컨테이너 기준 약 1.9GiB)이 프로세스에 남을 수 있다. 그래서 프로세스를 새로 띄워 배치마다 OS에 메모리를 돌려준다.
- 구조(`measure/duckdb_sql.py --per-batch-proc`, SQL 파일은 고치지 않음):
  1. 부모 프로세스가 heavy 잠금을 잡는다. 잠금을 잡으면 `/scratch`를 비우므로(S5) **자식은 잠금을 잡지 않는다.** 부모가 잠금 안에서 자식을 순서대로 부르고, 자식에게 "부모가 잠금을 가짐"을 인자로 넘긴다.
  2. 자식 하나 = 배치 하나: 원천 뷰를 배치로 걸러 `checked` → `clean`, `quarantine`을 계산해 `/scratch/duckdb/batches/<run_id>/<batch>/{clean,quarantine}.parquet`로 쓰고 끝난다. 자식마다 `--memwatch` 기록.
  3. 모든 배치 뒤 자식 하나가 Parquet 전체를 `clean`, `quarantine`으로 읽어 `22_batch_status`(배치를 넘는 `lag`)를 실행해 `batch_status.parquet`로 쓴다. 이 단계의 묶기 키는 배치 키 둘뿐이라 그룹이 10개다.
  4. `30_commute`도 묶기 키가 배치 키를 포함하므로 배치마다 자식 하나로 돌린다. `clean`은 그 배치 Parquet, `batch_status`는 전체 Parquet. 결과는 `commute.parquet`.
  5. 마지막 자식이 `--compare`로 Spark 표와 두 방향 `EXCEPT ALL`을 낸다.
- 예산: DuckDB `memory_limit` 16GiB, threads 12, 컨테이너 18g, 임시는 `temp_budget()`. J-15를 지킨다.
- 넘침 규칙: 어떤 배치가 메모리 오류(DuckDB 오류든 137이든)로 끝나면 그 배치만 threads 4로 한 번 더 돌린다. 그래도 넘치면 F 번호로 기록하고 1부를 "완주 못 함"으로 결론 낸 뒤 2부로 간다.
- 실패한 배치만 다시 돌릴 수 있게 한다(`--only-batch`). 설계 원칙 2를 따른다.

**2부**
- Spark 이벤트 로그는 레포의 `logs/spark-events/`(git 제외)를 계산 컨테이너에 쓰기로 붙여 남긴다. `EVENT_DIR`과 `dags/jobs.py` 마운트를 바꾼다. `measure/spark_jobs.py`가 그 폴더를 읽게 한다.
- 스케줄러 kill 시험:
  1. 생활이동 대신 가벼운 계산 작업을 쓴다. 잠금을 잡고 5분 자는 spark-job 시험 태스크를 시험용 DAG로 둔다.
  2. 실행 중 `docker kill airflow-scheduler` 뒤 다시 띄운다.
  3. 확인: 옛 계산 컨테이너가 계속 도는지, 새 시도가 잠금 때문에 바로 실패하는지(중복 실행 방지), 끝난 뒤 태스크 상태, 남는 컨테이너.
  4. 주인 없는 컨테이너가 남으면 계산 컨테이너에 `dag_id/run_id/task_id/try` 라벨을 달고, 스케줄러 시작 때 실행 중이 아닌 태스크의 라벨 컨테이너를 멈추는 정리 태스크나 스크립트를 둔다(지우지는 않고 멈추고 목록화).
- F-25: `docker compose config`와 컨테이너의 `com.docker.compose.config-hash` 라벨을 비교해 무엇이 바뀐 것으로 보이는지 찾는다. 원인이 확정되면 처방하고, 아니면 확인한 것을 남긴다. db를 다시 만드는 시험은 `pg_dump`를 먼저 뜬 뒤에만 한다.

**3부 (결정 112): 에이전트는 자기 계산 컨테이너에서, 레포 복제본으로 일한다.**
- 근거:
  - 결정 109 뒤 스케줄러 상한은 1792m다(측정 47). 에이전트(Claude CLI + dbt 빌드 2회)를 스케줄러 컨테이너에서 돌리면 P1(계산이 지휘자 cgroup에 있음)이 에이전트로 되살아난다.
  - F-22는 컨테이너가 호스트 `.git`을 쓰면서 났다.
- 구성:
  - `seoul-agent-job` 이미지(Dockerfile target): Python 3.12, dbt-venv(같은 버전), `claude-agent-sdk==0.2.182`, git.
  - 레포는 `/opt/project:ro`로 붙인다. 시작할 때 `git clone --no-hardlinks /opt/project /scratch/agent/<run>/repo`로 복제본을 만들고, 그 안에서 브랜치·편집·빌드를 한다. 호스트 `.git`에는 쓰지 않는다.
  - 고친 브랜치를 사용자에게 넘기는 길:
    - (A5 준비물 있음) 복제본에서 GitHub로 push하고 PR을 연다.
    - (없음) `git bundle`이나 patch 파일을 `logs/agent/<run>/`에 남긴다.
  - 그래서 `agent/run.py`의 worktree 코드(`forget_scratch_worktrees` 등)는 복제본 방식으로 바꾼다. F-22 재발 방지도 구조로 대신한다.
  - 상한: 측정 45의 RSS를 근거로 정한다. 없으면 A1.5 첫 실행에서 vm_watch로 잰 최대 × 1.5. 스왑 금지.
  - heavy 잠금: 에이전트의 격리 빌드는 지금처럼 heavy 잠금을 쓴다. 생활이동 실행 중이면 바로 실패하고, `agent_triage`는 그 신호를 다음 주기에 다시 본다.
  - 인증: 측정 45와 같은 `CLAUDE_CODE_OAUTH_TOKEN`. DockerOperator의 `private_environment`로 넘긴다. GitHub 토큰, Slack URL도 같은 방식이다.
- A2~A7은 design_agent.md 3~9절을 따른다. 정해지지 않았던 것은 이렇게 간다.
  - D2: 전체 빌드로 시작하고 시간을 잰다. 한 번에 5분이 넘으면 `state:modified+` + `source:raw.<출처>+`로 줄이고 판단으로 기록.
  - D4: A1에서 한 것을 그대로 둔다.
  - A6 반복: 정답표(`docs/agent_answer_key.json`)의 신호 사례(S1, M1, M7)는 각 3회, 대조군(M2, M3, M4, M6)과 S3는 각 1회. 시간과 SDK 계산값을 기록한다.
  - 허용 경로 밖 편집 시도는 매번 막혀야 한다. 막히지 않으면 3절에 따라 멈춘다.

## 5. 단계
### P0. 기록과 사전 점검
- decisions.md에 결정 111, 112를 위 4절 근거로 기록한다(형식: 상황, 선택지, 결정과 이유, 검증 [미검증], 결과와 남은 것).
- 기준선: `git status`, `docker compose ps`, 최신 상품 sha256을 `logs/p0/`에 남긴다. `seoul` DB `pg_dump`를 새 폴더에 뜬다. 앞의 F-24처럼 기존 덤프를 덮어쓰지 않는다.
- J-7 사전 점검.

### 1부
- D3. `--per-batch-proc`, `--only-batch`를 구현한다. 작은 범위(배치 2개, `--memory-limit 4GiB`)로 먼저 돌려 Parquet 넘김과 `EXCEPT ALL` 경로를 확인한다. 이 작은 비교는 Spark 표에서 같은 두 배치만 골라 한다.
- D4. 전체를 4절 예산으로 1회 돌린다(J-7 먼저). 결과를 측정 번호로 남긴다. 결정 111 검증·결과 칸, F-19·F-30 상태, 결정 94 답을 갱신한다.

### 2부
- S1. 이벤트 로그 보존을 바꾸고, 생활이동 전체 실행 1회(J-14 방식, J-7 먼저)를 한다.
  - 확인: 상품 sha256 같음, 이벤트 로그가 `logs/spark-events/`에 남음, `measure/spark_jobs.py`로 작업별 경과 표.
  - 측정 46과 시간을 나란히 놓고 속도 차이에 대한 해석을 갱신한다(2회 표본이라는 한계를 적는다).
- S2. 스케줄러 kill 시험(4절). 결과를 guarantees.md에 남기고, 처방을 넣었으면 같은 시험을 한 번 더 한다.
- S3. F-25 조사.

### 3부
- A1.5. `seoul-agent-job` 이미지, compose 서비스(profiles jobs), 복제본 방식으로 `agent/run.py`를 바꾸고 `--selftest`를 통과한다.
  - `docker compose run --rm agent-job python -m agent.run --data S1`로 측정 45를 재현한다(판정 pass, G1~G5, 운영 변화 0, 남은 `*_sbx_*` 스키마 0).
  - 같은 시간 스케줄러 RSS가 평시와 같은지 vm_watch로 확인한다.
- A2. 신호 수집(`ops.agent_signal`, wh_load 요약·실패 콜백에서 기록). 변이 M1을 넣었을 때 신호 한 건이 생기는지 본다.
- A3. 진단만(Edit 없음) 모드. 정답표 대비 원인 적중을 기록한다.
- A4. 수정안 + 격리 빌드 + 판정. `agent_triage` DAG가 신호를 받아 DockerOperator로 `seoul-agent-job`을 부른다. 정답표 대비 판정을 기록한다.
- A5. 준비물이 있으면 실제 PR 1건(본문에 G1~G5 표, 영향 범위, 원본 표본은 공공데이터라도 최소한으로)과 Slack 메시지(감지, PR 링크)를 보낸다. 없으면 dry-run 시험(생성될 PR 본문과 Slack 페이로드를 파일로)까지 하고 보고한다.
- A6. 4절 반복으로 평가한다. 거짓 통과가 0인지, 지표 표를 측정 번호로 남긴다.
- A7. 기록: decisions 108 검증, measurements, guarantees(새 성질과 증명 방법), failure_analysis, README, design_agent.md(D3 복제본, 구현 상태).

## 6. 문서 마무리와 마지막 보고
- 1절 9번 문서를 맞춘다. README 증거 표에 D4, S1, S2, A1.5, A6 결과를 넣는다.
- 마지막 보고(짧게):
  1. 1절 완료 정의 10개 각각 참/거짓과 근거 위치
  2. 핵심 수치: DuckDB 완주 여부·시간·배치별 메모리 최대 대 Spark, 이벤트 로그 보존·속도 2회째, 스케줄러 kill 시험 결과, 에이전트 A6 지표(적중, 거짓 통과, 시간)
  3. 사용자 할 일: A5 준비물(없었다면), 지울 후보 목록(컨테이너, 이미지, `cd` 파일)
  4. 커밋 명령: 이 작업 파일만 담는 `git add` 목록과 커밋 메시지 초안. 부분이 크면 1부·2부·3부로 나눈 커밋 셋

## 진행 기록
(Claude Code가 단계마다 시각, 한 일, 결과, 판단 번호를 덧붙인다.)

- 2026-11-15 00:40~00:46 P0. 결정 111·112를 decisions.md에 기록(검증 [미검증]).
  - 기준선: `logs/p0/git_status.txt`(`cd`, 이 지시서만), `logs/p0/compose_ps.txt`, 상품 v2026.11.14 데이터 파일 8개 sha256 `logs/p0/product.sha256`(H5 기준선과 같음).
  - 덤프: 새 폴더 `C:\Users\dudfo\pg_backup_20261115_p0\`(있는지 먼저 확인), `pg_dump -Fc` seoul 884,844,676B, airflow 623,604B, 141초, TABLE DATA seoul 64·airflow 71, `SHA256SUMS`.
  - J-7: `temp_budget()` 553.4GB ≥ 394GB(C: 여유 567G). 통과.
- 2026-11-15 00:47~00:58 D3. `measure/duckdb_sql.py`에 `--per-batch-proc`(부모가 잠금, 자식 단계 list → batch → status → commute → compare, Parquet 넘김, 메모리 오류면 그 단계만 threads 4로 한 번 더)와 `--only-batch`를 넣음. 설정 코드는 `engine_settings()`로 모아 옛 경로와 같이 씀.
  - 작은 시험(배치 202510·202511, `--memory-limit 4GiB`, threads 12): 배치 98.6·79.4초(최대 RSS 4.76GiB, 임시 최대 13.8GiB), status 1.4초, commute 2.9·3.0초, compare 17.3초, 전체 207.4초, Parquet 1.66GB. **Spark 표를 같은 두 배치로 거른 두 방향 `EXCEPT ALL`: living_commute 0, living_batch_status 0, living_quarantine 0**(`logs/duckdb_batches/duckdb-ec3827dd59ae.json`).
  - 판단 115: 첫 시도에서 `compare()`가 DuckDB 2.0에서 `con.execute(...).columns`로 실패(2.0의 execute는 연결을 돌려줌). 이 경로는 측정 50까지 한 번도 끝까지 가지 않아 드러나지 않았다. `con.sql(...).columns`로 고침.
  - 판단 116: `--only-batch`는 실행 안에서만 뜻이 있다. 잠금이 시작·끝에 `/scratch`를 비우므로 실행을 넘어 앞 배치 Parquet를 이어 쓰지 않는다. 실패한 배치는 같은 실행 안에서 다시 돌고(넘침 규칙), 실행을 넘는 재실행은 고른 배치를 처음부터 계산한다(코드 docstring ponytail).
- 2026-11-15 00:58~01:09 D4(측정 51). J-7 통과(553.5GB). 전체 1회 **658.8초에 완주, Spark 결과 세 표와 두 방향 `EXCEPT ALL` 모두 0행**(commute 13,782,826, batch_status 10, quarantine 31,935,758). 배치당 48.9~61.5초, 자식 최대 RSS 17.09~17.30GiB, 컨테이너 최대 17.62GiB / 18GiB. 202601이 threads 12에서 8초 만에 넘쳐 넘침 규칙으로 threads 4 재시도 성공(83.8초). 혼자 다시 돌려도 같음(F-31, 원인 [확인 필요]).
  - 기록: 결정 111 검증·결과, 결정 110에 이어짐 표시, 결정 94 답 갱신(같은 계산 범위에서 Spark 2,419초 대 DuckDB 658.8초), F-19·F-30 상태, F-31 새로.
- 2026-11-15 01:00~01:10 S3(F-25, 다시 만들지 않고 조사). `docker compose config --hash`와 컨테이너 라벨 비교: metabase, pgadmin만 다름. 두 서비스의 `mem_limit`·`memswap_limit` 줄만 뺀 사본의 해시가 라벨과 정확히 같음. 원인 확정: 상한을 `docker update`로 걸고 파일에 나중에 적어 라벨이 옛 설정 해시로 남음. 처방은 F-25(계획된 때 서비스 이름을 적어 다시 만들기).
- 2026-11-15 01:07~02:07 S1(측정 52). `EVENT_DIR`을 레포 `logs/spark-events/`로, `dags/jobs.py` spark_job에 그 폴더 쓰기 마운트, `measure/spark_jobs.py`는 인자 없으면 그 폴더 최신 파일. J-7 통과 뒤 202607 zip 재투입, `file_living_migration` 수동 실행 1회.
  - 결과: compute 2,964초(Iceberg 236.0 + 실행기 2,719.9초), wh_load·lineage 성공. **상품 v2026.11.15 sha256이 H5 기준선과 같음.** 컨테이너가 지워진 뒤 `logs/spark-events/local-1794673007379`(27.8MB)가 남음. 작업별 경과 표를 측정 52에.
  - 속도 2회째: 측정 46의 +11.3% 느림은 재현 안 됨. 이번이 세 번 중 가장 빠름(측정 42 대비 −18.3%). 세 표본이 36% 벌어져 1회씩으로는 분리 효과를 말할 수 없다고 기록. 결정 94 배수를 3.0~3.7배로, 결정 109 남은 것, guarantees 2.2 행 갱신.
  - wh_load `report`가 새 신호 기록 코드를 운영 실행에서 처음 탐(신호 0건, 거절 배치 없음).
- 2026-11-15 02:08~02:30 S2(F-32, guarantees 2.9). 시험 DAG `test_scheduler_kill`(spark-job에서 잠금 잡고 300초) 실행 중 스케줄러 `docker kill` → `docker start`.
  - 첫 시험: 옛 컨테이너가 주인 없이 끝까지(잠금도 끝까지), 5분 뒤 고아 재설정으로 시도 2가 처음부터 다시 돌아 **같은 작업이 두 번** 돎. 옛 컨테이너는 Exited로 남음. 5분보다 긴 작업이면 시도 2가 잠금에 막혀 실패할 것(추론).
  - 처방: `JobOperator.execute`가 라벨(`seoul.job`, dag_id, run_id, task_id, try)을 달고, 스케줄러 command가 시작 때 `python -m common stop-orphan-jobs`(멈춤만, 지우지 않음) 뒤 `exec airflow scheduler`. 스케줄러만 다시 만듦(`docker compose up -d --no-deps airflow-scheduler`).
  - 처방 뒤 다시: 시작 즉시 옛 컨테이너 1개 멈춤(143), 잠금 0, 5분 뒤 시도 2만 끝까지 success. 시험 DAG는 다시 일시정지.
  - 시험 컨테이너(지우지 않음): `admiring_williams`(Exited 0), `flamboyant_northcutt`(Exited 143).
- 2026-11-15 02:30~02:47 A1.5(측정 53). `seoul-agent-job` 빌드(1분 30초, 1.63GB), 컨테이너에서 `agent.run --selftest`, `agent.signals --selftest` 통과.
  - 첫 S1 실행 0초 실패: 복제가 git 소유자 검사에 걸림(F-33). 이미지에 `git config --system --add safe.directory '*'`, `git()`의 `-c` 지움.
  - 다시: **pass**(690.7초, 19턴, 1.119 USD 계산값, G1~G5 통과, 상품 = M0). 운영 변화 0(`measure/ops_snapshot.py` 새로: 표 49개, 상품 sha256, sbx 0). 호스트 `.git` 그대로.
  - 메모리 최대 0.28GiB → 상한 512m(판단 117). 스케줄러 RSS 493~598MB.
- 2026-11-15 02:51~02:54 A2. `tests/check_agent_signal.py`(M1 변이 데이터로 격리 빌드 → `agent.signals.collect`/`record`): 신호 1건(gate_reject, congestion, 거절 배치 6개, `congestion_quarantine_ratio`), 같은 신호 다시 넣기 0건, 운영 신호 표는 되돌림. 3분 20초. 기대 배치 수를 "모든 혼잡도 수집일"로 바꿈(판단 118).
- 2026-11-15 02:54~05:50 A3·A6(측정 54). 묶음 스크립트로 진단 3, 수정 9(S1은 A1.5 포함 3회), 대조군 4, S3 1회. 실행마다 `ops_snapshot` 비교(기록 `logs/agent/a6_runs.txt`).
  - 진단: 원인 적중 3/3. 수정: S1 pass 3/3, M1 pass 3/3, 대조군 no_run 4/4.
  - M7 수정 3회가 G3에서 `mart_sbx_base.dong_profile` 없음으로 끝남, S3는 `--data S3`로 잘못 부름(내 실수, 2초) 뒤 `--data real`로 다시 하자 G1에서 `stg_dbt_sbx_cand.batch_status` 없음. 원인: dbt가 실패한 테스트의 하류를 skip(F-34). 처방(판단 121) 뒤 M7은 에이전트 patch 3개로 판정만 다시 해 needs_decision 3/3(판단 120). S3는 fail(G1 단위 테스트 `rent_rows_rules`), 정답표 기대와 다름(판단 122). 보충 S3b(규칙 + 테스트 기대값)는 needs_decision, G5 116행.
  - 거짓 통과 0. 고칠 파일 준수 6/9(S1 3회 정답표 밖 `congestion_quarantine.sql`, 판단 119). 허용 밖 편집 시도 0. 에이전트 컨테이너 최대 0.286GiB / 512MiB. 모든 실행 운영 변화 0, 호스트 새 브랜치 0. SDK 계산값 합 9.60 USD.
- 2026-11-15 05:50~05:54 A4. 시험 신호 5(`found_by` `test:A4`)를 넣고 `agent_triage` 일시정지 해제 + 수동 실행. 해제와 함께 생긴 예약 실행이 신호를 집어 `run_agent`(DockerOperator, `seoul-agent-job`) 203초 → no_run, 신호 done. 수동 실행은 신호가 없어 `run_agent` 건너뜀. 운영 변화 0. DAG 다시 일시정지. 시험 신호 행은 지우지 않았다(done 상태).
- 2026-11-15 05:55~06:00 A5. 준비물(`GITHUB_TOKEN`, `SLACK_WEBHOOK_URL`)이 `.env`에 없어 실제 PR·Slack은 대기. 수정 실행마다 `pr.json`·`slack.json`(dry-run)과 `branch.bundle`·`branch.patch`가 남음.
  - Airflow 이미지 다시 빌드(SDK 뺀 Dockerfile 적용, 40초, 1,012.0MB → 840.8MB). 돌던 태스크 0을 확인하고 airflow 4개 서비스와 ui만 `up -d --no-deps`로 다시 만듦. 모두 healthy, SDK 없음, docker provider 있음, DAG 가져오기 오류 0, 시작 때 주인 없는 계산 컨테이너 0개 멈춤.
  - 호스트 샘플러(job_stats, 02:31~05:54) 멈춤.
- 2026-11-15 05:40~06:10 A7. 측정 54, F-34, 판단 119~122, 결정 108 검증·112 검증·결과, guarantees 2.8 새 행, design_agent.md 9절 구현 상태 칸, README 증거 표(D4, S1, S2, 에이전트 행)와 한계 문단.
