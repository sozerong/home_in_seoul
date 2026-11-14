# 판단 기준서 (지시서 부록, 2026-11-14)

`docs/handoff_decisions.md`와 `docs/handoff_ui.md`를 진행하다가 Claude Code가 스스로 판단해야 하는 지점을 모았다. 지점마다 "실제 현장에서는 어떻게 하나"와 그 근거(공식 문서, 이 프로젝트의 실측)를 적고, 이 프로젝트에서 그대로 따를 기준을 정했다.

## 쓰는 법

1. 지시서를 진행하다 아래 J 항목을 만나면 이 기준대로 한다.
2. 판단마다 `docs/judgment_log.md`에 남긴다. 형식은 이렇다.
   - 판단 N. 제목
   - 상황
   - 따른 기준(J-번호)
   - 근거(실측값, 출처)
   - 결과
3. **묻지 않고 스스로 정하는 판단 (사용자, 2026-11-14):** 크게 의미 없는 판단은 묻지 말고 이 기준대로 정해 실행한다. 판단 기록에는 한 줄만 남긴다. 기준은 되돌리기 쉽고, 결과 수치·데이터·설계에 영향이 없는 것이다.
   - 파일·함수 이름, 주석, 로그·임시 파일 위치
   - 도구 옵션, 작업 순서 바꾸기
   - 문서 표현, 측정 기록의 형식
   - 이미 정한 결정 안에서의 세부 구현
4. **커밋은 사용자가 한다.** 커밋할 때가 되면 묶음과 메시지를 정해 `git add`/`git commit` 명령을 보여 주고 멈춘다. 푸시도 사용자가 한다.
5. **멈추고 사용자에게 선택지 표를 보이는 경우:**
   - 기준에 없는 판단을 만났을 때
   - 기준과 실측이 어긋날 때(예: 계산한 상한이 허용 범위를 넘음)
   - 지우는 일일 때
   - 결정 번호가 정해지지 않은 설계 선택일 때

---

## A. 저장소와 커밋

### J-1. 줄바꿈 (handoff_decisions 1절)

- **상황:** 작업 트리가 CRLF로 바뀌어 있다. HEAD는 LF다. 줄바꿈을 무시하면 실제 변경은 245줄이다.
- **현장에서는:** 저장소 단위 `.gitattributes`로 고정한다. 개인 설정(`core.autocrlf`)보다 우선해서 모든 기여자에게 같은 규칙이 걸린다. 그다음 `git add --renormalize .`로 이미 있는 파일을 다시 맞추고, `git status`로 바뀔 파일을 본 뒤 커밋한다. 정규화하면 안 되는 바이너리는 `binary`(= `-text -diff`)로 명시한다.
- **여기서는:**
  - `* text=auto eol=lf`로 둔다.
  - `*.zip *.parquet *.png *.jpg *.xlsx *.shp *.dbf *.shx *.prj binary`로 둔다. 레포에 실제로 있는 확장자만 남긴다.
  - 정규화 커밋은 내용 변경 커밋과 **나눈다.** 정규화만 담은 커밋을 먼저 하고, 그 뒤 내용 커밋을 한다. 그래야 `git log -p`에서 실제 변경이 보인다.
- **근거:** gitattributes(5) "End-of-line conversion", GitHub Docs "Configuring Git to handle line endings".
- **남길 것:** 정규화 전후 `git diff --stat`, `git diff --stat --ignore-cr-at-eol`의 두 숫자.

### J-2. 커밋 단위와 푸시

- **현장에서는:** 한 커밋은 한 가지 이유만 담고, 되돌릴 수 있는 크기로 만든다. 공유 브랜치에 푸시하기 전에는 CI가 통과해야 한다.
- **여기서는:**
  - 지시서의 절 하나를 커밋 하나로 한다(이미 그렇게 해 왔다).
  - 푸시는 사용자 확인 뒤에 한다.
  - CI는 `tests/test_*.py`만 돈다. DB가 필요한 시험은 `check_` 이름으로 둔다(F-18).

### J-3. 지우기 전 확인 (옛 경로 2절, `/tmp` 2.5절)

- **현장에서는:** 지우는 일은 네 단계로 한다.
  1. 쓰는 곳이 0인지 확인한다(의존성, 쿼리 로그, 리니지).
  2. 복구할 방법을 확인한다(백업이 있거나, 다시 만들 수 있거나).
  3. 지운다.
  4. 다시 확인한다(이후 빌드, 결과 해시).
- **여기서는:**
  - 쓰는 곳 0: 이미 확인했다. `pg_depend` 0, 리니지 간선 0(회차 35), dbt·Metabase SQL 0(progress 6절). 지우기 직전에 한 번 더 본다.
  - 복구 방법: 옛 표 두 개는 레이크 원본과 옛 커밋의 코드로 다시 만들 수 있다. 그래서 **데이터 덤프는 하지 않는다.** 이 이유를 judgment_log에 적는다. 같음 증명은 측정 33, 원본 표는 `keep.commute_od_before_3_5`(보관)로 남는다.
  - 지운 뒤: wh_load 1회, 상품 sha256, 리니지 재추출, CI 시험 반복문.
  - `/tmp`는 J-4 기준만 따른다.

---

## B. 디스크와 작업 공간 (F-20, 결정 107)

### J-4. `/tmp`에서 무엇을 지워도 되나 (2.5절 S0, S1)

- **현장에서는:** 셔플과 spill은 그 애플리케이션이 살아 있는 동안에만 다시 읽는다. 클러스터 관리자는 애플리케이션이 끝나면 그 애플리케이션의 로컬 폴더를 지운다. k8s에서는 emptyDir이 파드와 함께 사라진다. 끝난 애플리케이션의 셔플 폴더는 아무도 다시 읽지 않는다.
- **여기서는 다섯 종류로 나눈다.**

  | 종류 | 예 | 처리 |
  |---|---|---|
  | 끝났거나 죽은 실행의 셔플·spill | `blockmgr-*`, `spark-local/*`, DuckDB `*.tmp` | 지울 후보. heavy 잠금이 비어 있을 때, 사용자 확인 뒤 |
  | 설치물·캐시 | `/tmp/duckdb_pkg`(측정 39가 `PYTHONPATH`로 씀), jar 캐시 | 남김. 지우면 다시 설치해야 함 |
  | 기록 | Spark 이벤트 로그 | 남김. 정리하려면 따로 사용자 확인 |
  | 잠금 | `heavy.lock` | 남김 |
  | 데이터 | Iceberg, DB 파일 | `/tmp`에 있으면 안 된다. 보이면 멈추고 보고 |

- **판단 근거로 남길 것:** 지울 후보마다 마지막 수정 시각, 그 시각에 돌던 실행(ops.engine_run, vm_watch, failure_analysis의 F 번호와 맞춰 봄), 크기.

### J-5. 작업 공간을 전용 볼륨으로 (S4, 결정 107 A)

- **현장에서는:**
  - Spark는 `spark.local.dir`(또는 `SPARK_LOCAL_DIRS`)를 빠른 로컬 디스크의 전용 폴더로 둔다.
  - k8s에서는 `spark.local.dir`마다 emptyDir을 만들고, 크면 executor마다 OnDemand PVC를 붙인다.
  - 컨테이너 쓰기 층은 작은 쓰기용이다. kubelet도 쓰기 층을 ephemeral storage로 세어 상한을 건다.
- **여기서는:**
  - 이름 볼륨 `scratch`를 `/scratch`에 붙인다.
  - 하위 폴더는 `/scratch/spark`(Spark), `/scratch/duckdb`(DuckDB)다.
  - `common.temp_budget()`의 기준 경로도 `/scratch`로 바꾼다.
  - 바꾼 뒤 시험 하나를 둔다: 실행기를 짧게(`--explain` 또는 작은 입력) 돌렸을 때 셔플 파일이 `/scratch/spark`에 생기는지.
- **근거:** Spark "Running on Kubernetes - Local Storage", Kubernetes "Local ephemeral storage".

### J-6. 시작과 끝에 작업 공간 비우기 (S5)

- **현장에서는:** 정리를 "정상 종료"에만 맡기지 않는다.
  - YARN과 Spark standalone은 애플리케이션이 끝나면 노드 관리자가 그 폴더를 지운다. standalone에는 끝난 앱 폴더를 주기적으로 지우는 `spark.worker.cleanup.enabled`도 있다.
  - k8s는 파드와 함께 지운다.
  - 공통점: 정리하는 주체가 작업 밖에 있다. 그래서 작업이 비정상으로 죽어도 정리된다.
- **여기서는:**
  - 정리 주체는 heavy 잠금을 잡은 실행 스크립트다. 잠금을 잡은 직후 비우고, 끝날 때(`try/finally` 또는 셸 `trap EXIT`) 다시 비운다.
  - 잠금(pool heavy 크기 1 + `flock`)이 한 번에 하나를 보장하므로 남의 실행 파일을 지울 위험이 없다.
  - **잠금을 못 잡으면 비우지 않는다.**
- **확인 시험:** 찌꺼기 파일을 일부러 남기고 다음 실행 시작에서 사라지는지 본다. 잠금이 잡혀 있을 때 다른 프로세스가 비우지 않는지도 본다.

### J-7. 사전 점검 문턱 (S6)

- **현장에서는:** 작업을 시작하기 전에 필요한 공간을 실측 최대값과 여유로 잡고, 모자라면 시작하지 않는다. 시작한 뒤 중간에 넘쳐 죽는 것보다 시작 전 거절이 싸다.
- **여기서 고칠 것:** 지금 시작 조건은 C: 여유 250GB다. 그런데 측정 38의 처방 전 실행은 C:를 **328GB** 썼다(641.24 → 313.00GB). 즉 지금 문턱으로는 시작했다가 넘칠 수 있다.
- **기준:**
  - 문턱 = 같은 작업의 실측 최대 C: 사용량 × 1.2 + `RESERVE_BYTES`(50GiB).
  - F-9 처방 전 값: 328GB × 1.2 + 50GiB ≈ 447GB.
  - F-9 처방 뒤에는 새로 잰 최대값으로 바꾼다.
  - 1.2는 제안값이다. 측정 37의 메모리 예산과 같은 방식으로 정했다.
  - 바꾼 값과 계산식을 코드 주석과 judgment_log에 남긴다.

### J-8. Docker 디스크 상한 값 (S3)

- **현장에서는:** 노드 디스크는 정해진 비율 이상 비워 둔다. 예를 들어 kubelet은 기본으로 `nodefs.available<10%`에서 축출을 시작한다. 상한은 "실제로 쓸 최대 + 여유"로 잡되, 물리적으로 줄 수 있는 크기를 넘지 않게 한다.
- **기준:**
  - 상한 = 회수 직후 VM 안 실제 사용(`df`) + 작업 공간 실측 최대(J-7과 같은 값) + PostgreSQL 증가 여유 + 그 합의 10%.
  - PostgreSQL 증가 여유는 측정 38 전후 DB 크기 차이로 근거를 댄다. 재지 않았으면 [확인 필요]로 두고 50GiB를 쓴다.
  - **조건: 상한 ≤ 회수 직후 호스트 C: 여유 − `RESERVE_BYTES`.** 넘으면 멈추고 보고한다. 이때 선택지는 F-9 처방 뒤로 미루기, 작업 공간 최대를 줄이기, 상한 없이 사전 점검만 두기다.
  - 사용자가 건 뒤, VM 안 `df`가 상한을 보이는지 확인한다. 보이지 않으면 `temp_budget()`의 호스트 기준(레포 바인드 마운트)을 그대로 둔다.

---

## C. F-9 처방 (결정 103)

### J-9. A: 충돌 검사를 숫자 min/max로

- **엔진 쪽 사실:** Spark는 집계 버퍼가 모두 고정 길이로 제자리 갱신이 되는 타입일 때만 HashAggregate를 고른다. 문자열 min/max는 버퍼가 StringType이라 그 조건을 못 맞춘다. 그래서 정렬 기반 집계(SortAggregate)로 간다. ObjectHashAggregate는 collect 계열 같은 다른 집계 함수용이라 min/max에는 해당하지 않는다. 이 프로젝트의 실행 계획에서도 `20_clean`이 SortAggregate였다(F-9 원인 2).
- **기준:**
  - 판정이 옛 문자열 비교와 **같아야 한다.** 숫자로는 같고 글자로는 다른 값("05"와 "5")과 `*`가 섞인 경우를 따로 센다(F-9 표 단점 칸).
  - 작은 입력으로 경계 사례 시험을 먼저 만들고, 옛 SQL과 새 SQL의 결과가 같은 것을 확인한 다음 바꾼다(정답 먼저, 결정 49와 같은 방식).
  - `--explain`에서 SortAggregate 수가 줄었는지 본다.

### J-10. C: 관문이 원본을 한 번만 읽게

- **엔진 쪽 사실:**
  - Spark SQL은 임시 뷰나 CTE를 참조할 때마다 계획을 펼친다(인라인).
  - 셔플 결과를 다시 쓰는 ReuseExchange는 셔플 아래 계획이 완전히 같을 때만 된다.
  - 그래서 같은 원본에서 조건이 조금씩 다른 집계를 여러 번 하면 원본 읽기도 그만큼 늘어난다. F-9에서 BatchScan이 5번이었다.
- **현장에서는 두 길이 있다.** 결과를 물질화(cache, 임시 표)하거나, 한 번 읽으면서 조건부 집계(`count(*) FILTER (WHERE ...)` 또는 `sum(CASE ...)`)로 여러 셈을 동시에 하게 SQL을 다시 쓴다. 결정 103은 다시 쓰기를 골랐다(B 캐시는 14억 행이 디스크로 간다).
- **기준:**
  - 관문의 셈(원본 행, 격리 사유별, 통과)을 `checked` 한 번 읽기의 조건부 집계로 모은다.
  - V8처럼 `clean`이 필요한 셈은 `21_quarantine`과 같은 정의를 쓰게 해서 **셈이 둘이 되지 않게** 한다.
  - 확인: 관문 격리 합 = `stg.living_quarantine` 행 수(기존 검사).

### J-11. D: 코드표 브로드캐스트

- **엔진 쪽 사실:**
  - Spark는 한쪽 크기가 `spark.sql.autoBroadcastJoinThreshold`(기본 10MB) 이하라고 **통계로 알 때** 브로드캐스트 조인을 고른다.
  - 코드표가 Python `createDataFrame`에서 와서 계획에 `Scan ExistingRDD`로 보이면 크기 통계가 없다. 그래서 SortMergeJoin이 됐다(F-9 원인 3).
  - 현장에서는 `/*+ BROADCAST(t) */` 힌트로 명시하거나 통계가 있는 형태로 만든다.
- **기준:**
  - SQL 파일에 힌트를 두는 쪽을 먼저 한다. SQL 파일 밖의 동적 로직을 늘리지 않는다(결정 88).
  - 코드표 크기(행, 바이트)를 재서 브로드캐스트가 맞는 크기인지 judgment_log에 적는다.
  - `--explain`에서 SortMergeJoin이 BroadcastHashJoin으로 바뀌었는지 본다.

### J-12. 처방 효과 측정

- **현장에서는:** 한 번에 한 가지를 바꾸고, 같은 조건에서 전후를 잰다. 반복하지 못하면 그 한계를 적는다.
- **기준:**
  - 변경 하나마다 `--explain` 연산자 수 표를 만든다(측정 41). 실행 계획은 결정적이라 반복이 필요 없다.
  - 전체 실행은 1회다(측정 42). 측정 38과 같은 조건이어야 한다: 결정 101 상한, cpus 12, `--save` 두 표, 같은 입력 범위. 새 달이 들어가면 입력이 달라지므로, 그 몫(행 수)을 함께 적어 비교 조건 차이를 밝힌다.
  - 1회 측정이라 실행 시간 차이의 잡음은 모른다. 개선이 10% 미만이면 "개선 확인 안 됨"으로 적는다(CLAUDE.md 8절 "개선이 없으면 없다고 쓴다").
  - `ops.engine_run`의 셔플 쓰기, 메모리·디스크 흘림, 단계 수, 단계 시간 합을 측정 38과 같은 열로 놓는다.

### J-13. 처방 전후 같음 증명

- **현장에서는:** 로직을 바꾼 뒤 결과가 같아야 하면, 바꾸기 전 결과를 보관하고 두 방향 차집합(EXCEPT ALL)이 0인지 본다. 이 프로젝트가 계속 써 온 방식이다(측정 23, 24, 33).
- **기준:**
  - 운영 표 세 개를 `keep`에 복사한 뒤 실행하고, 같은 입력 범위로 두 방향 EXCEPT ALL 0을 확인한다.
  - 새 달이 들어갔으면 그 달을 빼고 비교한다.
  - 차이가 나면 지우지 말고 멈춘다. 차이가 나는 행 몇 개를 들고 원인 후보(J-9의 "05"/"5" 등)를 보고한다.

### J-14. 새 달 파일이 아직 없을 때

- **현장에서는:** 레이크는 덧붙이기만 한다. 같은 파일을 다시 넣는 것은 멱등성 시험으로 흔히 쓴다.
- **기준:**
  - 202608이 없으면 사용자에게 알리고, 기존 202607 zip을 inbox에 다시 넣을지 묻는다.
  - 다시 넣으면 새 수집일 폴더가 생긴다(원본은 고치지 않음). 결과 표가 이전과 같은지(EXCEPT ALL 0)가 멱등 증명이 된다. guarantees.md에 강도 ①로 남긴다.

### J-15. DuckDB 재측정이 또 실패할 때 (4절)

- **기준:**
  - 결정 101 예산(메모리 16GiB, 임시 = 그때 `temp_budget()`)을 **넘겨서 다시 돌리지 않는다.** 예산을 넘기면 F-16 조건이 된다.
  - 실패한 지점, 메모리, 임시 사용을 측정 39와 같은 표에 적고, 그것을 결정 94의 답으로 둔다.
  - 예산을 늘려서라도 재 보고 싶다면, 그것은 결정 사항이라 사용자에게 묻는다.

---

## D. UI (handoff_ui.md)

### J-16. 실행 기록을 어디서 읽나

- **현장에서는:** Airflow 3는 메타데이터 DB 직접 접근을 막았다. 외부 도구는 Stable REST API, Python 클라이언트, Task SDK를 쓰라고 공식 문서가 권한다. 메타데이터 표는 버전마다 바뀌는 내부 구조라, 직접 읽으면 업그레이드 때 UI가 깨진다.
- **기준:**
  - Airflow REST API v2를 쓴다.
  - 인증(simple auth manager의 토큰 발급)이 막히면, 메타 DB 직접 읽기로 우회하지 말고 멈춰서 선택지 표를 보인다.

### J-17. 새 의존성

- **기준:**
  - Airflow 이미지에 이미 있는 것(FastAPI, uvicorn, psycopg 등)만 쓴다. U0에서 `pip list`로 확인하고 버전을 적는다.
  - 없는 것이 필요하면 멈춘다(설계 원칙 5 "이유를 댈 수 없는 도구는 넣지 않는다").

### J-18. 두 세션이 함께 돌 때

- **기준:**
  - UI 세션은 worktree(`../new_proj_ui`, 브랜치 `ui`)에서만 쓴다.
  - DB는 `ui_reader`로 읽기만 하고, heavy 작업은 돌리지 않는다.
  - 결정은 `docs/ui.md`에 쌓았다가 병합 때 decisions.md 다음 번호로 옮긴다.
  - 메인 세션이 compose를 바꾸면(2.5절 `scratch` 볼륨) UI 세션은 main을 다시 받아(rebase) compose 충돌을 먼저 푼다.

---

## 출처

- gitattributes(5): https://git-scm.com/docs/gitattributes
- GitHub Docs, Configuring Git to handle line endings: https://docs.github.com/en/get-started/git-basics/configuring-git-to-handle-line-endings
- Kubernetes, Local ephemeral storage: https://kubernetes.io/docs/concepts/storage/ephemeral-storage
- Spark, Running on Kubernetes (Local Storage): https://spark.apache.org/docs/latest/running-on-kubernetes.html#local-storage
- Spark, Spark Standalone Mode (`spark.worker.cleanup.enabled`): https://spark.apache.org/docs/latest/spark-standalone.html
- Spark SQL Performance Tuning (브로드캐스트 힌트, `autoBroadcastJoinThreshold`): https://spark.apache.org/docs/latest/sql-performance-tuning.html
- HashAggregateExec 조건(집계 버퍼 타입): https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-SparkPlan-HashAggregateExec.html
- Spark ContextCleaner와 주기 GC(SPARK-47764): https://issues.apache.org/jira/browse/SPARK-47764
- Airflow 3 Public Interface: https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html
- 이 프로젝트: 측정 23, 24, 33, 36~40, F-9, F-16~F-20, 결정 88, 94, 101, 103, 107, resource_guard.md 7절
