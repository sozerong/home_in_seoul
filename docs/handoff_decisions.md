# 남은 작업 지시서 2: 사용자 결정 반영 (2026-11-14)

`docs/handoff_finish.md`의 남은 일을 사용자 결정대로 끝낸다.

**판단이 필요한 지점은 `docs/handoff_judgments.md`(J-1~J-18)의 기준대로 하고, 근거와 함께 `docs/judgment_log.md`에 남긴다. 기준에 없거나 실측과 어긋나면 멈추고 선택지 표를 보인다.** 공통 규칙은 handoff_finish.md 0절을 그대로 따른다. 한 번에 한 단계씩 진행하고, 단계마다 보고한 뒤 "다음"을 기다린다. UI는 별도 세션에서 `docs/handoff_ui.md`로 진행하니 이 세션에서는 건드리지 않는다.

## 0. 사용자 결정 (2026-11-14 오전) → decisions.md 103번부터 기록

| 항목 | 결정 | 기록 |
|---|---|---|
| F-9 생활이동 병목 | **A + C + D 함께.** A: 충돌 검사를 숫자 min/max로 바꿔 해시 집계로. C: 관문이 원본을 한 번만 읽게. D: 코드표 브로드캐스트. B(캐시)와 E(그대로)는 고르지 않음 | 결정 103. 선택지와 이유는 F-9 표를 옮긴다 |
| F-19 DuckDB 재측정 | **B.** F-9 처방 뒤 같은 SQL로 두 엔진을 다시 잰다 | 결정 103에 함께 |
| F-1 dbt 배열 우회 | **A. 우회 유지** | 결정 104. F-1 상태를 "우회 유지(결정 104)"로 |
| 옛 생활이동 경로 | **progress 6절의 1~5는 지우고 6(`keep.commute_od_before_3_5`)은 보관** | 결정 105 (결정 85 이행) |
| F-20 디스크 고갈 | **작업 공간을 전용 볼륨 `scratch`로(S4 A), 공간 회수 직후 Docker 디스크 상한(S3)**, 그리고 시작·끝 비우기, 사전 점검, 관측(resource_guard.md 7.6) | 결정 107. F-20 상태 갱신 |
| UI 형태 | **레포 안 읽기 전용 웹 앱.** CLAUDE.md 단계 9의 "대시보드나 웹 서비스는 만들지 않는다"는 데이터 상품 시연에 대한 것이다. 이 UI는 운영과 개발용 도구라 범위가 다르다는 점을 함께 적는다 | 결정 106. 구현은 handoff_ui.md |

## 1. 커밋 (줄바꿈부터)

- 작업 트리 파일이 CRLF로 바뀌어 있다(HEAD는 LF). `git diff --stat`은 4,082줄인데, 줄바꿈을 무시하면 245줄이다. 이대로 커밋하면 문서 전체가 바뀐 것으로 남는다.
- 다음 순서로 진행한다.
  1. `.gitattributes`에 `* text=auto eol=lf`를 넣는다. 바이너리(zip, parquet, png 등)는 `binary`로 둔다.
  2. `git add --renormalize .` 뒤 `git diff --cached --stat`이 실제 변경 규모와 맞는지 본다.
  3. `lake/`, `.env`, `Claude outputs/`가 스테이지에 없는지 확인한다.
  4. 결과를 보고하고 멈춘다. 사용자가 "다음"이라고 하면 커밋한다. 푸시는 사용자 확인 뒤에 한다(CI는 `tests/test_*.py`, F-18).
- 이 커밋 뒤에 UI 세션이 worktree를 만든다. 그래서 이 단계가 가장 먼저다.

## 2. 옛 생활이동 경로 지우기 (progress 6절 1~5)

- 6절 표 그대로 지운다.
  - 표 2개(+ 인덱스, `07_ops.sql` 105줄의 인덱스 문장)
  - `04_stg_living_migration.sql`과 README 반복문
  - `transform/living_migration.py`: 쓰이는 `HEADER`, `LAKE`, `load_codes`를 `living_iceberg.py`로 옮기고 import 4곳을 고친다
  - `tests/test_living_migration.py`
- 남길 것도 6절 그대로다: 레이크, Iceberg, 과거 측정 재현 도구(첫 줄에 "8절 은퇴 뒤 재현 불가"), `keep.commute_od_before_3_5`.
- 확인:
  - wh_load 1회
  - 상품 데이터 파일 sha256이 그대로인지
  - 리니지 다시 추출 뒤 옛 표로 가는 간선 0
  - CI와 같은 시험 반복문 통과
- 결과는 progress 6절과 결정 105에 남긴다. 커밋 하나.

## 2.5 작업 공간 정리와 위치 바꾸기 (결정 107, resource_guard.md 7.6)

F-9 실행 전에 한다. 공간이 부족해 실행 자체를 할 수 없기 때문이다(C: 여유 141GB, 2026-11-14 10시).

1. **S0 조사:** Airflow 컨테이너 `/tmp`를 `du -sh /tmp/* | sort -h`로 보고 다섯 종류로 나눈다.
   - 끝났거나 죽은 실행의 셔플·spill(`blockmgr-*`, `spark-local/*`, DuckDB tmp)
   - 설치물·캐시(`/tmp/duckdb_pkg`, jar)
   - 기록(이벤트 로그)
   - 잠금(`heavy.lock`)
   - 데이터(있으면 안 됨)
   측정 38의 실행 전 74.6GB가 무엇이었는지 F-20에 적는다.
2. **S1 정리:** 첫 종류만 지울 목록으로 보고하고, 사용자 확인 뒤 heavy 잠금을 잡은 상태에서 지운다.
3. **S4 전용 볼륨 (결정 107 A):**
   - compose에 이름 볼륨 `scratch`를 두고 Airflow 컨테이너 `/scratch`에 붙인다.
   - Spark `SPARK_LOCAL`(`transform/iceberg.py`)과 DuckDB `temp_directory`(`measure/duckdb_sql.py`), `common.temp_budget()`의 기준 경로를 `/scratch` 아래로 옮긴다.
   - 볼륨을 붙이려면 컨테이너를 다시 만들어야 하므로 `pg_dump`를 먼저 한다(handoff_finish.md 0절). Airflow 컨테이너만 다시 만들고 db는 건드리지 않는다.
4. **S5 시작·끝 비우기:** heavy 잠금을 잡은 직후 `/scratch/spark`, `/scratch/duckdb`를 비우고 시작하고, 끝날 때(실패해도) 다시 비운다. 잠금이 한 번에 하나를 보장하니 다른 실행의 파일을 지울 위험은 없다. 확인용 시험 하나를 둔다: 일부러 남긴 찌꺼기 파일이 다음 실행 시작 때 사라지는지.
5. **S6 사전 점검:** 시작 조건(지금 C: 여유 250GB)을 측정 38의 작업 공간 최대 333.6GB와 여유로 다시 근거를 적는다. F-9 처방 뒤 다시 잰 값으로 바꾼다.
6. **S7 관측:** wh_load 알림 요약에 호스트 C: 여유와 `/scratch` 크기 한 줄을 넣는다.
7. **S2 회수 (사용자):** fstrim → Docker 종료, `wsl --shutdown` → diskpart compact(측정 36 순서). 전후 VHDX 크기와 C: 여유를 기록한다.
8. **S3 디스크 상한 (사용자, 결정 107):**
   - 회수 직후 Claude Code가 값을 제안한다. 근거는 VM 안 실제 사용, 작업 공간 실측 최대, PostgreSQL 증가 여유, 그리고 호스트 C: 여유보다 작아야 한다는 조건이다.
   - 사용자가 Docker Desktop Settings > Resources > Advanced에서 건다(메뉴 위치 [확인 필요]).
   - 건 뒤 VM 안 `df`가 상한을 보이는지, `temp_budget()`이 그 값을 쓰는지 확인한다.
9. 측정 44로 기록(회수 전후, 상한 값). F-20 상태를 갱신하고 커밋 하나.

## 3. F-9 처방 A + C + D (결정 103)

### 3.1 바꾸기와 실행 계획 증거 (전체 실행 없이)

- 한 번에 하나씩 바꾸고, 바꿀 때마다 `--explain`으로 실행 계획을 떠서 표 하나에 쌓는다. 그래야 각 변경의 효과를 따로 보일 수 있다.

  | 상태 | BatchScan | SortAggregate | HashAggregate | SortMergeJoin | BroadcastHashJoin | Exchange |
  |---|---|---|---|---|---|---|
  | 처방 전 (`batch_status` / `quarantine`) | 5 / 3 | 4 / 2 | 6 / 0 | 13 / 7 | 0 / 0 | 26 / 14 |
  | + A | | | | | | |
  | + C | | | | | | |
  | + D | | | | | | |

- **A의 판정이 옛 코드와 같아야 한다.** 숫자로는 같고 글자로는 다른 값("05"와 "5")과 `*` 섞임을 옛 문자열 비교와 똑같이 판정하는지, 작은 입력으로 확인하는 시험을 하나 남긴다(`spark_sql/living`의 test 파일 또는 `tests/`).
- **C에서 격리 수를 두 번 세지 않게 한다.** V8 격리 행 수를 `clean`에서 세면 `21_quarantine`과 셈이 둘이 된다(F-9 단점 칸). 관문의 격리 합이 `stg.living_quarantine` 행 수와 같아야 한다는 기존 검사로 확인한다.
- 측정 41로 기록한다. 계획만 뜨고 실행은 하지 않는다.

### 3.2 전체 실행 1회 (Airflow DAG로, DAG 증명 겸)

1. **공간 확인.** 2.5절이 끝났는지(회수와 상한) 보고, 시작 조건(S6)을 만족하는지 확인한다. 처방 전 실행은 C:를 328GB 썼다(측정 38).
2. **같음 비교를 위한 보관.** 지금 운영 표 세 개(`stg.living_commute`, `stg.living_batch_status`, `stg.living_quarantine`)를 `keep` 스키마에 복사해 둔다. 크기를 먼저 보고한다.
3. **입력 파일 (사용자).** 사용자가 생활이동 새 달(202608) zip을 열린데이터광장에서 받아 `lake/_inbox/living_migration/`에 넣는다. 아직 올라오지 않았으면 사용자에게 알린다. 그때는 기존 202607 zip을 다시 넣는 것으로 대신할지 묻는다. 같은 파일 재적재는 멱등 확인도 겸한다.
4. **실행 (사용자).** 사용자가 Airflow 화면에서 `file_living_migration`의 일시정지를 풀고 실행한다(CLAUDE.md 단계 7 체크포인트). Claude는 vm_watch를 켜고 지켜본다.
5. **확인:**
   - 같은 입력 범위의 경계·판정·격리 표가 보관본과 두 방향 `EXCEPT ALL` 0이어야 한다. 새 달이 들어갔으면 그 달을 빼고 비교한다.
   - `ops.engine_run`의 시간, 셔플, spill, 단계 수를 측정 38과 같은 열로 나란히 놓는다.
   - wh_load가 이어서 돌고, 상품 sha256이 바뀌면 이유를 적는다.
6. 측정 42, F-9 상태(처방, 수치)를 기록한다. 보관한 `keep` 표 세 개는 사용자 승인 뒤 지운다.

## 4. F-19 B: DuckDB 재측정

- F-9 처방 뒤 SQL로 측정 39와 같은 조건(16GiB, 임시 상한 = C: 여유 − 50GiB, threads 12와 4)에서 다시 잰다.
- 공간이 모자라면 회수부터 요청한다.
- 끝까지 가면 Spark와 결과가 같은지(`--compare`), 시간과 메모리를 나란히 적는다. 또 실패하면 실패한 지점과 이유를 그대로 결론으로 둔다.
- 측정 43, F-19 상태를 기록한다.

## 5. 파일 DAG 2개를 Airflow로

- `file_sbiz_store`, `file_parcel_boundary`: 사용자가 파일을 `lake/_inbox/{출처}/`에 넣고, Airflow 화면에서 일시정지를 풀어 실행한다.
  - 새 분기 파일이 없으면 레이크에 있는 같은 파일을 다시 넣는다. 그러면 결과가 같은지(멱등) 함께 본다.
- Claude는 넣을 파일 경로와 기대 결과(행 수, 상품 sha256)를 먼저 알려 주고, 실행 뒤 대조한다.
- guarantees.md 3절 "출처 파일 DAG가 Airflow로 끝까지 돈다"를 갱신한다.

## 6. 마무리 기록

- **README에 증거 표를 하나 둔다:** "주장 → 증명 방법 → 수치 → 다시 돌리는 명령". 주장은 다섯 가지다: 원천이 바뀌어도 SQL만 고친다, 옮겨도 결과가 같다, 다시 돌려도 같다, 전부 되거나 전부 안 된다, 실패는 드러난다. 각 칸은 측정 번호와 guarantees.md 절로 잇는다.
- guarantees.md 3절, failure_analysis.md 상태 칸, progress_decision99.md 7절을 마지막 상태로 닫는다.
- handoff_finish.md 6절 완성 기준 표를 다시 대조해 보고한다.
- 커밋한다. 푸시는 사용자 확인 뒤에 한다.

## 7. 사용자가 직접 할 일 (Claude는 요청하고 기다림)

| 언제 | 할 일 |
|---|---|
| 2.5 | `/tmp` 지울 목록 확인, 공간 회수(fstrim + diskpart), 회수 직후 Docker 디스크 상한 걸기 |
| 3.2 전 | 생활이동 zip 받기 |
| 3.2, 5 | Airflow 화면에서 DAG 일시정지 풀고 실행, 성공 확인 |
| 아무 때나 | `.wslconfig`(L1), `.env`의 `SLACK_WEBHOOK_URL` |
| 마지막 | README만 보고 처음부터 실행(CLAUDE.md 단계 10) |
