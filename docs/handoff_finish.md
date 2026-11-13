# 완성까지 남은 작업 지시서 (2026-11-13)

에이전트(변경 감지 1차 대응)는 이번 범위에서 뺀다. 그 외 남은 실행, 구현, 처방, 기록을 끝내 "완성" 상태로 만든다.

## 0. 공통 규칙

- 한 번에 한 단계. 단계가 끝날 때마다 아래 세 가지를 보고하고, 사용자가 "다음"이라고 하기 전에는 넘어가지 않는다.
  - 바뀐 것
  - 사용자가 직접 확인하는 방법
  - 수치와 조건
- 결정 99를 따른다. 남은 실행과 구현을 먼저 하고, 오류 처방은 그다음에 한다. 단, 실행을 막거나 같은 함수를 고치는 일이면 가장 작은 처방을 먼저 넣는다.
- 새로 만나는 오류는 크기와 상관없이 원인까지 분석해 `docs/failure_analysis.md`에 F-17부터 남긴다(결정 95).
- 수치는 실행해서 얻은 값만 쓴다. 측정 기록은 `docs/measurements.md`(측정 37부터), 증명은 `docs/guarantees.md`에 남긴다.
- 선택지가 둘 이상이면 고르지 말고 표로 보여 준 뒤 멈춘다. 결정은 `docs/decisions.md`에 102번부터 기록한다.
- 하지 않는 것:
  - Docker "Reset to factory defaults"나 "Clean / Purge data"를 누르지 않는다.
  - `wsl --unregister`를 하지 않고, vhdx를 지우지 않는다.
  - 파일이나 표를 지우는 일은 사용자 확인을 먼저 받는다.
  - 컨테이너를 다시 만들어야 하면 `pg_dump`를 먼저 한다.
- 무거운 실행(Spark 실행기, dbt 전체 빌드, DuckDB)에는 두 가지를 지킨다.
  - `measure/vm_watch.py`를 켜 두고(L0), heavy 잠금 파일(`/tmp/heavy.lock`)을 잡는다.
  - 시작 전에 호스트 C: 여유를 기록한다. 측정 32 기준 실행기 1회가 셔플 143GB와 spill 57GB를 쓴다.

## 1. 시작 전 정리

- `docs/progress_decision99.md`를 지금 상태로 고친다.
  - 단계 3은 결정 100으로 끝났다.
  - 디스크는 측정 36으로 회수했다(VHDX 80.16GB, C: 여유 649.31GB).
- 커밋하지 않은 변경(파일 14개 수정, 새 파일 6개)을 먼저 커밋한다. 이후 단계를 하나씩 되돌릴 수 있게 하기 위해서다. `Claude outputs/` 폴더는 커밋하지 않는다.

## 2. 미뤄 둔 실제 실행 4개 (이 순서로)

### 2.1 생활이동 실행기 1회

DAG와 같은 인자로 돌린다.

```
/opt/airflow/spark-venv/bin/python -m transform.spark_sql_runner spark_sql/living stg.living_commute \
  --show batch_status --save batch_status=stg.living_batch_status quarantine=stg.living_quarantine
```

확인할 것:

| 항목 | 기대 |
|---|---|
| `stg.living_batch_status`, `stg.living_quarantine` | 행이 찼다. 판정 10개 달 각각 |
| 경계 표 교체 | 성공. **F-13 처방 A 확인**. failure_analysis.md F-13 상태를 갱신 |
| 교체(비우고 복사) 시간 | `swap()`의 ponytail 주석이 [미측정]이다. 재서 적고 주석도 고친다 |
| `stg.living_commute` 행 수 | 13,782,826. 다르면 이유를 데이터로 설명 |
| `ops.engine_run` | 셔플·spill·시간을 측정 32와 나란히 |
| vm_watch 기록 | 최대 메모리, 스왑, 디스크 |

### 2.2 `warehouse/sql/07_ops.sql` DB 적용

- `ops.freshness`의 생활이동은 승인된 배치 기준이어야 한다.
- `ops.quality`의 분모는 관문과 같아야 한다.
- 옛 숫자(격리 93,880행)와 뜻이 바뀐 점을 측정 기록에 함께 적는다.

### 2.3 wh_load 1회

- `stg_dbt.batch_status`에 생활이동이 들어갔는지 본다.
- 상품 manifest의 `commute_quarantine_rows`가 0이 아닌지 본다.
- 상품 sha256이 직전과 같은지 본다. 다르면 무엇이 왜 바뀌었는지 설명한다.
- `.env`에 `SLACK_WEBHOOK_URL`이 있으면 요약 메시지가 실제로 도착하는지 사용자에게 확인받는다. 이 결과로 guarantees.md 3절 "알림 도착"을 갱신한다.
  - 주소가 없으면 그 칸은 "확인 전"으로 둔다.

### 2.4 `python -m warehouse.metabase_setup`

- 카드 06, 10, 14가 새 표를 읽는지 본다.
- 생활이동 판정이 화면에 보이는지 사용자가 직접 확인한다.

2.1~2.4가 끝나면 guarantees.md 3절의 "생활이동 Spark 경로 실제 실행 증명 전"을 갱신하고 커밋한다.

## 3. 남은 구현

### 3.1 8절 옛 경로 은퇴

- `dags/file_sources.py`의 생활이동 명령에 아직 `transform.living_migration`(옛 Python 경로)이 있다. 이것을 뺀다.
  - 그 결과만 읽는 표와 Asset(`stg.living_migration` 등)이 남는지 찾는다.
  - 쓰는 곳이 없으면 제거 목록을 만들어 사용자 확인을 받은 뒤 지운다(결정 85).
- 리니지를 다시 추출해 옛 표로 가는 간선이 0인지 본다.
- 상품 sha256이 그대로인지 본다.

### 3.2 자원 방지 장치 나머지 (resource_guard.md 4절 3~5)

1. **메모리 예산 측정** (2절 방법): 평시와 wh_load 중 컨테이너별 최대를 `measure/docker_mem.py`로 잰다.
2. **L1 `.wslconfig`**: 사용자 파일이다. 값을 제안만 하고, 사용자가 넣고 `wsl --shutdown`한다.
3. **L2 compose 상한**: 결정 101대로 Airflow 컨테이너에 `mem_limit`와 `memswap_limit`를 같은 값으로 건다. 컨테이너를 다시 만들기 전에 `pg_dump`를 한다.
4. **L3**: 예산에 맞춰 DuckDB `memory_limit`와 Spark 설정을 정한다.
5. **유도 시험**:
   - 상한을 작게 걸고 DuckDB를 일부러 넘치게 한다. (a) 그 컨테이너만 `OOMKilled=true`로 끝나는지 (b) PostgreSQL이 계속 응답하는지 (c) VM이 멈추지 않는지 본다.
   - `max_temp_directory_size`를 작게 걸어 디스크 쪽도 오류로 끝나는지 본다.
   - 결과는 guarantees.md 2.7(강도 ②)과 failure_analysis.md `[유도]` 항목으로 남긴다.

### 3.3 3.9 DuckDB 비교

- 3.2가 끝난 뒤에만 한다(F-16이 이 실행에서 났다).
- 같은 SQL을 Spark와 DuckDB로 돌려 시간, 최대 메모리, spill을 measurements.md에 나란히 적는다(결정 94).
- 개선이 없으면 없다고 쓴다.

## 4. 오류 처방 (결정 99 순서)

| 순서 | 오류 | 할 일 |
|---|---|---|
| 1 | F-13 | 2.1에서 확인되면 상태만 "처방 확인"으로. 안 되면 원인부터 |
| 2 | F-9 (설계서 3.10) | 실행기 34.9분. 원본 다섯 번 읽기, 문자열 min/max로 생긴 정렬 집계. 실행 계획(`--explain`)으로 원인을 다시 확인한 뒤 고치고, 전후 시간·셔플·spill을 같은 조건으로 잰다 |
| 3 | F-4 | `fact_store` 140초. 경계 표에 공간 인덱스를 두고 전후 시간과 실행 계획을 잰다. 결과(행)가 같은지 EXCEPT ALL로 본다 |
| 4 | F-16 | 3.2의 장치와 유도 시험 결과로 상태를 갱신한다 |
| 5 | F-3, F-14 | F-3은 결정 97대로 "그대로 둠"의 근거를 다시 본다. F-14는 측정 도구를 고치고 S6b 재측정 출력이 맞는지 본다 |
| 6 | F-1 | 우회 그대로 둘지 고칠지 선택지 표로 |

처방마다 failure_analysis.md에 "처방, 검증 방법, 수치, 남은 것"을 채우고, 처방 하나에 커밋 하나를 둔다.

## 5. 마무리 기록

- **README**: 목적, 아키텍처 그림, 데이터 소스, 실행 순서, 결과 화면.
  - 실행 순서에는 생활이동 Spark 경로와 Metabase가 들어가야 한다.
- **guarantees.md 3절**: 증명 전 항목이 몇 개 남았는지와 각각 이유.
- **progress 문서**: `docs/progress_decision99.md`를 마지막 상태로 닫는다.

## 6. 완성 기준 (마지막 체크포인트)

아래를 모두 만족하면 사용자에게 "완성 확인"을 요청한다. 하나라도 아니면 무엇이 왜 남았는지 적는다.

| 기준 | 확인 방법 |
|---|---|
| 출처 DAG 6개와 wh_load, lineage가 각각 1회 이상 성공 | Airflow 화면, `dag_run` |
| 미뤄 둔 실행 4개 완료 | 2절 보고 |
| 옛 Python 생활이동 경로 없음 | 3.1 리니지 간선 0 |
| 자원 상한이 걸려 있고 유도 시험 통과 | 3.2 기록 |
| 처방 대기 오류 0 (고침 또는 남기는 이유 기록) | failure_analysis.md |
| guarantees.md 3절 갱신 | 문서 |
| 작업 트리 깨끗 | `git status` |
| README만 보고 처음부터 실행 가능 | 사용자가 직접 (CLAUDE.md 단계 10) |
