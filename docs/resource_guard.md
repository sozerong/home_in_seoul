# 자원 고갈 방지: VM 멈춤(F-16)을 다시 겪지 않기 위한 작업 방법

F-16(DuckDB 실행 중 Docker VM 전체가 멈춤)을 다시 겪지 않도록 층별 방지 장치와 순서를 정리한다. 원칙은 설계 원칙 4다: **자원이 모자라면 기계 전체가 조용히 멈추는 대신, 넘친 작업 하나가 드러나는 오류로 끝나야 한다.** 값에 붙은 숫자 중 실측이 아닌 것은 "제안값"이라고 적었고, 적용 전 0단계 측정으로 다시 정한다.

## 0. 원인 정리 (2026-11-13 20:3x, Docker 로그와 Windows 이벤트로 다시 본 것)

| 후보 | 근거 | 판단 |
|---|---|---|
| 메모리 → 스왑 소진 → VM 전체 정체 | 옛 VM의 `swap.vhdx`가 설정 최대치 8.00GB까지 늘었다(마지막 쓰기 18:45:05, 새 VM 것은 0.04GB). DuckDB 기본 `memory_limit`는 보이는 메모리의 80%(24.7GiB)이고 앞선 실행의 프로세스 RSS는 27.03GB. 같은 VM에 PostgreSQL, Airflow, Metabase(JVM)가 있다. VM은 멈춘 동안에도 코어 약 2.3개를 썼고(백엔드 로그의 vmmemWSL CPU 시간) 네트워크와 디스크 쓰기만 끊겼다 | **가장 잘 맞음.** 커널 로그가 남지 않아 확정은 아니다 |
| 호스트 C: 부족으로 가상 디스크가 못 늘어남 | Windows 시스템 로그에 디스크 부족·가상 디스크 오류 없음. Hyper-V 일시 정지 기록 없음. 19:26 C: 여유 5.9GB(디스크가 찼다면 0 근처에 머물렀어야 함). VHDX 669GB 안에서 실제 사용은 209GB라 spill이 이미 할당된 블록을 다시 썼을 수 있다 | **약해짐.** 함께 작용했을 가능성은 남음 |
| CPU 독점 | 19:26 가상 CPU 13.3%로 내려간 뒤에도 응답 없음 | 주원인 아님 |

F-16 본문 정정이 필요한 곳: (1) "19:35 WSL VM은 새로 떴다"는 틀렸다. 19:46까지 아침 09:56에 뜬 같은 VM 프로세스(PID 28320)였고 실제 재부팅은 19:55:46이다. (2) "C:가 다시 비자 VM이 떴다"는 로그가 뒷받침하지 않는다. 19:45 시작 실패는 멈춘 옛 VM과 WSL 서비스가 응답하지 않아서였다. (3) `OOMKilled=false`는 컨테이너에 메모리 상한이 없을 때 VM 전체 고갈을 잡지 못하므로 "OOM 아님"의 근거로 약하다.

공통 뿌리: **엔진 기본값은 "이 기계를 혼자 쓴다"고 가정한다** (스레드 = 코어 수, 메모리 = 보이는 것의 80%, 임시 공간 = 보이는 디스크의 90%). 이 기계에서는 VM, 컨테이너, 가상 디스크가 겹겹이 자원을 나눠 쓰므로 세 가정이 모두 틀린다.

## 1. 층별 방지 장치

| 층 | 장치 | 무엇을 막나 | 지금 상태 |
|---|---|---|---|
| L0 관측 | 무거운 실행 동안 VM 메모리·스왑·디스크를 레포 밖이 아닌 바인드 마운트 파일에 5초마다 기록(`/proc/meminfo`, `vmstat`, `df`). 측정 도구는 `python -u` | 다음에 같은 일이 나면 원인을 판정할 수 있다(이번에는 커널 로그와 출력이 VM과 함께 사라짐) | **2026-11-13 완료**: `measure/vm_watch.py` → `logs/vm_watch/*.tsv`. 손으로 띄운다(DAG 연결은 아직) |
| L1 VM | `%USERPROFILE%\.wslconfig`에 `memory`, `swap`, `processors` 명시 | VM이 호스트를 얼마나 쓰는지 정해 둔다. 스왑을 작게(제안값 2GB) 두어 끝없는 스왑 대신 빨리 OOM으로 드러나게 | 파일 없음(기본값: 메모리 호스트의 절반 30.9GiB, 스왑 8GB). 제안만 함(사용자 파일) |
| L2 컨테이너 | `docker-compose.yml` 서비스마다 `mem_limit`와 `memswap_limit`(같은 값 = 컨테이너 스왑 금지), `cpus` | 넘친 컨테이너만 cgroup OOM으로 죽고 `OOMKilled=true`로 드러난다. PostgreSQL과 다른 서비스는 산다 | **2026-11-13 완료**: db 4g, airflow 22g(cpus 12), metabase 2g, pgadmin 512m, 스왑 금지(측정 37). `docker update`로 적용. **2026-11-14 바꿈(결정 109)**: Airflow를 구성 요소로 나누고 계산을 작업 컨테이너로 뺐다. apiserver 512m, scheduler 1792m(cpus 12), dag-processor 768m, triggerer 768m, socket-proxy 128m, 계산 spark-job·duckdb-job 18g(cpus 12, 작업마다 컨테이너, pool heavy로 한 번에 하나). 모두 스왑 금지(측정 47). 계산 컨테이너가 넘쳐도 스케줄러는 산다(측정 48) |
| L3 엔진 | DuckDB `memory_limit`, `threads`, `max_temp_directory_size`, `temp_directory` 명시. Spark `spark.driver.memory`, `SPARK_LOCAL_DIRS`(전용 폴더), 실행 뒤 `blockmgr-*` 정리 | 엔진이 자기 몫 안에서 spill하거나 오류로 끝난다 | **2026-11-13 완료**: `common.temp_budget()`(호스트 C: 여유 − 50GiB). DuckDB 도구는 이것을 `max_temp_directory_size`로, Spark 실행기는 200GB 미만이면 시작하지 않음, Spark `spark.local.dir` = `/tmp/spark-local`. 상한 뒤 DuckDB 기본 `memory_limit`는 cgroup의 80% |
| L4 디스크 | 가상 디스크 공간 회수(diskpart compact), 무거운 실행 전 여유 확인(사전 점검), 가능하면 Docker 가상 디스크 상한을 호스트 여유보다 작게 [확인 필요: 지금 크기보다 작게 줄일 수 있는지] | VM이 실제 용량을 알게 해 디스크 부족이 VM 안의 `ENOSPC` 오류로 드러나게 | **2026-11-13 회수**: fstrim 뒤 압축으로 VHDX 718.3GB → 80.2GB, C: 여유 649GB(측정 36). 가상 디스크 상한 축소는 [확인 필요] |
| L5 실행 규율 | 무거운 작업(Spark 실행기, DuckDB, dbt 전체 빌드)은 Airflow pool `heavy`(크기 1)로 한 번에 하나. 손으로 돌리는 측정도 같은 잠금을 잡는다 | 무거운 작업 둘이 겹쳐 함께 넘치는 일 | **2026-11-13 완료**: pool `heavy`(1), `/tmp/heavy.lock`(실행기, DuckDB 도구, dbt 빌드, 리니지 추출). **2026-11-14 바꿈(결정 109 H3)**: 계산이 여러 컨테이너로 나뉘어 컨테이너 안 파일 잠금이 서로 보이지 않으므로 PostgreSQL advisory lock(키 20261114, `common.heavy_lock()`, 셸 `python -m common lock -- <명령>`). 잡은 프로세스가 죽으면 연결이 끊겨 풀린다. 시험 `tests/check_heavy_lock.py` |
| L6 복구 | 런북: 증거 먼저(컨테이너 재생성 금지, `start`만), `taskkill /f /im wslservice.exe`로 WSL 강제 종료, Docker "Reset/Purge" 금지, 복구 뒤 `pg_dump` | 복구하다 증거나 데이터를 잃는 일 | 이번에 손으로 한 것을 문서로 |

## 2. 메모리 예산 정하는 법 (값은 0단계 측정 뒤)

1. 평시(무거운 작업 없음)와 wh_load 실행 중의 컨테이너별 최대 메모리를 `docker stats --no-stream`으로 10초마다 10분 잰다(db, airflow, metabase, pgadmin).
2. 이웃 몫 = 각 서비스 최대 × 1.2(여유), 운영체제·페이지 캐시 몫 = 2GiB(제안값).
3. 무거운 작업 몫 = VM 메모리 − 이웃 몫 합 − 운영체제 몫.
4. DuckDB `memory_limit` = 무거운 작업 몫 × 0.8(제안값). 근거: 앞선 실행에서 버퍼 상한 밖 할당까지 합친 RSS가 상한보다 약 2% 컸다(27.03GB 대 26.5GB). 넉넉히 20%를 남긴다.
5. Spark `spark.driver.memory`(지금 8g)는 JVM 힙 상한이라 그대로 두되, 힙 밖 사용까지 합친 RSS를 1의 방법으로 재서 무거운 작업 몫 안에 드는지 본다.
6. 무거운 작업이 Airflow 컨테이너 안에서 돌기 때문에 Airflow 컨테이너 상한 = 스케줄러 등 평시 몫 + 무거운 작업 몫이다. 이 상한에 걸리면 스케줄러도 같이 죽는다(아래 3절 선택지).

## 3. 정해야 할 것 (사용자)

| 선택지 | 장점 | 단점 |
|---|---|---|
| A. Airflow 컨테이너 하나에 상한(무거운 작업 포함) | 고칠 곳이 `docker-compose.yml` 한 줄 | 넘치면 스케줄러까지 함께 죽는다(그래도 VM과 PostgreSQL은 산다) |
| B. 무거운 작업용 컨테이너를 따로 두고(`docker compose run --rm`), 그 컨테이너에만 큰 상한 | 넘쳐도 스케줄러가 산다. k8s의 "작업마다 파드와 자원 한도"와 같은 구조라 이식 때 그대로 옮겨진다 | DAG의 실행 명령과 이미지 사용 방식이 바뀐다 |

제안: 지금은 A로 바닥을 깔고(설정 몇 줄), k8s 이식 때 B로 간다.

## 4. 순서

1. **L0 관측 도구**를 먼저 만든다(다음 사고를 판정할 수 있게).
2. **L4 공간 회수**: 사용자가 diskpart compact(관리자 권한). 전후 VHDX 크기와 C: 여유를 기록한다.
3. **2절 측정**으로 메모리 예산을 정한다.
4. **L1 `.wslconfig`**(사용자 파일, 적용하려면 `wsl --shutdown`) → **L2 컨테이너 상한** → **L3 엔진 설정** → **L5 pool**.
5. **유도 시험으로 증명**: 상한을 작게 걸고 DuckDB를 일부러 넘치게 해서 (a) 그 컨테이너만 `OOMKilled=true`로 끝나는지 (b) PostgreSQL이 계속 응답하는지 (c) VM이 멈추지 않는지 본다. 디스크도 `max_temp_directory_size`를 작게 걸어 DuckDB가 오류로 끝나는지 본다. 결과는 `guarantees.md` 2.7("실패는 드러난다")에 강도 ②로 남기고, `failure_analysis.md`에 `[유도]` 항목으로 남긴다.
6. 그다음 미뤄 둔 실제 실행(생활이동 실행기 1회, 3.9 DuckDB)을 한 번에 하나씩.

## 5. 하지 않는 것

- 디스크 증설 같은 하드웨어 해결. 이 문서는 "모자라도 드러나게"를 다룬다.
- Airflow 밖 감시(결정 86). 다만 L0 관측 파일은 남긴다.

## 6. 진행 기록 (2026-11-13)

- 결정: 3절은 A(결정 101), k8s 이식 때 B.
- 4절 1~5번 완료. 유도 시험 결과는 `failure_analysis.md` F-17, `guarantees.md` 2.7(강도 ②). 예산 근거는 `measurements.md` 측정 37.
- L1 `.wslconfig` 제안(사용자가 정함. 적용하려면 Docker Desktop 종료와 `wsl --shutdown`):
  ```
  [wsl2]
  memory=31GB
  processors=16
  swap=2GB
  ```
  - `memory=31GB`: 지금 기본값(30.91GiB)과 거의 같게 둬서 측정 37의 예산이 그대로 맞게 한다. 더 줄이면 예산을 다시 계산한다.
  - `swap=2GB`: 지금 8GB. F-16에서 옛 VM의 스왑 파일이 8GB까지 찼다. 컨테이너는 이제 스왑 금지(L2)라 VM 스왑은 컨테이너 밖 프로세스만 쓴다. 작게 두면 VM 전체가 모자랄 때 끝없는 스왑 대신 빨리 OOM으로 드러난다.
  - `processors=16`: 지금과 같다(명시만).
- 남은 것: 생활이동 실행기 메모리(5단계 실제 실행 때 vm_watch로), vm_watch를 무거운 DAG 태스크에 붙일지.

## 7. 저장소: 디스크가 찬 이유와 관리 (2026-11-14, F-20)

### 7.1 저장할 것은 "얼마나 오래 사는가"로 나눈다

| 종류 | 이 프로젝트의 예 | 수명 | 지우는 주체 | 둘 곳 |
|---|---|---|---|---|
| 영구 데이터 | PostgreSQL, Iceberg, 레이크, 상품 파일 | 계속 | 사람(결정) | 백업되는 전용 저장소 |
| 작업 공간(임시) | Spark 셔플·spill, DuckDB tmp | 작업 한 번 | 엔진, 작업이 끝날 때 | 작업과 함께 생기고 사라지는 곳 |
| 캐시 | jar, DuckDB 확장(계산 이미지 안), 컨테이너 이미지 | 지워도 다시 만들 수 있음 | 정리 정책(오래된 것부터) | 상한이 있는 곳 |
| 로그·기록 | Spark 이벤트 로그, vm_watch, Airflow 로그 | 정해진 기간 | 순환 정책 | 상한이 있는 곳, 또는 밖으로 보냄 |

넷은 저장 위치, 크기 상한, 지우는 주체가 달라야 한다. 이번에 디스크가 찬 것은 작업 공간(1회 최대 333.6GB, 측정 38)을 영구 데이터처럼 같은 디스크에, 상한 없이 두고, 지우는 일을 엔진의 정상 종료에만 맡겼기 때문이다.

### 7.2 컨테이너 안의 파일이 놓이는 네 곳 (Docker)

| 위치 | 수명 | 이 프로젝트에서 |
|---|---|---|
| 컨테이너 쓰기 층 | 컨테이너를 지울 때까지(재시작해도 남음) | (2026-11-14 결정 107 S4 뒤로는 작업 공간이 `scratch` 볼륨이라 여기 없음. 계산 컨테이너는 성공하면 지워지고 실패하면 남는다. 아래는 S4 전 상태) Spark 셔플(`/tmp/spark-local`), DuckDB tmp. **작업 공간이 여기 있는 것이 문제.** ("`/tmp`에 남은 약 79GB"는 틀린 해석이었다. 2026-11-14 S0 실측 `/tmp` 166MB, 쓰기 층 전체 340MB, F-21) |
| 이름 붙은 볼륨 | 컨테이너를 지워도 남음 | `pgdata`, `scratch`(작업 공간, 2026-11-14부터 Airflow 구성 요소와 계산 컨테이너 spark-job·duckdb-job이 함께 붙임. heavy 잠금을 잡은 쪽만 쓰고 비움), `airflow_logs`(작업 로그, 스케줄러가 쓰고 apiserver가 읽음) |
| 바인드 마운트(호스트 폴더) | 호스트 파일 그대로 | 레포, 레이크. 계산 컨테이너는 레포를 읽기 전용으로, `lakehouse/`만 쓰기로 붙인다(결정 109). DockerOperator는 호스트 경로(`HOST_PROJECT_DIR`)로 붙인다 |
| tmpfs(메모리) | 컨테이너가 멈출 때까지 | 안 씀. 메모리를 써서 수백 GB 셔플에 맞지 않음 |

### 7.3 이번에 찬 이유 (F-20)

1. 쓰는 양: F-9의 셔플·spill로 1회 최대 333.6GB.
2. 놓인 곳: 작업 공간이 쓰기 층이라 상한도 수명도 없다.
3. 정리는 정상 종료 때만: Spark ContextCleaner·종료 시 폴더 삭제, DuckDB 인스턴스 종료 시 임시 폴더 삭제 모두 정상 종료가 전제다. 죽은 실행(F-16 등)의 폴더는 아무도 지우지 않는다. (처음에는 `/tmp`의 약 79GB가 이것이라고 보았으나, 그 값은 VM 데이터 디스크 전체 사용량이었다. 2026-11-14 S0에서 `/tmp`에 남은 셔플·spill은 0이었다, F-21. 비정상 종료 대비 S5는 그대로 필요하다.)
4. 반환: TRIM이 없고 VHDX는 압축 때만 준다.
5. 거짓 용량: VM은 1TB로 안다. `common.temp_budget()`이 호스트 C: 여유로 이것을 맞춘다(L3).

### 7.4 관리 원칙

1. 분리: 작업 공간은 데이터와 다른 전용 위치.
2. 상한: 엔진 설정과 저장소 쪽 상한 둘 다. Spark에는 임시 공간 상한 설정이 없어 저장소 쪽(또는 사전 점검)이 유일한 방어선.
3. 수명 묶기: 작업 공간이 작업과 함께 생기고 사라지게. 비정상 종료에 대비해 시작할 때 지난 찌꺼기를 비운다.
4. 관측: 사용량을 재고 문턱을 넘으면 알린다.
5. 반환: 얇게 할당되는 저장소면 TRIM이 끝까지 내려가는지 본다.

### 7.5 k8s에서는

| 무엇 | 지금 PC | k8s |
|---|---|---|
| 작업 공간 | 컨테이너 `/tmp`(쓰기 층) | 파드 emptyDir(Spark는 `spark.local.dir`마다 emptyDir을 기본으로 만든다), 셔플이 크면 executor마다 OnDemand PVC(`claimName=OnDemand`, executor와 함께 생기고 반환) |
| 크기 상한 | DuckDB 설정, Spark는 없음, 사전 점검 | `resources.limits.ephemeral-storage`(쓰기 층 + 로그 + 디스크 emptyDir 합), emptyDir `sizeLimit`, PVC `sizeLimit` |
| 넘쳤을 때 | VM 전체 멈춤(F-16) → 지금은 컨테이너 오류(F-17) | 그 파드만 축출. 노드 전체가 차면 DiskPressure(기본 `nodefs.available<10%`)로 축출 |
| 죽은 실행의 찌꺼기 | 남음 | 파드와 함께 사라짐 |
| 이미지·죽은 컨테이너 | 사람이 정리 | kubelet GC(죽은 파드·컨테이너, 그다음 안 쓰는 이미지) |
| 영구 데이터 | `pgdata` 볼륨 | StatefulSet + PVC, 회수 정책 Retain(Delete면 PVC 삭제 때 데이터도 사라짐) |
| Iceberg | 같은 가상 디스크 | 오브젝트 스토리지 |
| 크기 근거 | 측정 38 `/tmp` 최대 333.6GB | 같은 실측으로 `sizeLimit`, 노드 디스크, IOPS를 정한다 |

하지 않을 것: 셔플을 tmpfs(`spark.kubernetes.local.dirs.tmpfs=true`)에 두는 것. spill이 노드 메모리를 먹어 OOM이 난다.

근거: Kubernetes 문서 "Local ephemeral storage", Spark 문서 "Running on Kubernetes - Local Storage", F-20의 출처.

### 7.6 지금 환경 처방 (계획, 2026-11-14)

| 단계 | 할 일 | 원칙 | 이미 있는 것 | 누가 |
|---|---|---|---|---|
| S0 | 컨테이너 `/tmp`를 다섯 종류로 나눠 보고: 끝났거나 죽은 실행의 셔플·spill / 설치물·캐시 / 기록 / 잠금 / 데이터(있으면 안 됨) | 관측 | | Claude Code |
| S1 | 끝났거나 죽은 실행의 셔플·spill만 지움. heavy 잠금이 비어 있을 때 | 수명 | `heavy_lock` | Claude Code(사용자 확인) |
| S2 | fstrim → Docker 종료, `wsl --shutdown` → diskpart compact | 반환 | 측정 36 순서 | 사용자 (적용, 측정 44) |
| S3 | Docker Desktop "Disk image max size"를 회수 직후 호스트 여유보다 작게 | 상한 | L4 | 적용 못 함: WSL2 백엔드 화면에 메뉴 없음. 사전 점검(S6)으로 대신(결정 107) |
| S4 | 작업 공간 위치 (아래 선택지) | 분리 | `SPARK_LOCAL` 상수 | **A 적용(2026-11-14)**: 이름 볼륨 `scratch` → `/scratch`, Spark `/scratch/spark`, DuckDB `/scratch/duckdb`, `temp_budget()` 기준 `/scratch`. 확인 `tests/check_scratch.py` |
| S5 | heavy 잠금을 잡은 뒤 작업 공간(Spark 로컬 폴더, DuckDB temp)을 비우고 시작, 끝날 때도 비움. 잠금이 한 번에 하나를 보장하므로 다른 실행의 파일을 지울 위험이 없다. emptyDir과 같은 효과 | 수명 | `heavy_lock`, `SPARK_LOCAL` | **적용(2026-11-14)**: `common.clear_scratch`, `tests/check_scratch.py` 1부 |
| S6 | 시작 조건을 "실측 최대 + 여유"로: 지금 250GB(측정 38 기준), F-9 뒤 다시 잰 값으로 | 상한 | `temp_budget()`, `RESERVE_BYTES` | **적용(2026-11-14)**: `--min-temp-gb` 394(328.24GB x 1.2), 호스트 C: 여유 약 447.7GB에서 시작 |
| S7 | wh_load 알림 요약에 호스트 C: 여유와 작업 공간 크기 한 줄 | 관측 | `dags/alerting.py` 요약 | 적용(`disk_line`, 작업 로그에는 늘, 슬랙 글에는 경고가 있을 때 붙음) |
| S8 | 쓰는 양 줄이기 | 근본 | F-9 처방(결정 103) | Claude Code |

S4 선택지:

| 선택지 | 장점 | 단점 |
|---|---|---|
| A. 전용 이름 볼륨 `scratch`(Spark·DuckDB 임시를 여기로) | 쓰기 층과 분리. 크기를 볼륨 단위로 보고 비우기 쉽다. 속도 그대로 | 같은 VHDX 안이라 호스트 반환은 여전히 S2가 필요 |
| B. 호스트 폴더 바인드 마운트 | 지우면 C:로 바로 돌아온다(VHDX를 거치지 않음) | WSL 파일 공유를 거쳐 느릴 수 있다 [미측정]. Windows 실시간 검사가 셔플 파일을 볼 수 있다 [확인 필요] |
| C. 지금 그대로(`/tmp/spark-local`) + S5 | 일이 없다 | 쓰기 층에 남아 크기를 따로 보기 어렵다 |

결정 (사용자, 2026-11-14): S4는 A(전용 볼륨 `scratch`), S3은 회수 직후 건다. decisions.md 결정 107로 기록(F-20 처방). 실행은 handoff_decisions.md 2.5절.
