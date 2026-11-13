# 결정 99 작업 진행 정리 (2026-11-13)

`docs/handoff_decision99.md` 순서대로 진행한 결과와, 사용자가 정할 것을 모은다. 2026-11-13 22시 이후의 남은 일은 `docs/handoff_finish.md`가 이어받는다.

| 단계 | 내용 | 상태 |
|---|---|---|
| 1 | 커밋 | 완료 (598b807, a042e58, 679e252) |
| 2 | 생활이동 관문 판정·격리 행을 PostgreSQL에 (판단 64) | **완료.** 실제 실행기 1회, `07_ops.sql`, wh_load, metabase_setup(측정 38, handoff_finish.md 2절) |
| 3 | "거절되면 직전 승인 배치" (판단 63) | **완료: 결정 100(B, 거절된 달만 빼고 갱신)**. `test_month_approved` → `warn_month_approved`(기록만). 실제 실행 확인은 handoff_finish.md 2.1 |
| 4 | 8절 옛 경로 은퇴 | 대기 → handoff_finish.md 3.1 |
| 5 | 3.9 DuckDB 비교 | 대기 → handoff_finish.md 3.3. 선행 조건(공간 회수 측정 36, 자원 상한과 유도 시험 측정 37·F-17)은 끝남 |
| 6 | 체크포인트 보고 | handoff_finish.md 6절 완성 기준으로 넘김 |
| (추가) | 자원 고갈 방지 (resource_guard.md, 결정 101) | L0·L2·L3·L4·L5와 유도 시험 완료(측정 36·37, F-17, guarantees 2.7). L1 `.wslconfig`는 제안만(사용자 파일) |

## 1. 단계 2 결과: 무엇이 바뀌었나

| 파일 | 바뀐 것 |
|---|---|
| `warehouse/sql/12_stg_living_commute.sql` | 새 표 `stg.living_batch_status`(한 행 = 배치: 대상연월, 수집일), `stg.living_quarantine`(한 행 = 원본 한 행). DB에 적용함 |
| `transform/spark_sql_runner.py` | `--save 뷰=표`: 마지막 뷰 말고도 PostgreSQL에 남길 뷰. 경계 표와 같은 트랜잭션에서 바꾼다. 테스트가 실패해도 이 표들은 쓰고, 경계 표만 이전 그대로 둔다 |
| 같은 파일 `swap()` | 교체를 "지우고 이름 바꾸기"에서 "비우고 복사"로 (F-13 처방 A) |
| `dags/file_sources.py` | 생활이동 DAG 명령에 `--save batch_status=stg.living_batch_status quarantine=stg.living_quarantine` |
| `warehouse/dbt/models/stg_dbt/batch_status_living.sql` | Spark 판정(쉼표 글자)을 다른 세 출처와 같은 열로 바꾼다. `batch_status` 합집합에 더함 |
| `warehouse/sql/07_ops.sql` | `ops.freshness` 생활이동은 승인된 배치 기준, `ops.quality`는 관문과 같은 분모. **DB에는 아직 적용 안 함** |
| `warehouse/sql/ops/06, 10, 14` | 새 표를 읽게. **Metabase에는 아직 반영 안 함** |
| `warehouse/export_product.py` | manifest `commute_quarantine_rows`를 `stg.living_quarantine`에서 |
| `tests/test_runner_swap.py` | 교체 시험(새 파일) |

### 왜 테스트가 실패해도 판정을 쓰나

생활이동 원본(Iceberg)은 대상연월마다 수집일 하나만 둔다(덮어쓰기). 그래서 어떤 배치가 거절되면 그 달에는 승인된 배치가 하나도 없게 되고, `test_month_approved`가 반드시 실패한다. 실패할 때 아무것도 쓰지 않으면 거절 사실이 PostgreSQL에 영영 닿지 않아, 운영 카드와 알림이 볼 수 없다.

비유: 검사에서 불합격한 물건을 창고에 들이지 않는 것(경계 표 그대로)과, 불합격 기록부에 적는 것(판정 표에 씀)은 별개다. 지금까지는 기록부에도 적지 않았다.

### 왜 F-13 처방을 함께 넣었나

`stg.living_commute`를 dbt 뷰(`wh.fact_commute`)가 읽고 있어서, 표를 지우고 이름을 바꾸는 교체가 거절된다(F-13). 새 판정 표도 dbt 뷰가 읽으니 같은 이유로 막힌다. 고치는 곳도 같은 함수라 결정 99 규칙의 "같은 일"로 보고, F-13 선택지 중 가장 작은 A를 넣었다(판단 78).

## 2. 단계 2 확인한 것과 못 한 것

| 확인 | 방법 | 결과 |
|---|---|---|
| 교체 중 의존 뷰 유지, 전부 되거나 전부 안 됨 | `tests/test_runner_swap.py` (시험 스키마, 0행 실패 주입 포함) | 통과 |
| 판정 열 모양 | dbt unit test `batch_status_living_shape` | 통과 |
| 다른 세 출처 판정이 그대로인지 | 임시 스키마 `stg_dbt_chk`에 `+batch_status` 빌드 뒤 지금 `stg_dbt.batch_status`와 두 방향 `EXCEPT ALL` | 0행, 0행 (임시 스키마는 지움) |
| 운영 뷰와 카드가 생활이동 거절을 보이는지 | 되돌리는 트랜잭션에서 `07_ops.sql` 적용, 시험 행 넣고 `ops.freshness`, `ops.quality`, 카드 06·10·14, 알림 요약 쿼리 실행 | 모두 보임. 되돌린 뒤 실제 DB는 그대로 |
| **실행기가 실제로 두 표를 채우는지** | 실제 Spark 실행 | **못 함** |

못 한 이유: 실행기 한 번이 컨테이너 `/tmp`에 셔플 143GB, spill 57GB를 쓴다(측정 32의 `ops.engine_run`). 그 시각 호스트 C: 여유는 11.06GB, `docker_data.vhdx`는 668.99GB였다. 지금 돌리면 F-16(VM 멈춤)이 다시 난다.

**갱신 (2026-11-13 21:29, 측정 36):** fstrim 뒤 diskpart 압축으로 VHDX 80.16GB(80,160,489,472B), C: 여유 649.31GB가 됐다. 이제 실행할 수 있다(handoff_finish.md 2.1).

### 공간 회수 뒤 할 순서

1. 실행기 1회 (DAG 명령과 같은 인자). 두 표가 찼는지, 경계 표 교체가 성공하는지(F-13 처방 확인), 교체 시간을 잰다.
2. `warehouse/sql/07_ops.sql`을 DB에 적용.
3. wh_load 1회 (`batch_status`에 생활이동 들어감).
4. `python -m warehouse.metabase_setup`으로 카드 반영.

그 전에 wh_load가 돌면 manifest의 `commute_quarantine_rows`가 0으로 나간다(새 표가 비어 있음).

### 바뀐 뜻 하나

`ops.quality`의 생활이동 "통과 행"이 옛 정제 표 행 수에서 관문과 같은 분모(C4 완전 중복을 하나로 센 뒤 원본 행, 범위 C 밖 포함)로 바뀐다. 격리 행도 범위 C 밖 행과 거절된 배치의 행을 포함한다. 옛 숫자(격리 93,880행)와 바로 비교하면 안 된다.

## 3. 단계 3: 거절되면 어떻게 할지 (사용자 선택)

### 지금 구조에서 확인한 사실

- Iceberg `lh.living.migration_rows`: 대상연월 10개(202510~202607), 모두 수집일 하나씩. 같은 달을 다른 수집일로 다시 받은 적은 없다.
  - 2026-10-21 수집: 202605, 202606, 202607
  - 2026-11-03 수집: 202510 ~ 202604
- Iceberg 스냅샷 13개, 만료(expire) 하지 않음. 그중 같은 zip을 다시 넣은 덮어쓰기가 4번 있다.
- 레이크 `lake/seoul_living_migration/collected_date=*/`에 받은 zip이 그대로 있다.
- 마트 `commute_od`는 최근 6개월만 쓴다(`limit 6`). 새 달이 빠지면 상품은 이전 6개월 그대로라, 다른 출처의 "직전 승인"과 같은 효과가 난다.
- **새 달이 처음 거절되면 어느 선택지든 같은 달의 직전 배치는 없다.** 선택지 사이의 차이는 (1) 같은 달을 다시 받았을 때와 (2) 한 달의 거절이 다른 달 갱신까지 막는지에서 난다.

### 선택지

| 선택지 | 거절된 달에 일어나는 일 | 장점 | 단점 | 고칠 곳 |
|---|---|---|---|---|
| A. 지금 그대로 (실행 실패) | 경계 표 전체가 이전 실행 그대로 | 이미 구현됨. 실패와 알림으로 드러나고, 이제 판정도 PG에 남는다 | 한 달의 거절이 다른 달 갱신까지 막는다. 결정 90 문구와 다르다 | 결정 90 문구만 |
| B. 거절된 달만 빼고 갱신 | 그 달이 경계 표에서 빠진다 (판정은 PG와 알림에 남음) | 다른 출처처럼 그 배치만 격리. 코드가 작다(`test_month_approved`를 실패 대신 기록만) | 다시 받은 달이 거절되면 이전에 승인된 내용까지 빠진다 | `test_month_approved` |
| C. Iceberg 스냅샷에서 직전 승인분 읽기 | 거절 전 스냅샷의 그 파티션을 읽는다 | 결정 90 문구 그대로. 원본을 더 저장하지 않는다 | 승인된 스냅샷 id를 판정에 남겨야 한다. 실행기에 SQL 파일 밖의 동적 로직이 필요하다. 스냅샷 만료 정책과 묶인다 | 실행기, 관문 표, 리니지 |
| D. Iceberg를 (대상연월, 수집일) 단위로 보관 | 경계 SQL이 이미 "승인된 가장 최근 수집일"을 고르므로 그대로 성립 | 관문과 같은 키. SQL 파일은 거의 그대로 | 다시 받을 때마다 원본이 1.3억~1.5억 행씩 늘고 전체 재계산 비용도 는다. 오래된 수집일 정리 규칙 필요 | `transform/living_iceberg.py` 파티션·멱등 단위 |
| E. 레이크 zip으로 사람이 되돌림 | 직전 수집일 zip으로 `living_iceberg`를 다시 돌린다 | 코드 없음. 원본은 레이크에 이미 있다 | 사람 손이 필요하다. 그동안은 A처럼 멈춰 있다 | 운영 문서 |

**결정 (2026-11-13 사용자): B.** `docs/decisions.md` 결정 100. 같은 달 재수집 거절은 E(레이크 zip으로 사람이 되돌림)를 런북으로, 승격 경로로 F(Iceberg 브랜치 WAP)를 기록만. C, D는 하지 않음.

## 4. 사용자가 할 일

- ~~단계 3 선택~~ 끝남(결정 100).
- ~~Docker 가상 디스크 공간 회수~~ 끝남(측정 36: fstrim 936.5GiB, 압축 뒤 VHDX 80.16GB, C: 여유 649.31GB).
- L1 `.wslconfig` 값 정하기(제안은 resource_guard.md 6절).
- `.env`의 `SLACK_WEBHOOK_URL` (알림 실제 도착 확인용).

## 5. 관련 기록

- 판단: `docs/judgment_log.md` 76~87
- 결정: 100(거절 처리), 101(자원 상한)
- 측정: 36(공간 회수), 37(메모리 예산)
- 오류: `docs/failure_analysis.md` F-13(처방 A 넣음), F-16(원인 정정: 메모리 → 스왑 소진이 가장 유력), F-17([유도] 자원 상한 시험)
- 보증: `docs/guarantees.md` 2.3(실행기 교체), 3절(생활이동 거절이 카드·알림에 닿는다, 실제 실행은 증명 전)
