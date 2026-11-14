"""손으로 받는 파일 출처(생활이동, 상가정보, SGIS 경계, 연속지적도) DAG. 단계 7 기본안 2.

사람은 받은 zip을 lake/_inbox/{출처}/에 넣기만 한다. DAG가 센서로 파일이 들어왔는지 기다렸다가(reschedule, 기다리는 동안
작업 칸을 차지하지 않음) register_file로 검사·등록하고, 정제한 뒤 inbox에서 지운다.
- 생활이동: 매월 21일 13:00 시작(PC가 켜져 있는 시간, 결정 34)(기준월 다음 달 20일 공개, 매뉴얼). 최대 7일 기다리고, 안 오면 실패와 알림.
- 나머지: 일정 없이 수동 실행. 파일을 넣고 실행하면 10분 안에 파일을 찾지 못하면 실패.
수집일(D)은 작업이 도는 순간에 한 번 정해 등록과 정제에 같이 넘긴다.
"""
import glob

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from alerting import notify_failure
from assets import RAW_SBIZ_STORE, STG_LIVING_COMMUTE, STG_PARCEL, STG_SGIS_DONG
from jobs import spark_job

INBOX = "/opt/project/lake/_inbox"

# 생활이동은 행정동 zip만 정제한다(자치구 zip은 등록만). 파일 이름 생활이동_행정동_YYYYMM.zip에서 대상연월을 뗀다.
# 고도화 4단계(결정 35): zip → Iceberg 표(전체 행) → Spark SQL 실행기 → stg. 결정 109: 둘 다 계산 컨테이너(seoul-spark-job)에서 돈다.
# 등록(레이크에 복사)은 Airflow에서, 계산은 계산 컨테이너에서, inbox 비우기는 다시 Airflow에서 한다. 등록 태스크가 마지막 줄에
# "수집일 파일이름..."을 남기면(XCom) 뒤 두 태스크가 그 값을 쓴다.
LIVING_REGISTER = ('set -euo pipefail; shopt -s nullglob; cd /opt/project; D=$(date +%F); '
                   'files=(lake/_inbox/living_migration/*.zip); [ -e "${files[0]:-}" ] || { echo "inbox가 비어 있습니다"; exit 1; }; '
                   'for f in "${files[@]}"; do python -m ingestion.register_file living_migration "$f" --collected-date "$D"; done; '
                   'names=(); for f in "${files[@]}"; do names+=("$(basename "$f")"); done; echo "$D ${names[*]}"')
REGISTERED = "set -- {{ ti.xcom_pull(task_ids='register') }}; D=$1; shift; "
LIVING_COMPUTE = (REGISTERED + 'for n in "$@"; do case "$n" in 생활이동_행정동_*.zip) '
                  'ym=${n#생활이동_행정동_}; ym=${ym%.zip}; python -m transform.living_iceberg "$D" "$ym";; esac; done; '
                  # 경계 표는 Iceberg 원본 전체에서 매번 다시 만든다(결정 88, 90). 옛 Python 정제(stg.living_migration)는 뺐다(8절)
                  'python -m transform.spark_sql_runner spark_sql/living stg.living_commute --show batch_status '
                  '--save batch_status=stg.living_batch_status quarantine=stg.living_quarantine')  # 판단 64
LIVING_CLEANUP = "set -euo pipefail; cd /opt/project; " + REGISTERED + 'for n in "$@"; do rm -f "lake/_inbox/living_migration/$n"; done'

SOURCES = {  # 출처: (일정, 센서 최대 대기 초, 정제 명령, 갱신하는 Asset 목록)
    "living_migration": ("0 13 21 * *", 7 * 24 * 3600, None, [STG_LIVING_COMMUTE]),  # 태스크 셋으로 나뉜다(아래)
    "sbiz_store": (None, 600, 'python -m transform.sbiz_store_raw "$D"', [RAW_SBIZ_STORE]),  # 파싱만, 정제는 dbt(결정 78)
    "sgis_boundary": (None, 600, 'python -m transform.boundary sgis_dong "$D"', [STG_SGIS_DONG]),
    "parcel_boundary": (None, 600, 'python -m transform.boundary parcel "$D"', [STG_PARCEL]),
}

for source, (schedule, timeout, clean, assets) in SOURCES.items():
    with DAG(
        dag_id=f"file_{source}",
        schedule=schedule,
        start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Seoul"),
        catchup=False,
        max_active_runs=1,
        default_args={"retries": 0, "on_failure_callback": notify_failure},
        tags=["file", "monthly" if schedule else "manual"],
    ):
        pattern = f"{INBOX}/{source}/*.zip"

        @task.sensor(task_id="wait_for_file", poke_interval=600 if schedule else 30, timeout=timeout, mode="reschedule")
        def wait_for_file(pattern=pattern):
            return bool(glob.glob(pattern))

        if source == "living_migration":
            register = BashOperator(task_id="register", bash_command=LIVING_REGISTER)
            compute = spark_job("compute", LIVING_COMPUTE)
            cleanup = BashOperator(task_id="cleanup_inbox", bash_command=LIVING_CLEANUP, outlets=assets)
            wait_for_file() >> register >> compute >> cleanup
            continue

        load = BashOperator(
            task_id="register_and_clean",
            bash_command=(f'set -euo pipefail; shopt -s nullglob; cd /opt/project; D=$(date +%F); '
                          f'files=(lake/_inbox/{source}/*.zip); [ -e "${{files[0]:-}}" ] || {{ echo "inbox가 비어 있습니다"; exit 1; }}; '
                          f'for f in "${{files[@]}}"; do python -m ingestion.register_file {source} "$f" --collected-date "$D"; done; '
                          f'{clean}; rm -f "${{files[@]}}"'),
            outlets=assets,
        )
        wait_for_file() >> load
