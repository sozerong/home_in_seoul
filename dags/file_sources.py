"""손으로 받는 파일 출처(생활이동, 상가정보, SGIS 경계, 연속지적도) DAG. 단계 7 기본안 2.

사람은 받은 zip을 lake/_inbox/{출처}/에 넣기만 한다. DAG가 센서로 파일이 들어왔는지 기다렸다가(reschedule, 기다리는 동안
작업 칸을 차지하지 않음) register_file로 검사·등록하고, 정제한 뒤 inbox에서 지운다.
- 생활이동: 매월 21일 06:00 시작(기준월 다음 달 20일 공개, 매뉴얼). 최대 7일 기다리고, 안 오면 실패와 알림.
- 나머지: 일정 없이 수동 실행. 파일을 넣고 실행하면 10분 안에 파일을 찾지 못하면 실패.
수집일(D)은 작업이 도는 순간에 한 번 정해 등록과 정제에 같이 넘긴다.
"""
import glob

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from alerting import notify_failure
from assets import STG_LIVING_MIGRATION, STG_PARCEL, STG_SBIZ_STORE, STG_SGIS_DONG

INBOX = "/opt/project/lake/_inbox"

# 생활이동은 행정동 zip만 정제한다(자치구 zip은 등록만). 파일 이름 생활이동_행정동_YYYYMM.zip에서 대상연월을 뗀다.
LIVING_CLEAN = ('for f in "${files[@]}"; do n=$(basename "$f"); case "$n" in 생활이동_행정동_*.zip) '
                'ym=${n#생활이동_행정동_}; python -m transform.living_migration "$D" "${ym%.zip}";; esac; done')

SOURCES = {  # 출처: (일정, 센서 최대 대기 초, 정제 명령, 갱신하는 Asset)
    "living_migration": ("0 6 21 * *", 7 * 24 * 3600, LIVING_CLEAN, STG_LIVING_MIGRATION),
    "sbiz_store": (None, 600, 'python -m transform.sbiz_store "$D"', STG_SBIZ_STORE),
    "sgis_boundary": (None, 600, 'python -m transform.boundary sgis_dong "$D"', STG_SGIS_DONG),
    "parcel_boundary": (None, 600, 'python -m transform.boundary parcel "$D"', STG_PARCEL),
}

for source, (schedule, timeout, clean, asset) in SOURCES.items():
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

        load = BashOperator(
            task_id="register_and_clean",
            bash_command=(f'set -euo pipefail; shopt -s nullglob; cd /opt/project; D=$(date +%F); '
                          f'files=(lake/_inbox/{source}/*.zip); [ -e "${{files[0]:-}}" ] || {{ echo "inbox가 비어 있습니다"; exit 1; }}; '
                          f'for f in "${{files[@]}}"; do python -m ingestion.register_file {source} "$f" --collected-date "$D"; done; '
                          f'{clean}; rm -f "${{files[@]}}"'),
            outlets=[asset],
        )
        wait_for_file() >> load
