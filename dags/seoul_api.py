"""서울 열린데이터광장 Open API(역사마스터, 지하철 혼잡도) 주간 수집·정제 DAG. 단계 7 기본안 1.

매주 월요일 05:00(한국 시간). 두 출처는 서로 기다리지 않고 따로 돈다. 처리 단위는 수집일 하나(전체 목록 스냅샷)라
data interval로 고를 기간이 없다. 혼잡도 정제는 직전 스냅샷과 값을 비교해 "직전과 같음/바뀜"을 로그에 남긴다(결정 7).
재시도는 스크립트 안에서만(일시 오류만). 정제가 끝나면 스테이징 Asset을 갱신해 웨어하우스 DAG가 돌게 한다.
"""
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from alerting import notify_failure
from assets import STG_CONGESTION, STG_STATION_MASTER

SOURCES = {  # 이름: (수집 스크립트 인자, 정제 모듈, 갱신하는 Asset)
    "station_master": ("station_master", "transform.station_master", STG_STATION_MASTER),
    "congestion": ("congestion", "transform.congestion", STG_CONGESTION),
}

with DAG(
    dag_id="seoul_api",
    schedule="0 5 * * 1",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "on_failure_callback": notify_failure},
    tags=["seoul", "weekly"],
):
    for name, (service, module, asset) in SOURCES.items():
        BashOperator(
            task_id=f"collect_and_clean_{name}",
            bash_command=(f'set -euo pipefail; cd /opt/project; D=$(date +%F); '
                          f'python -m ingestion.seoul_api {service} --collected-date "$D"; '
                          f'python -m {module} "$D"'),
            outlets=[asset],
        )
