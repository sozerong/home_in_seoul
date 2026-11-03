"""웨어하우스, 마트, 데이터 상품 다시 만들기 DAG. 단계 7 기본안 3, 단계 9, 고도화 2단계(결정 30~32).

일정 대신 Asset으로 돈다: 스테이징(역사마스터, 혼잡도, 생활이동, 상가정보, 경계, 지적도, 전월세) 중 하나라도 갱신되면
1) dbt_build       warehouse/dbt의 wh·mart 모델과 테스트를 빌드용 스키마(wh_build, mart_build)에 만든다.
                   지난번 실패로 남은 빌드 스키마를 먼저 지운다. 테스트 하나라도 실패하면 여기서 멈추고 실제 wh·mart는 그대로다.
2) swap_schemas    wh_build -> wh, mart_build -> mart로 한 트랜잭션에서 교체하고 옛 스키마를 지운다.
3) export_product  product/v{오늘}/에 상품 파일과 manifest를 쓴다. 같은 날 다시 돌면 그 폴더를 새로 만든다.
앞 단계가 실패하면 뒤 단계는 돌지 않는다. 모두 멱등이다.
"""
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, AssetAny

from alerting import notify_failure
from assets import STG_RENT, WH_INPUTS

with DAG(
    dag_id="wh_load",
    schedule=AssetAny(*WH_INPUTS, STG_RENT),  # ponytail: 전월세만 바뀌어도 wh 전체를 다시 만든다. 느려지면 마트 DAG를 따로 뗀다
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "on_failure_callback": notify_failure},
    tags=["warehouse"],
):
    in_dbt = "set -euo pipefail; cd /opt/project/warehouse/dbt; "
    dbt = "/opt/airflow/dbt-venv/bin/dbt"  # dbt 전용 가상환경 (결정 30 D3)
    build = BashOperator(task_id="dbt_build", bash_command=in_dbt + f"{dbt} run-operation drop_build_schemas; {dbt} build")
    swap = BashOperator(task_id="swap_schemas", bash_command=in_dbt + f"{dbt} run-operation swap_schemas")
    export = BashOperator(task_id="export_product",
                          bash_command="set -euo pipefail; cd /opt/project; python -m warehouse.export_product")
    build >> swap >> export
