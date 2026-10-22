"""웨어하우스 다시 만들기 DAG. 단계 7 기본안 3.

일정 대신 Asset으로 돈다: 웨어하우스 테이블에 들어가는 스테이징(역사마스터, 혼잡도, 생활이동, 상가정보, 경계, 지적도) 중
하나라도 갱신되면 warehouse/sql/08_wh_load.sql을 실행한다. 그 파일은 한 트랜잭션으로 wh 테이블 전체를 다시 만들고,
구조 검사(대응되지 않는 코드, 이어지지 않는 역 등)에 어긋나면 RAISE로 멈춘다(멱등).
"""
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, AssetAny

from alerting import notify_failure
from assets import WH_INPUTS

with DAG(
    dag_id="wh_load",
    schedule=AssetAny(*WH_INPUTS),
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "on_failure_callback": notify_failure},
    tags=["warehouse"],
):
    BashOperator(
        task_id="rebuild_warehouse",
        bash_command="set -euo pipefail; cd /opt/project; python -m warehouse.run_sql warehouse/sql/08_wh_load.sql",
    )
