"""웨어하우스, 마트, 데이터 상품 다시 만들기 DAG. 단계 7 기본안 3, 단계 9.

일정 대신 Asset으로 돈다: 스테이징(역사마스터, 혼잡도, 생활이동, 상가정보, 경계, 지적도, 전월세) 중 하나라도 갱신되면
1) warehouse/sql/08_wh_load.sql  wh 테이블 전체를 한 트랜잭션으로 다시 만든다. 구조 검사에 어긋나면 RAISE로 멈춘다.
2) warehouse/sql/09_mart.sql     mart 테이블 전체를 한 트랜잭션으로 다시 만든다. 행 수·중복 검사에 어긋나면 멈춘다.
3) warehouse/export_product      product/v{오늘}/에 상품 파일과 manifest를 쓴다. 같은 날 다시 돌면 그 폴더를 새로 만든다.
앞 단계가 실패하면 뒤 단계는 돌지 않는다. 모두 멱등이다.
"""
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, AssetAny

from alerting import notify_failure
from assets import STG_RENT, WH_INPUTS

with DAG(
    dag_id="wh_load",
    schedule=AssetAny(*WH_INPUTS, STG_RENT),  # ponytail: 전월세만 바뀌어도 08을 다시 돈다(약 65초). 느려지면 마트 DAG를 따로 뗀다
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "on_failure_callback": notify_failure},
    tags=["warehouse"],
):
    run = "set -euo pipefail; cd /opt/project; "
    warehouse = BashOperator(task_id="rebuild_warehouse", bash_command=run + "python -m warehouse.run_sql warehouse/sql/08_wh_load.sql")
    mart = BashOperator(task_id="build_mart", bash_command=run + "python -m warehouse.run_sql warehouse/sql/09_mart.sql")
    export = BashOperator(task_id="export_product", bash_command=run + "python -m warehouse.export_product")
    warehouse >> mart >> export
