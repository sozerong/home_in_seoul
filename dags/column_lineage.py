"""컬럼 리니지 추출 DAG (P4, 결정 50, 57). 파일 이름이 lineage.py가 아닌 것은 dags 폴더가 sys.path에 들어가 레포의 lineage 패키지를 가리기 때문이다.

wh_load가 상품까지 내보내면(wh.built Asset) 한 번 돈다. wh_load의 dbt_build가 남긴 warehouse/dbt/target(manifest, 컴파일 SQL)을
읽어 lineage 스키마에 회차 하나를 쌓는다. 엔진은 Spark다(결정 44, 측정 21).
wh_load와 떨어져 있어 여기서 실패해도 상품은 영향이 없다. 실패하면 notify_failure로 알린다.
코드를 고친 뒤 지도를 새로 만들 때는 Airflow 화면에서 손으로 돌린다. 그때는 trigger가 manual로 남는다.
"""
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from alerting import notify_failure
from assets import WH_BUILT

with DAG(
    dag_id="lineage",
    schedule=WH_BUILT,
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "on_failure_callback": notify_failure},
    tags=["lineage"],
):
    # Asset이 깨운 실행은 run_type이 asset_triggered, 손으로 돌린 실행은 manual이다
    BashOperator(
        task_id="extract_lineage",
        bash_command="set -euo pipefail; cd /opt/project; "
                     "/opt/airflow/spark-venv/bin/python -m lineage.extract --engine spark "
                     "--trigger {{ 'asset' if dag_run.run_type == 'asset_triggered' else 'manual' }}",
    )
