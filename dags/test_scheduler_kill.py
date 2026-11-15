"""스케줄러 kill 시험용 DAG (handoff_duckdb_agent.md S2). 손으로만 돈다.

계산 컨테이너(seoul-spark-job)에서 heavy 잠금을 잡고 5분 잔다. 실행 중 `docker kill new_proj-airflow-scheduler-1` 뒤 다시 띄워
옛 계산 컨테이너, 새 시도, 잠금, 남는 컨테이너가 어떻게 되는지 본다(guarantees.md).
"""
import pendulum
from airflow.sdk import DAG

from jobs import spark_job

with DAG(
    dag_id="test_scheduler_kill",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["test"],
):
    spark_job("hold_lock", "python -m common lock -- bash -c 'echo \"lock held $(hostname)\"; sleep 300; echo done'")
