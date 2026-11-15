"""SQL 에이전트 1차 대응 DAG (결정 108, 112, design_agent.md 1절, A4).

30분마다 ops.agent_signal에서 처리 전(new) 신호 하나를 집어(running) 에이전트 컨테이너(seoul-agent-job)를 부른다.
에이전트는 실제 데이터로 기준선을 격리 빌드하고, 신호가 있으면 SQL을 고쳐 후보를 판정한다(agent/run.py). 운영 스키마는 바꾸지 않는다.
- 생활이동 등 다른 무거운 작업이 heavy 잠금을 잡고 있으면 에이전트가 바로 실패하고 신호를 new로 되돌린다. 다음 주기에 다시 본다.
- 처리 전 신호가 없으면 아무것도 하지 않는다(short circuit). 한 번에 하나(max_active_runs=1, pool heavy).
"""
import os

import pendulum
from airflow.sdk import DAG, task

from alerting import notify_failure
from jobs import agent_job

with DAG(
    dag_id="agent_triage",
    schedule="*/30 * * * *",
    start_date=pendulum.datetime(2026, 11, 15, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "on_failure_callback": notify_failure},
    tags=["agent"],
):
    @task.short_circuit(task_id="pick_signal")
    def pick_signal():
        """가장 오래된 new 신호 하나를 running으로 바꾸고 id를 돌려준다. 없으면 None(뒤 태스크 건너뜀)."""
        import psycopg
        with psycopg.connect(os.environ["DATABASE_URL"], connect_timeout=5) as conn:
            row = conn.execute("UPDATE ops.agent_signal SET status = 'running' WHERE signal_id = (SELECT signal_id FROM "
                               "ops.agent_signal WHERE status = 'new' ORDER BY signal_id LIMIT 1 FOR UPDATE SKIP LOCKED) "
                               "RETURNING signal_id").fetchone()
        return row[0] if row else None

    pick_signal() >> agent_job("run_agent",
                               "python -m agent.run --data real --signal-id {{ ti.xcom_pull(task_ids='pick_signal') }}")
