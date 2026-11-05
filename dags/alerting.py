"""DAG 작업 실패 알림. 슬랙 웹훅이 없어서 실패 기록 파일에 한 줄씩 남긴다(단계 7 기본안 4).

파일: 프로젝트의 logs/alerts.log (JSON Lines). 컨테이너 안 경로는 /opt/project/logs/alerts.log.
운영 대시보드가 볼 수 있게 ops.failure_log에도 쓴다(결정 39). DB에 못 써도 파일 기록은 남는다.
슬랙 웹훅이 생기면 이 함수 안에서 같은 내용을 보내면 된다.
"""
import datetime
import json
import logging
import os
from pathlib import Path

ALERT_FILE = Path("/opt/project/logs/alerts.log")


def notify_failure(context):
    """on_failure_callback: 어느 DAG, 어느 작업, 어느 처리 단위가 왜 실패했는지 한 줄로 남긴다."""
    ti = context["task_instance"]
    record = {
        "at": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "map_index": ti.map_index,
        "run_id": ti.run_id,
        "try_number": ti.try_number,
        "error": repr(context.get("exception"))[:500],
    }
    ALERT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with ALERT_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
    try:
        import psycopg  # DAG 파일을 읽을 때마다 불러오지 않게 여기서 불러온다

        with psycopg.connect(os.environ["DATABASE_URL"], connect_timeout=5) as conn:
            conn.execute("INSERT INTO ops.failure_log (at, dag_id, task_id, map_index, run_id, try_number, error) "
                         "VALUES (%(at)s, %(dag_id)s, %(task_id)s, %(map_index)s, %(run_id)s, %(try_number)s, %(error)s) "
                         "ON CONFLICT DO NOTHING", record)
    except Exception:  # 알림 때문에 실패 처리가 깨지면 안 된다. 원인은 작업 로그에 남는다
        logging.exception("ops.failure_log 기록 실패")
