"""DAG 작업 실패 알림. 슬랙 웹훅이 없어서 실패 기록 파일에 한 줄씩 남긴다(단계 7 기본안 4).

파일: 프로젝트의 logs/alerts.log (JSON Lines). 컨테이너 안 경로는 /opt/project/logs/alerts.log.
슬랙 웹훅이 생기면 이 함수 안에서 같은 내용을 보내면 된다.
"""
import datetime
import json
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
