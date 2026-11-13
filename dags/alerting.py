"""DAG 알림 (단계 7 기본안 4, 결정 39, 86).

작업 실패: 프로젝트의 logs/alerts.log(JSON Lines, 컨테이너 안 /opt/project/logs/alerts.log)와 ops.failure_log에 한 줄씩.
.env(또는 환경 변수)에 SLACK_WEBHOOK_URL이 있으면 같은 내용을 슬랙에도 보낸다. 없으면 파일과 표에만 남긴다.
wh_load 요약: 마지막 빌드의 dbt 경고, 관문 거절 배치, 늦은 출처를 모아 메시지 하나로 보낸다(wh_load_report). 새 감시 DAG는 없다.
"""
import datetime
import json
import logging
import os
import urllib.request
from pathlib import Path

ALERT_FILE = Path("/opt/project/logs/alerts.log")
ENV_FILE = Path("/opt/project/.env")


def env(name):
    """환경 변수, 없으면 프로젝트 .env의 값. 둘 다 없으면 None(알림 주소는 선택이다)."""
    if os.environ.get(name):
        return os.environ[name]
    try:
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            key, sep, value = line.partition("=")
            if sep and key.strip() == name:
                return value.strip() or None
    except OSError:
        pass
    return None


def send_slack(text):
    """웹훅으로 메시지 하나. 주소가 없으면 False. 보내다 실패하면 예외(삼키지 않는다)."""
    url = env("SLACK_WEBHOOK_URL")
    if not url:
        return False
    body = json.dumps({"text": text}, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=10) as response:
        response.read()
    return True


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
    try:
        send_slack(f"[실패] {ti.dag_id}.{ti.task_id} (run {ti.run_id}, 시도 {ti.try_number}): {record['error']}")
    except Exception:
        logging.exception("슬랙 실패 알림 전송 실패")


def wh_load_report():
    """wh_load 마지막 작업: 경고, 거절 배치, 늦은 출처를 메시지 하나로. 보낼 것이 없으면 보내지 않는다. 보낸 글을 돌려준다."""
    import psycopg

    with psycopg.connect(os.environ["DATABASE_URL"], connect_timeout=5) as conn:
        warnings = conn.execute("SELECT warnings FROM ops.build_run ORDER BY build_id DESC LIMIT 1").fetchone()
        rejected = conn.execute("SELECT source, batch_key, array_to_string(failed_checks, ',') FROM stg_dbt.batch_status "
                                "WHERE NOT approved ORDER BY 1, 2").fetchall()
        late = conn.execute("SELECT source, last_collected::text FROM ops.freshness WHERE is_late ORDER BY 1").fetchall()
    lines = [f"- dbt 경고 {w['node']}: {w['message']}" for w in (warnings[0] if warnings else None) or []]
    lines += [f"- 관문 거절 {s} {k}: {c}" for s, k, c in rejected]
    lines += [f"- 늦음 {s} (마지막 수집 {d})" for s, d in late]
    if not lines:
        return None
    text = f"[wh_load] 경고 {len(lines)}건\n" + "\n".join(lines[:50]) + ("\n..." if len(lines) > 50 else "")
    print(text, "\n슬랙 전송:", send_slack(text))
    return text
