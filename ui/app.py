"""파이프라인 UI 읽기 API (결정 106, docs/ui.md). GET만 있다.

- DB: ui_reader 역할(warehouse/sql/13_ui_reader.sql), 쿼리는 ui/queries.sql에 이름을 붙여 모은다.
- 실행 기록: Airflow REST API v2(J-16). 메타 DB 표는 읽지 않는다.
- 오류 상태: docs/failure_analysis.md (ui/failures.py).
응답마다 sources[]에 출처 이름, 읽은 시각, 성공 여부를 붙인다. 출처 하나가 실패해도 나머지는 보이고, 실패한 출처는 error로 드러난다(설계 원칙 4).
실행: docker compose up ui  ->  http://127.0.0.1:8090/api/overview
"""
import os
import re
import tomllib
from datetime import datetime, timezone
from pathlib import Path

import httpx
import psycopg
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from psycopg.rows import dict_row

from ui import failures

DB_URL = os.environ["UI_DATABASE_URL"]
AIRFLOW = os.environ.get("AIRFLOW_API", "http://airflow-apiserver:8080")
QUERIES = dict(re.findall(r"^-- name: (\w+)\n(.*?)(?=^-- name: |\Z)",
                          (Path(__file__).parent / "queries.sql").read_text(encoding="utf-8"), re.M | re.S))
PIPELINES = Path(__file__).parent / "pipelines.toml"
# pipelines.toml의 일정 표기 -> Airflow API timetable_summary 표기
LIVE_SCHEDULE = {"manual": None, "asset": "Asset"}

app = FastAPI(title="seoul pipeline ui", docs_url=None, redoc_url=None)


def db(name, **params):
    # 화면 요청이 무거운 작업의 잠금 뒤에 줄 서서 멈추지 않게 상한을 둔다. 넘으면 그 출처만 실패로 드러난다(판단 U-13).
    # ponytail: ops.quality 생활이동 줄이 데운 상태에서 2.65초(격리 표 count). 상한은 그 몇 배.
    with psycopg.connect(DB_URL, row_factory=dict_row, connect_timeout=5,
                         options="-c statement_timeout=15s -c lock_timeout=3s") as conn:
        return conn.execute(QUERIES[name], params or None).fetchall()


def airflow(path, **params):
    # ponytail: 요청마다 토큰을 새로 받는다. GET /auth/token은 simple auth manager의 all-admins 모드에서만 된다(U0). 로그인을 켜면 여기만 바꾼다.
    token = httpx.get(f"{AIRFLOW}/auth/token", timeout=5).raise_for_status().json()["access_token"]
    r = httpx.get(f"{AIRFLOW}{path}", params=params, headers={"Authorization": f"Bearer {token}"}, timeout=10)
    return r.raise_for_status().json()


def read(sources, name, fn, *args, **kw):
    """출처 하나를 읽고 sources에 남긴다. 실패하면 값은 None, 사유는 error."""
    entry = {"name": name, "read_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "ok": True}
    sources.append(entry)
    if fn is db and any(e.get("db_unreachable") for e in sources):
        # DB에 닿지 못한 요청에서 나머지 DB 읽기마다 연결 시간을 다시 기다리지 않는다(판단 U-18, 실패 1회 3.85초).
        entry.update(ok=False, error="건너뜀: 이 요청에서 앞선 DB 연결이 실패함")
        return None
    try:
        return fn(*args, **kw)
    except Exception as e:  # 삼키지 않고 응답에 드러낸다
        entry.update(ok=False, error=f"{type(e).__name__}: {e}")
        # 연결 단계 실패(sqlstate 없음, 08 연결, 28 인증)만. 쿼리 시간 초과·잠금 대기는 그 쿼리만의 실패다.
        state = getattr(e, "sqlstate", "")
        if fn is db and isinstance(e, psycopg.OperationalError) and (state is None or state[:2] in ("08", "28")):
            entry["db_unreachable"] = True
        return None


def airflow_summary(dags, runs):
    if dags is None or runs is None:
        return None
    latest = {}
    for r in runs["dag_runs"]:  # run_after 내림차순이라 DAG마다 처음 본 것이 마지막 실행
        latest.setdefault(r["dag_id"], r)
    pick = lambda r: {k: r[k] for k in ("dag_id", "state", "run_after", "end_date")}
    return {
        "dags": dags["total_entries"],
        "paused": sum(d["is_paused"] for d in dags["dags"]),
        "runs_total": runs["total_entries"],
        "last_run": pick(runs["dag_runs"][0]) if runs["dag_runs"] else None,
        "failed": [pick(r) for r in latest.values() if r["state"] == "failed"],
        "running": [pick(r) for r in latest.values() if r["state"] in ("running", "queued")],
        # ponytail: 마지막 실행은 최근 100건 안에서 찾는다. 100건 안에 실행이 없는 DAG는 여기 빠진다(DAG별 limit=1 호출이 업그레이드 길).
        "dags_without_recent_run": sorted({d["dag_id"] for d in dags["dags"]} - latest.keys()),
    }


def dag_info(dag_id, declared, live, airflow_ok):
    info = {"dag_id": dag_id, "declared_schedule": declared}
    if not airflow_ok:  # Airflow를 못 읽었으면 '없음'으로 단정하지 않는다. 사유는 sources에
        return info
    info["in_airflow"] = live is not None
    if live:
        info |= {"schedule": live["timetable_summary"], "schedule_text": live["timetable_description"],
                 "is_paused": live["is_paused"], "next_run": live["next_dagrun_run_after"],
                 "schedule_matches": LIVE_SCHEDULE.get(declared, declared) == live["timetable_summary"]}
    return info


def toml(part):
    """pipelines.toml의 한 절. pipeline(파이프라인 정의), map(데이터 지도), scenarios(변경 시나리오)."""
    return tomllib.loads(PIPELINES.read_text(encoding="utf-8"))[part]


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.get("/api/pipelines")
def pipelines():
    s = []
    defs = read(s, "ui/pipelines.toml", toml, "pipeline")
    dags = read(s, "airflow:/api/v2/dags", airflow, "/api/v2/dags", limit=100)
    live = {d["dag_id"]: d for d in dags["dags"]} if dags else {}
    return {
        "pipelines": [
            {k: p[k] for k in ("id", "kind", "title", "trigger", "chain", "engines")}
            | {"dags": [dag_info(d, sched, live.get(d), dags is not None) for d, sched in p["dags"].items()]}
            for p in defs or []
        ],
        "sources": s,
    }


@app.get("/api/overview")
def overview():
    s = []
    dags = read(s, "airflow:/api/v2/dags", airflow, "/api/v2/dags", limit=100)
    runs = read(s, "airflow:/api/v2/dags/~/dagRuns", airflow, "/api/v2/dags/~/dagRuns", order_by="-run_after", limit=100)
    items = read(s, "docs/failure_analysis.md", failures.read)
    lineage = read(s, "lineage_status", db, "lineage_status")
    batches = read(s, "batch_rejected", db, "batch_rejected")
    return {
        "airflow": airflow_summary(dags, runs),
        "failures": failures.summary(items) if items is not None else None,
        "lineage": lineage[0] if lineage else None,  # 실패인지 회차가 없는지는 sources로 가른다
        "batches": None if batches is None else {
            "rejected": sum(b["rejected"] for b in batches), "by_source": batches},
        "sources": s,
    }


RUN_FIELDS = ("dag_run_id", "state", "run_type", "run_after", "start_date", "end_date", "duration")


@app.get("/api/pipelines/{pid}")
def pipeline(pid: str):
    s = []
    defs = read(s, "ui/pipelines.toml", toml, "pipeline")
    if defs is None:
        return {"pipeline": None, "sources": s}
    p = next((x for x in defs if x["id"] == pid), None)
    if p is None:
        raise HTTPException(404, f"pipelines.toml에 없는 파이프라인: {pid}")
    dags = read(s, "airflow:/api/v2/dags", airflow, "/api/v2/dags", limit=100)
    live = {d["dag_id"]: d for d in dags["dags"]} if dags else {}
    runs = {}
    for d in p["dags"]:
        r = read(s, f"airflow:/api/v2/dags/{d}/dagRuns", airflow, f"/api/v2/dags/{d}/dagRuns", order_by="-run_after", limit=30)
        runs[d] = None if r is None else {"total": r["total_entries"],
                                          "recent": [{k: x[k] for k in RUN_FIELDS} for x in r["dag_runs"]]}
    out = {"pipeline": p | {"dags": [dag_info(d, sched, live.get(d), dags is not None) for d, sched in p["dags"].items()]},
           "runs": runs}
    if p.get("batch_sources"):
        out["batches"] = read(s, "batch_detail", db, "batch_detail", src=p["batch_sources"])
    if p.get("ops_sources"):
        out["quality"] = read(s, "quality", db, "quality", src=p["ops_sources"])
        out["freshness"] = read(s, "freshness", db, "freshness", src=p["ops_sources"])
    if p.get("records"):
        out["records"] = {"name": p["records"], "rows": read(s, p["records"], db, p["records"])}
    items = read(s, "docs/failure_analysis.md", failures.read)
    if items is not None:  # toml의 F 번호가 문서에 없으면 missing으로 드러낸다
        by_id = {f["id"]: f for f in items}
        out["failures"] = [by_id.get(f, {"id": f, "missing": True}) for f in p["failures"]]
    out["sources"] = s
    return out


@app.get("/api/failures")
def failure_list():
    s = []
    items = read(s, "docs/failure_analysis.md", failures.read)
    return {"failures": items, "summary": failures.summary(items) if items is not None else None, "sources": s}


@app.get("/api/lineage/graph")
def lineage_graph():
    """지도 전체. 노드는 (표, 컬럼), 간선은 [출발 번호, 도착 번호, D|I|R]. 화면이 walk로 강조할 선을 고른다."""
    s = []
    defs = read(s, "ui/pipelines.toml", toml, "map")
    status = read(s, "lineage_status", db, "lineage_status")
    rows = read(s, "lineage_edges", db, "lineage_edges")
    nodes, index, edges = [], {}, []
    for r in rows or []:
        ids = []
        for key in ((r["source_table"], r["source_column"]), (r["target_table"], r["target_column"])):
            if key not in index:
                index[key] = len(nodes)
                nodes.append(key)
            ids.append(index[key])
        edges.append([*ids, r["kind"][0]])  # DIRECT, INDIRECT, ROWSET -> D, I, R
    known = (defs or {}).get("tables", {})
    return {"run": status[0] if status else None, "nodes": nodes, "edges": edges, "map": defs,
            "unmapped": sorted({t for t, _ in nodes} - known.keys()) if defs else None, "sources": s}


@app.get("/api/lineage/impact")
def lineage_impact(table: str, column: str | None = None, direction: str = "down"):
    if direction not in ("down", "up"):
        raise HTTPException(400, "direction은 down 또는 up")
    s = []
    rows = read(s, f"impact_{direction}", db, f"impact_{direction}", t=table, c=column)
    return {"table": table, "column": column, "direction": direction, "rows": rows, "sources": s}


@app.get("/api/scenarios")
def scenarios():
    s = []
    return {"scenarios": read(s, "ui/pipelines.toml", toml, "scenarios"), "sources": s}
