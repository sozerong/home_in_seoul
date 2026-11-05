"""운영 대시보드를 코드로 만든다 (결정 38, 39). 화면을 손으로 고쳐도 다시 실행하면 warehouse/sql/ops/의 내용으로 돌아간다.

하는 일: 읽기 전용 DB 계정, 지난 상품 manifest와 실패 기록을 ops로, Metabase 첫 설정, DB 연결 2개, 카드와 대시보드.
카드 하나 = warehouse/sql/ops/의 SQL 파일 하나. 맨 위 주석이 카드 설정이다:
  title, db, display, desc, pos(행 열 너비 높이), x, y, series, stack(쌓은 막대),
  alert(숫자 카드. 이 값 이상이면 빨강), status(표에서 상태 글자에 색을 칠할 열)
여러 번 실행해도 되고 대시보드 주소는 그대로다. 먼저: docker compose up -d metabase
사용법: python -m warehouse.metabase_setup
"""
import json
import re
import time

import psycopg
import requests
from psycopg import sql

from common import ROOT, read_env
from warehouse.export_product import record_product

URL = "http://localhost:3000/api"
EMAIL = "dev@example.com"
ROLE = "metabase_ro"
DASHBOARD = "파이프라인 운영"
SCHEMAS = ["stg", "wh", "mart", "ops"]
RED, YELLOW, GREEN = "#ED6E6E", "#F9CF48", "#84BB4C"
STATUS_COLORS = {"정상": GREEN, "성공": GREEN, "늦음": RED, "실패": RED, "초과": RED,
                 "미반영": YELLOW, "확인": YELLOW, "중지됨": YELLOW, "밀림": YELLOW}
HEADINGS = {0: "1. 운영 상태", 4: "2. 정합성 검증", 13: "3. 동작 현황", 29: "4. 데이터 품질"}


def setup_role():
    """읽기 전용 계정. wh·mart는 빌드마다 새로 만들어지므로 앞으로 생길 스키마와 표에도 기본 권한을 준다(결정 32의 교체)."""
    url = read_env("DATABASE_URL")
    password = read_env("METABASE_DB_PASSWORD")
    with psycopg.connect(url, autocommit=True) as conn:
        exists = conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", [ROLE]).fetchone()
        verb = "ALTER" if exists else "CREATE"
        conn.execute(sql.SQL(f"{verb} ROLE {ROLE} LOGIN PASSWORD {{}}").format(sql.Literal(password)))
        for schema in SCHEMAS:
            conn.execute(f"GRANT USAGE ON SCHEMA {schema} TO {ROLE}")
            conn.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {ROLE}")
        conn.execute(f"ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO {ROLE}")
        conn.execute(f"ALTER DEFAULT PRIVILEGES GRANT USAGE ON SCHEMAS TO {ROLE}")
    with psycopg.connect(url.rsplit("/", 1)[0] + "/airflow", autocommit=True) as conn:
        conn.execute(f"GRANT SELECT ON dag, dag_run, job TO {ROLE}")


def backfill():
    """지난 상품 버전(manifest)과 지난 실패 기록(logs/alerts.log)을 ops로 옮긴다."""
    alerts = ROOT / "logs/alerts.log"
    with psycopg.connect(read_env("DATABASE_URL")) as conn:
        for path in sorted((ROOT / "product").glob("v*/manifest.json")):
            record_product(conn, json.loads(path.read_text(encoding="utf-8")))
        for line in alerts.read_text(encoding="utf-8").splitlines() if alerts.exists() else []:
            conn.execute("INSERT INTO ops.failure_log (at, dag_id, task_id, map_index, run_id, try_number, error) "
                         "VALUES (%(at)s, %(dag_id)s, %(task_id)s, %(map_index)s, %(run_id)s, %(try_number)s, %(error)s) "
                         "ON CONFLICT DO NOTHING", json.loads(line))


def login():
    """처음이면 관리자 계정을 만들고, 아니면 로그인해 세션을 돌려준다."""
    for _ in range(90):  # 첫 시작은 DB를 만드느라 몇 분 걸린다
        try:
            if requests.get(f"{URL}/health", timeout=5).status_code == 200:
                break
        except requests.ConnectionError:
            pass
        time.sleep(2)
    else:
        raise SystemExit("Metabase가 3분 안에 뜨지 않았습니다: docker compose logs metabase")
    password = read_env("METABASE_ADMIN_PASSWORD")
    props = requests.get(f"{URL}/session/properties", timeout=30).json()
    if props.get("has-user-setup"):
        r = requests.post(f"{URL}/session", json={"username": EMAIL, "password": password}, timeout=30)
    else:
        r = requests.post(f"{URL}/setup", timeout=60, json={
            "token": props["setup-token"],
            "user": {"email": EMAIL, "first_name": "dev", "last_name": "local", "password": password,
                     "password_confirm": password},
            "prefs": {"site_name": "서울 동네 찾기", "site_locale": "ko", "allow_tracking": False}})
    r.raise_for_status()
    session = requests.Session()
    session.headers["X-Metabase-Session"] = r.json()["id"]
    return session


def call(session, method, path, **kwargs):
    r = session.request(method, f"{URL}{path}", timeout=120, **kwargs)
    if not r.ok:
        raise SystemExit(f"{method} {path}: {r.status_code} {r.text[:500]}")
    return r.json() if r.content else None


def databases(session):
    """Metabase에 데이터 DB 2개를 연결하고 {이름: id}를 돌려준다."""
    found = {d["name"]: d["id"] for d in call(session, "GET", "/database")["data"]}
    for name in ("seoul", "airflow"):
        if name not in found:
            found[name] = call(session, "POST", "/database", json={
                "name": name, "engine": "postgres", "is_full_sync": False,
                "details": {"host": "db", "port": 5432, "dbname": name, "user": ROLE,
                            "password": read_env("METABASE_DB_PASSWORD"), "ssl": False}})["id"]
    return found


def read_card(path):
    """SQL 파일을 (설정, sql)로 읽는다."""
    text = path.read_text(encoding="utf-8")
    return dict(re.findall(r"^-- (\w+): (.+)$", text, re.M)), text


def card_settings(meta):
    """카드 주석을 Metabase 화면 설정으로 바꾼다."""
    settings = {}
    if "x" in meta:
        settings["graph.dimensions"] = [meta["x"]] + ([meta["series"]] if "series" in meta else [])
        settings["graph.metrics"] = [meta["y"]]
    if "stack" in meta:
        settings["stackable.stack_type"] = "stacked"
        settings["series_settings"] = {k: {"color": c} for k, c in STATUS_COLORS.items()}
    if "alert" in meta:
        limit = float(meta["alert"])
        settings["scalar.segments"] = [{"min": 0, "max": limit, "color": GREEN, "label": "정상"},
                                       {"min": limit, "max": 1e9, "color": RED, "label": "확인"}]
    if "status" in meta:
        settings["table.column_formatting"] = [
            {"columns": [meta["status"]], "type": "single", "operator": "=", "value": value, "color": color,
             "highlight_row": False} for value, color in STATUS_COLORS.items()]
    return settings


def heading(n, row, text):
    return {"id": -1000 - n, "card_id": None, "row": row, "col": 0, "size_x": 24, "size_y": 1,
            "visualization_settings": {"text": text, "virtual_card": {
                "name": None, "display": "heading", "visualization_settings": {}, "archived": False}}}


def build(session, db_ids):
    """대시보드의 카드를 SQL 파일 내용으로 바꾼다. 대시보드 번호는 유지한다."""
    found = [d for d in call(session, "GET", "/dashboard") if d["name"] == DASHBOARD]
    dash_id = found[0]["id"] if found else call(session, "POST", "/dashboard", json={"name": DASHBOARD})["id"]
    old = [c["card_id"] for c in call(session, "GET", f"/dashboard/{dash_id}")["dashcards"] if c.get("card_id")]

    dashcards = [heading(n, row, text) for n, (row, text) in enumerate(HEADINGS.items())]
    for n, path in enumerate(sorted((ROOT / "warehouse/sql/ops").glob("*.sql"))):
        meta, query = read_card(path)
        row, col, width, height = map(int, meta["pos"].split())
        card = call(session, "POST", "/card", json={
            "name": meta["title"], "display": meta["display"], "description": meta.get("desc"),
            "visualization_settings": card_settings(meta),
            "dataset_query": {"type": "native", "database": db_ids[meta["db"]], "native": {"query": query}}})
        dashcards.append({"id": -(n + 1), "card_id": card["id"], "row": row, "col": col, "size_x": width, "size_y": height})
    call(session, "PUT", f"/dashboard/{dash_id}", json={"dashcards": dashcards})
    for card_id in old:
        call(session, "DELETE", f"/card/{card_id}")
    return dash_id, len(dashcards) - len(HEADINGS)


def main():
    setup_role()
    backfill()
    session = login()
    dash_id, count = build(session, databases(session))
    print(f"대시보드 카드 {count}개: http://localhost:3000/dashboard/{dash_id}#refresh=300")


if __name__ == "__main__":
    main()
