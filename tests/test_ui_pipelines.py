"""ui/pipelines.toml의 dag_id와 일정이 dags/*.py의 실제 DAG와 같은지 보는 어긋남 시험. DB와 Airflow 없이 돈다(CI).

DAG 파일을 Airflow 대신 가짜 모듈로 실행해 DAG(dag_id=, schedule=) 인자만 모은다. file_sources.py처럼
반복문으로 만드는 DAG도 실제 코드 그대로 돌기 때문에 잡힌다.
실행: python -m tests.test_ui_pipelines
"""
import os
import runpy
import sys
import tomllib
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
FAKE = ["airflow", "airflow.sdk", "airflow.providers", "airflow.providers.standard",
        "airflow.providers.standard.operators", "airflow.providers.standard.operators.bash", "pendulum",
        "airflow.providers.docker", "airflow.providers.docker.operators", "airflow.providers.docker.operators.docker",
        "docker", "docker.types",
        "common"]  # dags/jobs.py agent_job이 DAG를 읽을 때 .env를 읽는다(common.read_env). CI에는 .env가 없다(F-35)


def code_schedules(dag_dir=ROOT / "dags"):
    found = {}

    class DAG:  # with DAG(...) 문맥만 흉내 낸다
        def __init__(self, dag_id, schedule=None, **_):
            assert dag_id not in found, f"dag_id 중복: {dag_id}"
            found[dag_id] = "manual" if schedule is None else schedule if isinstance(schedule, str) else "asset"

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    fakes = {name: mock.MagicMock() for name in FAKE}
    fakes["airflow.sdk"].DAG = DAG
    # dags/jobs.py가 상속한다(class JobOperator(DockerOperator)). MagicMock은 부모 클래스로 쓸 수 없어 빈 클래스를 둔다(F-35)
    fakes["airflow.providers.docker.operators.docker"].DockerOperator = type("DockerOperator", (), {"__init__": lambda self, *a, **k: None})
    # dags/jobs.py가 DAG를 읽을 때 쓰는 compose 환경 변수(마운트 호스트 경로, 계산 컨테이너에 넘길 DB 값). 값은 시험에 쓰이지 않는다
    env = {"HOST_PROJECT_DIR": "/ci", "DATABASE_URL": "ci-unused", "POSTGRES_PASSWORD": "ci-unused"}
    with mock.patch.dict(sys.modules, fakes), mock.patch.object(sys, "path", [str(dag_dir), *sys.path]), \
            mock.patch.dict(os.environ, env):
        for f in sorted(dag_dir.glob("*.py")):
            if f.name.startswith("test_"):  # 손으로만 도는 시험 DAG(test_scheduler_kill)는 파이프라인이 아니다
                continue
            runpy.run_path(str(f))
        for helper in ("alerting", "assets", "jobs"):  # DAG 파일이 import한 도우미 모듈을 남기지 않는다
            sys.modules.pop(helper, None)
    return found


def toml_schedules(path=ROOT / "ui" / "pipelines.toml"):
    found = {}
    for p in tomllib.loads(Path(path).read_text(encoding="utf-8"))["pipeline"]:
        for dag_id, schedule in p["dags"].items():
            assert dag_id not in found, f"pipelines.toml에서 dag_id 중복: {dag_id}"
            found[dag_id] = schedule
    return found


def drift(code, declared):
    """어긋난 것을 사람이 읽을 문장으로. 비면 같다."""
    out = [f"코드에만 있음: {d}" for d in sorted(code.keys() - declared.keys())]
    out += [f"pipelines.toml에만 있음: {d}" for d in sorted(declared.keys() - code.keys())]
    out += [f"{d} 일정: 코드 {code[d]!r} / toml {declared[d]!r}"
            for d in sorted(code.keys() & declared.keys()) if code[d] != declared[d]]
    return out


# 비교 규칙 자체: 빠짐, 남음, 일정 다름을 모두 잡는다
assert drift({"a": "manual", "b": "0 1 * * *"}, {"a": "manual", "b": "0 1 * * *"}) == []
assert drift({"a": "manual", "b": "0 1 * * *"}, {"a": "asset", "c": "manual"}) == [
    "코드에만 있음: b", "pipelines.toml에만 있음: c", "a 일정: 코드 'manual' / toml 'asset'"]

code, declared = code_schedules(), toml_schedules()
assert len(code) == 9, code  # handoff_ui.md: DAG 8개 + agent_triage(결정 112)
problems = drift(code, declared)
assert not problems, "pipelines.toml이 실제 DAG와 어긋남:\n  " + "\n  ".join(problems)
print(f"ok: DAG {len(code)}개 일치 {code}")
