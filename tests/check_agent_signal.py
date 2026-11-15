"""A2 확인: 변이 M1(혼잡도 요일 '평일' -> '주중')을 넣은 격리 빌드에서 신호가 한 건 생기는가 (design_agent.md 9절 A2).

agent-job 컨테이너에서: docker compose run --rm agent-job python -m tests.check_agent_signal
main을 /scratch에 복제해 M1 데이터로 격리 빌드(*_sbx_sig)하고, agent.signals.collect가 그 batch_status에서 뽑은 신호를
ops.agent_signal에 넣어 센 뒤 되돌린다(트랜잭션 rollback, 운영 신호 표는 그대로). 끝나면 sbx 스키마를 지운다.
기대: congestion gate_reject 한 건, 배치는 혼잡도 모든 수집일(정답표 M1: 평일 행이 1/3이라 모든 배치가 5%를 넘음. 정답표를 쓴
2026-11-14에는 5개, 그 뒤 수집일이 늘어 지금 수와 비교한다, 판단 118), 검사 congestion_quarantine_ratio. 같은 신호를 두 번 넣으면 두 번째는 0건.
"""
from pathlib import Path

import psycopg

from agent import sandbox, signals
from agent.run import git
from common import ROOT, SCRATCH, heavy_lock, read_env


def main():
    lock = heavy_lock()  # noqa: F841  dbt 전체 빌드(L5). /scratch를 비우고 시작한다
    repo = Path(SCRATCH) / "agent" / "sigcheck"
    git("clone", "-q", "--no-hardlinks", "--branch", "main", str(ROOT), str(repo), cwd="/")
    with psycopg.connect(read_env("DATABASE_URL"), autocommit=True) as conn:
        try:
            sandbox.build(conn, repo, "sig", sandbox.data_changes("M1"))
            found = signals.collect(conn, schema="stg_dbt_sbx_sig")
            print("신호:", [(k, s, d["count"], d["checks"]) for k, s, _, d in found])
            with conn.transaction(force_rollback=True):
                first = signals.record(conn, found, "tests.check_agent_signal")
                again = signals.record(conn, found, "tests.check_agent_signal")
            assert [(k, s) for k, s, _, _ in found] == [("gate_reject", "congestion")], found
            every = conn.execute("SELECT count(*) FROM stg_dbt.batch_status WHERE source = 'congestion'").fetchone()[0]
            assert found[0][3]["count"] == every and "congestion_quarantine_ratio" in found[0][3]["checks"], found[0][3]
            assert len(first) == 1 and again == [], (first, again)
            print(f"통과: 신호 1건(배치 {found[0][3]['count']}개), 같은 신호 다시 넣기 0건, 운영 신호 표는 되돌림")
        finally:
            sandbox.drop(conn, "sig")


if __name__ == "__main__":
    main()
