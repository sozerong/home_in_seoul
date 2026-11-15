"""에이전트 신호 수집 (design_agent.md 3절, A2). wh_load가 부른다: report 태스크(관문 거절), dbt_build 실패 콜백(dbt 실패).

한 신호 = (종류, 출처, 배치 키). 관문 거절은 출처마다 거절된 배치 묶음 하나를 신호 하나로 본다(M1처럼 5개 배치가 함께 걸려도
원인은 하나다). 배치 키 자리에는 거절 배치 목록의 md5를 둔다(M7은 1,320개라 글자 그대로는 인덱스에 안 들어간다).
같은 묶음이 다음 wh_load에서 또 거절돼도 같은 키라 다시 들어가지 않는다(ops.agent_signal UNIQUE). 묶음이 바뀌면 새 신호다.
생활이동(stg.living_batch_status)은 1단계 범위 밖이라 보지 않는다(design_agent.md 2.2). 경고만 있는 신호는 넣지 않는다(3절).

확인: python -m agent.signals --selftest (DB 불필요). M1 신호 한 건: tests/check_agent_signal.py (agent-job 컨테이너)
"""
import hashlib
import json

SOURCES = ("congestion", "rent", "store")  # agent/sandbox.py SOURCES와 같은 출처(생활이동 제외)


def collect(conn, schema="stg_dbt", run_results=None):
    """신호 목록 [(kind, source, batch_key, detail)]. schema의 batch_status에서 관문 거절, run_results(dict)에서 dbt 실패."""
    found = []
    for src in SOURCES:
        rows = conn.execute(f"SELECT batch_key, failed_checks FROM {schema}.batch_status WHERE source = %s AND NOT approved "
                            "ORDER BY batch_key", [src]).fetchall()
        if rows:
            keys = [r[0] for r in rows]
            found.append(("gate_reject", src, hashlib.md5("\n".join(keys).encode()).hexdigest(),
                          {"count": len(keys), "batches": keys[:50], "checks": sorted({c for _, cs in rows for c in cs})}))
    for r in (run_results or {}).get("results", []):
        if r["status"] in ("error", "fail"):
            node = r["unique_id"]
            src = next((s for s in SOURCES if f".{s}" in node or f"{s}_" in node), "unknown")
            found.append(("dbt_fail", src, node, {"status": r["status"], "message": (r.get("message") or "")[:500]}))
    return found


def record(conn, signals, found_by):
    """새 신호만 넣는다. 넣은 signal_id 목록을 돌려준다."""
    ids = []
    for kind, src, key, detail in signals:
        row = conn.execute("INSERT INTO ops.agent_signal (kind, source, batch_key, detail, found_by) VALUES (%s, %s, %s, %s, %s) "
                           "ON CONFLICT (kind, source, batch_key) DO NOTHING RETURNING signal_id",
                           [kind, src, key, json.dumps(detail, ensure_ascii=False), found_by]).fetchone()
        if row:
            ids.append(row[0])
    return ids


def selftest():
    class Fake:  # 관문 거절 두 출처, dbt 실패 하나. 같은 배치 묶음은 같은 키
        def execute(self, sql, params):
            data = {"congestion": [("2026-11-13", ["congestion_raw_keys_present"]), ("2026-11-03", ["congestion_quarantine_ratio"])],
                    "rent": [], "store": [("x", ["store_raw_shrink"])]}[params[0]]
            return type("R", (), {"fetchall": lambda self: data})()
    rr = {"results": [{"unique_id": "test.seoul.rent_pnu_length", "status": "fail", "message": "3 rows"},
                      {"unique_id": "model.seoul.dong_profile", "status": "success"}]}
    s = collect(Fake(), run_results=rr)
    assert [(k, src) for k, src, _, _ in s] == [("gate_reject", "congestion"), ("gate_reject", "store"), ("dbt_fail", "rent")], s
    assert s[0][3]["count"] == 2 and s[0][3]["checks"] == ["congestion_quarantine_ratio", "congestion_raw_keys_present"]
    assert collect(Fake())[0][2] == s[0][2]  # 같은 묶음 = 같은 키(중복 처리 안 함)
    print("selftest 통과")


if __name__ == "__main__":
    import sys
    if "--selftest" in sys.argv:
        selftest()
