-- SQL 1차 대응 에이전트의 신호와 실행 기록 (design_agent.md 3절, 7절, 결정 108, 112). 다시 돌려도 같다(IF NOT EXISTS).

-- 한 행 = 신호 하나. 같은 (종류, 출처, 배치 키)는 한 번만 들어간다(같은 신호를 두 번 처리하지 않음, 비용 폭주 막기).
--   kind: gate_reject(관문 거절, wh_load report가 stg_dbt.batch_status에서) | dbt_fail(dbt 테스트·모델 실패, dbt_build 실패 콜백이 run_results.json에서)
--   batch_key: gate_reject는 batch_status.batch_key, dbt_fail은 실패한 노드 id
--   status: new(처리 전) -> running(agent_triage가 집음) -> done(판정까지 끝남) | failed(에이전트 실행이 실패, 다음 주기에 다시 new로 되돌리지 않음) | human(사람이 닫음)
--   경고만 있는 신호(모르는 키)는 넣지 않는다(3절: 알림만).
CREATE TABLE IF NOT EXISTS ops.agent_signal (
    signal_id   bigserial PRIMARY KEY,
    kind        text NOT NULL CHECK (kind IN ('gate_reject', 'dbt_fail')),
    source      text NOT NULL,
    batch_key   text NOT NULL,
    detail      jsonb NOT NULL,
    status      text NOT NULL DEFAULT 'new' CHECK (status IN ('new', 'running', 'done', 'failed', 'human')),
    found_by    text NOT NULL,          -- 기록한 Airflow 실행(dag_id/run_id)
    found_at    timestamptz NOT NULL DEFAULT now(),
    handled_at  timestamptz,
    UNIQUE (kind, source, batch_key)
);

-- 한 행 = 에이전트 실행 하나(agent/run.py). 평가(--data S1 등)도 같은 표에 남는다. 시간, 턴, SDK 계산값, 판정을 본다(7절 비용).
CREATE TABLE IF NOT EXISTS ops.agent_run (
    run_id      text PRIMARY KEY,       -- <시작 시각>_<데이터>
    signal_id   bigint REFERENCES ops.agent_signal,
    data        text NOT NULL,          -- real | S1 | M1 ...
    mode        text NOT NULL,          -- fix | diagnose | judge(사람이 준 후보)
    main_commit text,
    verdict     text,                   -- pass | needs_decision | fail | no_fix | no_run | diagnosed | error
    sec         numeric,
    turns       int,
    cost_usd    numeric,                -- SDK 계산값. Max 구독 토큰이라 청구액이 아니다(결정 108)
    report      jsonb NOT NULL,
    started_at  timestamptz NOT NULL
);

-- UI 읽기 역할이 운영 화면에서 볼 수 있게(결정 106 ui_reader)
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ui_reader') THEN
        GRANT SELECT ON ops.agent_signal, ops.agent_run TO ui_reader;
    END IF;
END $$;
