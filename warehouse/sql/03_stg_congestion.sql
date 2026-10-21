-- 단계 4 스테이징: 서울교통공사 지하철 혼잡도 (API). 규칙은 docs/rules.md 3절. 수집일마다 스냅샷을 남긴다.
CREATE SCHEMA IF NOT EXISTS stg;

-- 원본의 넓은 표(시간대 39칸)를 세로로 푼 긴 표. 한 행 = 수집일 x 요일 x 호선 x 역 x 방향 x 30분 구간.
CREATE TABLE IF NOT EXISTS stg.congestion (
    collected_date date         NOT NULL,
    source_file    text         NOT NULL,
    row_no         integer      NOT NULL,  -- 원본 파일 안에서 몇 번째 row인지
    dow            text         NOT NULL,  -- 평일 / 토요일 / 일요일
    line           text         NOT NULL,  -- 1~8호선
    sttn_no        text         NOT NULL,  -- 원래 역번호 (3~4자리)
    sttn_no4       char(4)      NOT NULL,  -- 앞에 0을 채운 4자리 (CC2)
    sttn_nm        text         NOT NULL,  -- 역 이름, 그대로
    direction      text         NOT NULL,  -- 상선 / 하선 / 내선 / 외선
    slot           time         NOT NULL,  -- 30분 구간의 시작 시각 (CC4)
    congestion     numeric(5, 1) NOT NULL, -- 정원 대비 %, 좌석 수만큼 타면 34 (CC5)
    PRIMARY KEY (collected_date, dow, line, sttn_no, direction, slot)  -- CF2의 안전망
);

CREATE TABLE IF NOT EXISTS stg.congestion_quarantine (
    collected_date date        NOT NULL,
    source_file    text        NOT NULL,
    row_no         integer     NOT NULL,
    reasons        text[]      NOT NULL,  -- 예: {CV5}
    raw            jsonb       NOT NULL,  -- CV1~CV4는 원본 행 전체, CV5는 신원 필드 + 그 시각 한 칸
    quarantined_at timestamptz NOT NULL DEFAULT now()
);