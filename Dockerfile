# 이미지 세 개를 target으로 나눈다(결정 109). 파일 하나에 두면 requirements.txt와 jar 버전을 한곳에서 본다.
#   airflow     : 지휘(스케줄러, API 서버, DAG 처리기, 트리거러)와 가벼운 태스크, dbt. compose의 airflow-init이 target을 적어 빌드한다
#   spark-job   : Spark 실행기, 생활이동 Iceberg 적재, 리니지 추출. DockerOperator와 docker compose run --rm spark-job이 쓴다
#   duckdb-job  : DuckDB 비교 측정. docker compose run --rm duckdb-job이 쓴다
#   agent-job   : SQL 1차 대응 에이전트(결정 112). agent_triage DAG(DockerOperator)와 docker compose run --rm agent-job이 쓴다

# ---------- 계산 이미지 공통: Airflow 이미지와 같은 Python 3.12 ----------
# bookworm으로 고정: Spark 3.5가 도는 JDK 17이 이 배포판 저장소에 있다.
FROM python:3.12-slim-bookworm AS job-base
# 사용자 uid 50000, gid 0은 Airflow 이미지의 airflow 사용자와 같다. scratch 이름 볼륨(airflow:root 소유)에 그대로 쓴다.
RUN useradd --uid 50000 --gid 0 --home-dir /home/job --create-home job \
 && mkdir -p /scratch && chown 50000:0 /scratch
COPY requirements.txt /requirements.txt
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 TZ=Asia/Seoul
WORKDIR /opt/project

# ---------- Spark 계산 이미지 ----------
FROM job-base AS spark-job
# git: 리니지 추출이 회차에 커밋과 작업 트리 상태를 남긴다(warehouse.build_record.git_state).
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless git && rm -rf /var/lib/apt/lists/*
# Iceberg 런타임과 PostgreSQL 드라이버(JDBC 카탈로그가 쓴다)를 이미지에 넣어 실행 중 내려받기가 없게 한다. 버전은 결정 35 그대로.
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.12.0/iceberg-spark-runtime-3.5_2.12-1.12.0.jar /opt/spark-jars/
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar /opt/spark-jars/
RUN chmod 644 /opt/spark-jars/*.jar
# Spark 3.5.9는 측정 5, 38, 42와 같은 버전이다. requirements.txt는 실행기가 psycopg로 PostgreSQL에 쓰기 위해 함께 넣는다.
RUN pip install --no-cache-dir "pyspark==3.5.9" -r /requirements.txt
USER job

# ---------- DuckDB 계산 이미지 ----------
FROM job-base AS duckdb-job
RUN pip install --no-cache-dir "duckdb==2.0.0" -r /requirements.txt
USER job
# 쓰는 확장(iceberg_scan, --compare의 postgres)을 빌드 때 받아 둔다. 실행 중 내려받기가 없다. 위치는 ~/.duckdb/extensions.
RUN python -c "import duckdb; c = duckdb.connect(); c.install_extension('iceberg'); c.install_extension('postgres')"

# ---------- SQL 에이전트 이미지 (결정 112) ----------
FROM job-base AS agent-job
# git: 레포를 /scratch 안에 복제해 그 안에서 브랜치·편집·빌드한다(호스트 .git에 쓰지 않음, F-22).
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
# 붙인 레포는 호스트 사용자 소유라 git이 거부한다. 명령줄 -c는 clone의 하위 git(upload-pack)까지 가지 않아 시스템 설정으로 둔다(F-33).
# 읽기 전용 레포와 /scratch 복제본만 다루는 이 이미지 안에서만의 설정이다.
RUN git config --system --add safe.directory '*'
# SDK(내장 Claude Code CLI 포함)는 Airflow 이미지에 깔던 것과 같은 버전. pyyaml은 agent/sandbox.py가 dbt yml을 읽는다.
RUN pip install --no-cache-dir "claude-agent-sdk==0.2.182" "pyyaml==6.0.3" -r /requirements.txt
# dbt는 Airflow 이미지와 같은 버전을 전용 가상환경에(결정 30 D3). 격리 빌드가 wh_load와 같은 dbt로 돌아야 G1~G5가 운영과 같은 뜻이다.
RUN python -m venv /opt/dbt-venv && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-core==1.12.6" "dbt-postgres==1.11.0"
USER job

# ---------- Airflow 이미지 ----------
FROM apache/airflow:3.3.2-python3.12 AS airflow
COPY requirements.txt /requirements.txt
# Airflow 버전을 함께 적어 두면, requirements.txt를 설치하면서 Airflow 자체가 다른 버전으로 바뀌지 않는다.
# DockerOperator(결정 109 H4)용 provider와 docker SDK는 Airflow 3.3.2 constraints(python 3.12)의 버전 그대로다.
RUN pip install --no-cache-dir "apache-airflow==3.3.2" "apache-airflow-providers-docker==4.5.9" "docker==7.2.0" -r /requirements.txt
# SQL 에이전트(Claude Agent SDK)는 이 이미지에 없다. agent-job 이미지에서 돈다(결정 112).
# 고도화 2단계: dbt는 Airflow와 라이브러리가 부딪히지 않게 전용 가상환경에 둔다 (결정 30 D3).
# dbt v2는 지원 목록에 PostgreSQL이 없어(2026-11-03 확인) Python 기반 v1의 안정판으로 고정한다.
RUN python -m venv /opt/airflow/dbt-venv \
 && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-core==1.12.6" "dbt-postgres==1.11.0"
# Spark(JDK, spark-venv, jar)는 이 이미지에 없다. 계산은 spark-job, duckdb-job 이미지에서 돈다(결정 109 H5).
# 작업 공간 볼륨의 마운트 지점(결정 107 S4). 빈 이름 볼륨은 처음 붙을 때 이 폴더의 소유자를 물려받으므로 airflow 사용자가 쓸 수 있다.
# Airflow 쪽에서 /scratch를 쓰는 것은 heavy 잠금을 잡은 프로세스의 비우기(common.clear_scratch)다.
USER root
RUN mkdir -p /scratch && chown airflow:root /scratch
USER airflow
