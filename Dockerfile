# 단계 7: 공식 Airflow 이미지에 이 프로젝트의 라이브러리를 더한다 (결정 19, 20).
FROM apache/airflow:3.3.2-python3.12

COPY requirements.txt /requirements.txt
# Airflow 버전을 함께 적어 두면, requirements.txt를 설치하면서 Airflow 자체가 다른 버전으로 바뀌지 않는다.
RUN pip install --no-cache-dir "apache-airflow==3.3.2" -r /requirements.txt
# 고도화 2단계: dbt는 Airflow와 라이브러리가 부딪히지 않게 전용 가상환경에 둔다 (결정 30 D3).
# dbt v2는 지원 목록에 PostgreSQL이 없어(2026-11-03 확인) Python 기반 v1의 안정판으로 고정한다.
RUN python -m venv /opt/airflow/dbt-venv \
 && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-core==1.12.6" "dbt-postgres==1.11.0"
# 고도화 4단계: 생활이동을 Iceberg 표로 만드는 Spark (결정 35). dbt처럼 Airflow 이미지 안 전용 가상환경에 둔다.
# Spark 3.5.9는 측정 5와 같은 버전이라 재측정에서 데이터 크기 말고는 바뀐 것이 없다. Spark 3.5는 Java 17에서 돈다.
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
# Iceberg 런타임과 PostgreSQL 드라이버(JDBC 카탈로그가 쓴다)를 이미지에 넣어 실행 중 내려받기가 없게 한다.
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.12.0/iceberg-spark-runtime-3.5_2.12-1.12.0.jar /opt/spark-jars/
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar /opt/spark-jars/
RUN chmod 644 /opt/spark-jars/*.jar
USER airflow
RUN python -m venv /opt/airflow/spark-venv \
 && /opt/airflow/spark-venv/bin/pip install --no-cache-dir "pyspark==3.5.9" -r /requirements.txt
# requirements.txt는 정제 규칙(transform.living_migration)을 그대로 불러 쓰기 위해 함께 넣는다.
