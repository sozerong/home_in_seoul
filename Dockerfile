# 단계 7: 공식 Airflow 이미지에 이 프로젝트의 라이브러리를 더한다 (결정 19, 20).
FROM apache/airflow:3.3.2-python3.12

COPY requirements.txt /requirements.txt
# Airflow 버전을 함께 적어 두면, requirements.txt를 설치하면서 Airflow 자체가 다른 버전으로 바뀌지 않는다.
RUN pip install --no-cache-dir "apache-airflow==3.3.2" -r /requirements.txt
# 고도화 2단계: dbt는 Airflow와 라이브러리가 부딪히지 않게 전용 가상환경에 둔다 (결정 30 D3).
# dbt v2는 지원 목록에 PostgreSQL이 없어(2026-11-03 확인) Python 기반 v1의 안정판으로 고정한다.
RUN python -m venv /opt/airflow/dbt-venv \
 && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-core==1.12.6" "dbt-postgres==1.11.0"
