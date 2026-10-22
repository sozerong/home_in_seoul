# 단계 7: 공식 Airflow 이미지에 이 프로젝트의 라이브러리를 더한다 (결정 19, 20).
FROM apache/airflow:3.3.2-python3.12

COPY requirements.txt /requirements.txt
# Airflow 버전을 함께 적어 두면, requirements.txt를 설치하면서 Airflow 자체가 다른 버전으로 바뀌지 않는다.
RUN pip install --no-cache-dir "apache-airflow==3.3.2" -r /requirements.txt