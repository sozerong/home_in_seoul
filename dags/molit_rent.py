"""전월세 실거래가(오피스텔, 연립다세대) 주간 수집·정제 DAG. 결정 18, 21.

매주 월요일 04:00(한국 시간)에 돈다. data interval이 끝나는 달의 **전달부터 N개월**을 서울 25개 구 x 2개 출처로
다시 받아 정제한다. N은 Airflow Variable `rent_recent_months`(없으면 3). 이번 달은 아직 진행 중이라 뺀다.
작업 하나 = 처리 단위 하나 (출처, 구, 계약년월). 실패하면 그 칸만 다시 돌린다.
재시도는 스크립트 안에서만 한다(일시 오류만 3번). Airflow 재시도는 0번이라, 여기 도착한 실패는 바로 드러난다.
"""
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Variable, task

from alerting import notify_failure

SEOUL_GU = ["11110", "11140", "11170", "11200", "11215", "11230", "11260", "11290", "11305", "11320",
            "11350", "11380", "11410", "11440", "11470", "11500", "11530", "11545", "11560", "11590",
            "11620", "11650", "11680", "11710", "11740"]  # 서울 25개 구 (법정동 코드 앞 5자리, stg.sbiz_store에서 확인)
KINDS = ["offi", "rh"]

# 수집일(D)은 작업이 실제로 도는 순간에 한 번 정해 수집과 정제에 같이 넘긴다.
# 며칠 뒤 이 작업만 다시 돌려도 "실제로 받은 날"이 붙는다. 종료 코드가 0이 아니면 바로 멈춘다(set -e).
COMMAND = (
    'set -euo pipefail; cd /opt/project; D=$(date +%F); '
    'python -m ingestion.molit_rent "$KIND" "$GU" "$YM" --collected-date "$D"; '
    'python -m transform.rent "$KIND" "$D" "$GU" "$YM"'
)

with DAG(
    dag_id="molit_rent",
    schedule="0 4 * * 1",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "on_failure_callback": notify_failure},
    tags=["rent", "weekly"],
):

    @task
    def plan(data_interval_end=None, dag_run=None):
        """처리 단위 목록을 만든다: data interval이 끝나는 달의 전달부터 N개월 x 25개 구 x 2개 출처.

        Airflow 3에서 logical_date 없이 수동 실행하면 data interval이 없다(None). 그때는 실행 예정 시각(run_after)을 쓴다.
        """
        n = int(Variable.get("rent_recent_months", default=3))
        end = data_interval_end or dag_run.run_after
        this_month = pendulum.instance(end).in_timezone("Asia/Seoul").start_of("month")
        months = [this_month.subtract(months=i).format("YYYYMM") for i in range(1, n + 1)]
        return [{"KIND": kind, "GU": gu, "YM": ym} for ym in months for kind in KINDS for gu in SEOUL_GU]

    BashOperator.partial(
        task_id="collect_and_clean",
        bash_command=COMMAND,
        append_env=True,  # 컨테이너의 환경 변수(DATABASE_URL 등)를 지우지 않고 KIND, GU, YM만 더한다
        max_active_tis_per_dagrun=4,  # 공공데이터포털에 한꺼번에 몰리지 않게
        map_index_template="{{ task.env['KIND'] }}_{{ task.env['GU'] }}_{{ task.env['YM'] }}",
    ).expand(env=plan())
