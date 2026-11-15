"""웨어하우스, 마트, 데이터 상품 다시 만들기 DAG. 단계 7 기본안 3, 단계 9, 고도화 2단계(결정 30~32).

일정 대신 Asset으로 돈다: 스테이징(역사마스터, 혼잡도, 생활이동, 상가정보, 경계, 지적도, 전월세) 중 하나라도 갱신되면
1) dbt_build       warehouse/dbt의 wh·mart 모델과 테스트를 빌드용 스키마(wh_build, mart_build)에 만든다.
                   지난번 실패로 남은 빌드 스키마를 먼저 지운다. 테스트 하나라도 실패하면 여기서 멈추고 실제 wh·mart는 그대로다.
2) record_build    빌드 기록(ops.build_run: 커밋, 깨끗 여부, 컴파일 SQL 해시, 행 수)과 정제 표의 단위별 차이(ops.build_diff)를
                   남긴다(결정 81). 지금 스키마와 비교해야 하므로 교체 직전에 돈다.
3) swap_schemas    wh_build -> wh, mart_build -> mart로 한 트랜잭션에서 교체하고 옛 스키마를 지운다.
4) export_product  product/v{오늘}/에 상품 파일과 manifest를 쓴다. 같은 날 다시 돌면 그 폴더를 새로 만든다.
                   끝나면 wh.built Asset을 갱신해 lineage DAG가 돈다(결정 50, 57).
5) report          dbt 경고, 관문 거절 배치, 늦은 출처를 모아 슬랙 메시지 하나(SLACK_WEBHOOK_URL이 있을 때, 결정 86).
앞 단계가 실패하면 뒤 단계는 돌지 않는다. 모두 멱등이다.
"""
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, AssetAny, task

from alerting import notify_failure, wh_load_dbt_failed, wh_load_report
from assets import RAW_RENT, WH_BUILT, WH_INPUTS

with DAG(
    dag_id="wh_load",
    schedule=AssetAny(*WH_INPUTS, RAW_RENT),  # ponytail: 전월세만 바뀌어도 wh 전체를 다시 만든다. 느려지면 마트 DAG를 따로 뗀다
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "on_failure_callback": notify_failure},
    tags=["warehouse"],
):
    in_dbt = "set -euo pipefail; cd /opt/project/warehouse/dbt; "
    dbt = "/opt/airflow/dbt-venv/bin/dbt"  # dbt 전용 가상환경 (결정 30 D3)
    # 전체 빌드는 무거운 작업이라 pool heavy와 heavy 잠금(PostgreSQL advisory lock, common.heavy_lock)으로 한 번에 하나(resource_guard.md L5)
    build = BashOperator(task_id="dbt_build", pool="heavy", on_failure_callback=wh_load_dbt_failed,  # dbt 실패는 에이전트 신호(A2)
                         bash_command="set -euo pipefail; cd /opt/project; python -m common lock -- "
                                      f"sh -c 'cd warehouse/dbt && {dbt} run-operation drop_build_schemas && {dbt} build'")
    # ponytail: 교체가 실패하면 마지막 build_run은 교체되지 않은 빌드다. 그 실패는 알림으로 따로 드러난다
    record = BashOperator(task_id="record_build",
                          bash_command="set -euo pipefail; cd /opt/project; python -m warehouse.build_record")
    swap = BashOperator(task_id="swap_schemas", bash_command=in_dbt + f"{dbt} run-operation swap_schemas")
    export = BashOperator(task_id="export_product",
                          bash_command="set -euo pipefail; cd /opt/project; python -m warehouse.export_product",
                          outlets=[WH_BUILT])  # 상품까지 나간 뒤에 리니지 DAG를 깨운다(결정 57)

    @task(task_id="report")
    def report(dag_run=None):
        return wh_load_report(f"wh_load/{dag_run.run_id}")

    build >> record >> swap >> export >> report()
