"""스테이징 테이블을 Airflow Asset으로 정의한다. 스테이징이 바뀌면 웨어하우스 DAG(wh_load)가 돈다(단계 7 기본안 3).

이름이 DAG마다 어긋나지 않도록 한 곳에서만 정의하고 가져다 쓴다.
"""
from airflow.sdk import Asset


def _stg(table):
    return Asset(name=f"stg.{table}", uri=f"postgres://db:5432/seoul/stg/{table}")


STG_STATION_MASTER = _stg("station_master")
# 혼잡도는 원본 표(raw.congestion)를 dbt가 정제한다(결정 42). wh_load가 이 원본 갱신을 보고 돈다.
RAW_CONGESTION = Asset(name="raw.congestion", uri="postgres://db:5432/seoul/raw/congestion")
# 생활이동 경계 표(결정 89, 96). Spark SQL 실행기(transform/spark_sql_runner.py)가 쓴다.
STG_LIVING_COMMUTE = _stg("living_commute")
# 상가정보도 원본 표(raw.sbiz_store, 서울 전체 업소)를 dbt가 정제한다(결정 78).
RAW_SBIZ_STORE = Asset(name="raw.sbiz_store", uri="postgres://db:5432/seoul/raw/sbiz_store")
STG_SGIS_DONG = _stg("sgis_dong")
STG_PARCEL = _stg("parcel")
# 전월세도 원본 표(raw.rent)를 dbt가 정제한다(결정 72).
RAW_RENT = Asset(name="raw.rent", uri="postgres://db:5432/seoul/raw/rent")

# 웨어하우스 테이블(wh.dim_*, wh.map_*, wh.fact_store)을 바꾸는 입력. 전월세(raw.rent)는 뷰(wh.fact_rent)로만 쓰여서 빠진다.
# 전월세는 마트(rent_listing)만 바꾸므로 wh_load 일정에 따로 더한다.
WH_INPUTS = [STG_STATION_MASTER, RAW_CONGESTION, STG_LIVING_COMMUTE, RAW_SBIZ_STORE, STG_SGIS_DONG, STG_PARCEL]

# 웨어하우스·마트 교체와 상품 내보내기까지 끝났다(wh_load의 export_product). 컬럼 리니지 DAG가 이것을 보고 돈다(결정 50, 57).
WH_BUILT = Asset(name="wh.built", uri="file:///opt/project/product")  # 내보낸 상품 폴더. postgres URI는 DB/스키마/표 세 단계가 있어야 해서 이쪽이 맞다
