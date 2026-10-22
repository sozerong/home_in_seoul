"""스테이징 테이블을 Airflow Asset으로 정의한다. 스테이징이 바뀌면 웨어하우스 DAG(wh_load)가 돈다(단계 7 기본안 3).

이름이 DAG마다 어긋나지 않도록 한 곳에서만 정의하고 가져다 쓴다.
"""
from airflow.sdk import Asset


def _stg(table):
    return Asset(name=f"stg.{table}", uri=f"postgres://db:5432/seoul/stg/{table}")


STG_STATION_MASTER = _stg("station_master")
STG_CONGESTION = _stg("congestion")
STG_LIVING_MIGRATION = _stg("living_migration")
STG_SBIZ_STORE = _stg("sbiz_store")
STG_SGIS_DONG = _stg("sgis_dong")
STG_PARCEL = _stg("parcel")

# 웨어하우스 테이블(wh.dim_*, wh.map_*, wh.fact_store)을 바꾸는 스테이징. stg.rent는 뷰(wh.fact_rent)로만 쓰여서 빠진다.
WH_INPUTS = [STG_STATION_MASTER, STG_CONGESTION, STG_LIVING_MIGRATION, STG_SBIZ_STORE, STG_SGIS_DONG, STG_PARCEL]
