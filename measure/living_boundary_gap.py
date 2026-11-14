"""3.4 체크포인트 차이 분석: 경계 표 행 수(정제 뒤)와 측정 29 후보 f 그룹 수(정제 전)의 차이가 어디서 오는지 센다.

실행기와 같은 뷰(spark_sql/living)를 만들고, f grain 그룹마다 통과 행이 하나라도 있는지와 격리 사유 묶음을 센다. 쓰지 않는다.
실행(계산 컨테이너): docker compose run --rm spark-job python -m measure.living_boundary_gap
"""
import time

from common import ROOT
from transform.iceberg import spark_session
from transform.living_iceberg import load_codes
from transform.spark_sql_runner import sql_files

KEYS = """target_ym, origin_cd, dest_cd, IF(weekday IN ('월','화','수','목','금'), '평일', '주말'),
          CASE WHEN file_hour BETWEEN '07' AND '09' THEN '07_09' WHEN file_hour BETWEEN '18' AND '20' THEN '18_20' ELSE '기타' END,
          move_type, IF(age IN ('20', '25', '30'), '20_34', '기타')"""
WHERE = ("n_fields = 10 AND length(origin_cd) = 7 AND origin_cd LIKE '11%' AND length(dest_cd) = 7 AND dest_cd LIKE '11%' "
         "AND move_type IN ('HW', 'WH')")


def main():
    spark = spark_session("living_boundary_gap")
    spark.createDataFrame([(c,) for c in sorted(load_codes())], "code string").createOrReplaceTempView("codes")
    views, _ = sql_files(ROOT / "spark_sql" / "living")
    for name, sql in views[:1]:  # checked만 있으면 된다
        spark.sql(sql).createOrReplaceTempView(name)
    started = time.perf_counter()
    rows = spark.sql(f"""
        SELECT any_pass, rs, count(*) groups, sum(n) raw_rows FROM (
            SELECT bool_or(reasons = '') any_pass, array_join(array_sort(collect_set(reasons)), '|') rs, count(*) n
            FROM checked WHERE {WHERE} GROUP BY {KEYS}
        ) g GROUP BY any_pass, rs ORDER BY any_pass, groups DESC""").collect()
    print(f"{time.perf_counter() - started:.1f}초")
    for r in rows:
        if not r.any_pass:
            print(f"  통과 행 없는 그룹  사유 {r.rs!r:40} 그룹 {r.groups:>10,} 행 {r.raw_rows:>12,}")
    print("  통과 행 있는 그룹:", f"{sum(r.groups for r in rows if r.any_pass):,}")
    print("  통과 행 없는 그룹:", f"{sum(r.groups for r in rows if not r.any_pass):,}")
    spark.stop()


if __name__ == "__main__":
    main()
