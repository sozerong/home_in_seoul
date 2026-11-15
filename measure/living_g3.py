"""3.2 첫 측정: 생활이동 경계 표(G3) 그룹 수와 데이터 성질 (design_remaining_work.md 3.2, 결정 89 조건, 94-2).

G3 한 행 = 대상연월 × 출발 × 도착 × 평일/주말 × 도착시간 × 이동유형 × 나이. Iceberg 원본 전체(정제 전, 열 수가 맞는 행)로 센다.
그룹 수가 40,382,008(지금 stg.living_migration)을 넘으면 결정 89에 따라 차원을 줄여야 한다. 이 스크립트는 재기만 하고 쓰지 않는다.
실행(계산 컨테이너): docker compose run --rm spark-job python -m measure.living_g3 [options]
  options [a b ...]: 기준을 넘었을 때 차원 줄이기 후보(OPTIONS)마다 그룹 수만 잰다. 글자를 주면 그 후보만
"""
import sys
import time

from transform.iceberg import spark_session
from transform.living_iceberg import TABLE

LIMIT = 40_382_008
G3 = "target_ym, origin_cd, dest_cd, IF(weekday IN ('월','화','수','목','금'), '평일', '주말'), file_hour, move_type, age"


def show(spark, title, sql):
    started = time.perf_counter()
    rows = spark.sql(sql).collect()
    print(f"\n[{title}] {time.perf_counter() - started:.1f}초")
    for r in rows:
        print("  ", " | ".join("" if v is None else str(v) for v in r))
    return rows


SEOUL = "length(origin_cd) = 7 AND origin_cd LIKE '11%' AND length(dest_cd) = 7 AND dest_cd LIKE '11%'"
AGE2 = "IF(age IN ('20', '25', '30'), '20_34', '기타')"  # 결정 22의 20~34세 = 나이 20, 25, 30
HOUR3 = "CASE WHEN file_hour BETWEEN '07' AND '09' THEN '07_09' WHEN file_hour BETWEEN '18' AND '20' THEN '18_20' ELSE '기타' END"  # 범위 C, S6b와 같은 경계
OPTIONS = {  # 3.2에서 기준을 넘었을 때 고를 차원 줄이기 후보: (조건, 그룹 키)
    "a 서울 출발·도착만": (SEOUL, G3),
    "b a + 나이 2구간": (SEOUL, G3.replace("age", AGE2)),
    "c b + 이동유형 HW·WH만": (f"{SEOUL} AND move_type IN ('HW', 'WH')", G3.replace("age", AGE2)),
    "d 범위 그대로 + 나이 2구간 + 도착시간 3묶음": ("true", G3.replace("age", AGE2).replace("file_hour", HOUR3)),
    "e b + 도착시간 3묶음": (SEOUL, G3.replace("age", AGE2).replace("file_hour", HOUR3)),
    "f c + 도착시간 3묶음": (f"{SEOUL} AND move_type IN ('HW', 'WH')", G3.replace("age", AGE2).replace("file_hour", HOUR3)),
}


def options(names=None):
    spark = spark_session("living_g3_options")
    spark.table(TABLE).createOrReplaceTempView("m")
    for name, (where, keys) in OPTIONS.items():
        if names and name[0] not in names:
            continue
        show(spark, f"{name}: 그룹 수, 원본 행 수", f"SELECT count(*) groups, sum(rows) rows FROM "
                                                f"(SELECT {keys}, count(*) rows FROM m WHERE n_fields = 10 AND {where} GROUP BY {keys})")
    spark.stop()


def main():
    if sys.argv[1:2] == ["options"]:
        return options(sys.argv[2:])
    spark = spark_session("living_g3")
    spark.table(TABLE).createOrReplaceTempView("m")
    show(spark, "대상연월별 행 수, 수집일, 열 수 틀린 행, 가려진 인구(*)", """
        SELECT target_ym, collect_set(cast(collected_date AS string)) dates, count(*) n,
               count_if(n_fields <> 10) bad_fields, count_if(population_raw = '*') masked,
               round(count_if(population_raw = '*') / count(*), 4) masked_share
        FROM m GROUP BY 1 ORDER BY 1""")
    show(spark, "열별 NULL(열 수가 맞는 행 중)", """
        SELECT count(*) n, """ + ", ".join(f"count_if({c} IS NULL OR {c} = '') {c}" for c in
                                            ("weekday", "file_hour", "origin_cd", "dest_cd", "sex", "age", "move_type",
                                             "avg_minutes_raw", "population_raw")) + " FROM m WHERE n_fields = 10")
    for key in ("dest_cd", "origin_cd"):
        show(spark, f"{key} 상위 20개 키의 행 비중", f"""
            SELECT {key}, n, round(n / sum(n) OVER (), 4) share, round(sum(n) OVER (ORDER BY n DESC) / sum(n) OVER (), 4) cum
            FROM (SELECT {key}, count(*) n FROM m WHERE n_fields = 10 GROUP BY 1) ORDER BY n DESC LIMIT 20""")
        show(spark, f"{key} 키 수, 키당 행 수 최대/중앙값/평균", f"""
            SELECT count(*) keys, max(n), percentile_approx(n, 0.5), round(avg(n)) FROM
            (SELECT {key}, count(*) n FROM m WHERE n_fields = 10 GROUP BY 1)""")
    groups = show(spark, "G3 그룹 수 (대상연월별과 전체)", f"""
        SELECT coalesce(target_ym, '전체') ym, count(*) groups, sum(rows) rows FROM
        (SELECT {G3}, count(*) rows FROM m WHERE n_fields = 10 GROUP BY {G3}) g
        GROUP BY ROLLUP(target_ym) ORDER BY ym""")
    total = [r for r in groups if r.ym == "전체"][0].groups
    print(f"\nG3 그룹 수 {total:,} / 기준 {LIMIT:,} -> {'넘음: 멈추고 차원 줄이는 선택지를 보인다' if total > LIMIT else '넘지 않음'}")
    spark.stop()


if __name__ == "__main__":
    main()
