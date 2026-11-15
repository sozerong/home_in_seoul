"""생활이동 V8 충돌 판정: 해시 min/max(F-9 처방 A)가 옛 글자 min/max와 같은 답을 내는지 작은 입력으로 본다 (handoff_judgments.md J-9).
Spark가 필요해 CI 밖이다. 실행(계산 컨테이너): docker compose run --rm spark-job python -m tests.check_living_conflict
"""
from decimal import Decimal

from pyspark.sql import SparkSession

from common import ROOT

OLD = ("SELECT origin_cd, min(avg_minutes_raw) <> max(avg_minutes_raw) OR min(population_raw) <> max(population_raw) AS conflict "
       "FROM checked WHERE reasons = '' GROUP BY origin_cd")  # 처방 전 20_clean의 conflict (커밋 3efb02f)
KEYS = "target_ym, collected_date, weekday, arrive_hour_raw, origin_cd, dest_cd, sex, age, move_type"
CASES = [  # (키, [(평균 이동 시간 글자, 이동인구 글자)], 옛 판정)
    ("같음", [("5", "3.5"), ("5", "3.5")], False),
    ("한 행", [("5", "3.5")], False),
    ("숫자 같고 글자 다름 05/5", [("05", "3.5"), ("5", "3.5")], True),
    ("공백", [(" 5", "3.5"), ("5", "3.5")], True),
    ("부호", [("+5", "3.5"), ("5", "3.5")], True),
    ("소수 자리 3.50/3.5", [("5", "3.50"), ("5", "3.5")], True),
    ("가려짐 섞임 */3.5", [("5", "*"), ("5", "3.5")], True),
    ("가려짐끼리", [("5", "*"), ("5", "*")], False),
    ("값 다름", [("5", "3.5"), ("6", "3.5")], True),
    ("셋 중 하나만 다름", [("5", "3.5"), ("5", "3.5"), ("5", "4.5")], True),
    ("두 열이 엇갈림", [("5", "4.5"), ("6", "3.5")], True),
]

spark = SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").getOrCreate()
rows = []
for k, values, _ in CASES:
    for avg, pop in values:
        num = None if pop == "*" else Decimal(pop)
        rows.append(("202607", "2026-10-01", "월", "08", k, "1101053", "M", "25", "HW",  # 키 9개 중 origin_cd 자리에 사례 이름
                     avg, pop, int(avg), num, None if num is None else float(num), ""))
spark.createDataFrame(rows, f"{KEYS.replace(',', ' string,')} string, avg_minutes_raw string, population_raw string, "
                            "avg_minutes int, population decimal(12,2), pop_double double, reasons string") \
    .createOrReplaceTempView("checked")
new_sql = (ROOT / "spark_sql/living/20_clean.sql").read_text(encoding="utf-8")
new = {r.origin_cd: r.conflict for r in spark.sql(new_sql).collect()}
old = {r.origin_cd: r.conflict for r in spark.sql(OLD).collect()}
for k, _, expected in CASES:
    print(f"{k}: 옛 {old[k]}, 새 {new[k]}")
    assert old[k] == expected, f"{k}: 옛 판정이 기대와 다름"
    assert new[k] == old[k], f"{k}: 새 판정 {new[k]}이 옛 판정 {old[k]}과 다름"
plan = spark.sql(f"EXPLAIN FORMATTED {new_sql}").collect()[0][0]
assert "SortAggregate" not in plan and "HashAggregate" in plan, "새 conflict가 아직 정렬 집계다"
print(f"사례 {len(CASES)}개 옛 판정과 같음, 계획에 SortAggregate 없음. tests.check_living_conflict: 통과")
