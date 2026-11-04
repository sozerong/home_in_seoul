"""생활이동(행정동) 레이크 zip 한 달치를 Iceberg 표 lh.living.migration_rows에 넣는다 (고도화 4단계, 결정 35).

24시간대 전체 행을 원본 글자 그대로 10개 열로 나눠 담는다. 형 변환과 검증(rules.md 4절)은 다음 단계(stg)가 한다:
격리 표에 원래 값을 남겨야 해서, 여기서 숫자로 바꾸다 깨진 값을 잃으면 안 된다.
처리 단위는 대상연월 파티션 하나. 다시 돌리면 그 파티션만 새 스냅샷으로 바뀐다(멱등). 예전 내용은 과거 스냅샷으로 남는다.
row_no는 CSV 안의 줄 번호(머리글 = 1)로, 지금 stg.living_migration의 row_no와 같은 뜻이다.
사용법: /opt/airflow/spark-venv/bin/python -m transform.living_iceberg 수집일YYYY-MM-DD 대상연월YYYYMM
"""
import argparse
import datetime
import shutil
import tempfile
import time
import zipfile

from transform.iceberg import spark_session
from transform.living_migration import HEADER, LAKE

TABLE = "lh.living.migration_rows"
COLUMNS = ["target_ym_raw", "weekday", "arrive_hour_raw", "origin_cd", "dest_cd", "sex", "age", "move_type",
           "avg_minutes_raw", "population_raw"]  # HEADER 순서. _raw는 다음 단계에서 숫자로 바꿀 열


def extract(path, folder):
    """zip 안 CSV 24개를 {시}.csv로 풀고 {시: zip 안 이름}을 돌려준다. 이름을 ASCII로 바꿔 Spark 경로 인코딩 문제를 피한다."""
    names = {}
    with zipfile.ZipFile(path, metadata_encoding="cp949") as zf:
        for name in zf.namelist():
            if name.endswith("시.csv"):
                hour = name[-7:-5]
                names[hour] = name
                with zf.open(name) as src, open(f"{folder}/{hour}.csv", "wb") as dst:
                    shutil.copyfileobj(src, dst, 1 << 24)
    if sorted(names) != [f"{h:02d}" for h in range(24)]:  # F3
        raise SystemExit(f"F3 {path.name} 안 시간대 CSV가 00~23시 24개가 아닙니다: {sorted(names)}")
    for hour, name in names.items():  # F1
        with open(f"{folder}/{hour}.csv", encoding="cp949", newline="") as f:
            header = f.readline().rstrip("\r\n").split(",")
        if header != HEADER:
            raise SystemExit(f"F1 스키마 변경: {name} 헤더 {header}")
    return names


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("collected_date", type=datetime.date.fromisoformat)
    parser.add_argument("target_ym")
    args = parser.parse_args()
    path = LAKE / f"collected_date={args.collected_date}" / f"생활이동_행정동_{args.target_ym}.zip"
    if not path.exists():
        raise SystemExit(f"레이크에 파일이 없습니다: {path}")

    started = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="living_") as folder:  # 컨테이너 /tmp. 끝나면 지운다
        names = extract(path, folder)
        unzipped = time.perf_counter()

        from pyspark.sql import functions as F
        spark = spark_session("living_iceberg")
        # 파일 하나 = Spark 파티션 하나가 되게 한다. 그래야 파티션 안 순번이 곧 파일 안 줄 번호다(아래에서 검사).
        spark.conf.set("spark.sql.files.maxPartitionBytes", "2g")
        spark.conf.set("spark.sql.files.openCostInBytes", "2g")
        # 쓰지 않는 글자(\x01)를 구분자로, 따옴표 처리를 끄고 읽어 한 줄을 통째로 받는다. 나누기는 아래에서 직접 한다.
        lines = (spark.read.option("encoding", "x-windows-949").option("sep", "\x01").option("quote", "")
                 .schema("line string").csv([f"{folder}/{h}.csv" for h in sorted(names)]))
        hour = F.regexp_extract(F.input_file_name(), r"/(\d\d)\.csv$", 1)
        fields = F.split("line", ",", -1)
        n = F.size(fields)
        rows = lines.select(
            F.lit(args.target_ym).alias("target_ym"),
            F.lit(args.collected_date).alias("collected_date"),
            F.create_map(*[x for h, name in sorted(names.items()) for x in (F.lit(h), F.lit(name))])[hour].alias("source_file"),
            ((F.monotonically_increasing_id().bitwiseAND((1 << 33) - 1)) + 1).cast("int").alias("row_no"),
            hour.alias("file_hour"),
            n.alias("n_fields"),
            *[F.when(n == len(HEADER), fields[i]).alias(c) for i, c in enumerate(COLUMNS)],
            F.when(n != len(HEADER), F.col("line")).alias("line"),  # 열 수가 틀린 줄만 원문을 남긴다(V1)
        ).where("row_no > 1")  # 1번 줄은 머리글(위 F1에서 확인)

        spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.living")
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
            target_ym string, collected_date date, source_file string, row_no int, file_hour string, n_fields int,
            {', '.join(f'{c} string' for c in COLUMNS)}, line string)
            USING iceberg PARTITIONED BY (target_ym)""")
        before = spark.sql(f"SELECT snapshot_id FROM {TABLE}.snapshots ORDER BY committed_at DESC").first()
        rows.writeTo(TABLE).overwritePartitions()

        # 줄 번호 검사: 시간대마다 2번부터 빈틈과 겹침 없이 이어져야 한다. 파일 두 개가 한 파티션에 섞이거나
        # 한 파일이 쪼개지거나 Spark가 빈 줄을 건너뛰면 여기서 걸린다. 걸리면 쓰기 전 스냅샷으로 되돌린다.
        check = spark.sql(f"""SELECT file_hour, count(*) n, count(DISTINCT row_no) d, min(row_no) lo, max(row_no) hi
                              FROM {TABLE} WHERE target_ym = '{args.target_ym}' GROUP BY 1""").collect()
        bad = [r for r in check if not (r.n == r.d == r.hi - 1 and r.lo == 2)]
        if bad or len(check) != 24:
            if before:
                spark.sql(f"CALL lh.system.rollback_to_snapshot('living.migration_rows', {before.snapshot_id})")
            else:
                spark.sql(f"DELETE FROM {TABLE} WHERE target_ym = '{args.target_ym}'")
            raise SystemExit(f"줄 번호 검사 실패(쓰기를 되돌림): 시간대 {len(check)}개, 어긋난 시간대 {bad}")
        snapshot = spark.sql(f"SELECT snapshot_id FROM {TABLE}.snapshots ORDER BY committed_at DESC").first()
        spark.stop()

    total = sum(r.n for r in check)
    print(f"{TABLE} {args.target_ym} (수집일 {args.collected_date}): {total:,}행, 시간대 {len(check)}개, "
          f"스냅샷 {snapshot.snapshot_id}, 압축 해제 {unzipped - started:.1f}초, 전체 {time.perf_counter() - started:.1f}초")


if __name__ == "__main__":
    main()
