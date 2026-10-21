"""SGIS 행정동 경계와 연속지적도 shp(레이크 zip)를 정제·검증해 stg.sgis_dong, stg.parcel에 넣는다.

규칙은 docs/rules.md 6, 7절. 처리 단위는 수집일 하나(zip 하나)이고, 그 수집일의 기존 행을 지우고 다시 넣는다(멱등).
사용법: python -m transform.boundary {sgis_dong|parcel} 수집일YYYY-MM-DD
예:     python -m transform.boundary sgis_dong 2026-10-21
"""
import argparse
import collections
import datetime
import io
import json
import re
import time
import zipfile

import psycopg
import shapefile
from psycopg.types.json import Jsonb

from common import ROOT, read_env

# BF2, PF2: 좌표계는 이름이 아니라 숫자 매개변수로 비교한다. 프로그램마다 이름 표기가 다르기 때문이다.
EPSG_5179 = {"central_meridian": 127.5, "latitude_of_origin": 38.0, "false_easting": 1_000_000.0,
             "false_northing": 2_000_000.0, "scale_factor": 0.9996}
EPSG_5186 = {"central_meridian": 127.0, "latitude_of_origin": 38.0, "false_easting": 200_000.0,
             "false_northing": 600_000.0, "scale_factor": 1.0}

SOURCES = {
    "sgis_dong": {"folder": "sgis_boundary", "zip": "bnd_dong_*.zip", "srid": 5179, "projection": EPSG_5179,
                  "fields": {"BASE_DATE", "ADM_CD", "ADM_NM"},                 # BF1
                  "key": "adm_cd", "table": "stg.sgis_dong", "geom_rule": "BV2", "max_ratio": None,
                  "columns": [("base_date", "date"), ("adm_cd", "text"), ("adm_nm", "text")]},
    "parcel": {"folder": "parcel_boundary", "zip": "LSMD_CONT_LDREG_*.zip", "srid": 5186, "projection": EPSG_5186,
               "fields": {"SGG_OID", "JIBUN", "BCHK", "PNU", "COL_ADM_SE"},  # PF1
               "key": "pnu", "table": "stg.parcel", "geom_rule": "PV2", "max_ratio": 0.05,  # PF4
               "columns": [("base_ym", "text"), ("pnu", "text"), ("jibun", "text"), ("sgg_cd", "text")]},
}


def projection_matches(prj_text, expected):
    """.prj(WKT)의 GRS 1980 타원체와 투영 매개변수가 기대값과 같은지 본다."""
    params = {name.lower(): float(value)
              for name, value in re.findall(r'PARAMETER\["([^"]+)",\s*([-\d.eE+]+)\]', prj_text)}
    return (re.search(r"GRS[ _]1980", prj_text) is not None
            and all(name in params and abs(params[name] - value) < 1e-6 for name, value in expected.items()))


def clean_sgis(rec):
    """BV1을 먼저 보고, 그다음 BC1(서울만)을 본다. 서울이 아닌 정상 행은 None(범위 밖)."""
    adm_cd = str(rec["ADM_CD"] or "").strip()
    adm_nm = str(rec["ADM_NM"] or "").strip()
    reasons = []
    try:
        base_date = datetime.datetime.strptime(str(rec["BASE_DATE"]).strip(), "%Y%m%d").date().isoformat()
    except ValueError:
        base_date = None
    if not re.fullmatch(r"\d{8}", adm_cd) or not adm_nm or base_date is None:
        reasons.append("BV1")  # 형식이 깨진 행은 서울인지 알 수 없으므로 범위 판정 전에 격리한다
    elif not adm_cd.startswith("11"):
        return None  # BC1: 범위 밖
    return {"base_date": base_date, "adm_cd": adm_cd, "adm_nm": adm_nm}, reasons


def clean_parcel(rec, base_ym):
    """PV1. 서울 파일이라 범위 판정은 하지 않는다."""
    pnu = str(rec["PNU"] or "").strip()
    values = {"base_ym": base_ym, "pnu": pnu,
              "jibun": str(rec["JIBUN"] or "").strip() or None,
              "sgg_cd": str(rec["COL_ADM_SE"] or "").strip() or None}
    return values, ([] if re.fullmatch(r"\d{19}", pnu) else ["PV1"])


def open_shapefile(zip_path):
    """zip 안의 shp 묶음을 압축 풀지 않고 메모리로 읽어 (Reader, .prj 글자, .shp 이름)을 돌려준다."""
    with zipfile.ZipFile(zip_path) as zf:
        shp_names = [n for n in zf.namelist() if n.lower().endswith(".shp")]
        if len(shp_names) != 1:
            raise SystemExit(f"zip 안의 .shp가 하나가 아닙니다: {shp_names}")
        stem = shp_names[0][:-4]
        try:
            data = {ext: zf.read(f"{stem}.{ext}") for ext in ("shp", "shx", "dbf", "prj", "cpg")}
        except KeyError as e:
            raise SystemExit(f"shp 묶음에 빠진 파일이 있습니다: {e}")
    encoding = data["cpg"].decode("ascii").strip()
    if encoding.upper() in ("EUC-KR", "CP949", "UHC"):
        encoding = "cp949"
    reader = shapefile.Reader(shp=io.BytesIO(data["shp"]), shx=io.BytesIO(data["shx"]),
                              dbf=io.BytesIO(data["dbf"]), encoding=encoding)
    return reader, data["prj"].decode("utf-8", errors="replace"), shp_names[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=SOURCES)
    parser.add_argument("collected_date", type=datetime.date.fromisoformat)
    args = parser.parse_args()
    src, day = SOURCES[args.kind], args.collected_date
    started = time.perf_counter()

    zips = sorted((ROOT / "lake" / src["folder"] / f"collected_date={day}").glob(src["zip"]))
    if len(zips) != 1:
        raise SystemExit(f"레이크에 zip이 하나가 아닙니다: {zips}")
    reader, prj, shp_name = open_shapefile(zips[0])
    if not projection_matches(prj, src["projection"]):  # BF2, PF2
        raise SystemExit(f"좌표계가 EPSG:{src['srid']}와 다릅니다: {prj[:200]}")
    fields = {f[0] for f in reader.fields[1:]}
    if fields != src["fields"]:  # BF1, PF1
        raise SystemExit(f"필드 집합이 다릅니다: {sorted(fields ^ src['fields'])}")
    base_ym = (re.search(r"(\d{6})\.shp$", shp_name) or [None, None])[1]

    total = skipped = 0
    load, bad = [], []
    for row_no, sr in enumerate(reader.iterShapeRecords(), start=1):
        total += 1
        rec = sr.record.as_dict()
        result = clean_sgis(rec) if args.kind == "sgis_dong" else clean_parcel(rec, base_ym)
        if result is None:
            skipped += 1
            continue
        values, reasons = result
        raw = {k: str(v) for k, v in rec.items()}
        if sr.shape.shapeType == shapefile.NULL:
            reasons.append(src["geom_rule"])  # 도형이 아예 없음
        if reasons:
            bad.append({"row_no": row_no, "reasons": reasons, "raw": Jsonb(raw)})
            continue
        load.append((row_no, json.dumps(values, ensure_ascii=False), json.dumps(raw, ensure_ascii=False),
                     json.dumps(sr.shape.__geo_interface__)))

    keys = collections.Counter(json.loads(a)[src["key"]] for _, a, _, _ in load)
    duplicated = [k for k, n in keys.items() if n > 1]
    if duplicated:  # BF3, PF3
        raise SystemExit(f"{src['key']} 중복: {duplicated[:10]}")

    columns = ", ".join(c for c, _ in src["columns"])
    selects = ", ".join(f"(a->>'{c}')::{t}" for c, t in src["columns"])
    meta = {"day": day, "file": zips[0].name}
    with psycopg.connect(read_env("DATABASE_URL")) as conn:  # 블록이 정상으로 끝나면 커밋, 예외면 롤백
        conn.execute("CREATE TEMP TABLE load (row_no integer, a jsonb, raw jsonb, gj text) ON COMMIT DROP")
        with conn.cursor() as cur, cur.copy("COPY load (row_no, a, raw, gj) FROM STDIN") as copy:
            for row in load:
                copy.write_row(row)
        # BC2, BV2/PV2: 고쳐서(ST_MakeValid) 폴리곤만 남기고 MultiPolygon으로. 원래 유효했는지도 함께 남긴다.
        conn.execute(
            f"CREATE TEMP TABLE fixed ON COMMIT DROP AS"
            f" SELECT row_no, a, raw, NOT ST_IsValid(g) AS made_valid,"
            f"        ST_Multi(ST_CollectionExtract(ST_MakeValid(g), 3)) AS geom"
            f" FROM (SELECT row_no, a, raw, ST_SetSRID(ST_GeomFromGeoJSON(gj), {src['srid']}) AS g FROM load) s")
        for table in (src["table"], src["table"] + "_quarantine"):
            conn.execute(f"DELETE FROM {table} WHERE collected_date = %(day)s", meta)
        inserted = conn.execute(
            f"INSERT INTO {src['table']} (collected_date, source_file, row_no, {columns}, made_valid, geom)"
            f" SELECT %(day)s, %(file)s, row_no, {selects}, made_valid, geom FROM fixed"
            f" WHERE NOT ST_IsEmpty(geom) AND ST_IsValid(geom)", meta).rowcount
        geom_bad = conn.execute(
            f"INSERT INTO {src['table']}_quarantine (collected_date, source_file, row_no, reasons, raw)"
            f" SELECT %(day)s, %(file)s, row_no, ARRAY['{src['geom_rule']}'], raw FROM fixed"
            f" WHERE ST_IsEmpty(geom) OR NOT ST_IsValid(geom)", meta).rowcount
        with conn.cursor() as cur:
            cur.executemany(f"INSERT INTO {src['table']}_quarantine (collected_date, source_file, row_no, reasons, raw)"
                            f" VALUES (%(day)s, %(file)s, %(row_no)s, %(reasons)s, %(raw)s)",
                            [{**meta, **b} for b in bad])
        made_valid = conn.execute(f"SELECT count(*) FROM {src['table']} WHERE collected_date = %(day)s"
                                  f" AND made_valid", meta).fetchone()[0]

        quarantined = len(bad) + geom_bad
        in_scope = total - skipped
        ratio = quarantined / in_scope if in_scope else 1.0
        counts = collections.Counter(r for b in bad for r in b["reasons"])
        if geom_bad:
            counts[src["geom_rule"]] += geom_bad
        print(f"{args.kind} (수집일 {day}, {shp_name}): 전체 {total}, 범위 밖 {skipped}, 통과 {inserted}"
              f" (그중 고친 도형 {made_valid}), 격리 {quarantined} ({ratio:.2%}), 사유 {dict(sorted(counts.items()))},"
              f" {time.perf_counter() - started:.1f}초")
        if in_scope == 0 or inserted == 0:
            raise SystemExit("통과한 도형이 0개입니다")
        if src["max_ratio"] is not None and ratio > src["max_ratio"]:  # PF4: 예외로 롤백된다
            raise SystemExit(f"격리 비율 {ratio:.2%}가 기준 {src['max_ratio']:.0%}를 넘었습니다")
    print("적재 완료")


if __name__ == "__main__":
    main()
