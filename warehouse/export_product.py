"""데이터 상품 내보내기 (단계 9, 결정 12). 마트 테이블을 버전 폴더에 Parquet + CSV로 내보낸다.

출력: product/v{YYYY.MM.DD}/ (git에 올리지 않는다). 같은 날 다시 실행하면 그 폴더를 새로 만든다(멱등).
  dong_profile.*   행정동 하나 = 한 행. 시세 요약, 가장 가까운 역과 아침 혼잡, 필수 5종 편의 밀도
  commute_od.*     출발 행정동 x 직장 행정동 = 한 행. 평일 아침 출근 평균 이동 시간
  rent_listing.*   최근 6개월 신규 전월세 계약 한 건 = 한 행
  dong_mois_map.*  행정안전부 행정동 코드 하나 = 한 행. 통계청 dong_id로 잇는 대응표(결정 25)
  manifest.json    버전, 파일마다 행 수와 sha256, 원천 수집일, 품질 지표
경계 도형(geojson)은 내보내지 않는다: SGIS 이용약관이 사전 승낙 없는 제3자 제공을 막는다(결정 26).
CSV는 엑셀에서 한글이 깨지지 않게 UTF-8 BOM으로 쓴다. 계약 내용은 docs/data_contract.md.
사용법: python -m warehouse.export_product
"""
import csv
import datetime
import hashlib
import json
import shutil

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq

from common import ROOT, read_env

TABLES = {  # 파일 이름: 정렬 기준. 행마다 순서가 하나로 정해져야 같은 데이터가 같은 파일(sha256)이 된다
    "dong_profile": "dong_id",
    "commute_od": "origin_dong_id, dest_dong_id",
    # 거래 고유 키가 없어(동·호 미제공) 모든 컬럼으로 정렬한다. mois_dong_cd, dong_nm, deal_ymd는 dong_id, deal_date로 정해져 뺐다.
    # 모든 컬럼이 같은 행은 바이트까지 같아 그 행들끼리 순서가 바뀌어도 파일은 같다 (측정 8).
    "rent_listing": "dong_id, deal_date, source_type, house_type, lease_type, deposit_10k_won, monthly_rent_10k_won, "
                    "excl_area_m2, floor, build_year",
    "dong_mois_map": "mois_dong_cd",
}


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    version = datetime.date.today().strftime("v%Y.%m.%d")
    out = ROOT / "product" / version
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    manifest = {"version": version, "created_at": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
                "files": {}}

    with psycopg.connect(read_env("DATABASE_URL")) as conn:
        for name, order in TABLES.items():
            cur = conn.execute(f"SELECT * FROM mart.{name} ORDER BY {order}")
            columns = [c.name for c in cur.description]
            rows = cur.fetchall()
            table = pa.Table.from_pylist([dict(zip(columns, r)) for r in rows])
            pq.write_table(table, out / f"{name}.parquet")
            with open(out / f"{name}.csv", "w", encoding="utf-8-sig", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)
            for ext in ("parquet", "csv"):
                manifest["files"][f"{name}.{ext}"] = {"rows": len(rows), "sha256": sha256(out / f"{name}.{ext}")}

        manifest["sources"] = dict(conn.execute("""
            SELECT 'rent_months', string_agg(DISTINCT deal_ymd, ',' ORDER BY deal_ymd) FROM mart.rent_listing
            UNION ALL SELECT 'rent_collected_date', max(collected_date)::text FROM wh.fact_rent
            UNION ALL SELECT 'commute_months', string_agg(DISTINCT m, ',' ORDER BY m)  -- 마트가 실제로 쓴 달 (결정 36)
                      FROM mart.commute_od, unnest(string_to_array(target_months, ',')) m
            UNION ALL SELECT 'congestion_collected_date', max(collected_date)::text FROM wh.fact_congestion
            UNION ALL SELECT 'store_collected_date', max(collected_date)::text FROM wh.fact_store
            UNION ALL SELECT 'boundary_base_date', max(boundary_base_date)::text FROM wh.dim_dong""").fetchall())
        manifest["quality"] = dict(conn.execute("""
            SELECT 'rent_quarantine_rows', count(*)::text FROM stg.rent_quarantine  -- fact_rent처럼 처리 단위마다 최신 수집분만
            WHERE (source_type, sgg_cd, deal_ymd, collected_date) IN (SELECT source_type, sgg_cd, deal_ymd, max(collected_date) FROM stg.rent GROUP BY 1, 2, 3)
            UNION ALL SELECT 'commute_quarantine_rows', count(*)::text FROM stg.living_migration_quarantine
            UNION ALL SELECT 'dongs_without_mois_code', count(*)::text FROM wh.dim_dong WHERE mois_dong_cd IS NULL
            UNION ALL SELECT 'commute_pairs_age20_34_basis', count(*)::text FROM mart.commute_od WHERE basis = 'age20_34'
            UNION ALL SELECT 'commute_pairs_all_ages_basis', count(*)::text FROM mart.commute_od WHERE basis = 'all_ages'
            UNION ALL SELECT 'dongs_without_monthly_rent_deal', count(*)::text FROM mart.dong_profile WHERE monthly_deals IS NULL""").fetchall())

    (out / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    for name, info in manifest["files"].items():
        print(f"{name}: {info['rows']:,}행")
    print(f"-> {out}")


if __name__ == "__main__":
    main()
