"""F-6 전수 탐색: dbt 모델 밖(Python, warehouse/sql)에서 SQL 글자로 표를 읽는 곳. 옛 표만 줄 번호와 함께 보인다 (결정 97).

실행: python -m measure.table_refs   (8절에서 옛 표를 지우기 전에 "옛 경로 밖 0"을 확인한다)
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OLD = {"stg.rent", "stg.rent_quarantine", "stg.congestion", "stg.congestion_quarantine", "stg.sbiz_store",
       "stg.sbiz_store_quarantine", "stg.bjd_code", "stg.living_migration", "stg.living_migration_quarantine"}
REF = re.compile(r"\b(raw|stg|stg_dbt|wh|mart|ops|lineage)\.([a-z_][a-z0-9_]*)\b")
files = [p for pat in ("**/*.py", "warehouse/sql/**/*.sql") for p in ROOT.glob(pat)
         if not any(x in p.parts for x in (".venv", "__pycache__", "dbt_packages", "target", "Claude outputs", "lakehouse"))]
pairs = set()
for f in files:
    rel = f.relative_to(ROOT).as_posix()
    if rel.startswith("warehouse/dbt/") or rel == "measure/table_refs.py":
        continue  # 모델 SQL은 리니지가 본다. 이 파일은 옛 표 목록을 글자로 가진다
    for i, line in enumerate(f.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
        for m in REF.finditer(line):
            t = f"{m[1]}.{m[2]}"
            if "." in m[2] or m[2] in {"py", "sql", "csv", "json", "md"}:
                continue
            pairs.add((rel, t, i if t in OLD else 0))
by_file = {}
for rel, t, i in pairs:
    by_file.setdefault((rel, t), []).append(i)
old = sorted(k for k in by_file if k[1] in OLD)
print(f"파일 {len({k[0] for k in by_file})}개, (파일, 표) 쌍 {len(by_file)}개, 그중 옛 표 {len(old)}개")
for rel, t in old:
    print(f"  {rel}  {t}  줄 {sorted(x for x in by_file[(rel, t)] if x)}")
