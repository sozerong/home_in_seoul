"""데이터 상품 버전 하나를 공개 정적 사이트(GitHub Pages) 폴더로 만든다 (결정 113, docs/handoff_pages.md 단계 P1).

입력: product/<버전>/ (읽기만 한다), docs/public_columns.json(컬럼별 공개 여부와 이용조건의 원본), docs/product_contract.json,
      docs/data_contract.md 3절(표의 grain과 컬럼 뜻), warehouse/site/index.html(데이터 포털 화면, 그대로 복사).
출력(--out, 사용자가 받아 둔 데이터 레포 폴더):
  <버전>/          표마다 공개 .parquet, .csv와 공개 manifest.json. 한 번 만든 공개 버전은 불변이다:
                   이미 있는 폴더와 바이트가 하나라도 다르면 아무것도 쓰지 않고 실패한다.
  index.html       데이터 포털(warehouse/site/index.html). 아래 JSON만 읽고, SQL·미리보기는 브라우저 안 DuckDB-WASM이
                   공개 Parquet를 직접 읽는다. 서버가 없다. <!--STATIC_SUMMARY--> 자리에 JavaScript 없이도 읽히는
                   평범한 HTML 요약(표, 파일 링크, 컬럼 사전)을 넣는다(결정 114)
  catalog.json     기계가 읽는 목록(DCAT 참고): 표 이름·뜻·이용 시 유의사항(notes), 컬럼과 쉬운 말 사전(label, help,
                   unit, values, example: docs/public_columns.json, 결정 115), 배포 파일, 버전, 조인 키, 이용조건
  profiles/<버전>.json  컬럼 프로필(결측, 고유값 수, 최소·최대·사분위, 히스토그램, 자주 나오는 값). 공개 버전 폴더는
                   불변이라 그 밖에 두고 매번 다시 만든다
  latest.json      최신 공개 버전을 가리키는 고정 주소
  NOTICE.md        원천별 출처·이용조건
  sql.html         예전 주소용. index.html#/sql로 넘긴다
  .nojekyll        Pages가 파일을 바꾸지 않고 그대로 내보내게 하는 빈 파일
  .gitattributes   git이 줄바꿈을 바꾸지 않게(* -text). 없으면 core.autocrlf가 CSV 바이트를 바꾼다
  위 파일들은 --out에 있는 공개 버전 폴더 전체로 매번 다시 만든다.
같은 입력이면 같은 바이트를 낸다: JSON 키 정렬, 새 시각을 찍지 않음(created_at은 원본 manifest 값), 행 순서는 원본 그대로.
CSV는 원본 CSV에서 빠진 컬럼만 지워 다시 쓴다(값의 글자 표기를 원본과 같게 두려고). Parquet는 pyarrow로 컬럼만 골라 다시 쓴다.
사용법:
  python -m warehouse.publish_site --version v2026.11.16 --out ../home_in_seoul-data
  python -m warehouse.publish_site --selftest [--version v2026.11.16]
  python -m warehouse.publish_site --site-only --out ../home_in_seoul-data            사이트 파일만(product/ 불필요, 배포 워크플로가 씀)
  python -m warehouse.publish_site --selftest --site-only --out ../home_in_seoul-data  product/ 없이 도는 확인(CI). --out에는 쓰지 않는다
"""
import argparse
import csv
import copy
import hashlib
import json
import math
import re
import shutil
import sys
import tempfile
from html import escape
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from common import ROOT
from warehouse.export_product import schema_changes

PRODUCT = ROOT / "product"
PUBLIC_COLUMNS = ROOT / "docs" / "public_columns.json"
CONTRACT = ROOT / "docs" / "product_contract.json"
DATA_CONTRACT = ROOT / "docs" / "data_contract.md"
SITE_HTML = ROOT / "warehouse" / "site" / "index.html"
VERSION_RE = re.compile(r"^v\d{4}\.\d{2}\.\d{2}$")
SUMMARY_MARK = "<!--STATIC_SUMMARY-->"
TITLES = {  # 포털에 보이는 표 이름. 뜻(grain)은 data_contract.md 3절에서 읽는다
    "dong_profile": "행정동 프로필",
    "commute_od": "출근 이동 (출발 행정동 × 직장 행정동)",
    "rent_listing": "전월세 계약",
    "dong_mois_map": "행정동 코드 대응표 (행정안전부 → 통계청)",
}
# 표끼리 잇는 키. 모두 통계청 행정동 코드 8자리(dong_id)로 dong_profile에 붙는다
RELATIONS = [
    {"from": ["commute_od", "origin_dong_id"], "to": ["dong_profile", "dong_id"], "label": "출발 행정동"},
    {"from": ["commute_od", "dest_dong_id"], "to": ["dong_profile", "dong_id"], "label": "직장 행정동"},
    {"from": ["rent_listing", "dong_id"], "to": ["dong_profile", "dong_id"], "label": "계약 주택이 있는 행정동"},
    {"from": ["dong_mois_map", "dong_id"], "to": ["dong_profile", "dong_id"], "label": "통계청 행정동"},
]
# 표마다 한 줄을 구분하는 컬럼(키). selftest가 이 버전에서 값이 비지 않고 겹치지 않는지 확인한다.
# rent_listing은 동·호를 주지 않아 거래 고유 키가 없다(CLAUDE.md 7절)
KEYS = {"dong_profile": ["dong_id"], "commute_od": ["origin_dong_id", "dest_dong_id"],
        "rent_listing": [], "dong_mois_map": ["mois_dong_cd"]}
REPOSITORY = "https://github.com/sozerong/home_in_seoul"  # 이 데이터를 만든 코드 레포(공개). 근거(source_ref)의 파일이 여기 있다
HIST_BINS = 12
TOP_VALUES = 8
INT_TYPES = ("smallint", "integer", "bigint")
# 데이터 상품 카드(handoff_delivery.md 1절)의 필수 항목. public_columns.json card에서 오고, 비면 만들지 않는다
CARD_KEYS = ("owner", "contact_url", "contact_label", "collection_cycle", "publication_cycle", "change_policy")


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def dumps(obj):
    return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def write_text(path, text):
    path.write_text(text, encoding="utf-8", newline="\n")


def check_public_columns(cfg, contract):
    """public_columns.json이 계약의 컬럼 집합과 정확히 같고, 키·이름 컬럼이 제외로 표시되지 않았는지 본다."""
    if set(cfg["tables"]) != set(contract):
        raise SystemExit(f"public_columns.json의 표 {sorted(cfg['tables'])}가 계약 {sorted(contract)}와 다릅니다")
    for table, cols in contract.items():
        want, got = [c for c, _ in cols], list(cfg["tables"][table])
        if sorted(want) != sorted(got):
            raise SystemExit(f"{table}: public_columns.json 컬럼이 계약과 다릅니다. 빠짐 {sorted(set(want) - set(got))}, "
                             f"남음 {sorted(set(got) - set(want))}")
        for col, v in cfg["tables"][table].items():
            if v.get("key") and not v["public"]:  # 키가 빠지면 상품을 못 쓴다: 빼지 않고 공개를 멈춘다(handoff 2절)
                raise SystemExit(f"{table}.{col}은 키·이름 컬럼인데 제외로 표시돼 공개를 멈춥니다")
            missing = [s for s in v["sources"] if s not in cfg["sources"]]
            if missing:
                raise SystemExit(f"{table}.{col}의 원천 {missing}이 public_columns.json sources에 없습니다")
            if v["public"] and not all(v.get(k) for k in ("label", "help", "source_ref")):
                raise SystemExit(f"{table}.{col}: 공개 컬럼인데 쉬운 말 사전(label, help, source_ref)이 비어 있습니다")
            if not v["public"] and not v.get("display_reason"):
                raise SystemExit(f"{table}.{col}: 비공개 컬럼인데 화면에 보일 사유(display_reason)가 비어 있습니다")
    if set(cfg.get("table_notes", {})) != set(contract) or not all(cfg["table_notes"].values()):
        raise SystemExit("public_columns.json table_notes에 표마다 '이용 시 유의사항'이 있어야 합니다")
    missing = [k for k in CARD_KEYS if not cfg.get("card", {}).get(k)]
    if missing:
        raise SystemExit(f"public_columns.json card(데이터 상품 카드)의 필수 항목이 비어 있습니다: {missing}")


def contract_docs():
    """data_contract.md 3절에서 표마다 grain(제목의 괄호)과 컬럼 뜻(표의 행)을 읽는다.
    행의 첫 칸은 'a, b, c' 또는 '*_per_km2', 'nearest_congestion_station_*' 같은 와일드카드다."""
    text = DATA_CONTRACT.read_text(encoding="utf-8")
    section = text[text.index("## 3. 파일별 스키마"):text.index("## 4. 품질")]
    docs = {}
    for block in re.split(r"^### ", section, flags=re.M)[1:]:
        m = re.match(r"(\w+) \((.*)\)\s*$", block.splitlines()[0])  # 괄호 안 괄호가 있어 바깥 괄호까지
        rows = []
        for line in block.splitlines():
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if line.startswith("|") and len(cells) == 2 and cells[0] not in ("컬럼", "---"):
                rows.append(([p.strip() for p in cells[0].split(",")], cells[1]))
        docs[m.group(1)] = {"grain": m.group(2), "rows": rows}
    return docs


def describe(docs, table, col):
    for names, meaning in docs[table]["rows"]:
        if any(n == col or ("*" in n and re.fullmatch(re.escape(n).replace(r"\*", ".*"), col)) for n in names):
            meaning = meaning.replace("**", "")
            return f"{meaning} (한 줄로 함께 설명: {', '.join(names)})" if len(names) > 1 else meaning
    raise SystemExit(f"data_contract.md 3절에 {table}.{col}의 뜻이 없습니다")


def build_version(src, dst, cfg, contract):
    """src(product 버전 폴더)에서 공개 파일과 공개 manifest를 dst에 쓴다."""
    manifest = json.loads((src / "manifest.json").read_text(encoding="utf-8"))
    for name, info in manifest["files"].items():  # 원본이 manifest와 다르면 무엇을 공개하는지 모른다
        if sha256(src / name) != info["sha256"]:
            raise SystemExit(f"{src / name}의 sha256이 원본 manifest와 다릅니다")
    if manifest["schema"] != contract:
        raise SystemExit(f"{src.name} manifest의 schema가 지금 계약(product_contract.json)과 다릅니다")
    dst.mkdir(parents=True)
    files, schema, excluded, column_sources = {}, {}, {}, {}
    for table, cols in contract.items():
        spec = cfg["tables"][table]
        keep = [c for c, _ in cols if spec[c]["public"]]
        schema[table] = [[c, t] for c, t in cols if spec[c]["public"]]
        excluded[table] = {c: spec[c]["reason"] for c, _ in cols if not spec[c]["public"]}
        column_sources[table] = {c: spec[c]["sources"] for c in keep}

        pq.write_table(pq.read_table(src / f"{table}.parquet", columns=keep), dst / f"{table}.parquet")
        with open(src / f"{table}.csv", encoding="utf-8-sig", newline="") as f_in, \
                open(dst / f"{table}.csv", "w", encoding="utf-8-sig", newline="") as f_out:
            reader, writer = csv.reader(f_in), csv.writer(f_out)
            header = next(reader)
            if header != [c for c, _ in cols]:
                raise SystemExit(f"{table}.csv 머리글이 계약과 다릅니다")
            idx = [header.index(c) for c in keep]
            writer.writerow(keep)
            rows = 0
            for row in reader:
                writer.writerow([row[i] for i in idx])
                rows += 1
        for ext in ("parquet", "csv"):
            p = dst / f"{table}.{ext}"
            n = pq.read_metadata(p).num_rows if ext == "parquet" else rows
            if n != manifest["files"][f"{table}.{ext}"]["rows"]:
                raise SystemExit(f"{p.name} 행 수 {n}가 원본 manifest와 다릅니다")
            files[p.name] = {"rows": n, "sha256": sha256(p), "bytes": p.stat().st_size}

    used = sorted({s for t in column_sources.values() for v in t.values() for s in v})
    public = {
        "version": manifest["version"],
        "created_at": manifest["created_at"],
        "source_manifest_sha256": sha256(src / "manifest.json"),
        "files": files,
        "schema": schema,
        "excluded_columns": excluded,
        "column_sources": column_sources,
        "licenses": {s: cfg["sources"][s] for s in used},
        "terms_checked_date": cfg["checked_date"],
        "sources": manifest["sources"],
        "quality": manifest["quality"],
    }
    write_text(dst / "manifest.json", dumps(public))


def publish_version(version, out, cfg, contract):
    """공개 버전 폴더를 만든다. 이미 있으면 새로 만든 것과 바이트가 같을 때만 그대로 두고, 다르면 실패한다(불변)."""
    src = PRODUCT / version
    if not VERSION_RE.match(version) or not (src / "manifest.json").exists():
        raise SystemExit(f"상품 버전 폴더가 없습니다: {src}")
    out.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp(prefix=f".{version}-", dir=out))
    try:
        build_version(src, tmp / version, cfg, contract)
        new = {p.name: sha256(p) for p in (tmp / version).iterdir()}
        dst = out / version
        if dst.exists():
            old = {p.name: sha256(p) for p in dst.iterdir()}
            if old != new:
                diff = sorted(n for n in new.keys() | old.keys() if old.get(n) != new.get(n))
                raise SystemExit(f"공개 버전 {version}이 이미 있고 내용이 다릅니다(공개 버전은 불변): {diff}")
            return False
        (tmp / version).rename(dst)
        return True
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _num(v):
    """JSON에 쓸 숫자. 플랫폼마다 마지막 자리가 달라지지 않게 유효숫자 10자리로 자른다."""
    if v is None:
        return None
    if isinstance(v, int) and not isinstance(v, bool):
        return v
    return float(f"{float(v):.10g}")


def profile_column(col):
    """컬럼 하나의 프로필. 모두 정확한 값(근사 없음)이라 같은 파일이면 같은 결과가 나온다."""
    n, nulls = len(col), col.null_count
    p = {"rows": n, "nulls": nulls, "distinct": pc.count_distinct(col, mode="only_valid").as_py()}
    t = col.type
    if pa.types.is_decimal(t):
        col, t = col.cast(pa.float64()), pa.float64()
    if (pa.types.is_integer(t) or pa.types.is_floating(t)) and n > nulls:
        mm = pc.min_max(col).as_py()
        q = pc.quantile(col, q=[0.25, 0.5, 0.75], interpolation="linear").to_pylist()
        p.update(kind="number", min=_num(mm["min"]), max=_num(mm["max"]), mean=_num(pc.mean(col).as_py()),
                 q25=_num(q[0]), median=_num(q[1]), q75=_num(q[2]))
        lo, hi = float(mm["min"]), float(mm["max"])
        counts = [0] * HIST_BINS
        width = (hi - lo) / HIST_BINS or 1.0
        for v in col.drop_null().to_pylist():
            counts[min(int((float(v) - lo) / width), HIST_BINS - 1)] += 1
        p["histogram"] = {"min": _num(lo), "width": _num(width), "counts": counts}
        return p
    if pa.types.is_temporal(t) and n > nulls:
        mm = pc.min_max(col).as_py()
        p.update(kind="date", min=str(mm["min"]), max=str(mm["max"]))
    else:
        p["kind"] = "text"
    vc = pc.value_counts(col.drop_null()).to_pylist()
    vc.sort(key=lambda x: (-x["counts"], str(x["values"])))
    p["top"] = [[str(x["values"]), x["counts"]] for x in vc[:TOP_VALUES]]
    return p


def profile_version(folder, schema):
    return {table: {c: profile_column(pq.read_table(folder / f"{table}.parquet", columns=[c]).column(c).combine_chunks())
                    for c, _ in cols} for table, cols in schema.items()}


def example(pr, col_type):
    """프로필에서 고른 실제 값 하나: 숫자는 가운데 값, 날짜는 처음 날, 글자는 가장 자주 나오는 값.
    정수 컬럼은 소수점 없이 쓴다(handoff_delivery.md P5)."""
    if pr.get("kind") == "number":
        # ponytail: 정수 컬럼의 가운데 값이 x.5면 아래로 내린다(예시값이라 반올림 방향은 상관없고, 있는 정수 쪽으로)
        return str(math.floor(pr["median"])) if col_type in INT_TYPES else str(pr["median"])
    if pr.get("kind") == "date":
        return pr["min"]
    return pr["top"][0][0] if pr.get("top") else ""


def column_entry(docs, table, c, t, spec, sources, pr):
    e = {"name": c, "type": t, "description": describe(docs, table, c), "sources": sources,
         "label": spec["label"], "help": spec["help"], "unit": spec["unit"], "source_ref": spec["source_ref"],
         "example": example(pr, t)}
    if "values" in spec:
        e["values"] = spec["values"]
    return e


def _diff(a, b):
    return {k: [a.get(k), b.get(k)] for k in sorted(a.keys() | b.keys()) if a.get(k) != b.get(k)}


def version_changes(prev, cur, marks):
    """직전 공개 버전(prev manifest) 대비 cur에서 바뀐 것(handoff_delivery.md P3, 변경 정책 3번).
    공개 파일에서 빠진 컬럼과 형이 바뀐 컬럼은 public_columns.json column_changes[버전]에 사유가 있어야 하고,
    표시와 실제가 하나라도 다르면 만들지 않는다. 컬럼 추가, 행 수, 품질 지표, 출처 기준일 변화는 표시 없이 싣는다."""
    # 빈 목록을 깔아 표 하나가 통째로 빠져도 그 컬럼이 모두 removed로 잡힌다(schema_changes는 새 버전의 표만 돈다)
    sc = schema_changes(prev["schema"], {**{t: [] for t in prev["schema"]}, **cur["schema"]})
    found = {"removed": [f"{t}.{c}" for t, x in sc.items() for c in x["removed"]],
             "changed": [f"{t}.{k}" for t, x in sc.items() for k, _, _ in x["type_changed"]]}
    for kind, cols in found.items():
        have = marks.get(kind, {})
        if set(cols) != set(have) or not all(have.values()):
            raise SystemExit(f"{cur['version']}: 직전 공개 버전 {prev['version']} 대비 {kind} 컬럼 {sorted(cols)}와 "
                             f"public_columns.json column_changes 의도 표시 {sorted(have)}가 다릅니다. 의도한 변경이면 사유와 함께 표시하세요")
    rows = lambda m: {n.removesuffix(".parquet"): i["rows"] for n, i in m["files"].items() if n.endswith(".parquet")}
    return {"previous": prev["version"],
            "added": [f"{t}.{c}" for t, x in sc.items() for c in x["added"]],
            "removed": {c: marks["removed"][c] for c in found["removed"]},
            "type_changed": [{"column": f"{t}.{k}", "from": a, "to": b, "reason": marks["changed"][f"{t}.{k}"]}
                             for t, x in sc.items() for k, a, b in x["type_changed"]],
            "rows": _diff(rows(prev), rows(cur)),
            "quality": _diff(prev["quality"], cur["quality"]),
            "sources": _diff(prev["sources"], cur["sources"])}


def static_card(catalog, m):
    """데이터 상품 카드(handoff_delivery.md 1절)의 평범한 HTML. 같은 입력이면 같은 바이트라 '며칠 전' 같은 계산은 넣지 않는다."""
    c = catalog["card"]
    ch = catalog["versions"][0]["changes"]
    change = ("첫 공개 버전이라 비교할 직전 버전이 없습니다." if ch is None else
              f"직전 공개 버전 {ch['previous']} 대비: 추가 컬럼 {len(ch['added'])}개, 빠진 컬럼 {len(ch['removed'])}개, "
              f"형 변경 {len(ch['type_changed'])}개, 행 수가 바뀐 테이블 {len(ch['rows'])}개.")
    return ('<section><h2>데이터 상품 카드</h2><ul>'
            f'<li>주인: {escape(c["owner"])}. 문의·오류 신고: <a href="{escape(c["contact_url"])}">{escape(c["contact_label"])}</a></li>'
            f'<li>수집 주기: {escape(c["collection_cycle"])}</li><li>공개 주기: {escape(c["publication_cycle"])}</li>'
            f'<li>마지막 공개: {escape(catalog["latest"])} ({escape(m["created_at"])}). {escape(change)}</li>'
            '<li>변경 정책:<ol>' + "".join(f"<li>{escape(x)}</li>" for x in c["change_policy"]) + "</ol></li></ul></section>")


def static_summary(catalog, m):
    """JavaScript를 돌리지 않는 도구(글만 읽는 에이전트, 검색엔진)용 평범한 HTML. index.html의 <main>에 들어가고,
    JavaScript가 돌면 포털 화면이 덮어쓴다. catalog와 공개 manifest의 값만 쓴다(비공개 컬럼은 이름과 이유만)."""
    h = [f"<h1>{escape(catalog['title'])}</h1>", f"<p>{escape(catalog['description'])}</p>",
         f"<p>최신 버전 <b>{escape(catalog['latest'])}</b> (만든 시각 {escape(m['created_at'])}). "
         "공개한 버전 폴더의 파일은 수정하지 않습니다. 기계 판독용 데이터 목록은 "
         '<a href="catalog.json">catalog.json</a>, 최신 버전 주소는 <a href="latest.json">latest.json</a>, '
         '출처와 이용조건은 <a href="NOTICE.md">NOTICE.md</a>에 있습니다. JavaScript를 켜면 테이블 탐색과 SQL 편집기를 쓸 수 있습니다.</p>',
         static_card(catalog, m),
         "<p>모든 테이블은 통계청 행정동 코드 8자리(dong_id)로 dong_profile과 조인합니다. 같은 이름의 행정동이 있으므로(신사동: 강남구·관악구) "
         "행정동 이름으로 조인하지 마세요.</p>"]
    for d in catalog["datasets"]:
        files = [x for x in d["distributions"] if x["version"] == catalog["latest"]]
        grain = re.sub(r", [\d,]+행$", "", d["grain"].removeprefix("한 행 = "))  # index.html grainText와 같은 규칙
        h.append(f'<section><h2>{escape(d["title"])} ({escape(d["identifier"])})</h2><p>행 단위: {escape(grain)}</p>')
        h.append("<ul>" + "".join(f'<li><a href="{escape(x["url"])}">{escape(x["url"])}</a> ({x["format"]}, {x["rows"]}행, '
                                  f'{x["bytes"]}바이트, sha256 {x["sha256"]})</li>' for x in files) + "</ul>")
        key = ", ".join(d["primary_key"]) or "없음(출처가 동·호수를 제공하지 않아 거래 고유 키가 없음)"
        h.append(f"<p>기본 키: {escape(key)}</p>")
        h.append("<p>이용 시 유의사항:</p><ul>" + "".join(f"<li>{escape(n['text'])}</li>" for n in d["notes"]) + "</ul>")
        h.append("<table><tr><th>컬럼</th><th>한글 이름</th><th>단위</th><th>타입</th><th>설명</th><th>코드 값</th></tr>")
        for c in d["columns"]:
            vals = "; ".join(f"{k} = {v}" for k, v in c.get("values", {}).items())
            h.append(f"<tr><td>{escape(c['name'])}</td><td>{escape(c['label'])}</td><td>{escape(c['unit'])}</td>"
                     f"<td>{escape(c['type'])}</td><td>{escape(c['help'])}</td><td>{escape(vals)}</td></tr>")
        h.append("</table>")
        if d["excluded_columns"]:
            h.append('<p>비공개 컬럼(출처의 이용조건에 따라 제외, 값 미공개):</p><ul class="excluded">' + "".join(
                f"<li>{escape(c)}: {escape(r)}</li>" for c, r in d["excluded_columns"].items()) + "</ul>")
        h.append("</section>")
    h.append(f'<p>설명의 근거 문서(data_contract.md, decisions.md 등)는 코드 저장소 <a href="{REPOSITORY}">{REPOSITORY}</a>에 있습니다.</p>')
    return "\n".join(h)


def site_files(out, cfg, contract):
    """--out의 공개 버전 폴더 전체로 index.html, catalog.json, latest.json, NOTICE.md, sql.html, profiles/를 다시 쓴다."""
    versions = sorted(p.name for p in out.iterdir() if p.is_dir() and VERSION_RE.match(p.name))
    if not versions:
        raise SystemExit(f"{out}에 공개 버전 폴더가 없습니다")
    manifests = {v: json.loads((out / v / "manifest.json").read_text(encoding="utf-8")) for v in versions}
    latest = versions[-1]
    m = manifests[latest]
    docs = contract_docs()
    marks = cfg.get("column_changes", {})
    stray = sorted(set(marks) - set(versions[1:]))  # 첫 버전은 비교 대상이 없어 표시할 수 없다
    if stray:
        raise SystemExit(f"public_columns.json column_changes에 직전 버전이 있는 공개 버전이 아닌 키가 있습니다: {stray}")
    changes = {v: version_changes(manifests[p], manifests[v], marks.get(v, {})) for p, v in zip(versions, versions[1:])}

    write_text(out / "latest.json", dumps({"version": latest, "manifest": f"{latest}/manifest.json"}))

    profiles = {v: profile_version(out / v, manifests[v]["schema"]) for v in versions}
    (out / "profiles").mkdir(exist_ok=True)
    for v in versions:
        write_text(out / "profiles" / f"{v}.json", dumps(profiles[v]))

    # 화면용 출처 이름·문구. 공개 manifest의 licenses 원문은 그대로 두고 source_display만 덮어쓴다
    licenses = {k: {**s, **cfg.get("source_display", {}).get(k, {})} for k, s in m["licenses"].items()}
    datasets = []
    for table in contract:
        datasets.append({
            "identifier": table,
            "title": TITLES[table],
            "grain": docs[table]["grain"],
            "notes": cfg["table_notes"][table],
            "primary_key": KEYS[table],
            "columns": [column_entry(docs, table, c, t, cfg["tables"][table][c], m["column_sources"][table][c],
                                     profiles[latest][table][c]) for c, t in m["schema"][table]],
            # 공개 manifest의 사유 원문은 바꿀 수 없어(불변) 화면에는 public_columns.json의 display_reason을 쓴다
            "excluded_columns": {c: cfg["tables"][table][c]["display_reason"] for c in m["excluded_columns"][table]},
            "distributions": [{"version": v, "format": ext, "url": f"{v}/{table}.{ext}",
                               **manifests[v]["files"][f"{table}.{ext}"]}
                              for v in reversed(versions) for ext in ("parquet", "csv")],
        })
    catalog = {
        "title": "서울 행정동 주거 지표",
        "description": "서울 426개 행정동의 전월세 시세, 출근 이동 시간, 가장 가까운 역, 생활 편의 업소 수를 공공데이터로 집계한 데이터입니다.",
        "latest": latest,
        "versions": [{"version": v, "manifest": f"{v}/manifest.json", "created_at": manifests[v]["created_at"],
                      "sources": manifests[v]["sources"], "changes": changes.get(v)} for v in reversed(versions)],
        "card": cfg["card"],
        "datasets": datasets,
        "relations": RELATIONS,
        "profiles": {v: f"profiles/{v}.json" for v in reversed(versions)},
        "licenses": licenses,
        "terms_checked_date": m["terms_checked_date"],
        "notice": "NOTICE.md",
        "legal_note": cfg["note"],
        "repository": REPOSITORY,
    }
    write_text(out / "catalog.json", dumps(catalog))

    notice = ["# 출처와 이용조건", "",
              f"확인일: {cfg['checked_date']}. {cfg['note']}. 공공기관이 이 데이터를 후원하거나 보증하지 않습니다.", ""]
    for key, s in sorted(licenses.items()):
        notice += [f"## {s['name']}", f"- 이용조건: {s['license']}"]
        notice += [f"- 출처 표시 문구: {s['attribution']}"] if s["attribution"] else []
        notice += [f"- 링크: {u}" for u in s["urls"]] + [""]
    notice += ["## 비공개 컬럼", "", "출처의 이용조건에 따라 공개 파일에서 제외한 컬럼입니다.", ""]
    notice += [f"- {d['identifier']}.{c}: {r}" for d in datasets for c, r in d["excluded_columns"].items()]
    write_text(out / "NOTICE.md", "\n".join(notice) + "\n")

    if not SITE_HTML.exists():
        raise SystemExit(f"{SITE_HTML}이 없습니다")
    html = SITE_HTML.read_text(encoding="utf-8")
    if html.count(SUMMARY_MARK) != 1:
        raise SystemExit(f"{SITE_HTML}에 {SUMMARY_MARK}가 정확히 한 번 있어야 합니다")
    write_text(out / "index.html", html.replace(SUMMARY_MARK, static_summary(catalog, m)))
    write_text(out / "sql.html", '<!doctype html><meta charset="utf-8"><title>SQL 편집기로 이동</title>'
               '<script>location.replace("index.html#/sql")</script><a href="index.html#/sql">SQL 편집기로 이동</a>\n')
    write_text(out / ".nojekyll", "")  # Pages가 Jekyll로 NOTICE.md 등을 바꾸지 않고 파일 그대로 내보내게
    # git이 줄바꿈을 바꾸지 않게. Git for Windows 기본 core.autocrlf=true는 CSV의 CRLF를 LF로 커밋해
    # 공개 파일 sha256이 manifest와 달라진다(2026-11-22 첫 푸시에서 실제로 일어남)
    write_text(out / ".gitattributes", "* -text\n")


def publish(version, out, cfg=None):
    cfg = cfg or json.loads(PUBLIC_COLUMNS.read_text(encoding="utf-8"))
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    check_public_columns(cfg, contract)
    created = publish_version(version, out, cfg, contract)
    site_files(out, cfg, contract)
    return created


def tree_hashes(root):
    return {str(p.relative_to(root)).replace("\\", "/"): sha256(p) for p in sorted(root.rglob("*")) if p.is_file()}


def check_versions(out):
    """공개 버전 폴더마다 파일 목록과 sha256이 그 폴더의 공개 manifest와 같은지 본다. 다르면 실패한다.
    manifest.json 자체가 바뀐 것은 여기서 못 잡는다. 그것은 배포 워크플로의 git diff 검사가 잡는다."""
    versions = sorted(p.name for p in out.iterdir() if p.is_dir() and VERSION_RE.match(p.name))
    if not versions:
        raise SystemExit(f"{out}에 공개 버전 폴더가 없습니다")
    for v in versions:
        files = json.loads((out / v / "manifest.json").read_text(encoding="utf-8"))["files"]
        have = {p.name for p in (out / v).iterdir()}
        if have != set(files) | {"manifest.json"}:
            raise SystemExit(f"공개 버전 {v}의 파일 목록이 manifest와 다릅니다(공개 버전은 불변): "
                             f"{sorted(have ^ (set(files) | {'manifest.json'}))}")
        bad = sorted(n for n, info in files.items() if sha256(out / v / n) != info["sha256"])
        if bad:
            raise SystemExit(f"공개 버전 {v}의 파일 sha256이 manifest와 다릅니다(공개 버전은 불변): {bad}")
    return versions


def check_static_summary(out):
    """index.html 정적 요약(결정 114)에 최신 버전 공개 파일 링크가 모두 있고, 비공개 컬럼은 이름·이유 목록에만 나오는지 본다."""
    cat = json.loads((out / "catalog.json").read_text(encoding="utf-8"))
    version = cat["latest"]
    m = json.loads((out / version / "manifest.json").read_text(encoding="utf-8"))
    page = (out / "index.html").read_text(encoding="utf-8")
    if SUMMARY_MARK in page:
        raise SystemExit("index.html에 정적 요약 자리표시가 남아 있습니다")
    summary = page[page.index('<main id="main">'):page.index("</main>")]
    for name in m["files"]:
        if name.endswith((".parquet", ".csv")) and f'href="{version}/{name}"' not in summary:
            raise SystemExit(f"정적 요약에 {name} 링크가 없습니다")
    listed = "".join(re.findall(r'<ul class="excluded">.*?</ul>', summary, flags=re.S))
    rest = re.sub(r'<ul class="excluded">.*?</ul>', "", summary, flags=re.S)
    for ds in cat["datasets"]:
        for c in ds["excluded_columns"]:
            if c not in listed:
                raise SystemExit(f"정적 요약의 비공개 컬럼 목록에 {c}가 없습니다")
            if re.search(rf"\b{re.escape(c)}\b", rest):
                raise SystemExit(f"정적 요약에 비공개 컬럼 {c}가 목록 밖에 나옵니다")


def publish_site_only(out):
    """product/ 없이 --out의 기존 공개 버전 폴더로 사이트 파일만 다시 만든다(배포 워크플로가 쓰는 모드, handoff_deploy.md D1).
    공개 버전 폴더는 읽기만 한다: 쓰기 전에 manifest와 맞는지 보고(틀리면 아무것도 쓰지 않음), 쓴 뒤 바이트가 그대로인지 다시 본다."""
    cfg = json.loads(PUBLIC_COLUMNS.read_text(encoding="utf-8"))
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    check_public_columns(cfg, contract)
    versions = check_versions(out)
    before = {v: tree_hashes(out / v) for v in versions}
    site_files(out, cfg, contract)
    if {v: tree_hashes(out / v) for v in versions} != before:
        raise SystemExit("사이트 파일을 만드는 중 공개 버전 폴더가 바뀌었습니다")
    check_static_summary(out)
    return versions


def _next_version(src_dir, dst_dir, table, col):
    """시험용: src_dir 공개 버전을 복사하고 table.col 하나만 뺀 다음 버전을 dst_dir에 만든다(manifest도 맞춘다)."""
    shutil.copytree(src_dir, dst_dir)
    m = json.loads((dst_dir / "manifest.json").read_text(encoding="utf-8"))
    m["version"] = dst_dir.name
    m["schema"][table] = [x for x in m["schema"][table] if x[0] != col]
    m["column_sources"][table].pop(col)
    pq.write_table(pq.read_table(dst_dir / f"{table}.parquet").drop_columns([col]), dst_dir / f"{table}.parquet")
    with open(src_dir / f"{table}.csv", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f))
    i = rows[0].index(col)
    with open(dst_dir / f"{table}.csv", "w", encoding="utf-8-sig", newline="") as f:
        csv.writer(f).writerows([r[:i] + r[i + 1:] for r in rows])
    for ext in ("parquet", "csv"):
        p = dst_dir / f"{table}.{ext}"
        m["files"][p.name].update(sha256=sha256(p), bytes=p.stat().st_size)
    write_text(dst_dir / "manifest.json", dumps(m))


def selftest_site(src):
    """product/ 없이 도는 확인(CI용). src(데이터 레포 폴더)의 공개 버전 폴더를 임시 폴더로 복사해 쓰고, src에는 쓰지 않는다."""
    src = src.resolve()
    versions = sorted(p.name for p in src.iterdir() if p.is_dir() and VERSION_RE.match(p.name))
    if not versions:
        raise SystemExit(f"{src}에 공개 버전 폴더가 없습니다")
    original = {v: tree_hashes(src / v) for v in versions}
    with tempfile.TemporaryDirectory() as d:
        copies = []
        for name in ("a", "b", "c"):
            for v in versions:
                shutil.copytree(src / v, Path(d) / name / v)
            copies.append(Path(d) / name)
        a, b, c = copies
        publish_site_only(a)
        publish_site_only(b)
        ha, hb = tree_hashes(a), tree_hashes(b)
        assert ha == hb, f"두 번 만든 사이트의 바이트가 다릅니다: {sorted(k for k in ha.keys() | hb.keys() if ha.get(k) != hb.get(k))}"
        assert {v: tree_hashes(a / v) for v in versions} == original, "공개 버전 폴더가 바뀌었습니다"
        print(f"S1. 사이트만 두 번 만든 결과가 같다(파일 {len(ha)}개), 공개 버전 폴더 {len(versions)}개는 원본과 같은 바이트")
        print("S2. 정적 요약에 공개 파일 링크가 모두 있고 비공개 컬럼은 이름·이유 목록에만 나온다(만들 때마다 검사)")

        page = a / "index.html"
        good = page.read_text(encoding="utf-8")
        cat = json.loads((a / "catalog.json").read_text(encoding="utf-8"))
        hidden = next(col for ds in cat["datasets"] for col in ds["excluded_columns"])
        write_text(page, good.replace("</main>", f"<p>{hidden}</p></main>", 1))
        try:
            check_static_summary(a)
            raise AssertionError("목록 밖 비공개 컬럼을 잡지 못했습니다")
        except SystemExit as err:
            assert "목록 밖" in str(err), err
        write_text(page, good)
        print(f"S3. 정적 요약 목록 밖에 비공개 컬럼({hidden})을 넣으면 실패한다")

        victim = c / versions[-1] / "dong_profile.csv"
        victim.write_bytes(victim.read_bytes() + b"x")
        before = tree_hashes(c)
        try:
            publish_site_only(c)
            raise AssertionError("바뀐 공개 버전 폴더로 사이트를 만들었습니다")
        except SystemExit as err:
            assert "불변" in str(err), err
        assert tree_hashes(c) == before, "실패했는데 파일을 썼습니다"
        print("S4. 공개 버전 파일 하나가 manifest와 다르면 아무것도 쓰지 않고 실패한다")

        cfg = json.loads(PUBLIC_COLUMNS.read_text(encoding="utf-8"))
        contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
        e, nxt, gone = Path(d) / "e", "v2099.12.31", "dong_profile.laundry"
        for v in versions:
            shutil.copytree(src / v, e / v)
        _next_version(e / versions[-1], e / nxt, *gone.split("."))
        before = tree_hashes(e)
        for marks, word in [({}, "의도 표시"), ({nxt: {"removed": {gone: ""}}}, "의도 표시"),
                            ({nxt: {"removed": {gone: "시험"}}, versions[0]: {"removed": {gone: "시험"}}}, "아닌 키")]:
            try:
                site_files(e, {**cfg, "column_changes": marks}, contract)
                raise AssertionError(f"잘못된 column_changes {marks}가 통과했습니다")
            except SystemExit as err:
                assert word in str(err), err
        assert tree_hashes(e) == before, "실패했는데 파일을 썼습니다"
        site_files(e, {**cfg, "column_changes": {nxt: {"removed": {gone: "시험: 의도한 삭제"}}}}, contract)
        ch = json.loads((e / "catalog.json").read_text(encoding="utf-8"))["versions"][0]["changes"]
        assert ch["previous"] == versions[-1] and ch["removed"] == {gone: "시험: 의도한 삭제"}, ch
        assert not ch["added"] and not ch["type_changed"] and not ch["rows"], ch
        print(f"S5. 컬럼 하나({gone})를 뺀 다음 버전은 의도 표시가 없거나 사유가 비면 아무것도 쓰지 않고 실패하고, "
              "표시하면 catalog 변경 기록에 사유와 함께 실린다. 없는 버전에 단 표시도 실패")

    for mutate, word in [(lambda x: next(t for t in x["tables"].values() if col in t)[col].pop("display_reason"), "display_reason"),
                         (lambda x: x["card"].pop("contact_url"), "card"),
                         (lambda x: x["card"].update(change_policy=[]), "card")]:
        bad = copy.deepcopy(cfg)
        col = next(c for t in bad["tables"].values() for c, v in t.items() if not v["public"])
        mutate(bad)
        try:
            check_public_columns(bad, contract)
            raise AssertionError(f"잘못된 public_columns.json이 통과했습니다: {word}")
        except SystemExit as err:
            assert word in str(err), err
    assert {v: tree_hashes(src / v) for v in versions} == original
    print(f"S6. 비공개 컬럼마다 display_reason이, 데이터 상품 카드에는 필수 항목({', '.join(CARD_KEYS)})이 있어야 한다. "
          f"원본 폴더 {src}는 그대로")
    print("selftest(사이트만) 통과")


def selftest(version):
    cfg = json.loads(PUBLIC_COLUMNS.read_text(encoding="utf-8"))
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    with tempfile.TemporaryDirectory() as d:
        a, b = Path(d) / "a", Path(d) / "b"
        assert publish(version, a) and publish(version, b)
        ha, hb = tree_hashes(a), tree_hashes(b)
        assert ha == hb, "두 번 만든 결과의 바이트가 다릅니다"
        print(f"1. 두 번 만든 결과가 같다: 파일 {len(ha)}개")

        m = json.loads((a / version / "manifest.json").read_text(encoding="utf-8"))
        for name, info in m["files"].items():
            assert sha256(a / version / name) == info["sha256"], name
        print(f"2. 공개 manifest sha256 = 실제 파일 ({len(m['files'])}개)")
        prof = json.loads((a / "profiles" / f"{version}.json").read_text(encoding="utf-8"))
        for table, cols in m["schema"].items():
            assert [c for c, _ in cols] == list(prof[table]) or sorted(c for c, _ in cols) == sorted(prof[table]), table
            for c, pr in prof[table].items():
                assert pr["rows"] == m["files"][f"{table}.parquet"]["rows"], (table, c)
                assert "histogram" not in pr or sum(pr["histogram"]["counts"]) == pr["rows"] - pr["nulls"], (table, c)
        print("2b. 프로필이 공개 컬럼마다 있고 행 수·히스토그램 합이 맞다")

        cat = json.loads((a / "catalog.json").read_text(encoding="utf-8"))
        n_vals = 0
        for ds in cat["datasets"]:
            for c in ds["columns"]:
                assert c["label"] and c["help"] and c["source_ref"], (ds["identifier"], c["name"])
                if "values" not in c:
                    continue
                col = pq.read_table(a / version / f"{ds['identifier']}.parquet", columns=[c["name"]]).column(0)
                seen = {str(v).lower() for v in pc.unique(col.drop_null()).to_pylist()}
                keys = {k.lower() for k in c["values"]}
                assert keys <= seen, f"{ds['identifier']}.{c['name']}: 이 버전에 없는 코드를 풀이합니다 {sorted(keys - seen)}"
                assert seen <= keys, f"{ds['identifier']}.{c['name']}: 풀이가 빠진 코드 {sorted(seen - keys)}"
                n_vals += 1
            assert ds["notes"], ds["identifier"]
            assert ds["grain"].count("(") == ds["grain"].count(")"), f"{ds['identifier']} grain 괄호가 잘렸습니다: {ds['grain']}"
            k = ds["primary_key"]
            if k:
                t = pq.read_table(a / version / f"{ds['identifier']}.parquet", columns=k)
                assert all(t.column(x).null_count == 0 for x in k), (ds["identifier"], k)
                assert t.group_by(k).aggregate([]).num_rows == t.num_rows, f"{ds['identifier']} 키 {k}가 겹칩니다"
        print(f"2c. 공개 컬럼 {sum(len(ds['columns']) for ds in cat['datasets'])}개 모두 한글 이름·설명·근거가 있고, "
              f"코드 풀이 {n_vals}개가 이 버전 실제 값과 빠짐없이 맞고, 표 키가 비지 않고 겹치지 않는다")

        check_static_summary(a)
        print("2d. index.html 정적 요약에 공개 파일 링크가 모두 있고, 비공개 컬럼은 이름·이유 목록에만 나온다")

        n = 0
        for table, cols in cfg["tables"].items():
            gone = {c for c, v in cols.items() if not v["public"]}
            pcols = set(pq.read_schema(a / version / f"{table}.parquet").names)
            with open(a / version / f"{table}.csv", encoding="utf-8-sig", newline="") as f:
                ccols = set(next(csv.reader(f)))
            assert not (gone & pcols) and not (gone & ccols), (table, gone & (pcols | ccols))
            assert pcols == ccols == {c for c, v in cols.items() if v["public"]}, table
            keep = [c for c, _ in contract[table] if cols[c]["public"]]
            assert pq.read_table(a / version / f"{table}.parquet").equals(
                pq.read_table(PRODUCT / version / f"{table}.parquet", columns=keep)), f"{table}.parquet 값·타입이 원본과 다릅니다"
            n += len(gone)
        print(f"3. 제외 컬럼 {n}개가 parquet·csv 어디에도 없고, 남은 parquet 값·타입은 원본과 같다")

        assert not publish(version, a), "같은 내용인데 새로 만들었다고 했습니다"
        victim = a / version / "dong_profile.csv"
        victim.write_bytes(victim.read_bytes() + b"x")
        try:
            publish(version, a)
            raise AssertionError("바뀐 공개 버전을 덮어썼습니다")
        except SystemExit as err:
            assert "불변" in str(err), err
        print("4. 같은 내용 재실행은 그대로, 바이트가 다른 기존 공개 버전은 실패")

        all_cols = copy.deepcopy(cfg)  # 모든 컬럼 공개로 돌리면 CSV가 원본과 바이트까지 같아야 한다(값 표기를 바꾸지 않음)
        for cols in all_cols["tables"].values():
            for v in cols.values():
                v["public"] = True
        full = Path(d) / "full"
        publish_version(version, full, all_cols, contract)
        orig = json.loads((PRODUCT / version / "manifest.json").read_text(encoding="utf-8"))["files"]
        for table in contract:
            assert sha256(full / version / f"{table}.csv") == orig[f"{table}.csv"]["sha256"], table
        print("5. 모든 컬럼을 공개로 두면 CSV가 원본과 같은 바이트")
        selftest_site(b)  # product/ 없이 도는 사이트만 모드(S1~S6). a는 4번에서 일부러 바꿨으므로 b로

    check_public_columns(cfg, contract)
    for mutate, word in [(lambda c: c["tables"]["dong_profile"]["dong_id"].update(public=False), "키·이름"),
                         (lambda c: c["tables"]["dong_profile"].pop("laundry"), "계약과 다릅니다"),
                         (lambda c: c["tables"]["rent_listing"].update(extra={"public": True, "sources": [], "reason": ""}),
                          "계약과 다릅니다"),
                         (lambda c: c["tables"]["commute_od"]["avg_minutes"].pop("help"), "쉬운 말 사전"),
                         (lambda c: c["table_notes"].pop("commute_od"), "table_notes"),
                         (lambda c: c["tables"]["dong_profile"]["area_km2"].pop("display_reason"), "display_reason")]:
        bad = copy.deepcopy(cfg)
        mutate(bad)
        try:
            check_public_columns(bad, contract)
            raise AssertionError(f"잘못된 public_columns.json이 통과했습니다: {word}")
        except SystemExit as err:
            assert word in str(err), err
    print("6. public_columns.json = 계약 컬럼 집합, 키 제외·컬럼 빠짐·남음·사전 빠짐·notes 빠짐·비공개 사유 빠짐은 실패")

    docs = contract_docs()
    for table, cols in contract.items():
        for c, _ in cols:
            describe(docs, table, c)
    print("7. 모든 컬럼의 뜻이 data_contract.md 3절에 있다")
    print("selftest 통과")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--version")
    p.add_argument("--out", type=Path)
    p.add_argument("--selftest", action="store_true")
    p.add_argument("--site-only", action="store_true", help="product/ 없이 --out의 공개 버전 폴더로 사이트 파일만 다시 만든다")
    a = p.parse_args()
    if a.site_only:
        if not a.out:
            p.error("--site-only에는 --out이 필요합니다")
        if a.selftest:
            return selftest_site(a.out)
        versions = publish_site_only(a.out.resolve())
        print(f"사이트 파일만 다시 만듦(공개 버전 {len(versions)}개, 최신 {versions[-1]}) -> {a.out.resolve()}")
        return
    if a.selftest:
        return selftest(a.version or sorted(x.parent.name for x in PRODUCT.glob("v*/manifest.json"))[-1])
    if not (a.version and a.out):
        p.error("--version과 --out이 필요합니다")
    created = publish(a.version, a.out.resolve())
    print(f"{a.version}: {'새로 만듦' if created else '이미 같은 내용이 있어 그대로 둠'} -> {a.out.resolve()}")


if __name__ == "__main__":
    sys.exit(main())
