"""6절 체크포인트(S4 재측정): 상품 계약 검사가 스키마 변경을 막고, 의도한 변경은 깨는 변경으로 적는지 확인한다 (결정 83).

1. 계약 파일만 바꾼 사본(dong_mois_map.share -> share_ratio)을 CONTRACT로 두고 export_product.main()을 부른다.
   실제 마트와 계약이 다르므로 아무 파일도 쓰기 전에 SystemExit가 나야 한다(마트를 고친 것과 같은 상황을 거꾸로 만든다).
2. 같은 변경을 '의도한 변경'으로 보고 schema_changes(지금 계약, 바뀐 스키마)가 깨는 변경으로 적는지 본다.
실제 마트, 계약 파일, product/ 폴더는 바꾸지 않는다.
실행(Airflow 컨테이너): python -m measure.contract_check
"""
import copy
import datetime
import json
import tempfile
from pathlib import Path

from common import ROOT
from warehouse import export_product as E


def main():
    contract = json.loads(E.CONTRACT.read_text(encoding="utf-8"))
    changed = copy.deepcopy(contract)
    changed["dong_mois_map"] = [["share_ratio" if c == "share" else c, t] for c, t in changed["dong_mois_map"]]
    today = ROOT / "product" / datetime.date.today().strftime("v%Y.%m.%d")
    before = sorted((p.name, p.stat().st_mtime_ns) for p in today.glob("*")) if today.exists() else None
    with tempfile.TemporaryDirectory() as tmp:
        fake = Path(tmp) / "product_contract.json"
        fake.write_text(json.dumps(changed, ensure_ascii=False), encoding="utf-8")
        E.CONTRACT = fake
        try:
            E.main()
            raise AssertionError("계약과 다른데 내보내기가 통과했습니다")
        except SystemExit as e:
            print("1. 내보내기 실패(기대대로):", str(e)[:300])
    after = sorted((p.name, p.stat().st_mtime_ns) for p in today.glob("*")) if today.exists() else None
    assert before == after, "실패했는데 오늘 버전 폴더가 바뀌었습니다"
    print("   오늘 버전 폴더 그대로:", today.name, "파일", len(after or []))
    changes = E.schema_changes(contract, changed)
    print("2. 의도한 변경의 schema_changes:", json.dumps(changes, ensure_ascii=False))
    assert changes["dong_mois_map"]["breaking"] and changes["dong_mois_map"]["removed"] == ["share"]
    print("통과")


if __name__ == "__main__":
    main()
