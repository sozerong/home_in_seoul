"""국토교통부 전월세 실거래가 API 응답을 받은 그대로 레이크에 저장한다.

사용법: python ingestion/molit_rent.py {offi|rh} 구코드5자리 계약년월YYYYMM
예:     python ingestion/molit_rent.py offi 11680 202609
"""
import argparse
import datetime
import shutil
import tempfile
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
SERVICES = {"offi": "RTMSDataSvcOffiRent", "rh": "RTMSDataSvcRHRent"}
ROWS_PER_PAGE = 1000
MAX_TRIES = 3
PAUSE_SECONDS = 0.5


def read_env(name):
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        key, sep, value = line.partition("=")
        if sep and key.strip() == name:
            return value.strip()
    raise SystemExit(f".env에 {name}가 없습니다")


def get_page(service, params):
    url = f"https://apis.data.go.kr/1613000/{service}/get{service}"
    for attempt in range(1, MAX_TRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=30)
        except (requests.ConnectionError, requests.Timeout) as e:
            error = type(e).__name__  # 예외 메시지에는 키가 든 URL이 있어 이름만 남긴다
        else:
            if resp.status_code == 200:
                return resp.content
            if resp.status_code < 500 and resp.status_code != 429:
                raise RuntimeError(f"HTTP {resp.status_code} (영구 오류): {resp.text[:300]}")
            error = f"HTTP {resp.status_code}"
        if attempt < MAX_TRIES:
            time.sleep(2 ** attempt)
    raise RuntimeError(f"{MAX_TRIES}번 시도 후 실패: {error}")


def check(body):
    """정상 응답인지 확인하고 (totalCount, 이 쪽의 item 수)를 돌려준다."""
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        raise RuntimeError(f"XML이 아닌 응답: {body[:300]!r}")
    code = root.findtext("header/resultCode")
    if code != "000":
        raise RuntimeError(f"API 오류 resultCode={code}: {body[:300]!r}")
    return int(root.findtext("body/totalCount")), len(root.findall("body/items/item"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=SERVICES)
    parser.add_argument("lawd_cd")
    parser.add_argument("deal_ymd")
    args = parser.parse_args()

    key = read_env("DATA_GO_KR_SERVICE_KEY")
    today = datetime.date.today().isoformat()
    out_dir = ROOT / "lake" / f"molit_{args.kind}_rent" / f"collected_date={today}"
    prefix = f"{args.lawd_cd}_{args.deal_ymd}"

    with tempfile.TemporaryDirectory() as tmp:
        page, got, total = 1, 0, None
        while total is None or got < total:
            params = {"serviceKey": key, "LAWD_CD": args.lawd_cd, "DEAL_YMD": args.deal_ymd,
                      "pageNo": page, "numOfRows": ROWS_PER_PAGE}
            body = get_page(SERVICES[args.kind], params)
            total, n = check(body)
            if n == 0:
                raise RuntimeError(f"{page}쪽에 item이 0개 (totalCount={total}, 지금까지 {got}개)")
            Path(tmp, f"{prefix}_p{page:03d}.xml").write_bytes(body)
            got += n
            page += 1
            time.sleep(PAUSE_SECONDS)

        out_dir.mkdir(parents=True, exist_ok=True)
        for old in out_dir.glob(f"{prefix}_p*.xml"):
            old.unlink()
        for f in sorted(Path(tmp).iterdir()):
            shutil.move(f, out_dir / f.name)

    print(f"{args.kind} {prefix}: totalCount={total}, {page - 1}쪽 -> {out_dir}")


if __name__ == "__main__":
    main()