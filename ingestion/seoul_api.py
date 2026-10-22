"""서울 열린데이터광장 Open API 응답을 받은 그대로 레이크에 저장한다.

사용법: python -m ingestion.seoul_api {station_master|congestion}
"""
import argparse
import datetime
import shutil
import tempfile
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

from common import ROOT, read_env

# 이름: (서비스명, 레이크 폴더)
SERVICES = {
    "station_master": ("subwayStationMaster", "seoul_station_master"),
    "congestion": ("subwConfusion", "seoul_subway_congestion"),
}
ROWS_PER_PAGE = 1000
MAX_TRIES = 3
PAUSE_SECONDS = 0.5
RETRY_CODES = {"ERROR-500", "ERROR-600"}  # 명세서상 서버 오류, DB 연결 오류


def parse(body):
    """XML을 읽어 (루트, 결과 코드)를 돌려준다. 오류 응답은 루트 자체가 RESULT다."""
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        raise RuntimeError(f"XML이 아닌 응답: {body[:300]!r}")
    result = root if root.tag == "RESULT" else root.find("RESULT")
    return root, (result.findtext("CODE") if result is not None else None)


def fetch(url):
    """한 쪽을 받아 (루트, 원본 바이트)를 돌려준다. 일시 오류만 재시도한다."""
    for attempt in range(1, MAX_TRIES + 1):
        try:
            resp = requests.get(url, timeout=30)
        except (requests.ConnectionError, requests.Timeout) as e:
            error = type(e).__name__  # 예외 메시지에는 키가 든 URL이 있어 이름만 남긴다
        else:
            if resp.status_code == 200:
                root, code = parse(resp.content)
                if code == "INFO-000":
                    return root, resp.content
                if code not in RETRY_CODES:
                    raise RuntimeError(f"API 오류 CODE={code}: {resp.content[:300]!r}")
                error = code
            elif resp.status_code < 500 and resp.status_code != 429:
                raise RuntimeError(f"HTTP {resp.status_code} (영구 오류)")  # 본문에 키가 든 경로가 찍힐 수 있어 넣지 않는다
            else:
                error = f"HTTP {resp.status_code}"
        if attempt < MAX_TRIES:
            time.sleep(2 ** attempt)
    raise RuntimeError(f"{MAX_TRIES}번 시도 후 실패: {error}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("name", choices=SERVICES)
    parser.add_argument("--collected-date", type=datetime.date.fromisoformat, default=datetime.date.today(),
                        help="레이크 수집일 폴더 날짜. 안 주면 오늘. Airflow가 정제와 같은 날짜를 넘길 때 쓴다")
    args = parser.parse_args()

    service, folder = SERVICES[args.name]
    key = read_env("SEOUL_OPEN_API_KEY")
    today = args.collected_date.isoformat()
    out_dir = ROOT / "lake" / folder / f"collected_date={today}"

    with tempfile.TemporaryDirectory() as tmp:
        page, got, total = 1, 0, None
        while total is None or got < total:
            start = (page - 1) * ROWS_PER_PAGE + 1
            end = start + ROWS_PER_PAGE - 1
            root, body = fetch(f"http://openapi.seoul.go.kr:8088/{key}/xml/{service}/{start}/{end}/")
            total = int(root.findtext("list_total_count"))
            n = len(root.findall("row"))
            if n == 0:
                raise RuntimeError(f"{page}쪽에 row가 0개 (list_total_count={total}, 지금까지 {got}개)")
            Path(tmp, f"{service}_p{page:03d}.xml").write_bytes(body)
            got += n
            page += 1
            time.sleep(PAUSE_SECONDS)

        out_dir.mkdir(parents=True, exist_ok=True)
        for old in out_dir.glob(f"{service}_p*.xml"):
            old.unlink()
        for f in sorted(Path(tmp).iterdir()):
            shutil.move(f, out_dir / f.name)

    print(f"{args.name}: list_total_count={total}, {page - 1}쪽 -> {out_dir}")


if __name__ == "__main__":
    main()