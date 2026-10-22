"""여러 스크립트가 함께 쓰는 설정 읽기.

스크립트는 프로젝트 루트에서 `python -m 폴더.파일이름` 으로 실행한다.
예: python -m ingestion.molit_rent offi 11680 202609
"""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent



def read_env(name):
    """환경 변수에 이름이 있으면 그 값을, 없으면 프로젝트 루트의 .env에서 `이름=값` 줄을 찾아 돌려준다.

    Airflow 컨테이너처럼 실행 위치마다 다른 값(DB 주소 등)은 환경 변수로 덮어쓴다.
    """
    if name in os.environ:
        return os.environ[name]
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        key, sep, value = line.partition("=")
        if sep and key.strip() == name:
            return value.strip()
    raise SystemExit(f".env에 {name}가 없습니다")