"""docs/failure_analysis.md에서 오류 항목(F-N)의 제목과 상태를 읽는다. 문서가 원본이다(docs/handoff_ui.md 2절).

표준 라이브러리만 쓴다. CI(tests/test_ui_failures.py)가 fastapi 없이 부르기 때문이다.
상태는 "- 상태:" 또는 "- 상태 갱신 (...):"으로 시작하는 줄 가운데 마지막 줄이다. 굵은 글씨가 있으면 그 안만 쓴다.
상태 줄이 없는 항목은 status=None으로 둔다. 지어서 채우지 않는다(판단 U-3).
"""
import re
from pathlib import Path

PATH = Path(__file__).resolve().parent.parent / "docs" / "failure_analysis.md"
HEAD = re.compile(r"^## (F-\d+)\. (.+)$", re.M)
STATUS = re.compile(r"^- 상태[^:\n]*:\s*(.+)$", re.M)
BOLD = re.compile(r"\*\*(.+?)\*\*")


def parse(text):
    heads = list(HEAD.finditer(text))
    items = []
    for i, h in enumerate(heads):
        body = text[h.end(): heads[i + 1].start() if i + 1 < len(heads) else len(text)]
        lines = STATUS.findall(body)
        status = None
        if lines:
            m = BOLD.search(lines[-1])
            status = (m.group(1) if m else lines[-1]).strip().rstrip(".")
        items.append({"id": h.group(1), "title": h.group(2).strip(), "status": status})
    return items


def summary(items):
    """개요 카드용. 상태는 자유 글이라 낱말로 나눈다: '대기'(선택 대기 등), '진행 중'(처방 결정됨, 진행 중 등)."""
    return {
        "total": len(items),
        "waiting": [f["id"] for f in items if f["status"] and "대기" in f["status"]],
        "in_progress": [f["id"] for f in items if f["status"] and "진행 중" in f["status"]],
        "no_status": [f["id"] for f in items if f["status"] is None],
    }


def read(path=PATH):
    return parse(Path(path).read_text(encoding="utf-8"))
