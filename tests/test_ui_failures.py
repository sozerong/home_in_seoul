"""ui/failures.py가 failure_analysis.md의 F 항목과 상태를 읽는 규칙 시험. DB 없이 돈다.

실행: python -m tests.test_ui_failures
"""
from ui.failures import parse, read, summary

SAMPLE = """
## F-N. 제목 (증상 한 줄) [유도 여부]
- 상태: **템플릿이라 읽히면 안 됨**

## F-1. 첫 오류
- 상태: **선택 대기**(위 표, 사용자).

## F-2. 상태 줄 없음
- 남은 것: 없음.

## F-3. 상태가 두 번
- 상태: **선택 대기.**
- 상태 갱신 (2026-11-13, 4절): **처방함, 확인됨.**

## F-4. 굵은 글씨 없음
- 상태: 그대로 둠

## F-5. 진행 중
- 상태: **처방 결정됨, 진행 중**
"""

items = parse(SAMPLE)
assert [f["id"] for f in items] == ["F-1", "F-2", "F-3", "F-4", "F-5"], items  # 템플릿 F-N 제외
assert items[0] == {"id": "F-1", "title": "첫 오류", "status": "선택 대기"}, items[0]
assert items[1]["status"] is None  # 없으면 지어내지 않음
assert items[2]["status"] == "처방함, 확인됨"  # 마지막 상태 줄
assert items[3]["status"] == "그대로 둠"
assert summary(items) == {"total": 5, "waiting": ["F-1"], "in_progress": ["F-5"], "no_status": ["F-2"]}

# 지금 문서: F-1부터 빠짐없이 차례로 읽힌다. 개수는 문서가 자라므로 보지 않는다
# (worktree는 커밋된 F-1~F-17, 메인 작업 트리는 F-20까지라 개수로 박으면 한쪽에서 깨진다).
real = read()
ids = [f["id"] for f in real]
assert ids and ids == [f"F-{i}" for i in range(1, len(ids) + 1)], ids
print(f"ok: 표본 5개, 실제 문서 {len(real)}개 ({summary(real)})")
