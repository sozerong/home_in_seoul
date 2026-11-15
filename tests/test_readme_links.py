"""README의 상대 링크가 실제 파일과 제목을 가리키는지 본다(handoff_agent_finish.md R1). DB 없이 돈다(CI).
앵커는 GitHub 방식으로 만든다: 제목을 소문자로, 글자·숫자·밑줄·하이픈·공백만 남기고 공백은 하이픈, 같은 앵커가 또 나오면 -1, -2.
실행: python -m tests.test_readme_links
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def anchors(md):
    seen, out = {}, set()
    in_code = False
    for line in md.splitlines():
        if line.startswith("```"):
            in_code = not in_code
        m = None if in_code else re.match(r"#{1,6} (.+)", line)
        if m:
            a = re.sub(r"[^\w\- ]", "", m.group(1).strip().lower()).replace(" ", "-")
            n = seen.get(a, 0)
            seen[a] = n + 1
            out.add(a if n == 0 else f"{a}-{n}")
    return out


def broken(readme=ROOT / "README.md"):
    bad = []
    for target in re.findall(r"\]\(([^)\s]+)\)", readme.read_text(encoding="utf-8")):
        if re.match(r"[a-z]+:", target):  # http, mailto 등 바깥 링크는 보지 않는다
            continue
        path, _, frag = target.partition("#")
        f = (readme.parent / path) if path else readme
        if not f.exists():
            bad.append(f"파일 없음: {target}")
        elif frag and f.suffix == ".md" and frag not in anchors(f.read_text(encoding="utf-8")):
            bad.append(f"제목 없음: {target}")
    return bad


assert anchors("# 결정 1. A (B)\n## 결정 1. A (B)\n```\n# 코드\n```") == {"결정-1-a-b", "결정-1-a-b-1"}
problems = broken()
assert not problems, "README 깨진 링크:\n  " + "\n  ".join(problems)
print("README 상대 링크 모두 맞음")
