"""단계 P5(작업 PC 밖 인수 시험): 바깥 에이전트가 낸 결과 CSV를 정답 CSV와 비교한다 (결정 113, docs/pages_acceptance.md).

왜: 바깥 에이전트는 다른 도구로 계산하므로 행 순서, 소수 자릿수, 정수/실수 표기가 정답과 다를 수 있다.
    그래서 바이트(sha256)가 아니라 규칙으로 비교한다.
규칙: 행 순서는 무시하고 키 컬럼(기본: 정답의 첫 컬럼) 값으로 행을 맞춘다.
      숫자 칸은 상대 오차 0.1% 이하면 같다(정답이 0이면 절대 차이 1e-9 이하).
      빈 칸(NULL)은 빈 칸과만 같다. 숫자가 아닌 칸은 앞뒤 공백을 뺀 글자가 같아야 한다.
      컬럼 집합이 다르거나, 빠진 행·남는 행·다른 칸이 하나라도 있으면 종료 코드 1.
실행: python -m measure.pages_compare 정답.csv 결과.csv [--key col,col]
      python -m measure.pages_compare --selftest
표준 라이브러리만 쓴다(정답 폴더가 있는 아무 PC에서나 돌리려고).
"""
import csv
import io
import math
import sys

REL_TOL = 0.001
ZERO_TOL = 1e-9


def read_csv(fh):
    reader = csv.DictReader(fh)
    name = lambda c: c.strip().lstrip("﻿")  # 파일을 utf-8-sig로 열지 않았을 때 남는 BOM
    return [name(c) for c in reader.fieldnames or []], [{name(k): (v or "").strip() for k, v in row.items() if k is not None} for row in reader]


def as_number(text):
    try:
        x = float(text)
    except ValueError:
        return None
    return x if math.isfinite(x) else None


def same_cell(expected, actual):
    if expected == "" or actual == "":
        return expected == actual
    e, a = as_number(expected), as_number(actual)
    if e is None or a is None:
        return expected == actual
    if e == 0:
        return abs(a) <= ZERO_TOL
    return abs(a - e) / abs(e) <= REL_TOL


def index(rows, key, side, problems):
    out = {}
    for row in rows:
        k = tuple(row.get(c, "") for c in key)
        if k in out:
            problems.append(f"{side}에 키가 겹치는 행: {k}")
        out[k] = row
    return out


def compare(exp_cols, exp_rows, act_cols, act_rows, key=None):
    """다른 점을 글로 모은 목록을 돌려준다. 빈 목록이면 같다."""
    problems = []
    key = key or exp_cols[:1]
    missing_cols = [c for c in exp_cols if c not in act_cols]
    extra_cols = [c for c in act_cols if c not in exp_cols]
    if missing_cols:
        problems.append(f"결과에 없는 컬럼: {missing_cols}")
    if extra_cols:
        problems.append(f"결과에만 있는 컬럼: {extra_cols}")
    if any(c not in exp_cols or c not in act_cols for c in key):
        problems.append(f"키 컬럼 {key}이 두 파일 모두에 있지 않아 행을 맞출 수 없습니다")
        return problems
    exp, act = index(exp_rows, key, "정답", problems), index(act_rows, key, "결과", problems)
    for k in exp.keys() - act.keys():
        problems.append(f"빠진 행: {k}")
    for k in act.keys() - exp.keys():
        problems.append(f"남는 행: {k}")
    for k in exp.keys() & act.keys():
        for c in exp_cols:
            if c in act_cols and not same_cell(exp[k][c], act[k][c]):
                problems.append(f"다른 칸: 키 {k}, {c}: 정답 {exp[k][c]!r}, 결과 {act[k][c]!r}")
    return sorted(problems)


def selftest():
    exp_text = "dong_id,deals,median\nA,3,98.5\nB,0,\nC,10,0\n"
    def run(act_text, key=None):
        ec, er = read_csv(io.StringIO(exp_text))
        ac, ar = read_csv(io.StringIO(act_text))
        return compare(ec, er, ac, ar, key)
    assert run(exp_text) == []
    assert run("﻿median,dong_id,deals\n,B,0\n0.0,C,10.0\n98.5,A,3\n") == [], "순서·컬럼 순서·BOM·정수 표기가 달라도 같아야 함"
    assert run("dong_id,deals,median\nA,3,98.549\nB,0,\nC,10,0\n") == [], "0.05% 차이는 통과해야 함"
    assert run("dong_id,deals,median\nA,3,98.697\nB,0,\nC,10,0\n") != [], "0.2% 차이는 실패해야 함"
    assert run("dong_id,deals,median\nA,3,98.5\nC,10,0\n") == ["빠진 행: ('B',)"], "빠진 행은 실패해야 함"
    assert run("dong_id,deals,median\nA,3,98.5\nB,0,0\nC,10,0\n") != [], "빈 칸과 0은 달라야 함"
    assert run("dong_id,deals,median\nA,3,98.5\nB,0,\nC,10,0.0000000001\n") == [], "정답 0이면 절대 차이 1e-9 이하는 통과"
    assert run("dong_id,deals,median\nA,3,98.5\nB,0,\nC,10,0\nD,1,1\n") == ["남는 행: ('D',)"]
    assert run("dong_id,deals\nA,3\nB,0\nC,10\n") == ["결과에 없는 컬럼: ['median']"]
    assert run(exp_text, key=["dong_id", "deals"]) == []
    print("selftest ok")


def main(argv):
    if argv == ["--selftest"]:
        return selftest()
    if len(argv) not in (2, 4) or (len(argv) == 4 and argv[2] != "--key"):
        raise SystemExit("사용법: python -m measure.pages_compare 정답.csv 결과.csv [--key col,col]")
    key = argv[3].split(",") if len(argv) == 4 else None
    with open(argv[0], encoding="utf-8-sig", newline="") as f:
        exp_cols, exp_rows = read_csv(f)
    with open(argv[1], encoding="utf-8-sig", newline="") as f:
        act_cols, act_rows = read_csv(f)
    problems = compare(exp_cols, exp_rows, act_cols, act_rows, key)
    print(f"정답 {len(exp_rows)}행, 결과 {len(act_rows)}행, 키 {key or exp_cols[:1]}")
    for p in problems:
        print(" ", p)
    print("같음" if not problems else f"다름 {len(problems)}건")
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main(sys.argv[1:])
