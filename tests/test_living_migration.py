"""transform/living_migration.py의 정제·검증 규칙 시험. DB와 레이크 없이 돈다.

실행: python -m tests.test_living_migration
"""
import pathlib
import tempfile
import zipfile

from transform.living_migration import HEADER, clean, out_of_scope, read_month

CODES = {"1101053", "1123064", "3101154", "38000"}  # 코드표 대신 쓰는 작은 표 (서울 2, 경기 1, 경상남도 1)
LINE = "202607,월,08,1101053,1123064,F,25,HW,40,3.32"  # 정상 행: 사직동 → 역삼1동, 월요일 08시 도착
BASE = dict(zip(HEADER, LINE.split(",")))


def check(changes, expected, hour="08"):
    """BASE에서 changes만 바꾼 행을 정제해, 걸린 규칙 번호가 expected와 같은지 본다."""
    row, reasons = clean({**BASE, **changes}, "202607", hour, CODES)
    assert reasons == expected, f"{changes}: 기대 {expected}, 실제 {reasons}"
    return row


# 정상 행: 아무 규칙에도 걸리지 않고 값이 바르게 바뀌어야 한다
row = check({}, [])
assert row["avg_minutes"] == 40 and row["population"] == 3.32 and row["arrive_hour"] == 8  # C3
assert check({"이동인구(합)": "*"}, [])["population"] is None  # C2: 가려진 값은 격리하지 않고 None

# 규칙마다 일부러 어긴 행
check({"이동인구(합)": "abc"}, ["C3"])                       # 숫자가 아님
check({"이동인구(합)": "nan"}, ["C3"])                       # float()는 받지만 숫자가 아님
check({"성별": ""}, ["V1", "V5"])                            # 빈 값 (빈 성별은 범주에도 없다)
check({"대상연월": "202606"}, ["V2"])                        # 처리 단위의 연월과 다름
check({"요일": "?"}, ["V3"])                                 # 없는 요일
check({}, ["V3"], hour="07")                                 # 08시 행이 07시 파일에 있음
check({"도착 행정동 코드": "9999999"}, ["V4"])               # 코드표에 없음
check({"나이": "12"}, ["V5"])                                # 없는 나이 구간
check({"평균 이동 시간(분)": "0"}, ["V6"])                   # 1분 미만
check({"평균 이동 시간(분)": "241"}, ["V6"])                 # 240분 초과
check({"이동인구(합)": "2.99"}, ["V7"])                      # 가려지지 않았는데 3 미만

# 경계값은 통과해야 한다
check({"평균 이동 시간(분)": "240", "이동인구(합)": "3"}, [])

# 범위 판정: 올바른 값만으로 범위 밖이 확정될 때만 True
assert not out_of_scope(BASE, CODES)
assert out_of_scope({**BASE, "요일": "토"}, CODES)
assert out_of_scope({**BASE, "이동유형": "EE"}, CODES)
assert out_of_scope({**BASE, "출발 행정동 코드": "3101154"}, CODES)   # 경기 출발
assert out_of_scope({**BASE, "도착 행정동 코드": "38000"}, CODES)     # 경상남도 도착 (시·도 단위 5자리)
assert not out_of_scope({**BASE, "요일": "?"}, CODES)                 # 깨진 값은 검증으로 보낸다
assert not out_of_scope({**BASE, "출발 행정동 코드": "9999999"}, CODES)


def month(lines, header=",".join(HEADER), hours=("07", "08", "09")):
    """08시 파일에 lines를 넣은 작은 zip을 임시 폴더에 만들어 read_month를 돌린다. 다른 시간 파일은 헤더만 있다."""
    with tempfile.TemporaryDirectory() as folder:
        path = pathlib.Path(folder) / "test.zip"
        with zipfile.ZipFile(path, "w") as zf:
            for hour in hours:
                body = [header] + (lines if hour == "08" else [])
                zf.writestr(f"생활이동_행정동_2026.07_{hour}시.csv", "\n".join(body).encode("cp949"))
        return read_month(path, "202607", CODES)


# read_month: C4, V8, 범위 밖, 열 수
other = LINE.replace(",25,", ",30,")       # 나이만 다름 = 다른 키
conflict = LINE.replace(",40,", ",45,")    # 키는 같고 이동 시간만 다름
saturday = LINE.replace(",월,", ",토,")
good, bad, counts = month([LINE, LINE, other, other, conflict, saturday, "1,2,3"])
assert len(good) == 1 and good[0]["age"] == "30" and good[0]["row_no"] == 4   # other 첫 행 (헤더가 1번 줄)
assert sorted(b["reasons"][0] for b in bad) == ["V1", "V8", "V8"]              # 열 수 3개, 키 충돌 두 행
assert counts == {"읽은 행": 7, "C4 완전 중복": 2, "범위 밖": 1}


def fails(rule, **kwargs):
    """zip 모양이 잘못되면 그 규칙 번호로 멈춰야 한다."""
    try:
        month([], **kwargs)
    except SystemExit as e:
        assert str(e).startswith(rule), e
    else:
        raise AssertionError(f"{rule}에서 멈추지 않았다")


fails("F1", header="a,b")             # 헤더가 다름
fails("F3", hours=("07", "08"))       # 09시 파일 없음

print("tests.test_living_migration: 모든 시험 통과")