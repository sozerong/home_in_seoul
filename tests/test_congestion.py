"""transform/congestion.py의 정제·검증 규칙 시험. DB와 레이크 없이 돈다.

실행: python -m tests.test_congestion
"""
import datetime

from transform.congestion import TIME_FIELDS, clean

BASE = {"DOW_SE": "평일", "LINE": "1호선", "STTN_NO": "150", "DPTRE_STTN": "서울역", "UP_DOWN_SE": "상선",
        **{f: "10.5" for f in TIME_FIELDS}}


def check(changes, expected_reasons, expected_rows):
    """BASE에서 changes만 바꾼 원본 행을 정제해, 격리 사유 목록과 통과한 칸 수를 본다."""
    rows, quarantined = clean({**BASE, **changes})
    reasons = [r for r, _ in quarantined]
    assert reasons == expected_reasons, f"{changes}: 기대 사유 {expected_reasons}, 실제 {reasons}"
    assert len(rows) == expected_rows, f"{changes}: 기대 {expected_rows}칸, 실제 {len(rows)}칸"
    return rows, quarantined


# 시간대 이름 39개가 맞게 만들어졌는지
assert len(TIME_FIELDS) == 39
assert TIME_FIELDS[0] == "TIME0530" and TIME_FIELDS[1] == "TIME0600"
assert TIME_FIELDS[-2:] == ["TIME0000", "TIME0030"]

# 정상 행: 39칸이 모두 통과하고 값이 바르게 바뀐다
rows, _ = check({}, [], 39)
assert rows[0]["sttn_no4"] == "0150"                      # CC2: 앞에 0을 채움
assert rows[0]["sttn_no"] == "150"                        # CC2: 원래 값도 남김
assert rows[0]["slot"] == datetime.time(5, 30)            # CC4
assert rows[-1]["slot"] == datetime.time(0, 30)           # CC4: TIME0030 -> 00:30
assert rows[0]["congestion"] == 10.5                      # CC5

# 신원 정보가 잘못되면 원본 행 전체를 격리한다 (통과 0칸)
check({"STTN_NO": " "}, [["CV1"]], 0)                     # 빈 값
check({"DOW_SE": "공휴일"}, [["CV2"]], 0)                 # 모르는 요일
check({"UP_DOWN_SE": "북행"}, [["CV2"]], 0)               # 모르는 방향
check({"LINE": "9호선"}, [["CV3"]], 0)                    # 1~8호선 밖
check({"STTN_NO": "15"}, [["CV4"]], 0)                    # 2자리
check({"STTN_NO": "A150"}, [["CV4"]], 0)                  # 숫자가 아님
check({"LINE": "", "STTN_NO": "15"}, [["CV1", "CV4"]], 0) # 여러 규칙에 함께 걸림

# 값만 잘못되면 그 칸만 격리한다 (나머지 38칸은 통과)
_, quarantined = check({"TIME0800": "-0.1"}, [["CV5"]], 38)  # 음수
assert set(quarantined[0][1]) == {"DOW_SE", "LINE", "STTN_NO", "DPTRE_STTN", "UP_DOWN_SE", "TIME0800"}
check({"TIME0800": "300.1"}, [["CV5"]], 38)               # 상한 초과
check({"TIME0800": "abc"}, [["CV5"]], 38)                 # 숫자가 아님
check({"TIME0800": "nan"}, [["CV5"]], 38)                 # 숫자처럼 읽히지만 비교가 안 되는 값

# 경계값은 통과해야 한다
check({"TIME0800": "0"}, [], 39)
check({"TIME0800": "300"}, [], 39)

print("tests.test_congestion: 모든 시험 통과")