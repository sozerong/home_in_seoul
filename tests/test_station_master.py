"""transform/station_master.py의 정제·검증 규칙 시험. DB와 레이크 없이 돈다.

실행: python -m tests.test_station_master
"""
from transform.station_master import clean

BASE = {"BLDN_ID": "0150", "BLDN_NM": "서울역", "ROUTE": "1호선", "LAT": "37.556228", "LOT": "126.972135"}


def check(changes, expected):
    row, reasons = clean({**BASE, **changes})
    assert reasons == expected, f"{changes}: 기대 {expected}, 실제 {reasons}"
    return row


row = check({}, [])
assert row["bldn_id"] == "0150"                            # SC2: 앞자리 0 보존
assert (row["lat"], row["lot"]) == (37.556228, 126.972135)  # SC3: 숫자로

check({"BLDN_NM": " "}, ["SV1"])                    # 빈 값
check({"LAT": ""}, ["SV1"])                         # 좌표가 비면 SV1만 (SV3는 걸지 않음)
check({"BLDN_ID": "150"}, ["SV2"])                  # 3자리
check({"BLDN_ID": "01A0"}, ["SV2"])                 # 숫자가 아님
check({"LAT": "abc"}, ["SV3"])                      # 숫자가 아님
check({"LAT": "126.972135", "LOT": "37.556228"}, ["SV3"])  # 위도와 경도가 뒤바뀜
check({"BLDN_ID": "15", "LAT": "0"}, ["SV2", "SV3"])       # 여러 규칙에 함께 걸림

print("tests.test_station_master: 모든 시험 통과")