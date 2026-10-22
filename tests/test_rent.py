"""transform/rent.py의 정제·검증 규칙 시험. DB와 레이크 없이 돈다.

실행: python -m tests.test_rent
"""
from transform.rent import clean

BJD = {("11680", "역삼동"): "1168010100"}  # 법정동 코드표 대신 쓰는 작은 표
BASE = {  # 단계 1에서 본 오피스텔 item 하나(역삼동 824-25)를 본뜬 정상 item
    "buildYear": "2004", "contractTerm": " ", "contractType": " ", "dealDay": "9", "dealMonth": "9",
    "dealYear": "2026", "deposit": "24,500", "excluUseAr": "38.96", "floor": "3", "jibun": "824-25",
    "monthlyRent": "0", "offiNm": "대우디오빌플러스", "preDeposit": " ", "preMonthlyRent": " ",
    "sggCd": "11680", "sggNm": "강남구", "umdNm": "역삼동", "useRRRight": " ",
}


def check(changes, expected, kind="offi", deal_ymd="202609"):
    """BASE에서 changes만 바꾼 item을 정제해, 걸린 규칙 번호가 expected와 같은지 본다."""
    row, reasons = clean({**BASE, **changes}, kind, deal_ymd, "11680", BJD)
    assert reasons == expected, f"{changes}: 기대 {expected}, 실제 {reasons}"
    return row


# 정상 item: 아무 규칙에도 걸리지 않고 값이 바르게 바뀌어야 한다
row = check({}, [])
assert row["deposit"] == 24500 and row["monthly_rent"] == 0   # C3: 쉼표 제거, 정수
assert row["contract_type"] is None                            # C2: 공백 한 칸은 None
assert str(row["deal_date"]) == "2026-09-09"                   # C4: 년월일 합치기
assert row["pnu"] == "1168010100108240025"                     # C6: 법정동코드 + 1 + 본번 + 부번
assert check({"jibun": "824"}, [])["pnu"] == "1168010100108240000"  # 부번이 없으면 0000

# 규칙마다 일부러 어긴 item
check({"deposit": " "}, ["V1"])                                   # 필수값 없음
check({"dealMonth": "10"}, ["V2"])                                # 요청한 계약년월과 다름
check({"dealMonth": "2", "dealDay": "30"}, ["V3"], deal_ymd="202602")  # 없는 날짜
check({"deposit": "-1"}, ["V4"])                                  # 음수 금액
check({"excluUseAr": "0"}, ["V5"])                                # 면적 0
check({"excluUseAr": "300.01"}, ["V5"])                           # 면적 상한 초과
check({"floor": "81"}, ["V6"])                                    # 층 상한 초과
check({"contractType": "기타"}, ["V7"])                           # 모르는 계약 종류
check({"umdNm": "없는동"}, ["V8"])                                # 법정동 코드를 못 찾음
assert check({"jibun": "산12"}, [])["pnu"] == "1168010100200120000"  # 산 지번: 대지구분 2
assert check({"jibun": "산10-22"}, [])["pnu"] == "1168010100200100022"
check({"jibun": "12가"}, ["V9"])                                  # 지번 모양이 아님
check({"jibun": "산"}, ["V9"])                                    # 본번이 없음
check({"deposit": "abc"}, ["C3"])                                 # 금액 칸에 숫자가 아님
check({"deposit": " ", "umdNm": "없는동"}, ["V1", "V8"])          # 여러 규칙에 함께 걸림

# 경계값은 통과해야 한다
check({"excluUseAr": "300"}, [])
check({"floor": "-5"}, [])

# 연립다세대: 건물 이름 필드가 다르다 (C1)
rh = check({"mhouseNm": "청아람", "houseType": "다세대"}, [], kind="rh")
assert rh["building_name"] == "청아람" and rh["house_type"] == "다세대"

print("tests.test_rent: 모든 시험 통과")