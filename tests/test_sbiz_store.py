"""transform/sbiz_store.py의 정제·검증 규칙 시험. DB와 레이크 없이 돈다(파일 단위 검사는 임시 zip으로).

실행: python -m tests.test_sbiz_store
"""
import csv
import io
import pathlib
import tempfile
import zipfile

from transform.sbiz_store import ESSENTIAL, HEADER, clean, read_seoul, split

BASE = {h: "" for h in HEADER} | {  # 서울 202606 파일의 9번째 행(성북구 이마트)을 본뜬 정상 행
    "상가업소번호": "MA0106202201A0563942", "상호명": " 이마트 ", "상권업종소분류코드": "G20404",
    "상권업종소분류명": "슈퍼마켓", "시도코드": "11", "시군구코드": "11290", "행정동코드": "11290685",
    "법정동코드": "1129013400", "경도": "127.026828914647", "위도": "37.6060342944306",
}


def check(changes, expected):
    """BASE에서 changes만 바꾼 행을 정제해, 걸린 규칙 번호가 expected와 같은지 본다."""
    row, reasons = clean({**BASE, **changes})
    assert reasons == expected, f"{changes}: 기대 {expected}, 실제 {reasons}"
    return row


# 정상 행: 아무 규칙에도 걸리지 않고 값이 바르게 바뀌어야 한다
row = check({}, [])
assert row["store_name"] == "이마트"                                  # MC2: 앞뒤 공백 제거
assert (row["lon"], row["lat"]) == (127.026828914647, 37.6060342944306)  # MC3: 실수, 자릿수 유지
assert check({"상호명": " "}, [])["store_name"] is None           # 상호명은 필수가 아니다(MC2로 None)

# 규칙마다 일부러 어긴 행
check({"상가업소번호": " "}, ["MV1"])                    # 필수값 없음
check({"시군구코드": ""}, ["MV1"])                       # 시군구코드가 비면 MV1만 (MV4는 걸지 않음)
check({"경도": ""}, ["MV1"])                             # 좌표가 비면 MV1만
check({"경도": "abc"}, ["MC3"])                          # 숫자가 아님
check({"경도": "127.5"}, ["MV3"])                        # 서울 상자 밖
check({"경도": "37.6060342944306", "위도": "127.026828914647"}, ["MV3"])  # 경도와 위도가 뒤바뀜
check({"위도": "nan"}, ["MV3"])                          # float("nan")은 숫자로 바뀌지만 범위 밖
check({"시도코드": "41"}, ["MV4"])                       # 서울이 아님
check({"행정동코드": "11680660"}, ["MV4"])               # 앞 5자리가 시군구코드와 다름
check({"법정동코드": "112901340"}, ["MV4"])              # 9자리
check({"시군구코드": "1129"}, ["MV4"])                   # 4자리 (char(5)에 공백이 채워지는 것을 막음)
check({"상가업소번호": "", "경도": "200"}, ["MV1", "MV3"])  # 여러 규칙에 함께 걸림

# 경계값은 통과해야 한다
check({"경도": "126.7", "위도": "37.8"}, [])

# MV2: 같은 업소번호가 두 번 나오면 둘 다 격리하고, 다른 사유와 함께 남긴다
other = {**BASE, "상가업소번호": "MA0000000000000000001"}
good, bad = split([(1, BASE), (2, {**BASE, "경도": "200"}), (3, other)])
assert [g["row_no"] for g in good] == [3]
assert [(b["row_no"], b["reasons"]) for b in bad] == [(1, ["MV2"]), (2, ["MV2", "MV3"])]


# 파일 단위 검사: 임시 zip을 만들어 read_seoul에 넣는다
def csv_text(rows, header=HEADER):
    out = io.StringIO()
    writer = csv.writer(out, quoting=csv.QUOTE_ALL)
    writer.writerow(header)
    writer.writerows([[r.get(h, "") for h in header] if isinstance(r, dict) else r for r in rows])
    return out.getvalue()


ESSENTIAL_ROWS = [{**BASE, "상가업소번호": f"MA{i:018d}", "상권업종소분류코드": code, "상권업종소분류명": name}
                  for i, (code, name) in enumerate(ESSENTIAL.items())]
NORMAL = ESSENTIAL_ROWS + [{**BASE, "상권업종소분류코드": "I21201", "상권업종소분류명": "카페"}]
SEOUL = "소상공인시장진흥공단_상가(상권)정보_서울_202606.csv"


def read(files):
    """{zip 안 파일 이름: 내용}으로 zip을 만들어 read_seoul을 부른다."""
    with tempfile.TemporaryDirectory() as folder:  # 실패 경로에서 핸들이 열려 있으면 Windows가 삭제를 막아 시험이 깨진다
        path = pathlib.Path(folder) / "t.zip"
        with zipfile.ZipFile(path, "w") as zf:
            for name, text in files.items():
                zf.writestr(name, text)
        return read_seoul(path)


def fails(rule, files):
    """read_seoul이 rule 번호로 시작하는 메시지로 실패하는지 본다."""
    try:
        read(files)
    except SystemExit as e:
        assert str(e).startswith(rule), f"기대 {rule}, 실제 {e}"
        return
    raise AssertionError(f"{rule}로 실패해야 하는데 통과했다")


name, base_ym, rows = read({SEOUL: csv_text(NORMAL), "소상공인시장진흥공단_상가(상권)정보_경기_202606.csv": "x"})
assert (name, base_ym) == (SEOUL, "202606")
assert [n for n, _ in rows] == [1, 2, 3, 4, 5, 6]            # 필수 6코드 행만, 행 번호는 1부터 (카페 7번은 빠짐)

fails("MF1", {})                                             # 서울 CSV가 없음
fails("MF1", {SEOUL: csv_text(NORMAL), SEOUL.replace("202606", "202603"): csv_text(NORMAL)})  # 둘
fails("MF1", {SEOUL: csv_text([])})                          # 헤더만 있고 0행
fails("MF2", {SEOUL: csv_text(NORMAL, HEADER[:-1] + ["위도2"])})  # 열 이름이 바뀜
fails("MF2", {SEOUL: csv_text(NORMAL + [["MA1", "x"]])})      # 열 수가 모자란 행
fails("MF3", {SEOUL: csv_text(NORMAL[1:])})                  # 편의점 코드가 없음
fails("MF3", {SEOUL: csv_text([{**ESSENTIAL_ROWS[0], "상권업종소분류명": "편의점(24시)"}] + NORMAL[1:])})  # 이름이 바뀜

print("tests.test_sbiz_store: 모든 시험 통과")
