"""transform/boundary.py의 좌표계 비교와 속성 검증 규칙 시험. DB와 레이크 없이 돈다.

실행: python -m tests.test_boundary
"""
from transform.boundary import EPSG_5179, EPSG_5186, clean_parcel, clean_sgis, projection_matches

# 단계 1에서 본 실제 .prj 두 개 (data_sources.md 10, 14절)
SGIS_PRJ = ('PROJCS["Korea_2000_Korea_Unified_Coordinate_System",GEOGCS["GCS_Korea_2000",DATUM["D_Korea_2000",'
            'SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],'
            'PROJECTION["Transverse_Mercator"],PARAMETER["False_Easting",1000000.0],PARAMETER["False_Northing",2000000.0],'
            'PARAMETER["Central_Meridian",127.5],PARAMETER["Scale_Factor",0.9996],PARAMETER["Latitude_Of_Origin",38.0],'
            'UNIT["Meter",1.0]]')
PARCEL_PRJ = ('PROJCS["Korea 2000 / Central Belt 2010", GEOGCS["Korea 2000", DATUM["Geocentric datum of Korea", '
              'SPHEROID["GRS 1980", 6378137.0, 298.257222101, AUTHORITY["EPSG","7019"]]], PRIMEM["Greenwich", 0.0], '
              'UNIT["degree", 0.017453292519943295]], PROJECTION["Transverse_Mercator"], '
              'PARAMETER["central_meridian", 127.00000000000001], PARAMETER["latitude_of_origin", 38.00000000000001], '
              'PARAMETER["scale_factor", 1.0], PARAMETER["false_easting", 200000.0], PARAMETER["false_northing", 600000.0], '
              'UNIT["m", 1.0], AUTHORITY["EPSG","5186"]]')

# BF2, PF2: 이름 표기가 달라도 숫자가 같으면 통과, 숫자가 다르면 실패
assert projection_matches(SGIS_PRJ, EPSG_5179)
assert projection_matches(PARCEL_PRJ, EPSG_5186)
assert not projection_matches(SGIS_PRJ, EPSG_5186)                       # 두 좌표계를 뒤바꾸면 실패
assert not projection_matches(PARCEL_PRJ, EPSG_5179)
assert not projection_matches(SGIS_PRJ.replace("127.5", "127.0"), EPSG_5179)   # 중앙자오선만 달라도 실패
assert not projection_matches(SGIS_PRJ.replace('PARAMETER["Scale_Factor",0.9996],', ""), EPSG_5179)  # 매개변수가 빠짐
assert not projection_matches(SGIS_PRJ.replace("GRS_1980", "Bessel_1841"), EPSG_5179)  # 타원체가 다름

# SGIS 경계: BV1을 먼저, 그다음 BC1(서울만)
OK = {"BASE_DATE": "20250630", "ADM_CD": "11010530", "ADM_NM": "사직동"}
values, reasons = clean_sgis(OK)
assert reasons == [] and values == {"base_date": "2025-06-30", "adm_cd": "11010530", "adm_nm": "사직동"}
assert clean_sgis({**OK, "ADM_CD": "31010530"}) is None                  # 서울 밖 정상 행은 범위 밖
assert clean_sgis({**OK, "ADM_CD": "1101053"})[1] == ["BV1"]             # 7자리
assert clean_sgis({**OK, "ADM_CD": "3101A530"})[1] == ["BV1"]            # 형식이 깨지면 서울 밖이어도 격리
assert clean_sgis({**OK, "ADM_NM": " "})[1] == ["BV1"]                   # 이름 빈 값
assert clean_sgis({**OK, "BASE_DATE": "2025-06"})[1] == ["BV1"]          # 기준일이 날짜가 아님

# 연속지적도: PV1
OKP = {"SGG_OID": 1.0, "JIBUN": "301-7 대", "BCHK": "1", "PNU": "1114016200103010007", "COL_ADM_SE": "11140"}
values, reasons = clean_parcel(OKP, "202610")
assert reasons == [] and values == {"base_ym": "202610", "pnu": "1114016200103010007",
                                    "jibun": "301-7 대", "sgg_cd": "11140"}
assert clean_parcel({**OKP, "PNU": "111401620010301000"}, "202610")[1] == ["PV1"]   # 18자리
assert clean_parcel({**OKP, "PNU": "11140162001030100A7"}, "202610")[1] == ["PV1"]  # 숫자가 아님
assert clean_parcel({**OKP, "JIBUN": " "}, "202610")[0]["jibun"] is None             # 빈 지번은 NULL

print("tests.test_boundary: 모든 시험 통과")
