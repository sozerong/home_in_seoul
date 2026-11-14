# 8절 은퇴 뒤 재현 불가(옛 표 stg.living_migration·옛 정제 코드 필요, 결정 105로 지움). 재현하려면 커밋 3efb02f를 체크아웃한다.
"""변경 시나리오(change_resilience_plan.md 7절)를 실제 레이크·DB를 건드리지 않고 재현해 변경 비용을 잰다.

P0에서 기준값을 재고, 각 작업 뒤 같은 명령으로 다시 재서 전후를 비교한다.
  s1  원천 필드 이름 변경(혼잡도): 쓰는 필드 DPTRE_STTN이 STTN_NM으로 바뀌었다고 가정한다.
      ① 대조군 ② 개명된 입력 + 지금 코드 ③ 개명된 입력 + 흔한 고침 ④ 옛 수집일(개명 전) + 흔한 고침.
      흔한 고침 = 정제 파일 안의 옛 이름을 새 이름으로 모두 바꾼 사본(레포 파일은 고치지 않는다).
  s2  무해한 필드 추가(전월세): 실제 XML의 item마다 쓰지 않는 필드를 하나 덧붙여 transform.rent를 돌린다.
  s3  규칙 정정(전월세 C6, 산 지번): 레이크 전월세 XML 전체를 옛 규칙(git S3_OLD_COMMIT)과 지금 규칙으로 각각 정제해
      결과가 달라지는 처리 단위(= 다시 만들어야 할 범위의 정답)를 센다. DB는 쓰지 않는다.
  unit 실제 레이크의 전월세 처리 단위 하나를 transform.rent.main()으로 돌려 시간과 최대 메모리를 본다(DB 차단).
      예: python -m measure.change_cost unit offi 2026-11-03 11680 202610
  s6a 요구사항 변경(편의 업종 추가): 실제 레이크 상가정보 zip을 transform.sbiz_store.main()으로 돌린다.
      대조군은 지금 6코드, 변경은 ESSENTIAL에 S6A_CODE를 더한 7코드(레포 파일은 고치지 않는다).
  s6b 요구사항 변경(저녁 귀가 18~20시): Airflow 컨테이너 spark-venv에서 Iceberg 한 달을 transform.living_migration으로 돌린다.
      대조군은 지금 코드(HW, 07~09시), 변경은 흔한 고침 사본(WH, 18~20시. 레포 파일은 고치지 않는다). DB는 막는다.
      예: docker compose run --rm --no-deps airflow bash -c "cd /opt/project && /opt/airflow/spark-venv/bin/python -m measure.change_cost s6b"
  s6a-map 업종을 더하면 wh.map_dong_mois(업소 다수결, 상품 dong_mois_map)가 바뀌는지 본다. 더할 업종 행을
      배열 파라미터로 넘겨(읽기 전용 트랜잭션, 임시 표도 만들지 않음) wh/map_dong_mois.sql과 같은 다수결을 다시 내 지금 표와 비교한다.
      대조: wh.fact_store만으로 다시 낸 다수결이 지금 표와 같아야 이 재현을 믿는다. 실제 표에는 쓰지 않는다.
안전장치: 레이크는 읽기만 하고(s1, s2는 임시 폴더 사본), DB 접속은 막는다(접속 시도 = 정제가 통과해 적재 직전까지 왔다는 뜻).
사용법: python -m measure.change_cost {s1|s2|s3|s6a|s6a-map|s6b|unit ...}
"""
import collections
import contextlib
import importlib.util
import io
import shutil
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import transform.congestion as congestion
import transform.rent as rent
import transform.sbiz_store as sbiz_store
from common import ROOT

# s2 대상 처리 단위: 오피스텔, 강남구, 계약월 202610, 수집일 2026-11-03 (stg 통과 207행, 2026-11-12 확인)
S2_UNIT = ("offi", "2026-11-03", "11680", "202610")
NEW_FIELD = "newField"  # 원천에 새로 생겼다고 가정하는, 정제가 쓰지 않는 필드
# s1: 혼잡도 처리 단위는 수집일 하나. 새 형식은 최신 수집일의 사본을 개명해 만들고, 옛 수집일은 실제 과거 수집일
S1_NEW_DATE, S1_OLD_DATE = "2026-11-03", "2026-10-21"
OLD_NAME, NEW_NAME = "DPTRE_STTN", "STTN_NM"
# s3: C6 정정 전 마지막 커밋(2026-10-21). 이 커밋과 지금 transform/rent.py의 규칙 차이는 JIBUN 정규식과 대지구분 두 곳뿐이다
S3_OLD_COMMIT = "b824215"
# s6a: 더할 업종(2026-11-13 사용자 선택). 6코드 밖, 서울 202606 기준 3,570곳, 행정동 427개 중 97.7%에 있음
S6A_CODE, S6A_NAME = "R10307", "헬스장"
S6A_DATE = "2026-10-21"  # 레이크 상가정보 수집일(지금 하나뿐)


class DBBlocked(Exception):
    """측정 중 DB 접속을 막았다는 표시."""


def block_db(name):
    raise DBBlocked(f"DB 접속 차단({name}): 정제와 검증을 모두 통과해 적재 직전까지 왔다")


def run(module, tmp_root, argv, patches=None):
    """module.main()을 레이크 위치만 tmp_root로 바꿔 돌리고 (결과, 출력, 초)를 돌려준다. DB 접속은 막는다."""
    patches = {"ROOT": tmp_root, "read_env": block_db, **(patches or {})}
    saved = {name: getattr(module, name) for name in patches}
    saved_argv = sys.argv
    for name, value in patches.items():
        setattr(module, name, value)
    sys.argv = [module.__name__, *argv]
    out = io.StringIO()
    start = time.perf_counter()
    try:
        with contextlib.redirect_stdout(out):
            module.main()
        result = "끝까지 실행(DB 차단이 안 걸림, 측정 장치 오류)"
    except DBBlocked as e:
        result = f"통과: {e}"
    except SystemExit as e:
        result = f"실패(SystemExit): {e}"
    finally:
        for name, value in saved.items():
            setattr(module, name, value)
        sys.argv = saved_argv
    return result, out.getvalue().strip(), time.perf_counter() - start


def report(label, note, result, printed, secs):
    print(f"[{label}] {note}")
    if printed:
        print(f"  정제 출력: {printed}")
    print(f"  결과: {result}")
    print(f"  걸린 시간: {secs:.2f}초")


def s2():
    kind, collected_date, lawd_cd, deal_ymd = S2_UNIT
    real_load_bjd = rent.load_bjd_codes

    def load_bjd_from_real_lake():  # 법정동 코드표는 실제 레이크의 상가정보에서 읽기만 한다
        saved, rent.ROOT = rent.ROOT, ROOT
        try:
            return real_load_bjd()
        finally:
            rent.ROOT = saved

    def add_field(root):
        items = list(root.iter("item"))
        for item in items:
            ET.SubElement(item, NEW_FIELD).text = "x"
        return len(items)

    for label, edit in (("대조군(원본 그대로)", None), (f"변형(item마다 <{NEW_FIELD}> 추가)", add_field)):
        with tempfile.TemporaryDirectory() as tmp:
            src = ROOT / "lake" / f"molit_{kind}_rent" / f"collected_date={collected_date}"
            dst = Path(tmp) / "lake" / f"molit_{kind}_rent" / f"collected_date={collected_date}"
            dst.mkdir(parents=True)
            files = sorted(src.glob(f"{lawd_cd}_{deal_ymd}_p*.xml"))
            edited = 0
            for path in files:
                tree = ET.parse(path)
                edited += edit(tree.getroot()) if edit else 0
                tree.write(dst / path.name, encoding="utf-8", xml_declaration=True)
            outcome = run(rent, Path(tmp), S2_UNIT, {"load_bjd_codes": load_bjd_from_real_lake})
        report(label, f"파일 {len(files)}개" + (f", 필드를 덧붙인 item {edited}개" if edit else ""), *outcome)


def load_module(name, source, tmp):
    """소스 문자열을 tmp에 파일로 쓰고 별도 모듈로 불러온다(레포 파일은 건드리지 않는다)."""
    path = Path(tmp) / f"{name}.py"
    path.write_text(source, encoding="utf-8")
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def fixed_congestion(tmp):
    """흔한 고침: transform/congestion.py의 옛 이름을 새 이름으로 모두 바꾼 사본을 모듈로 불러온다."""
    source = (ROOT / "transform" / "congestion.py").read_text(encoding="utf-8")
    return load_module("congestion_fixed", source.replace(OLD_NAME, NEW_NAME), tmp), source.count(OLD_NAME)


def s1():
    def rename(root):
        found = [e for e in root.iter(OLD_NAME)]
        for e in found:
            e.tag = NEW_NAME
        return len(found)

    lake = Path("lake") / "seoul_subway_congestion"
    cases = (  # (이름, 입력 수집일, 개명 여부, 흔한 고침 여부)
        ("① 대조군: 새 수집일 원본 + 지금 코드", S1_NEW_DATE, False, False),
        (f"② 개명된 입력({OLD_NAME} -> {NEW_NAME}) + 지금 코드", S1_NEW_DATE, True, False),
        ("③ 개명된 입력 + 흔한 고침", S1_NEW_DATE, True, True),
        ("④ 옛 수집일 원본(개명 전) + 흔한 고침", S1_OLD_DATE, False, True),
    )
    for label, day, renamed, fixed in cases:
        with tempfile.TemporaryDirectory() as tmp:
            src = ROOT / lake / f"collected_date={day}"
            dst = Path(tmp) / lake / f"collected_date={day}"
            dst.mkdir(parents=True)
            edited = 0
            for path in sorted(src.glob("subwConfusion_p*.xml")):  # 분기 파일(csv, xlsx)은 이 정제가 읽지 않는다
                if renamed:
                    tree = ET.parse(path)
                    edited += rename(tree.getroot())
                    tree.write(dst / path.name, encoding="utf-8", xml_declaration=True)
                else:
                    shutil.copy2(path, dst / path.name)
            module, places = fixed_congestion(tmp) if fixed else (congestion, None)
            outcome = run(module, Path(tmp), [day])
        note = f"수집일 {day}" + (f", 개명한 요소 {edited}개" if renamed else "") + \
               (f", 정제 파일에서 바꾼 곳 {places}곳" if fixed else "")
        report(label, note, *outcome)


def peak_mb():
    import psutil  # Windows .venv에만 있다(s6b가 도는 컨테이너 spark-venv에는 없음)
    return psutil.Process().memory_info().peak_wset / 2**20  # Windows 최대 작업 집합(프로세스가 쓴 물리 메모리의 최댓값)


def s3():
    start = time.perf_counter()
    source = subprocess.run(["git", "show", f"{S3_OLD_COMMIT}:transform/rent.py"], cwd=ROOT,
                            capture_output=True, encoding="utf-8", check=True).stdout
    with tempfile.TemporaryDirectory() as tmp:
        old = load_module("rent_old", source, tmp)
    bjd_codes = rent.load_bjd_codes()  # 코드표는 두 규칙에 같은 것을 써서 규칙 차이만 본다
    print(f"법정동 코드표 {len(bjd_codes)}개, {time.perf_counter() - start:.2f}초, 최대 메모리 {peak_mb():.0f}MB")

    units = collections.defaultdict(list)  # 처리 단위(종류, 수집일, 구, 계약월) -> XML 파일들
    for kind in rent.EXPECTED_FIELDS:
        for path in sorted((ROOT / "lake" / f"molit_{kind}_rent").glob("collected_date=*/*_p*.xml")):
            lawd_cd, deal_ymd, _ = path.name.split("_")
            units[(kind, path.parent.name.removeprefix("collected_date="), lawd_cd, deal_ymd)].append(path)

    changed, san_units = collections.Counter(), set()  # 결과가 달라진 단위별 item 수, 산 지번이 있는 단위
    items = 0
    for unit, paths in units.items():
        kind, _, lawd_cd, deal_ymd = unit
        for path in paths:
            for raw in rent.read_items(path):
                items += 1
                if raw.get("jibun", "").startswith("산"):
                    san_units.add(unit)
                if rent.clean(raw, kind, deal_ymd, lawd_cd, bjd_codes) != old.clean(raw, kind, deal_ymd, lawd_cd, bjd_codes):
                    changed[unit] += 1
    print(f"레이크 처리 단위 {len(units)}개, XML {sum(map(len, units.values()))}개, item {items}개")
    print(f"규칙 정정으로 결과가 달라지는 처리 단위(정답) {len(changed)}개, item {sum(changed.values())}개")
    print(f"  종류별 단위: {dict(collections.Counter(u[0] for u in changed))}")
    print(f"  수집일별 단위: {dict(sorted(collections.Counter(u[1] for u in changed).items()))}")
    print(f"규칙 전용 고르기(지번이 '산'으로 시작하는 item이 있는 단위) {len(san_units)}개, "
          f"정답과 같음: {san_units == set(changed)}")
    print(f"전부 다시 돌리면 {len(units)}개 중 {len(units) - len(changed)}개는 결과가 같은데 다시 돈다")
    print(f"걸린 시간 {time.perf_counter() - start:.2f}초, 최대 메모리 {peak_mb():.0f}MB")


def s6a():
    cases = (("대조군: 지금 6코드", sbiz_store.ESSENTIAL),
             (f"변경: 7코드(+{S6A_NAME} {S6A_CODE})", {**sbiz_store.ESSENTIAL, S6A_CODE: S6A_NAME}))
    for label, essential in cases:
        outcome = run(sbiz_store, ROOT, [S6A_DATE], {"ESSENTIAL": essential})  # 레이크 zip은 읽기만 한다
        report(label, f"수집일 {S6A_DATE}, 코드 {len(essential)}개", *outcome)
    print(f"  최대 메모리(두 경우 합친 프로세스): {peak_mb():.0f}MB")


# s6b: 생활이동 한 달(Iceberg). 귀가 방향 이동유형은 WH(주간상주지 -> 야간상주지, data_sources.md 생활이동 절)
S6B_UNIT = ("2026-10-21", "202607")
S6B_EDITS = (('HOURS = ("07", "08", "09")', 'HOURS = ("18", "19", "20")'),  # 흔한 고침: 정확히 이 세 곳만 바꾼다
             ('raw["이동유형"] != "HW"', 'raw["이동유형"] != "WH"'),
             ('F.col("move_type") != "HW"', 'F.col("move_type") != "WH"'))


def s6b():
    import transform.living_migration as living
    source = (ROOT / "transform" / "living_migration.py").read_text(encoding="utf-8")
    for old, new in S6B_EDITS:
        assert source.count(old) == 1, f"고칠 곳이 정확히 한 곳이 아니다: {old}"
        source = source.replace(old, new)
    with tempfile.TemporaryDirectory() as tmp:
        changed = load_module("living_evening", source, tmp)
        for label, module in (("대조군: 지금 코드(HW, 07~09시)", living), ("변경: 흔한 고침(WH, 18~20시)", changed)):
            outcome = run(module, ROOT, list(S6B_UNIT))  # 레이크·Iceberg는 읽기만 한다
            report(label, f"수집일 {S6B_UNIT[0]}, 대상연월 {S6B_UNIT[1]}, 정제 파일에서 바꾼 곳 {len(S6B_EDITS) if module is changed else 0}곳", *outcome)


# wh/map_dong_mois.sql의 다수결을 그대로 옮겼다. {fact}는 (dong_id, mois_dong_cd)를 내는 표
VOTE_SQL = """
with vote as (
    select dong_id, mois_dong_cd, count(*) as stores,
           count(*)::numeric / sum(count(*)) over (partition by dong_id) as share,
           row_number() over (partition by dong_id order by count(*) desc, mois_dong_cd) as rank
    from {fact} where dong_id is not null
    group by dong_id, mois_dong_cd
),
ranked as (
    select v.*, row_number() over (partition by mois_dong_cd order by stores desc, dong_id) as rank_in_code
    from vote v
)
select mois_dong_cd, dong_id, stores::integer, round(share, 3)::numeric(4, 3) as share, rank = 1 as is_primary
from ranked
where rank = 1 or (rank_in_code = 1 and mois_dong_cd not in (select mois_dong_cd from vote where rank = 1))
"""
# 더할 업종 행(배열 파라미터)에 wh/fact_store.sql과 같은 공간 조인으로 dong_id를 붙이고, 두 다수결을 CTE로 만든다
CTES = """
with added as (
    select b.adm_cd as dong_id, a.mois_dong_cd
    from unnest(%(cd)s::text[], %(lon)s::float8[], %(lat)s::float8[]) a(mois_dong_cd, lon, lat)
    left join stg.sgis_dong b
      on b.collected_date = (select max(collected_date) from stg.sgis_dong)
     and ST_Contains(b.geom, ST_Transform(ST_SetSRID(ST_MakePoint(a.lon, a.lat), 4326), 5179))
),
map_control as ({control}),
map_changed as ({changed})
"""


def s6a_map():
    import psycopg
    from common import read_env
    zip_path = next((ROOT / "lake" / "sbiz_store" / f"collected_date={S6A_DATE}").glob("*.zip"))
    saved, sbiz_store.ESSENTIAL = sbiz_store.ESSENTIAL, {S6A_CODE: S6A_NAME}  # 더할 업종 행만 고른다
    try:
        _, _, rows = sbiz_store.read_seoul(zip_path)
    finally:
        sbiz_store.ESSENTIAL = saved
    good, bad = sbiz_store.split(rows)
    print(f"더할 업종 {S6A_NAME} {S6A_CODE}: 통과 {len(good)}, 격리 {len(bad)}")
    params = {"cd": [r["adm_cd"] for r in good], "lon": [r["lon"] for r in good], "lat": [r["lat"] for r in good]}
    ctes = CTES.format(control=VOTE_SQL.format(fact="wh.fact_store"), changed=VOTE_SQL.format(
        fact="(select dong_id, mois_dong_cd from wh.fact_store union all select dong_id, mois_dong_cd from added) f"))

    def q(body):
        return conn.execute(ctes + body, params).fetchall()

    key = "select mois_dong_cd, dong_id, is_primary from "
    with psycopg.connect(read_env("DATABASE_URL")) as conn:
        conn.read_only = True  # 이 측정은 DB에 아무것도 쓰지 않는다. 쓰려 하면 DB가 거부한다
        inside, outside = q("select count(dong_id), count(*) - count(dong_id) from added")[0]
        print(f"  공간 조인: 행정동 안 {inside}, 경계 밖(dong_id NULL) {outside}")
        total = q("select count(*) from wh.map_dong_mois")[0][0]
        for label, t in (("대조(wh.fact_store만)", "map_control"), (f"변경(+{S6A_NAME})", "map_changed")):
            n, only_t, only_now = q(f"select (select count(*) from {t}),"
                                    f" (select count(*) from (select * from {t} except all select * from wh.map_dong_mois) x),"
                                    f" (select count(*) from (select * from wh.map_dong_mois except all select * from {t}) y)")[0]
            diff = q(f"select '추가', * from ({key}{t} except all {key}wh.map_dong_mois) x union all "
                     f"select '삭제', * from ({key}wh.map_dong_mois except all {key}{t}) y order by 2, 1")
            print(f"[{label}] 행 {n} (지금 표 {total}), 전체 컬럼 차이: 이쪽에만 {only_t}, 지금 표에만 {only_now}, "
                  f"대응(코드, 동, 1위) 차이 {len(diff)}행")
            for row in diff:
                print("   ", *row)
        moved = q("select count(*) from (select dong_id, mois_dong_cd from map_changed where is_primary"
                  " except select dong_id, mois_dong_cd from wh.map_dong_mois where is_primary) x")[0][0]
        print(f"  1위 코드가 바뀐 행정동(dim_dong에 번짐): {moved}")
    print(f"  최대 메모리: {peak_mb():.0f}MB")


def unit():
    outcome = run(rent, ROOT, sys.argv[2:])
    report("실제 처리 단위 하나: " + " ".join(sys.argv[2:]), "", *outcome)
    print(f"  최대 메모리: {peak_mb():.0f}MB")


if __name__ == "__main__":
    {"s1": s1, "s2": s2, "s3": s3, "s6a": s6a, "s6a-map": s6a_map, "s6b": s6b, "unit": unit}[sys.argv[1]]()
