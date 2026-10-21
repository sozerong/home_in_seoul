"""사이트에서 손으로 받은 zip을 검사한 뒤 레이크의 정해진 자리에 복사한다.

사용법: python ingestion/register_file.py {출처} 받은파일경로
예:     python ingestion/register_file.py sgis_boundary "C:\\Users\\me\\Downloads\\bnd_dong_00_2025_2Q.zip"
"""
import argparse
import datetime
import fnmatch
import hashlib
import shutil
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def has_shapefile(names):
    return all(any(n.lower().endswith(ext) for n in names) for ext in (".shp", ".shx", ".dbf", ".prj"))


# 출처: (레이크 폴더, 허용하는 파일 이름 모양, 꼭 있어야 할 것 설명, 검사 함수)
SOURCES = {
    "living_migration": ("seoul_living_migration", ["생활이동_행정동_*.zip", "생활이동_자치구_*.zip"],
                         "CSV 24개(도착시간 00~23시)",
                         lambda names: sum(n.endswith(".csv") for n in names) == 24),
    "sgis_boundary": ("sgis_boundary", ["bnd_dong_*.zip"], ".shp .shx .dbf .prj", has_shapefile),
    "parcel_boundary": ("parcel_boundary", ["LSMD_CONT_LDREG_*.zip"], ".shp .shx .dbf .prj", has_shapefile),
    "sbiz_store": ("sbiz_store", ["소상공인시장진흥공단_상가(상권)정보_*.zip"], "이름에 '서울'이 든 CSV",
                   lambda names: any("서울" in n and n.endswith(".csv") for n in names)),
}

def member_names(zf):
    names = []
    for info in zf.infolist():
        name = info.filename
        if not info.flag_bits & 0x800:  # UTF-8 표시가 없는 zip은 한글 이름이 CP949로 들어 있다
            name = name.encode("cp437").decode("cp949", errors="replace")
        names.append(name)
    return names


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("name", choices=SOURCES)
    parser.add_argument("path")
    args = parser.parse_args()

    folder, patterns, required, has_required = SOURCES[args.name]
    src = Path(args.path)
    if not src.is_file():
        raise SystemExit(f"파일이 없습니다: {src}")
    if not any(fnmatch.fnmatch(src.name, p) for p in patterns):
        raise SystemExit(f"{args.name}에 맞지 않는 파일 이름: {src.name} (허용: {', '.join(patterns)})")

    try:
        # UTF-8 표시가 없는 zip(상가정보 등)은 한글 파일 이름이 CP949로 들어 있다
        with zipfile.ZipFile(src, metadata_encoding="cp949") as zf:
            broken = zf.testzip()
            names = zf.namelist()
    except zipfile.BadZipFile:
        raise SystemExit(f"zip 파일이 아닙니다: {src.name}")
    if broken is not None:
        raise SystemExit(f"zip이 손상되었습니다. 처음 깨진 파일: {broken}")
    if not has_required(names):
        raise SystemExit(f"zip 구성이 예상과 다릅니다. 필요한 것: {required}")

    today = datetime.date.today().isoformat()
    dest = ROOT / "lake" / folder / f"collected_date={today}" / src.name
    digest = sha256(src)
    if dest.exists():
        if sha256(dest) == digest:
            print(f"이미 같은 파일이 있습니다: {dest} (sha256 {digest})")
            return
        raise SystemExit(f"같은 이름의 다른 내용 파일이 이미 있습니다: {dest}")

    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    shutil.copyfile(src, part)
    if sha256(part) != digest:
        part.unlink()
        raise SystemExit("복사본의 해시가 원본과 다릅니다")
    part.replace(dest)
    print(f"등록: {dest} ({dest.stat().st_size:,} B, sha256 {digest})")


if __name__ == "__main__":
    main()