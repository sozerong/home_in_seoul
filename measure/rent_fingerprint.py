"""측정 1, 결정 18: 같은 계약월을 두 수집일에 받은 레이크 XML을 거래 지문으로 비교한다.

지문 = item의 모든 칸 값 묶음. 동·호가 없어 같은 지문의 거래가 여러 건일 수 있으므로 개수까지 센다(Counter 차집합).
  추가 = 나중 수집에만 있는 거래(늦은 신고), 사라짐 = 처음 수집에만 있는 거래(취소, 또는 칸 값 정정).
  칸 하나가 정정되면 "사라짐 1 + 추가 1"로 보인다.
적재에는 쓰지 않는다(적재는 처리 단위 스냅샷 교체, 결정 18 C).
사용법: python -m measure.rent_fingerprint 처음수집일 나중수집일 계약월...
예:     python -m measure.rent_fingerprint 2026-10-21 2026-10-28 202607 202608 202609
"""
import sys
from collections import Counter

from common import ROOT
from transform.rent import read_items


def fingerprints(kind, collected_date, deal_ymd):
    folder = ROOT / "lake" / f"molit_{kind}_rent" / f"collected_date={collected_date}"
    files = sorted(folder.glob(f"*_{deal_ymd}_p*.xml"))
    if len(files) < 25:  # 25개 구가 모두 있어야 비교한다
        raise SystemExit(f"{folder}에 {deal_ymd} 파일이 {len(files)}개뿐입니다")
    return Counter(tuple(sorted(item.items())) for path in files for item in read_items(path))


def compare(old, new):
    return sum(old.values()), sum(new.values()), sum((new - old).values()), sum((old - new).values())


def main():
    old_date, new_date, *months = sys.argv[1:]
    print("출처|계약월|처음|나중|추가|사라짐|순증|순증률")
    for kind in ("offi", "rh"):
        for ym in months:
            n_old, n_new, added, removed = compare(fingerprints(kind, old_date, ym), fingerprints(kind, new_date, ym))
            print(f"{kind}|{ym}|{n_old}|{n_new}|{added}|{removed}|{n_new - n_old:+}|{(n_new - n_old) / n_old:+.2%}")


def selftest():
    a = Counter({("x",): 2, ("y",): 1})
    b = Counter({("x",): 1, ("y",): 1, ("z",): 3})
    assert compare(a, b) == (3, 5, 3, 1)  # 같은 지문 2건 중 1건이 사라지고, 새 지문 3건이 생김
    print("selftest ok")


if __name__ == "__main__":
    selftest() if sys.argv[1:] == ["--selftest"] else main()
