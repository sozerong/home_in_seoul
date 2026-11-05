"""운영 대시보드 카드 설정 확인 (결정 39). Metabase 없이 돈다."""
from common import ROOT
from warehouse.metabase_setup import RED, STATUS_COLORS, card_settings, read_card


def test_alert_turns_red_at_limit():
    first, second = card_settings({"alert": "0.5"})["scalar.segments"]
    assert first["max"] == second["min"] == 0.5
    assert second["color"] == RED


def test_status_colors_every_value():
    rules = card_settings({"status": "상태"})["table.column_formatting"]
    assert {r["value"] for r in rules} == set(STATUS_COLORS)
    assert all(r["columns"] == ["상태"] for r in rules)


def test_cards_do_not_overlap():
    cells = set()
    paths = sorted((ROOT / "warehouse/sql/ops").glob("*.sql"))
    assert paths
    for path in paths:
        row, col, width, height = map(int, read_card(path)[0]["pos"].split())
        assert col + width <= 24
        area = {(r, c) for r in range(row, row + height) for c in range(col, col + width)}
        assert not area & cells, path.name
        cells |= area


if __name__ == "__main__":
    test_alert_turns_red_at_limit()
    test_status_colors_every_value()
    test_cards_do_not_overlap()
    print("metabase 카드 시험 통과")
