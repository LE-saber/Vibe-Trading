import json
from datetime import date, datetime
from decimal import Decimal

import pytest

from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.instruments import (
    EFFECTIVE_FROM,
    RULES_VERIFIED_ON,
    SUPPORTED_THROUGH,
    QuantityRules,
    floor_buy_quantity,
    is_valid_quantity,
    quantity_rules,
)
from contracts.symbol import CanonicalSymbol, SymbolContractError


RULE_DATE = date(2026, 9, 2)


@pytest.mark.parametrize(
    ("symbol", "board", "limit_max", "market_max", "min_buy", "step"),
    [
        ("600000.SH", "MAIN", 1_000_000, 1_000_000, 100, 100),
        ("000001.SZ", "MAIN", 1_000_000, 1_000_000, 100, 100),
        ("300750.SZ", "CHINEXT", 300_000, 150_000, 100, 100),
        ("688001.SH", "STAR", 100_000, 50_000, 200, 1),
    ],
)
def test_quantity_rules_are_board_specific_and_source_backed(
    symbol, board, limit_max, market_max, min_buy, step
):
    rules = quantity_rules(symbol, RULE_DATE)

    assert isinstance(rules, QuantityRules)
    assert rules.board == board
    assert rules.min_buy == min_buy
    assert rules.buy_step == step
    assert rules.limit_order_max == limit_max
    assert rules.market_order_max == market_max
    assert rules.tick_size == Decimal("0.01")
    assert rules.effective_from == EFFECTIVE_FROM
    assert rules.supported_through == SUPPORTED_THROUGH
    assert rules.rules_verified_on == RULES_VERIFIED_ON
    assert all(url.startswith("https://") for url in rules.source_urls)


def test_star_metadata_includes_education_source_and_one_share_increment():
    rules = quantity_rules("688001.SH", RULE_DATE)

    assert "https://edu.sse.com.cn/tib/qa/" in rules.source_urls
    assert rules.min_sell == 200
    assert rules.sell_step == 1
    assert rules.is_valid_quantity("BUY", 200, 0, 0)
    assert rules.is_valid_quantity("BUY", 201, 0, 0)
    assert not rules.is_valid_quantity("BUY", 199, 0, 0)


def test_main_buy_boundaries_and_conservative_sell_baseline():
    rules = quantity_rules("600000.SH", RULE_DATE)

    assert rules.sell_policy == "conservative_whole_lot_or_full_cleanup_MAIN_CHINEXT"

    assert not rules.is_valid_quantity("BUY", 99, 0, 0)
    assert rules.is_valid_quantity("BUY", 100, 0, 0)
    assert not rules.is_valid_quantity("BUY", 101, 0, 0)

    assert rules.is_valid_quantity("SELL", 100, 101, 100)
    assert rules.is_valid_quantity("SELL", 101, 101, 101)
    assert not rules.is_valid_quantity("SELL", 101, 101, 100)
    assert not rules.is_valid_quantity("SELL", 1, 101, 1)
    assert rules.is_valid_quantity("SELL", 1, 1, 1)


def test_chinext_has_same_buy_step_but_separate_order_caps():
    rules = quantity_rules("300750.SZ", RULE_DATE)

    assert rules.is_valid_quantity("BUY", 300_000, 0, 0, "LIMIT")
    assert not rules.is_valid_quantity("BUY", 300_100, 0, 0, "LIMIT")
    assert rules.is_valid_quantity("BUY", 150_000, 0, 0, "MARKET")
    assert not rules.is_valid_quantity("BUY", 150_100, 0, 0, "MARKET")
    assert rules.is_valid_quantity("SELL", 199, 199, 199)
    assert not rules.is_valid_quantity("SELL", 199, 200, 199)


def test_star_sell_boundaries_and_explicit_sellable_quantity():
    rules = quantity_rules("688001.SH", RULE_DATE)

    assert rules.is_valid_quantity("SELL", 199, 199, 199)
    assert rules.is_valid_quantity("SELL", 200, 200, 200)
    assert rules.is_valid_quantity("SELL", 201, 201, 201)
    assert not rules.is_valid_quantity("SELL", 199, 200, 199)
    assert not rules.is_valid_quantity("SELL", 200, 200, 0)
    assert rules.is_valid_quantity("SELL", 50_000, 100_000, 50_000, "MARKET")
    assert not rules.is_valid_quantity("SELL", 50_001, 100_000, 50_001, "MARKET")


def test_module_level_quantity_validator_is_equivalent_to_method():
    rules = quantity_rules("600000.SH", RULE_DATE)

    expected = rules.is_valid_quantity("SELL", 101, 101, 101)
    assert is_valid_quantity(rules, "SELL", 101, 101, 101) is expected


def test_sellable_quantity_is_not_inferred_and_t_plus_one_is_locked():
    rules = quantity_rules("000001.SZ", RULE_DATE)

    assert not rules.is_valid_quantity("SELL", 100, 200, 0)
    assert not rules.is_valid_quantity("SELL", 100, 200, None)
    assert rules.is_valid_quantity("SELL", 100, 200, 100)
    assert not rules.is_valid_quantity("SELL", 100, 100, 101)


@pytest.mark.parametrize("value", [True, False, float("nan"), -1, 100.0, Decimal("100")])
def test_non_integer_or_invalid_quantities_are_rejected(value):
    rules = quantity_rules("600000.SH", RULE_DATE)

    assert not rules.is_valid_quantity("BUY", value, 0, 0)
    assert not rules.is_valid_quantity("SELL", value, 100, 100)


def test_decimal_budget_boundary_and_zero_budget():
    rules = quantity_rules("600000.SH", RULE_DATE)

    assert floor_buy_quantity(Decimal("19.99"), Decimal("0.20"), rules) == 0
    assert floor_buy_quantity(Decimal("20.00"), Decimal("0.20"), rules) == 100
    assert floor_buy_quantity(19.99, 0.20, rules) == 0
    assert floor_buy_quantity(0, Decimal("10"), rules) == 0


def test_floor_buy_quantity_caps_one_order_without_splitting():
    rules = quantity_rules("300750.SZ", RULE_DATE)

    result = floor_buy_quantity(Decimal("1000000000"), Decimal("1"), rules)

    assert result == rules.limit_order_max
    assert rules.is_valid_quantity("BUY", result, 0, 0)


@pytest.mark.parametrize(
    ("budget", "price"),
    [
        (True, Decimal("1")),
        (Decimal("NaN"), Decimal("1")),
        (Decimal("-0.01"), Decimal("1")),
        (Decimal("1"), False),
        (Decimal("1"), Decimal("NaN")),
        (Decimal("1"), Decimal("0")),
        (Decimal("1"), Decimal("-1")),
    ],
)
def test_floor_buy_quantity_rejects_invalid_numeric_inputs(budget, price):
    rules = quantity_rules("600000.SH", RULE_DATE)

    with pytest.raises((TypeError, ValueError)):
        floor_buy_quantity(budget, price, rules)


def test_metadata_is_immutable_and_json_serializable():
    rules = quantity_rules("688001.SH", RULE_DATE)
    payload = rules.to_dict()

    assert json.loads(json.dumps(payload)) == payload
    assert payload["effective_from"] == "2026-07-06"
    assert payload["supported_through"] == "2026-09-30"
    assert payload["rules_verified_on"] == "2026-09-03"
    assert payload["source_urls"]
    assert payload["sell_policy"] == "minimum_200_step_1_or_full_odd_lot_cleanup_STAR"
    assert payload["tick_size"] == "0.01"
    payload["source_urls"].clear()
    assert rules.source_urls
    with pytest.raises((AttributeError, TypeError)):
        rules.board = "MAIN"


@pytest.mark.parametrize("as_of", [date(2026, 7, 5), date(2026, 10, 1), date(2025, 1, 1)])
def test_historical_or_unverified_dates_fail_closed(as_of):
    with pytest.raises(PipelineError) as error:
        quantity_rules("600000.SH", as_of)

    assert error.value.code == "NOT_READY"


def test_datetime_is_not_accepted_as_a_date_and_end_boundary_is_inclusive():
    with pytest.raises(TypeError):
        quantity_rules("600000.SH", datetime(2026, 9, 2))
    assert quantity_rules("600000.SH", date(2026, 9, 30)).supported_through == date(2026, 9, 30)


@pytest.mark.parametrize("symbol", ["510300.SH", "159915.SZ", "600000.SZ", "600000.SH "])
def test_etf_or_conflicting_symbol_is_rejected(symbol):
    with pytest.raises((SymbolContractError, ValueError)):
        quantity_rules(symbol, RULE_DATE)


def test_existing_canonical_lot_size_contract_remains_unchanged():
    assert CanonicalSymbol.LOT_SIZE == 100
