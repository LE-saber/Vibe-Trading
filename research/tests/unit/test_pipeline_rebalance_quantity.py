"""Synthetic position arithmetic, not competition-account acceptance."""

from datetime import date
from decimal import Decimal

import pytest

from competition_core.pipeline.instruments import quantity_rules
from competition_core.pipeline.rebalance_quantity import plan_quantity


def plan(symbol="600519.SH", **overrides):
    params = dict(current_quantity=0, sellable_quantity=0, target_value=Decimal("10000"),
                  reference_price=Decimal("10"), rules=quantity_rules(symbol, date(2026, 9, 3)),
                  slippage_buffer=Decimal("0.005"))
    return plan_quantity(**(params | overrides))


def test_buy_uses_buffered_price_and_raw_inventory():
    result = plan(current_quantity=150, sellable_quantity=50)
    assert result["delta_quantity"] == 800 and result["target_quantity"] == 950
    assert result["order_price"] == "10.05" and result["gross_buy_cash"] == "8040.00"
    assert result["target_market_value"] == "9500.00"


@pytest.mark.parametrize("symbol,expected", [("600519.SH", 200), ("300750.SZ", 200), ("688001.SH", 248)])
def test_board_buy_increment(symbol, expected):
    assert plan(symbol, target_value=Decimal("2500"))["delta_quantity"] == expected


@pytest.mark.parametrize("symbol", ["600519.SH", "300750.SZ", "688001.SH"])
def test_odd_lot_cleanup_requires_all_sellable(symbol):
    cleared = plan(symbol, current_quantity=51, sellable_quantity=51, target_value=0)
    assert cleared["side"] == "SELL" and cleared["target_quantity"] == 0
    locked = plan(symbol, current_quantity=51, sellable_quantity=50, target_value=0)
    assert locked["side"] == "HOLD" and locked["target_quantity"] == 51
    assert "sellable_quantity_constraint" in locked["reasons"]


def test_locked_position_is_retained_and_sale_cash_is_net_of_price_buffer_only():
    result = plan(current_quantity=1000, sellable_quantity=300, target_value=0)
    assert result["delta_quantity"] == -300 and result["target_quantity"] == 700
    assert result["gross_sell_proceeds"] == "2985.00" and result["gross_buy_cash"] == "0.00"
    assert "residual_position_above_target_budget" in result["reasons"]


def test_star_sell_can_reduce_one_share_increments_above_minimum():
    result = plan("688001.SH", current_quantity=1000, sellable_quantity=1000, target_value="5505")
    assert result["delta_quantity"] == -450 and result["target_quantity"] == 550


@pytest.mark.parametrize("symbol,maximum", [("600519.SH", 1000000), ("300750.SZ", 300000), ("688001.SH", 100000)])
def test_one_order_maximum_is_not_bypassed(symbol, maximum):
    result = plan(symbol, target_value="100000000")
    assert result["delta_quantity"] == maximum and "single_order_maximum" in result["reasons"]


@pytest.mark.parametrize("changes", [
    {"current_quantity": True}, {"sellable_quantity": -1}, {"sellable_quantity": 1},
    {"target_value": "NaN"}, {"target_value": -1}, {"reference_price": "Infinity"},
    {"reference_price": "10.001"}, {"reference_price": 0}, {"slippage_buffer": True},
    {"slippage_buffer": "0.11"}, {"target_value": []}, {"rules": None},
])
def test_invalid_inputs_fail(changes):
    with pytest.raises(ValueError):
        plan(**changes)


def test_no_division_roundoff_at_buy_boundary():
    result = plan(target_value="1005.00")
    assert result["delta_quantity"] == 100
    assert plan(target_value="1004.99")["delta_quantity"] == 0


def test_tiny_sell_price_does_not_make_zero_price_order():
    result = plan(current_quantity=100, sellable_quantity=100, target_value=0, reference_price="0.01")
    assert result["side"] == "HOLD" and result["order_price"] is None
