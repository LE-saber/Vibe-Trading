"""Focused synthetic reproductions for the independent S6 safety review.

Regression guards for independently reproduced false-ready behavior.
All accounts and prices here are synthetic, including declared-source branches.
"""

from datetime import timedelta
from decimal import Decimal

import pytest

from competition_core.pipeline.account import AccountSnapshot
from competition_core.pipeline.portfolio import build_portfolio
from competition_core.pipeline.portfolio_runner import read_portfolio, run_portfolio_draft
from competition_core.pipeline.instruments import quantity_rules
from competition_core.pipeline.rebalance_quantity import plan_quantity
from tests.integration.test_portfolio_pipeline import setup
from tests.unit.test_pipeline_portfolio import inputs


def test_review_finding_sealed_read_rejects_changed_account_state(tmp_path):
    """Changed cash/pending orders cannot retain current execution readiness."""

    repository, kwargs = setup(tmp_path, declared=True)
    sealed = run_portfolio_draft(**kwargs, draft_id="review-stale-account")
    changed = AccountSnapshot.from_dict(
        kwargs["account"].to_dict()
        | {"available_cash": "0.00", "pending_order_count": 1}
    )

    current = read_portfolio(
        repository=repository,
        observed_at=kwargs["account"].as_of + timedelta(seconds=1),
        account=changed,
    )

    assert changed.snapshot_hash != sealed["summary"]["account_hash"]
    assert changed.pending_order_count == 1 and changed.available_cash == Decimal("0.00")
    assert current["current_execution_ready"] is False
    assert "ACCOUNT_CHANGED_SINCE_DRAFT" in current["current_account_revalidation"]["reasons"]
    assert "PENDING_ORDERS_REQUIRE_RECONCILIATION" in current["current_account_revalidation"]["reasons"]


@pytest.mark.parametrize("raw_hash", [None, "a" * 64])
def test_current_read_requires_explicit_unchanged_account_and_source(tmp_path, raw_hash):
    repository, kwargs = setup(tmp_path, declared=True)
    sealed = run_portfolio_draft(**kwargs, draft_id="fresh-account", account_source_hash=raw_hash)
    assert sealed["current_execution_ready"]
    params = dict(repository=repository, observed_at=kwargs["account"].as_of + timedelta(seconds=1))
    assert not read_portfolio(**params)["current_execution_ready"]
    assert read_portfolio(**params, account=kwargs["account"], account_source_hash=raw_hash)["current_execution_ready"]
    if raw_hash:
        for mismatch in (None, "b" * 64):
            result = read_portfolio(**params, account=kwargs["account"], account_source_hash=mismatch)
            assert not result["current_execution_ready"]
            assert "ACCOUNT_SOURCE_NOT_REVALIDATED" in result["current_account_revalidation"]["reasons"]


def test_review_finding_order_maximum_cannot_publish_incomplete_target():
    """A capped STAR order blocks the draft; no implicit order splitting."""

    params = inputs()
    old_symbol, star_symbol = "600001.SH", "688001.SH"
    for row in params["candidates"][:1] + params["reviews"][:1]:
        row["symbol"] = star_symbol
    params["quotes"][0].update(
        symbol=star_symbol,
        price=0.50,
        prev_close=0.50,
        open=0.50,
        high=0.505,
        low=0.495,
        bid1=0.499,
        ask1=0.501,
        amount=500000.0,
    )
    status = params["trading_metadata"].pop(old_symbol)
    params["trading_metadata"][star_symbol] = status | {
        "up_limit": "0.55",
        "down_limit": "0.45",
    }
    params["account"] = AccountSnapshot.from_dict(
        params["account"].to_dict() | {"source_kind": "manual_user"}
    )

    result = build_portfolio(**params)
    row = next(row for row in result["rows"] if row["symbol"] == star_symbol)

    assert result["portfolio_status"] == "NOT_READY"
    assert result["execution_ready"] is False
    assert row["delta_quantity"] is None and row["target_quantity"] is None
    assert f"TARGET_REQUIRES_MULTIPLE_ORDERS:{star_symbol}" in result["risk_checks"]["blocking_reasons"]
    assert result["risk_checks"]["attempted_plan_hash"]


@pytest.mark.parametrize("symbol", ["600001.SH", "300750.SZ", "688001.SH"])
def test_fractional_budget_leftover_is_not_false_multi_order_requirement(symbol):
    rules = quantity_rules(symbol, inputs()["context"].research_date)
    result = plan_quantity(current_quantity=0, sellable_quantity=0,
        target_value=Decimal(rules.limit_order_max) + Decimal("0.50"), reference_price=Decimal("1"),
        rules=rules, slippage_buffer=Decimal("0"))
    assert result["delta_quantity"] == rules.limit_order_max
    assert "single_order_maximum" not in result["reasons"]
