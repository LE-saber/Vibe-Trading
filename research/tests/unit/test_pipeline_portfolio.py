"""All account, status, price and fee inputs here are explicit synthetic fixtures."""

from copy import deepcopy
from datetime import date, timedelta
from decimal import Decimal

import pytest

from competition_core.pipeline.account import AccountSnapshot, AccountPosition
from competition_core.pipeline.config import load_pipeline_config, PipelineConfig
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.portfolio import FeeSchedule, build_portfolio
from competition_core.pipeline.holding_review_consumer import disposition_for
from tests.pipeline_fixtures import context
from tests.integration.test_realtime_pipeline import quotes as quote_fixture


def inputs():
    ctx = context()
    candidates = [{"symbol": f"600{i + 1:03d}.SH", "quant_score": 90 - i, "selection_score": 85 - i,
                   "overheat_score": 90 if i % 3 == 0 else 50, "correlation_cluster": f"cluster-{i}",
                   "sector_metadata": {"status": "unavailable"}, "correlation_to_selected": {}}
                  for i in range(12)]
    reviews = [c | {"ai_status": "SUCCESS", "ai_eligible": True, "ai_verdict": "WATCH",
                    "ai_review": {"risks": ["synthetic risk"], "missing_critical_info": []}}
               for c in candidates]
    symbols = [c["symbol"] for c in candidates]
    account = AccountSnapshot(as_of=ctx.as_of, source="explicit.synthetic.fixture", source_kind="synthetic_test",
        currency="CNY", cash_balance=Decimal("1000000"), available_cash=Decimal("1000000"),
        current_total_asset=Decimal("1000000"), positions=(), pending_order_count=0)
    fees = FeeSchedule(source="synthetic.costs.not.actual", as_of=ctx.as_of,
        valid_through=date(2026, 9, 30), commission_rate=Decimal("0.001"), minimum_commission=Decimal("5"),
        buy_other_rate=Decimal("0.0001"), sell_other_rate=Decimal("0.0005"))
    metadata = {s: {"source": "explicit.synthetic.status", "as_of": ctx.as_of.isoformat(),
        "trade_date": ctx.research_date.isoformat(), "tradable": True, "suspended": False,
        "new_listing_special_period": False, "up_limit": "11.00", "down_limit": "9.00"} for s in symbols}
    return dict(candidates=candidates, reviews=reviews, quotes=quote_fixture(symbols)["response"]["quotes"],
                account=account, fees=fees, trading_metadata=metadata, context=ctx, config=load_pipeline_config(),
                snapshot_id="explicit-synthetic-snapshot", observed_at=ctx.as_of)


def with_holdings(params, positions, *, available=None):
    payload = params["account"].to_dict()
    payload["positions"] = [p.to_dict() for p in positions]
    cash = Decimal("1000000") - sum(p.market_value for p in positions)
    payload.update(cash_balance=str(cash), available_cash=str(cash if available is None else available))
    params["account"] = AccountSnapshot.from_dict(payload)


def holding_consumption(params, *, verdict="KEEP"):
    symbol = "000001.SZ"
    item = disposition_for(symbol=symbol, verdict=verdict,
                           attempt_id="holding-" + "a" * 32, status="SUCCESS")
    return {"schema_version": "p10.holding_review_consumption.v1",
            "account_hash": params["account"].snapshot_hash,
            "account_source_hash": "b" * 64, "index_hash": "c" * 64,
            "dispositions": [item.to_dict()],
            "blocking_reasons": [item.blocker] if item.blocker else [],
            "portfolio_authority": False}


def test_full_math_is_deterministic_and_synthetic_never_production():
    params = inputs()
    before = deepcopy(params)
    result = build_portfolio(**params)
    assert result == build_portfolio(**params)
    assert params == before
    assert result["status"] == "SUCCESS" and result["portfolio_status"] == "DRY_RUN"
    assert not result["execution_ready"] and not result["production_accepted"]
    assert len(result["output_symbols"]) == 8
    checks = result["risk_checks"]
    assert not checks["blocking_reasons"] and checks["max_single_weight"] <= .2
    assert Decimal(checks["cash_remaining"]) >= Decimal("200000")
    assert Decimal(checks["estimated_fees"]) > 0
    for row in result["rows"]:
        if row["selected"]:
            assert row["target_quantity"] % 100 == 0 and row["target_quantity"] == row["delta_quantity"]
            assert row["ai_risks"] and row["reference_time"]
    assert sum(row["target_weight"] or 0 for row in result["rows"]) <= .8


def test_s4_integer_clusters_are_valid_and_caps_still_apply():
    params = inputs()
    for i, candidate in enumerate(params["candidates"], 1):
        candidate["correlation_cluster"] = i
    before = deepcopy(params)
    result = build_portfolio(**params)
    assert result["portfolio_status"] == "DRY_RUN" and len(result["output_symbols"]) == 8
    assert params == before and not result["execution_ready"]
    assert all(type(row["correlation_cluster"]) is int for row in result["rows"] if row["selected"])
    for i, candidate in enumerate(params["candidates"]):
        candidate["correlation_cluster"] = 1 if i % 2 else "1"
    result = build_portfolio(**params)
    assert result["portfolio_status"] == "NOT_READY"
    assert "INSUFFICIENT_ELIGIBLE_POSITIONS" in result["risk_checks"]["blocking_reasons"]
    assert all(row["target_quantity"] is None for row in result["rows"])


@pytest.mark.parametrize("cluster", [None, True, False, 0, -1, 1.5, "", " ", []])
def test_invalid_cluster_is_still_rejected(cluster):
    params = inputs()
    for candidate in params["candidates"]:
        candidate["correlation_cluster"] = cluster
    result = build_portfolio(**params)
    assert result["portfolio_status"] == "NOT_READY"
    assert all("missing_ranking_or_cluster_metadata" in row["reject_reasons"] for row in result["rows"])


@pytest.mark.parametrize("field", ["account", "fees"])
def test_missing_required_input_has_no_target_quantities(field):
    params = inputs()
    params[field] = None
    result = build_portfolio(**params)
    assert result["portfolio_status"] == "NOT_READY" and not result["execution_ready"]
    assert all(row["target_quantity"] is None and row["delta_quantity"] is None for row in result["rows"])


@pytest.mark.parametrize("change", ["stale", "future", "time_only", "missing", "wrong_date"])
def test_bad_quotes_never_produce_executable_quantities(change):
    params = inputs()
    for quote in params["quotes"]:
        if change == "stale":
            quote["server_time"] = (context().as_of - timedelta(seconds=61)).isoformat()
        elif change == "future":
            quote["server_time"] = (context().as_of + timedelta(seconds=11)).isoformat()
        elif change == "time_only":
            quote.update(server_time="14:50:00", timestamp_quality="time_only", trade_date=None)
        elif change == "wrong_date":
            quote["trade_date"] = "2026-09-01"
    if change == "missing":
        params["quotes"] = []
    result = build_portfolio(**params)
    assert result["portfolio_status"] == "NOT_READY"
    assert all(row["target_quantity"] is None for row in result["rows"])


@pytest.mark.parametrize("field,value", [("tradable", False), ("suspended", True),
    ("new_listing_special_period", True), ("new_listing_special_period", None), ("up_limit", None),
    ("source", ""), ("as_of", "2026-09-03T14:50:00+08:00")])
def test_unverified_status_is_not_silently_inferred(field, value):
    params = inputs()
    for metadata in params["trading_metadata"].values():
        metadata[field] = value
    assert build_portfolio(**params)["portfolio_status"] == "NOT_READY"


def test_metadata_identity_is_not_silently_remapped():
    params = inputs()
    for metadata in params["trading_metadata"].values():
        metadata["symbol"] = "000001.SZ"
    result = build_portfolio(**params)
    assert result["portfolio_status"] == "NOT_READY"
    assert all("trading_metadata_identity_conflict" in row["reject_reasons"] for row in result["rows"])


def test_ai_exclusions_and_review_do_not_change_quant_ranking():
    params = inputs()
    params["reviews"][0].update(ai_verdict="EXCLUDE", ai_eligible=False)
    params["reviews"][1].update(ai_verdict="REVIEW", ai_eligible=False)
    result = build_portfolio(**params)
    assert result["status"] == "SUCCESS" and "600001.SH" not in result["output_symbols"]
    assert "600002.SH" not in result["output_symbols"]
    params["reviews"][2]["quant_score"] = 99
    with pytest.raises(PipelineError, match="scores"):
        build_portfolio(**params)


def test_cluster_and_sector_caps_do_not_relax_to_pad():
    params = inputs()
    for c in params["candidates"]:
        c["correlation_cluster"] = "one-cluster"
    result = build_portfolio(**params)
    assert result["portfolio_status"] == "NOT_READY"
    assert "INSUFFICIENT_ELIGIBLE_POSITIONS" in result["risk_checks"]["blocking_reasons"]
    params = inputs()
    for c in params["candidates"]:
        c["sector_metadata"] = {"status": "available", "source": "synthetic.sector", "classification": "test",
                                "value": "one-sector", "as_of": context().as_of.isoformat()}
    assert build_portfolio(**params)["portfolio_status"] == "NOT_READY"


def test_locked_large_holding_blocks_cap_and_never_vanishes():
    params = inputs()
    with_holdings(params, [AccountPosition("600002.SH", 40000, 0, Decimal("400000"))])
    result = build_portfolio(**params)
    assert result["portfolio_status"] == "NOT_READY"
    assert "SINGLE_NAME_CAP:600002.SH" in result["risk_checks"]["blocking_reasons"]
    held = next(r for r in result["rows"] if r["symbol"] == "600002.SH")
    assert held["current_quantity"] == 40000 and held["target_quantity"] is None


def test_locked_ineligible_holding_needs_review_not_invented_sale():
    params = inputs()
    with_holdings(params, [AccountPosition("600001.SH", 100, 0, Decimal("1000"))])
    params["reviews"][0].update(ai_eligible=False, ai_verdict="EXCLUDE")
    result = build_portfolio(**params)
    assert "LOCKED_INELIGIBLE_HOLDING:600001.SH" in result["risk_checks"]["blocking_reasons"]


def test_outside_holding_requires_its_own_quote_and_is_included_in_inputs():
    params = inputs()
    with_holdings(params, [AccountPosition("000001.SZ", 100, 100, Decimal("1000"))])
    result = build_portfolio(**params)
    assert "000001.SZ" in result["input_symbols"]
    assert "UNVERIFIED_HELD_INSTRUMENT:000001.SZ" in result["risk_checks"]["blocking_reasons"]
    params["quotes"] += quote_fixture(["000001.SZ"])["response"]["quotes"]
    params["trading_metadata"]["000001.SZ"] = deepcopy(next(iter(params["trading_metadata"].values())))
    result = build_portfolio(**params)
    assert result["portfolio_status"] == "NOT_READY"
    assert "MISSING_HOLDING_RESEARCH_COVERAGE:000001.SZ" in result["risk_checks"]["blocking_reasons"]
    row = next(r for r in result["rows"] if r["symbol"] == "000001.SZ")
    assert row["target_quantity"] is None and row["delta_quantity"] is None
    assert "holding_outside_s4" in row["reject_reasons"]
    assert "000001.SZ" not in result["output_symbols"]


@pytest.mark.parametrize("verdict,disposition", [("KEEP", "RETAIN"), ("WATCH", "RETAIN_WATCH")])
def test_validated_outside_holding_is_retained_at_exact_quantity(verdict, disposition):
    params = inputs()
    with_holdings(params, [AccountPosition("000001.SZ", 100, 100, Decimal("1000"))])
    params["quotes"] += quote_fixture(["000001.SZ"])["response"]["quotes"]
    params["trading_metadata"]["000001.SZ"] = deepcopy(next(iter(params["trading_metadata"].values())))
    params["holding_dispositions"] = holding_consumption(params, verdict=verdict)

    result = build_portfolio(**params)

    row = next(item for item in result["rows"] if item["symbol"] == "000001.SZ")
    assert result["portfolio_status"] == "DRY_RUN"
    assert row["holding_disposition"] == disposition and row["selected"]
    assert row["target_quantity"] == row["current_quantity"] == 100
    assert row["delta_quantity"] == 0 and row["side"] == "HOLD"
    assert "000001.SZ" in result["output_symbols"]
    assert not any(reason.startswith("MISSING_HOLDING_RESEARCH_COVERAGE")
                   for reason in result["risk_checks"]["blocking_reasons"])


def test_exit_review_and_tampered_retain_never_produce_quantities():
    params = inputs()
    with_holdings(params, [AccountPosition("000001.SZ", 100, 100, Decimal("1000"))])
    params["quotes"] += quote_fixture(["000001.SZ"])["response"]["quotes"]
    params["trading_metadata"]["000001.SZ"] = deepcopy(next(iter(params["trading_metadata"].values())))
    params["holding_dispositions"] = holding_consumption(params, verdict="EXCLUDE")
    result = build_portfolio(**params)
    assert result["portfolio_status"] == "NOT_READY"
    assert "HOLDING_EXIT_REQUIRES_HUMAN_REVIEW:000001.SZ" in result["risk_checks"]["blocking_reasons"]
    assert all(row["target_quantity"] is None for row in result["rows"])

    params["holding_dispositions"] = holding_consumption(params)
    params["holding_dispositions"]["dispositions"][0]["delta_quantity"] = 0
    with pytest.raises(PipelineError, match="schema"):
        build_portfolio(**params)


def test_available_cash_not_balance_or_unfilled_sale_proceeds_funds_buys():
    params = inputs()
    with_holdings(params, [AccountPosition("600012.SH", 60000, 60000, Decimal("600000"))], available=Decimal("10000"))
    result = build_portfolio(**params)
    checks = result["risk_checks"]
    assert Decimal(checks["required_buy_cash"]) <= 10000
    assert Decimal(checks["available_cash_without_sale_fills"]) >= 0
    assert not result["execution_ready"]
    assert result["policy"]["sell_proceeds_fund_buys"] is False


def test_price_limit_breach_is_observation_only():
    params = inputs()
    for meta in params["trading_metadata"].values():
        meta["up_limit"] = "10.02"
    result = build_portfolio(**params)
    assert result["portfolio_status"] == "NOT_READY"
    assert any(r.startswith("ORDER_PRICE_OUTSIDE_LIMITS") for r in result["risk_checks"]["blocking_reasons"])
    assert all(r["delta_quantity"] is None for r in result["rows"])


def test_frozen_cash_scales_buys_across_sleeves_without_spending_it():
    params = inputs()
    params["account"] = AccountSnapshot.from_dict(params["account"].to_dict() | {"available_cash": "100000.00"})
    result = build_portfolio(**params)
    assert result["portfolio_status"] == "DRY_RUN"
    assert len(result["output_symbols"]) == 8
    assert Decimal(result["risk_checks"]["required_buy_cash"]) <= 100000
    assert Decimal(result["risk_checks"]["available_cash_without_sale_fills"]) >= 0


def test_real_account_branch_is_schema_validation_not_broker_authentication():
    params = inputs()
    params["account"] = AccountSnapshot.from_dict(params["account"].to_dict() | {"source_kind": "manual_user"})
    result = build_portfolio(**params)
    assert result["portfolio_status"] == "DRAFT_READY" and result["execution_ready"]
    assert not result["production_accepted"]
    assert result["account_assessment"]["authentication_scope"] == "user_supplied_snapshot_not_independently_broker_authenticated"


def test_invested_and_cash_reconcile_with_price_buffers_and_fees():
    params = inputs()
    with_holdings(params, [AccountPosition("600012.SH", 1000, 1000, Decimal("10000"))])
    result = build_portfolio(**params)
    assert result["status"] == "SUCCESS"
    rows = [r for r in result["rows"] if r["delta_quantity"] is not None]
    slippage = sum((Decimal(r["order_price"]) - Decimal(r["reference_price"])) * r["delta_quantity"]
                   for r in rows if r["delta_quantity"])
    fees = Decimal(result["risk_checks"]["estimated_fees"])
    checks = result["risk_checks"]
    assert Decimal(checks["marked_asset"]) - slippage - fees == Decimal(checks["cash_remaining"]) + Decimal(checks["target_invested"])


def test_fee_roundtrip_and_conservative_component_rounding():
    fees = inputs()["fees"]
    assert FeeSchedule.from_dict(fees.to_dict()) == fees
    assert fees.charge("BUY", Decimal("1000")) == Decimal("5.10")
    assert fees.charge("SELL", Decimal("1000")) == Decimal("5.50")
    assert fees.charge("BUY", Decimal(0)) == 0


@pytest.mark.parametrize("field,value", [("commission_rate", "NaN"), ("buy_other_rate", -1),
    ("minimum_commission", True), ("sell_other_rate", ".2"), ("source", "")])
def test_invalid_fee_schedule_is_rejected(field, value):
    payload = inputs()["fees"].to_dict() | {field: value}
    with pytest.raises(ValueError):
        FeeSchedule.from_dict(payload)
