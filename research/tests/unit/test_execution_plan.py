"""W6 fixed-plan tests. Fixtures are local calculations, never broker evidence."""

from contextlib import nullcontext
from copy import deepcopy
from datetime import date
from decimal import Decimal
import json

import pytest

from competition_core.pipeline.account import AccountSnapshot
from competition_core.pipeline.artifacts import atomic_json, content_hash
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.execution_plan import (
    PLAN_PATH,
    _has_terminal_shadow,
    _plan_record,
    create_execution_plan,
    read_execution_plan,
)
from competition_core.pipeline.portfolio import FeeSchedule, build_portfolio
from competition_core.pipeline.execution_status_runner import run_execution_status_check
from competition_core.pipeline.portfolio_runner import run_portfolio_draft
from tests.integration.test_execution_status_pipeline import prepared
from tests.unit.test_pipeline_portfolio import inputs


class Repository:
    def __init__(self, directory):
        self.directory = directory
        self.run_id = directory.name

    def lock(self):
        return nullcontext()


def bound(tmp_path):
    params = inputs()
    ctx = params["context"]
    params["account"] = AccountSnapshot.from_dict(params["account"].to_dict() | {
        "source": "user supplied competition account",
        "source_kind": "manual_user",
    })
    params["fees"] = FeeSchedule(
        source="user supplied competition fees",
        as_of=ctx.as_of,
        valid_through=date(2026, 9, 30),
        commission_rate=Decimal("0.001"),
        minimum_commission=Decimal("5"),
        buy_other_rate=Decimal("0.0001"),
        sell_other_rate=Decimal("0.0005"),
    )
    for row in params["trading_metadata"].values():
        row["source"] = "verified local market profile"
    portfolio = build_portfolio(**params)
    assert portfolio["portfolio_status"] == "DRAFT_READY"
    root = tmp_path / "run-w6"
    root.mkdir()
    atomic_json(root / "portfolio.json", portfolio)
    symbols = list(portfolio["input_symbols"])
    request = {
        "context": ctx.to_dict(),
        "snapshot_id": params["snapshot_id"],
        "config_hash": params["config"].config_hash,
        "symbols": symbols,
        "observed_at": ctx.as_of.isoformat(),
    }
    gate_rows = []
    quote_by = {row["symbol"]: row for row in params["quotes"]}
    for row in portfolio["rows"]:
        quote = quote_by[row["symbol"]]
        gate_rows.append({"symbol": row["symbol"], "execution_eligible": True,
                          "realtime_gate": "PASS", "quote": quote})
    return {
        "root": root,
        "request": request,
        "portfolio": portfolio,
        "rows": {row["symbol"]: row for row in portfolio["rows"]},
        "context": ctx,
        "account": params["account"],
        "fees": params["fees"],
        "metadata": params["trading_metadata"],
        "gate": {"rows": gate_rows},
        "news": {"snapshot_hash": "1" * 64, "file_hash": "2" * 64,
                 "review_inputs_hash": "3" * 64},
        "status": {"status_id": "status-w6"},
        "quote": {"check_id": "quote-w6"},
        "seal_file_hash": "4" * 64,
        "attempt_id": "draft-w6",
        "directory": root / "portfolio-attempts" / "draft-w6",
        "s6": {"artifact_hash": "5" * 64, "semantic_hash": "6" * 64},
        "stages": {"S5": {"input_symbols": symbols}},
        "stage_hashes": {f"S{i}": str(i) * 64 for i in range(6)},
        "account_source_hash": "7" * 64,
        "source_receipt_hash": "8" * 64,
    }


def test_fixed_plan_preserves_s6_direction_quantity_price_fee_and_cash(tmp_path):
    source = bound(tmp_path)
    plan = _plan_record(source, "plan-w6")

    by_symbol = {row["symbol"]: row for row in source["portfolio"]["rows"]}
    assert plan["orders"]
    for order in plan["orders"]:
        row = by_symbol[order["symbol"]]
        assert order["side"] == row["side"]
        assert order["quantity"] == abs(row["delta_quantity"])
        assert order["target_quantity"] == row["target_quantity"]
        assert order["limit_price"] == row["order_price"]
        assert order["estimated_fee"] == row["estimated_fees"]
        assert order["quantity_rules"] == row["quantity_rules"]
    assert Decimal(plan["economics"]["available_cash_after_buy_orders"]) >= 0
    assert plan["research_only"] is True
    assert plan["external_write_allowed"] is False
    assert plan["orders_submitted"] == 0
    assert plan["approval"]["state"] == "NOT_APPROVED"


def test_public_creation_is_immutable_idempotent_and_has_no_side_effect_file(tmp_path, monkeypatch):
    source = bound(tmp_path)
    repository = Repository(source["root"])
    monkeypatch.setattr("competition_core.pipeline.execution_plan._validate_seal", lambda _: source)

    first = create_execution_plan(repository)
    second = create_execution_plan(repository)

    assert first == second
    assert sorted(path.name for path in source["root"].iterdir()) == [PLAN_PATH, "portfolio.json"]
    assert first["plan_hash"] == second["plan_hash"]


@pytest.mark.parametrize(
    "mutate,match",
    [
        (lambda value: value["portfolio"]["rows"][0].update(side="SELL"), "side"),
        (lambda value: value["portfolio"]["rows"][0].update(target_quantity=1), "target/delta"),
        (lambda value: value["portfolio"]["rows"][0].update(estimated_fees="0.00"), "fee"),
        (lambda value: value["portfolio"]["rows"][0]["quantity_rules"].update(buy_step=1), "quantity rules"),
    ],
)
def test_tampered_s6_order_terms_fail_closed(tmp_path, mutate, match):
    source = bound(tmp_path)
    mutate(source)
    with pytest.raises(PipelineError, match=match):
        _plan_record(source, "plan-w6")


def test_pending_orders_fail_closed(tmp_path):
    source = bound(tmp_path)
    payload = source["account"].to_dict() | {"pending_order_count": 1}
    source["account"] = AccountSnapshot.from_dict(payload)
    with pytest.raises(PipelineError, match="pending orders"):
        _plan_record(source, "plan-w6")


def test_plan_hash_tamper_is_rejected(tmp_path, monkeypatch):
    source = bound(tmp_path)
    repository = Repository(source["root"])
    monkeypatch.setattr("competition_core.pipeline.execution_plan._validate_seal", lambda _: source)
    plan = create_execution_plan(repository)
    atomic_json(source["root"] / PLAN_PATH, plan | {"orders_submitted": 1})

    with pytest.raises(PipelineError, match="integrity hash"):
        read_execution_plan(repository)


def test_account_and_fee_changes_produce_different_plan_binding(tmp_path):
    source = bound(tmp_path)
    original = _plan_record(source, "plan-w6")
    changed = deepcopy(source)
    changed["account_source_hash"] = "9" * 64
    changed["source_receipt_hash"] = "a" * 64
    assert _plan_record(changed, "plan-w6")["plan_hash"] != original["plan_hash"]

    changed = deepcopy(source)
    changed["fees"] = FeeSchedule.from_dict(source["fees"].to_dict() | {"commission_rate": "0.002"})
    with pytest.raises(PipelineError, match="fee"):
        _plan_record(changed, "plan-w6")


def test_bridge_consumes_real_core_seal_structure_without_network_or_orders(tmp_path, monkeypatch):
    repository, kwargs, status_source = prepared(tmp_path, monkeypatch)
    imported = {"account": kwargs["account"].to_dict(), "fees": kwargs["fees"].to_dict()}
    source_hash = content_hash(imported)
    atomic_json(repository.directory / f"execution-imports/{source_hash}.json", imported, immutable=True)
    atomic_json(repository.directory / "execution-inputs.json", {"id": source_hash})
    run_execution_status_check(repository=repository, client=status_source,
                               source_name="explicit.synthetic", account=kwargs["account"],
                               clock=kwargs["clock"], status_id="synthetic-status")
    run_portfolio_draft(**kwargs, status_id="synthetic-status", draft_id="sealed-w6",
                        account_source_hash=source_hash)

    # This test intentionally reuses synthetic transport/status fixtures. Only
    # their production-label rejection and the independently tested W4 news
    # binding are bypassed; the actual S6 seal/hash/account/status/quote readers run.
    monkeypatch.setattr("competition_core.pipeline.execution_plan._reject_demo", lambda *args: None)
    monkeypatch.setattr("competition_core.pipeline.execution_plan._validate_news", lambda *args: {
        "snapshot_hash": "1" * 64, "file_hash": "2" * 64, "review_inputs_hash": "3" * 64,
        "status": "AVAILABLE",
    })
    plan = create_execution_plan(repository)

    assert plan["source"]["portfolio_attempt_id"] == "sealed-w6"
    assert plan["account"]["snapshot_hash"] == kwargs["account"].snapshot_hash
    assert plan["orders"] and plan["orders_submitted"] == 0


@pytest.mark.parametrize(
    "payload",
    [
        {"allocation_version": "inventory_sleeves.v1+terminal_shadow.v1"},
        {"risk_checks": {"terminal_policy_shadow_only": True}},
        {"risk_checks": {"blocking_reasons": ["TERMINAL_POLICY_SHADOW_ONLY"]}},
        {"policy": {"terminal_shadow": {}}},
        {"summary": {"terminal_evaluation_id": "a" * 64}},
        {"terminal_evaluation_id": "a" * 64},
    ],
)
def test_terminal_shadow_markers_are_detected_even_when_readiness_is_forged(payload):
    forged = payload | {"execution_ready": True, "production_accepted": True}
    assert _has_terminal_shadow(forged)


def test_forged_terminal_shadow_portfolio_is_rejected_before_plan_derivation(tmp_path, monkeypatch):
    repository, kwargs, status_source = prepared(tmp_path, monkeypatch)
    imported = {"account": kwargs["account"].to_dict(), "fees": kwargs["fees"].to_dict()}
    source_hash = content_hash(imported)
    atomic_json(repository.directory / f"execution-imports/{source_hash}.json", imported, immutable=True)
    atomic_json(repository.directory / "execution-inputs.json", {"id": source_hash})
    run_execution_status_check(repository=repository, client=status_source,
                               source_name="explicit.synthetic", account=kwargs["account"],
                               clock=kwargs["clock"], status_id="synthetic-status")
    monkeypatch.setattr("competition_core.pipeline.execution_plan._reject_demo", lambda *args: None)
    monkeypatch.setattr("competition_core.pipeline.execution_plan._validate_news", lambda *args: {
        "snapshot_hash": "1" * 64, "file_hash": "2" * 64, "review_inputs_hash": "3" * 64,
        "status": "AVAILABLE",
    })
    run_portfolio_draft(**kwargs, status_id="synthetic-status", draft_id="terminal-forged",
                        account_source_hash=source_hash)

    portfolio_path = repository.directory / "portfolio.json"
    portfolio = json.loads(portfolio_path.read_text(encoding="utf-8"))
    portfolio["execution_ready"] = True
    portfolio["production_accepted"] = True
    portfolio["policy"] = dict(portfolio["policy"])
    portfolio["policy"]["terminal_shadow"] = {"forged": True}
    atomic_json(portfolio_path, portfolio)

    with pytest.raises(PipelineError, match="terminal shadow") as error:
        create_execution_plan(repository)
    assert error.value.code == "NOT_READY"
