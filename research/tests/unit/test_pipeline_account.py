"""Synthetic account fixtures cannot satisfy a real-account acceptance gate."""

import copy
from datetime import datetime, timedelta
from decimal import Decimal
import json

import pytest

from competition_core.pipeline.account import AccountSnapshot, assess_account, load_account_snapshot
from competition_core.pipeline.config import load_pipeline_config
from competition_core.pipeline.errors import PipelineError
from tests.pipeline_fixtures import context


def payload():
    return {"schema_version": "competition.account.v1", "as_of": context().as_of.isoformat(),
            "source": "explicit synthetic test fixture", "source_kind": "synthetic_test", "currency": "CNY",
            "cash_balance": "600000.00", "available_cash": "550000.00", "current_total_asset": "1000000.00",
            "pending_order_count": 0,
            "positions": [{"symbol": "300750.SZ", "quantity": 2000, "sellable_quantity": 500,
                           "market_value": "400000.00"}]}


def assessment(account, *, seconds=0):
    return assess_account(account, context=context(), config=load_pipeline_config(),
                          observed_at=context().as_of + timedelta(seconds=seconds))


def test_raw_quantity_cash_and_missing_not_inferred():
    original = payload()
    saved = copy.deepcopy(original)
    account = AccountSnapshot.from_dict(original)
    assert account.to_dict() == original and original == saved
    assert account.available_cash == Decimal("550000")
    assert account.positions[0].sellable_quantity == 500
    assert assessment(account)["reasons"] == ["SYNTHETIC_ACCOUNT_NOT_PRODUCTION"]
    assert not assessment(account)["execution_ready"]
    assert assessment(None)["reasons"] == ["MISSING_ACCOUNT_STATE"]
    assert len(account.snapshot_hash) == 64


@pytest.mark.parametrize("field", ["positions", "as_of", "available_cash", "current_total_asset", "pending_order_count"])
def test_required_fields_have_no_empty_account_defaults(field):
    data = payload()
    del data[field]
    with pytest.raises(PipelineError):
        AccountSnapshot.from_dict(data)


@pytest.mark.parametrize("value", [True, None, "NaN", "Infinity", -1, "0.001", 1e100])
def test_bad_cash_rejected_without_echoing_value(value):
    data = payload()
    data["available_cash"] = value
    with pytest.raises(PipelineError) as error:
        AccountSnapshot.from_dict(data)
    assert error.value.code == "INVALID_ACCOUNT_STATE"


@pytest.mark.parametrize("patch", [
    {"quantity": True}, {"quantity": 20.5}, {"quantity": 0}, {"quantity": -1},
    {"sellable_quantity": None}, {"sellable_quantity": 2001}, {"sellable_quantity": -1},
    {"symbol": "300750.SH"}, {"symbol": "510300.SH"}, {"market_value": "NaN"},
])
def test_position_contract(patch):
    data = payload()
    data["positions"][0].update(patch)
    with pytest.raises(PipelineError):
        AccountSnapshot.from_dict(data)


def test_missing_sellable_and_duplicate_positions_rejected():
    data = payload()
    del data["positions"][0]["sellable_quantity"]
    with pytest.raises(PipelineError):
        AccountSnapshot.from_dict(data)
    data = payload()
    data["positions"] *= 2
    with pytest.raises(PipelineError):
        AccountSnapshot.from_dict(data)


@pytest.mark.parametrize("patch", [
    {"available_cash": "600000.01"}, {"current_total_asset": "999999"},
    {"currency": "USD"}, {"source": ""}, {"source_kind": "tushare"},
    {"pending_order_count": True}, {"as_of": "2026-09-02T10:00:00"},
])
def test_inconsistent_account_rejected(patch):
    data = payload()
    data.update(patch)
    with pytest.raises(PipelineError):
        AccountSnapshot.from_dict(data)


def test_fresh_declared_user_export_readiness_is_not_authentication_or_execution():
    data = payload()
    # Tests the declaration parser only. This is NOT a genuine broker export.
    data["source_kind"] = "manual_user"
    account = AccountSnapshot.from_dict(data)
    result = assessment(account, seconds=900)
    assert result["account_eligible"] and not result["execution_ready"]
    assert "not_independently_broker_authenticated" in result["authentication_scope"]
    assert "STALE_ACCOUNT_STATE" in assessment(account, seconds=901)["reasons"]
    data["as_of"] = (context().as_of + timedelta(seconds=1)).isoformat()
    assert "FUTURE_ACCOUNT_STATE" in assessment(AccountSnapshot.from_dict(data))["reasons"]
    data["as_of"] = (context().as_of - timedelta(days=1)).isoformat()
    assert "ACCOUNT_DATE_MISMATCH" in assessment(AccountSnapshot.from_dict(data))["reasons"]


def test_pending_orders_block_new_draft_and_cash_is_not_available_by_default():
    data = payload()
    data["pending_order_count"] = 1
    assert "PENDING_ORDERS_REQUIRE_RECONCILIATION" in assessment(AccountSnapshot.from_dict(data))["reasons"]
    with pytest.raises(PipelineError):
        assess_account(None, context=context(), config=load_pipeline_config(), observed_at=datetime(2026, 9, 2))


def test_export_hash_roundtrip_and_unsafe_json(tmp_path):
    path = tmp_path / "synthetic-account.json"
    path.write_text(json.dumps(payload()), encoding="utf-8")
    account, digest = load_account_snapshot(path)
    assert account.to_dict() == payload() and len(digest) == 64
    path.write_text('{"source":"x", "source":"y"}', encoding="utf-8")
    with pytest.raises(PipelineError):
        load_account_snapshot(path)
    path.write_text('{"x": NaN}', encoding="utf-8")
    with pytest.raises(PipelineError):
        load_account_snapshot(path)
    with pytest.raises(PipelineError) as error:
        load_account_snapshot(None)
    assert error.value.code == "MISSING_ACCOUNT_STATE"


def test_explicit_empty_account_allowed_only_with_reconciled_balance():
    data = payload()
    data.update(positions=[], cash_balance="1000000.00", available_cash="1000000.00")
    account = AccountSnapshot.from_dict(data)
    assert not account.positions and not assessment(account)["account_eligible"]
    data["current_total_asset"] = "0.00"
    with pytest.raises(PipelineError):
        AccountSnapshot.from_dict(data)
