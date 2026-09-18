"""Explicit offline testing never grants portfolio or order readiness."""

from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo

import pytest

from competition_core.pipeline.account import AccountSnapshot
from competition_core.pipeline.config import load_pipeline_config
from competition_core.pipeline.input_audit import audit_execution_inputs
from competition_core.pipeline.portfolio import FeeSchedule
from competition_core.pipeline.time_context import ResearchContext

NOW = datetime(2026, 9, 3, 10, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
CTX = ResearchContext(date(2026, 9, 3), "intraday", date(2026, 9, 2), NOW-timedelta(hours=1))


def account(*, age=1800, pending=4):
    return AccountSnapshot.from_dict(dict(schema_version="competition.account.v1",
        as_of=(NOW-timedelta(seconds=age)).isoformat(), source="explicit test fixture",
        source_kind="competition_export", currency="CNY", cash_balance="10000.00",
        available_cash="9000.00", current_total_asset="10000.00", positions=[], pending_order_count=pending))


def audit(**kwargs):
    values = dict(account=account(), fees=None, context=CTX, config=load_pipeline_config(), observed_at=NOW)
    return audit_execution_inputs(**(values | kwargs))


def quote(symbol="600519.SH"):
    return dict(symbol=symbol, price=10.0, prev_close=9.9, open=10.0, high=10.2, low=9.8,
        volume=1000, amount=10000, volume_unit="shares", amount_unit="CNY", bid1=9.99,
        ask1=10.01, source="fixture", server_time=(NOW-timedelta(seconds=1)).isoformat(),
        trade_date="2026-09-03", received_at=NOW.isoformat(), timestamp_quality="verified")


def test_test_mode_preserves_strict_result_and_pending_orders():
    source = account()
    original = source.to_dict()
    config = load_pipeline_config()
    config_hash = config.config_hash
    strict = audit(account=source, config=config)
    testing = audit(account=source, config=config, test_mode=True)
    assert "STALE_ACCOUNT_STATE" in strict["issues"]
    assert "STALE_ACCOUNT_STATE" not in testing["issues"]
    assert testing["account_test_warnings"] == ["STALE_ACCOUNT_STATE"]
    assert testing["strict_account"] == strict["strict_account"]
    assert "PENDING_ORDERS_REQUIRE_RECONCILIATION" in testing["issues"]
    assert testing["validation_completed"] is True
    assert source.to_dict() == original
    assert config.config_hash == config_hash and config.section("portfolio")["account_max_age_seconds"] == 900


@pytest.mark.parametrize(("age", "reason"), [(-1, "FUTURE_ACCOUNT_STATE"), (86400, "ACCOUNT_DATE_MISMATCH")])
def test_test_mode_does_not_waive_future_or_date_mismatch(age, reason):
    result = audit(account=account(age=age), test_mode=True)
    assert reason in result["issues"]


@pytest.mark.parametrize("testing", [False, True])
def test_even_fresh_inputs_never_grant_execution_readiness(testing):
    result = audit(account=account(age=1, pending=0), test_mode=testing,
                   quotes=[quote()], symbols=["600519.SH"])
    assert result["strict_account"]["account_eligible"] is True
    assert result["quote_audit"]["strict_gate"]["counts"]["PASS"] == 1
    assert result["execution_ready"] is False
    assert result["production_accepted"] is False
    assert result["orders_submitted"] == 0
    assert result["portfolio_generated"] is False


def test_missing_prev_close_does_not_hide_ohlc_error_or_coverage_gap():
    rows = [quote() | dict(high=8.0, low=11.0, prev_close=None), quote("000001.SZ")]
    original = deepcopy(rows)
    result = audit(test_mode=True, quotes=rows, symbols=["600519.SH", "300750.SZ"])
    view = result["quote_audit"]
    assert view["ohlc_inconsistent_symbols"] == ["600519.SH"]
    assert view["missing_symbols"] == ["300750.SZ"]
    assert view["outside_requested_symbols"] == ["000001.SZ"]
    assert rows == original


def test_time_only_quote_is_not_promoted_to_a_dated_quote():
    result = audit(test_mode=True, quotes=[quote() | dict(server_time="10:30:00", trade_date=None,
        timestamp_quality="time_only")], symbols=["600519.SH"])
    assert result["quote_audit"]["strict_gate"]["counts"]["MISSING_REALTIME"] == 1
    assert result["quote_audit"]["strict_gate_relaxed"] is False


@pytest.mark.parametrize("testing", [False, True])
def test_historical_quotes_complete_diagnostics_without_faking_a_live_gate(testing):
    rows = [quote() | dict(high=8.0, low=11.0)]
    original = deepcopy(rows)
    observed = NOW + timedelta(days=1)
    result = audit(test_mode=testing, observed_at=observed,
                   quotes=rows, symbols=["600519.SH", "300750.SZ"])
    view = result["quote_audit"]
    assert result["validation_completed"] is True
    assert result["observed_at"] == observed.isoformat()
    assert "ACCOUNT_DATE_MISMATCH" in result["issues"]
    assert "PENDING_ORDERS_REQUIRE_RECONCILIATION" in result["issues"]
    assert "QUOTE_EXECUTION_CHECKS_NOT_PASSED" in result["issues"]
    assert view["strict_gate"] is None
    assert view["strict_gate_error"] == {"code": "NOT_READY"}
    assert view["strict_gate_relaxed"] is False
    assert view["missing_symbols"] == ["300750.SZ"]
    assert view["ohlc_inconsistent_symbols"] == ["600519.SH"]
    assert rows == original
    assert not result["execution_ready"] and not result["production_accepted"]
    assert not result["portfolio_generated"] and result["orders_submitted"] == 0


@pytest.mark.parametrize("kwargs", [dict(test_mode="yes"), dict(fees={}),
    dict(quotes=[quote(), quote()], symbols=["600519.SH"]), dict(quotes=[quote()], symbols=[]),
    dict(quotes=[], symbols=[{}])])
def test_invalid_inputs_fail(kwargs):
    with pytest.raises(ValueError):
        audit(**kwargs)


def test_observed_fee_rule_minimum_and_conservative_reserve():
    fees = FeeSchedule(source="test of supplied fee rule", as_of=NOW,
        valid_through=NOW.date(), commission_rate=Decimal("0.0003"), minimum_commission=Decimal("5"),
        buy_other_rate=Decimal(0), sell_other_rate=Decimal("0.0005"))
    assert fees.charge("BUY", Decimal("1000")) == Decimal("5.00")
    assert fees.charge("SELL", Decimal("1000")) == Decimal("5.50")
    assert fees.charge("SELL", Decimal("18850")) == Decimal("15.09")
    assert "MISSING_COMPETITION_FEE_SCHEDULE" not in audit(fees=fees)["issues"]
    assert "MISSING_COMPETITION_FEE_SCHEDULE" in audit()["issues"]


def test_test_mode_preserves_fee_time_boundaries():
    fees = FeeSchedule(source="explicit test fixture", as_of=NOW,
        valid_through=NOW.date()+timedelta(days=2), commission_rate=Decimal("0.0003"),
        minimum_commission=Decimal("5"), buy_other_rate=Decimal(0), sell_other_rate=Decimal("0.0005"))
    future = audit(fees=replace(fees, as_of=NOW+timedelta(seconds=1)), test_mode=True)
    assert "FUTURE_FEE_SCHEDULE" in future["issues"]
    subsequent = audit(fees=replace(fees, as_of=NOW+timedelta(days=1)),
        observed_at=NOW+timedelta(days=2), test_mode=True)
    assert "FEE_SCHEDULE_DATE_MISMATCH" in subsequent["issues"]
    expired = audit(fees=replace(fees, as_of=NOW-timedelta(days=2),
        valid_through=NOW.date()-timedelta(days=1)), test_mode=True)
    assert "FEE_SCHEDULE_DATE_MISMATCH" in expired["issues"]


def test_cli_diagnostics_complete_without_writing_inputs_or_a_run(tmp_path):
    path = tmp_path / "account.json"
    payload = account().to_dict()
    path.write_text(json.dumps(payload), encoding="utf-8")
    original = path.read_bytes()
    root = Path(__file__).resolve().parents[2]
    command = [sys.executable, "-B", str(root / "scripts/audit-execution-inputs.py"),
        "--account-file", str(path), "--research-date", "2026-09-03",
        "--market-data-reference-date", "2026-09-02", "--phase", "intraday",
        "--as-of", CTX.as_of.isoformat(), "--test-mode"]
    result = subprocess.run(command, capture_output=True, text=True, cwd=tmp_path, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    output = json.loads(result.stdout)
    assert output["validation_completed"] and not output["execution_ready"]
    assert path.read_bytes() == original and list(tmp_path.iterdir()) == [path]
    path.write_text('{"private_secret":"never-echo-this"}', encoding="utf-8")
    result = subprocess.run(command, capture_output=True, text=True, cwd=tmp_path, timeout=30)
    assert result.returncode == 1 and "never-echo-this" not in result.stdout + result.stderr


@pytest.mark.parametrize("testing", [False, True])
def test_cli_historical_quotes_remain_inspectable(tmp_path, testing):
    path = tmp_path / "account.json"
    path.write_text(json.dumps(account().to_dict()), encoding="utf-8")
    quotes_path = tmp_path / "quotes.json"
    quotes_path.write_text(json.dumps([quote()]), encoding="utf-8")
    symbols_path = tmp_path / "symbols.json"
    symbols_path.write_text(json.dumps(["600519.SH"]), encoding="utf-8")
    original = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    root = Path(__file__).resolve().parents[2]
    command = [sys.executable, "-B", str(root / "scripts/audit-execution-inputs.py"),
        "--account-file", str(path), "--quotes-file", str(quotes_path),
        "--symbols-file", str(symbols_path), "--research-date", "2026-09-03",
        "--market-data-reference-date", "2026-09-02", "--phase", "intraday",
        "--as-of", CTX.as_of.isoformat()]
    if testing:
        command.append("--test-mode")
    result = subprocess.run(command, capture_output=True, text=True, cwd=tmp_path, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    output = json.loads(result.stdout)
    assert output["validation_completed"] and not output["execution_ready"]
    assert output["quote_audit"]["covered_symbols"] == ["600519.SH"]
    observed = datetime.fromisoformat(output["observed_at"])
    if observed.date() != CTX.research_date:
        assert output["quote_audit"]["strict_gate"] is None
        assert output["quote_audit"]["strict_gate_error"] == {"code": "NOT_READY"}
        assert "ACCOUNT_DATE_MISMATCH" in output["issues"]
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == original
