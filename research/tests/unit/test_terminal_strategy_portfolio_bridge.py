from copy import deepcopy
from dataclasses import replace
from datetime import date
from decimal import Decimal

from competition_core.pipeline.portfolio import build_portfolio
from competition_core.terminal_strategy import (
    TerminalAllocationPolicy,
    TerminalMode,
    validate_terminal_policy,
)
from tests.pipeline_fixtures import context

from test_pipeline_portfolio import inputs


def _hash(label: str) -> str:
    return (label * 64)[:64]


def _policy(*, mode=TerminalMode.NORMAL, same_day=()):
    ctx = context()
    if mode is TerminalMode.PROTECT:
        core = tuple(f"600{i:03d}.SH" for i in range(1, 7))
        aggressive = ()
        core_budget, aggressive_budget, cash = Decimal("0.80"), Decimal("0"), Decimal("0.20")
    elif mode is TerminalMode.ATTACK:
        core = tuple(f"600{i:03d}.SH" for i in range(1, 5))
        aggressive = ("600005.SH", "600006.SH")
        core_budget, aggressive_budget, cash = Decimal("0.50"), Decimal("0.40"), Decimal("0.10")
    else:
        core = tuple(f"600{i:03d}.SH" for i in range(1, 5))
        aggressive = ("600005.SH", "600006.SH")
        core_budget, aggressive_budget, cash = Decimal("0.55"), Decimal("0.30"), Decimal("0.15")
    return TerminalAllocationPolicy(
        mode=mode,
        as_of=ctx.as_of,
        trade_date=ctx.research_date,
        core_symbols=core,
        aggressive_symbols=aggressive,
        total_position_count=len(core) + len(aggressive),
        terminal_candidate_count=len(aggressive),
        core_budget=core_budget,
        aggressive_budget=aggressive_budget,
        cash_target=cash,
        same_day_exit_unavailable=tuple(same_day),
        mode_decision_hash=_hash("a"),
        funnel_hash=_hash("b"),
        config_hash=_hash("c"),
        input_hash=_hash("d"),
    )


def _build(policy):
    values = inputs()
    values["terminal_policy"] = policy
    return build_portfolio(**values)


def test_terminal_policy_drives_roles_and_budget_but_stays_shadow_only():
    result = _build(_policy())
    assert result["allocation_version"] == "inventory_sleeves.v1+terminal_shadow.v1"
    assert result["status"] == "PARTIAL"
    assert result["portfolio_status"] == "NOT_READY"
    assert result["execution_ready"] is False and result["production_accepted"] is False
    assert "TERMINAL_POLICY_SHADOW_ONLY" in result["risk_checks"]["blocking_reasons"]
    selected = [row for row in result["rows"] if row["selected"]]
    assert len(selected) == 6
    assert {row["symbol"] for row in selected if row["terminal_role"] == "AGGRESSIVE"} == {
        "600005.SH", "600006.SH"
    }
    assert all(row["terminal_role"] == "CORE" for row in selected if row["symbol"] in {"600001.SH", "600002.SH", "600003.SH", "600004.SH"})
    assert result["summary"]["terminal_policy_hash"] == _policy().policy_hash


def test_protect_preserves_core_and_adds_no_aggressive_risk():
    result = _build(_policy(mode=TerminalMode.PROTECT))
    selected = [row for row in result["rows"] if row["selected"]]
    assert len(selected) == 6
    assert all(row["terminal_role"] == "CORE" for row in selected)
    assert result["policy"]["terminal_shadow"]["aggressive_symbols"] == []
    assert result["policy"]["terminal_shadow"]["aggressive_budget"] == "0"


def test_same_day_exit_fact_is_retained_in_policy_and_rows():
    result = _build(_policy(same_day=("600005.SH",)))
    row = next(item for item in result["rows"] if item["symbol"] == "600005.SH")
    assert row["same_day_exit_unavailable"] is True
    assert result["policy"]["terminal_shadow"]["same_day_exit_unavailable"] == ["600005.SH"]


def test_policy_budget_math_shortfall_blocks_without_cross_sleeve_scaling():
    values = inputs()
    values["account"] = values["account"].__class__.from_dict(
        values["account"].to_dict() | {"available_cash": "1000.00"}
    )
    values["terminal_policy"] = _policy()
    result = build_portfolio(**values)
    assert result["portfolio_status"] == "NOT_READY"
    assert "TERMINAL_POLICY_CASH_CONSTRAINT" in result["risk_checks"]["blocking_reasons"]
    assert result["risk_checks"]["terminal_policy_no_cross_sleeve_redistribution"] is True
    assert all(row["target_quantity"] is None for row in result["rows"])


def test_invalid_policy_hash_or_candidate_identity_never_falls_back_to_legacy_selector():
    invalid_hash = deepcopy(_policy().to_dict())
    invalid_hash["config_hash"] = "not-a-hash"
    result = _build(invalid_hash)
    assert result["portfolio_status"] == "NOT_READY"
    assert "TERMINAL_POLICY_SCHEMA_INVALID" not in result["risk_checks"]["blocking_reasons"]
    assert "TERMINAL_POLICY_HASH_INVALID" in result["risk_checks"]["blocking_reasons"]
    assert result["output_symbols"] == []

    wrong_candidate = replace(
        _policy(),
        aggressive_symbols=("600099.SH", "600010.SH"),
        total_position_count=6,
        terminal_candidate_count=2,
    )
    result = _build(wrong_candidate)
    assert result["portfolio_status"] == "NOT_READY"
    assert "TERMINAL_POLICY_CANDIDATE_IDENTITY" in result["risk_checks"]["blocking_reasons"]
    assert result["output_symbols"] == []


def test_policy_dict_requires_exact_v1_and_hash_binding():
    policy = _policy()
    valid = validate_terminal_policy(
        policy.to_dict(),
        indexed_symbols=[f"600{i:03d}.SH" for i in range(1, 13)],
        holding_symbols=(),
        eligible_symbols=[f"600{i:03d}.SH" for i in range(1, 13)],
        expected_trade_date=context().research_date,
        observed_at=context().as_of,
    )
    assert valid.valid and valid.policy == policy

    extra = policy.to_dict() | {"unexpected": True}
    invalid = validate_terminal_policy(extra, indexed_symbols=policy.core_symbols + policy.aggressive_symbols,
                                       holding_symbols=())
    assert not invalid.valid and "TERMINAL_POLICY_SCHEMA_INVALID" in invalid.blockers

    tampered_hash = policy.to_dict() | {"policy_hash": "0" * 64}
    invalid = validate_terminal_policy(tampered_hash, indexed_symbols=policy.core_symbols + policy.aggressive_symbols,
                                       holding_symbols=())
    assert not invalid.valid and "TERMINAL_POLICY_HASH_MISMATCH" in invalid.blockers


def test_last_shot_policy_requires_explicit_final_session_when_context_supplies_one():
    policy = _policy(mode=TerminalMode.LAST_SHOT)
    result = validate_terminal_policy(
        policy,
        indexed_symbols=[f"600{i:03d}.SH" for i in range(1, 13)],
        holding_symbols=(),
        expected_trade_date=context().research_date,
        observed_at=context().as_of,
        expected_final_trade_date=date(2026, 9, 30),
    )
    assert not result.valid
    assert "TERMINAL_POLICY_FINAL_DAY_MISMATCH" in result.blockers
