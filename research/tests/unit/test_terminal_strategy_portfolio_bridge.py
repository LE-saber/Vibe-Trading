from datetime import date
from decimal import Decimal
from inspect import signature

from competition_core.pipeline.portfolio import build_portfolio
from competition_core.pipeline.portfolio_runner import run_portfolio_draft
from competition_core.terminal_strategy import (
    TerminalAllocationPolicy,
    TerminalMode,
    validate_terminal_policy,
)
from tests.pipeline_fixtures import context


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


def test_terminal_allocation_contract_is_not_consumed_by_s6():
    assert "terminal_policy" not in signature(build_portfolio).parameters
    assert "terminal_evaluation_id" not in signature(run_portfolio_draft).parameters


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
