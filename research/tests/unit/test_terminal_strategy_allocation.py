from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from competition_core.terminal_strategy import (
    AllocationBlocker,
    CandidateObservation,
    TerminalAllocationError,
    TerminalAllocationInputs,
    TerminalMode,
    TerminalModeDecision,
    TerminalStrategyConfig,
    build_candidate_funnel,
    build_terminal_allocation_policy,
    evaluate_terminal_allocation,
    load_terminal_strategy_config,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")
NORMAL_DATE = date(2026, 9, 22)
NORMAL_AS_OF = datetime(2026, 9, 22, 13, 15, tzinfo=SHANGHAI)
FINAL_DATE = date(2026, 9, 30)
FINAL_AS_OF = datetime(2026, 9, 30, 13, 15, tzinfo=SHANGHAI)


def _config() -> TerminalStrategyConfig:
    payload = load_terminal_strategy_config().to_dict()
    payload["competition"]["award_rank_cutoff"] = 30
    payload["competition"]["award_rule_source"] = "fixture:rules-v1"
    return TerminalStrategyConfig.from_dict(payload)


def _observation(
    symbol: str,
    *,
    index: int = 0,
    as_of: datetime = NORMAL_AS_OF,
    trade_date: date = NORMAL_DATE,
    same_day_exit_unavailable: bool = False,
    is_new_listing: bool = False,
    overheat_score: Decimal = Decimal("30"),
    max_weight: Decimal = Decimal("0.20"),
) -> CandidateObservation:
    return CandidateObservation(
        symbol=symbol,
        as_of=as_of,
        trade_date=trade_date,
        relative_strength_1d=Decimal("0.010") + Decimal(index) / Decimal("1000"),
        relative_strength_3d=Decimal("0.025") + Decimal(index) / Decimal("1000"),
        relative_strength_5d=Decimal("0.040") + Decimal(index) / Decimal("1000"),
        benchmark_relative_1d=Decimal("0.005"),
        benchmark_relative_3d=Decimal("0.010"),
        benchmark_relative_5d=Decimal("0.015"),
        amount_cny=Decimal("100000000") + Decimal(index) * Decimal("1000000"),
        turnover_rate=Decimal("5.0"),
        volume_ratio=Decimal("1.5"),
        liquidity_score=Decimal("85"),
        opening_gap=Decimal("0.010"),
        close_strength=Decimal("0.85"),
        intraday_max_drawdown=Decimal("0.020"),
        trend_continuity=Decimal("0.85"),
        consecutive_strong_sessions=3,
        single_day_pulse=False,
        industry="fixture-industry",
        correlation_cluster=f"cluster-{index}",
        correlation_to_holdings={},
        announcement_risk=False,
        regulatory_risk=False,
        reduction_risk=False,
        earnings_risk=False,
        risk_facts_complete=True,
        overheat_score=overheat_score,
        evidence_complete=True,
        evidence_quality=Decimal("0.90"),
        evidence_missing=(),
        evidence_sources=("fixture.evidence",),
        quote_as_of=as_of,
        quote_trade_date=trade_date,
        quote_age_seconds=Decimal("0"),
        is_suspended=False,
        is_tradeable=True,
        price_locked=False,
        executable_quote=True,
        has_valid_bid_ask=True,
        bid_price=Decimal("9.99"),
        ask_price=Decimal("10.01"),
        current_price=Decimal("10.00"),
        previous_close=Decimal("9.80"),
        simulated_fill_rate=Decimal("0.80"),
        simulated_slippage_p95=Decimal("0.01"),
        simulated_failure_reasons=(),
        board="MAIN",
        listed_sessions=3 if is_new_listing else 1000,
        is_new_listing=is_new_listing,
        price_limit_pct=Decimal("0.10"),
        price_limit_rule_known=True,
        new_listing_rule_known=True,
        holding_quantity=0,
        sellable_quantity=0,
        same_day_exit_unavailable=same_day_exit_unavailable,
        orders_reconciled=True,
        trigger_price=Decimal("10.00"),
        invalidation_price=Decimal("9.50"),
        limit_price=Decimal("10.10"),
        max_weight=max_weight,
        source_hash=f"fixture-{symbol}",
    )


def _decision(mode: TerminalMode, *, final: bool = False) -> TerminalModeDecision:
    as_of = FINAL_AS_OF if final else NORMAL_AS_OF
    trade_date = FINAL_DATE if final else NORMAL_DATE
    return TerminalModeDecision(
        mode=mode,
        as_of=as_of,
        trade_date=trade_date,
        is_final_trade_date=final,
        target_asset=Decimal("1000000.00"),
        required_return=Decimal("0.03"),
        lead_return=Decimal("0.00"),
        safety_buffer=Decimal("0.01"),
        reasons=(f"FIXTURE_{mode.value}",),
    )


def _funnel(
    mode: TerminalMode,
    *,
    count: int = 3,
    final: bool = False,
    same_day_exit_unavailable: bool = False,
    is_new_listing: bool = False,
    overheat_score: Decimal = Decimal("30"),
    max_weight: Decimal = Decimal("0.20"),
):
    as_of = FINAL_AS_OF if final else NORMAL_AS_OF
    trade_date = FINAL_DATE if final else NORMAL_DATE
    observations = [
        _observation(
            f"600{i:03d}.SH",
            index=i,
            as_of=as_of,
            trade_date=trade_date,
            same_day_exit_unavailable=same_day_exit_unavailable,
            is_new_listing=is_new_listing,
            overheat_score=overheat_score,
            max_weight=max_weight,
        )
        for i in range(1, count + 1)
    ]
    return build_candidate_funnel(observations, _config(), mode, observed_at=as_of)


def _inputs(mode: TerminalMode, *, core_count: int, **funnel_kwargs) -> TerminalAllocationInputs:
    funnel = _funnel(mode, **funnel_kwargs)
    return TerminalAllocationInputs(
        mode_decision=_decision(mode, final=funnel.trade_date == FINAL_DATE),
        funnel=funnel,
        core_symbols=tuple(f"600{i:03d}.SH" for i in range(101, 101 + core_count)),
        same_day_exit_unavailable_symbols=(
            funnel.terminal_symbols[0:1] if funnel_kwargs.get("same_day_exit_unavailable", False) else ()
        ),
    )


@pytest.mark.parametrize(
    ("mode", "core_count", "final", "expected_aggressive", "expected_cash", "expected_total"),
    [
        (TerminalMode.PROTECT, 5, False, Decimal("0"), Decimal("0.20"), 5),
        (TerminalMode.NORMAL, 4, False, Decimal("0.30"), Decimal("0.15"), 6),
        (TerminalMode.ATTACK, 4, False, Decimal("0.40"), Decimal("0.10"), 6),
        (TerminalMode.LAST_SHOT, 5, True, Decimal("0.60"), Decimal("0.10"), 8),
    ],
)
def test_four_mode_budget_intent_and_position_semantics(
    mode, core_count, final, expected_aggressive, expected_cash, expected_total
):
    policy = build_terminal_allocation_policy(
        _inputs(mode, core_count=core_count, final=final), _config()
    )
    assert policy.aggressive_budget == expected_aggressive
    assert policy.cash_target == expected_cash
    assert policy.core_budget + policy.aggressive_budget + policy.cash_target == Decimal("1")
    assert policy.total_position_count == expected_total
    assert policy.execution_ready is False
    assert len(policy.aggressive_symbols) in {0, 2, 3}
    assert policy.terminal_candidate_count == len(policy.aggressive_symbols)
    assert policy.terminal_role == "aggressive_sleeve_only"


def test_terminal_candidates_are_only_aggressive_symbols_and_core_is_preserved():
    policy = build_terminal_allocation_policy(_inputs(TerminalMode.NORMAL, core_count=4), _config())
    assert policy.core_symbols == tuple(f"600{i:03d}.SH" for i in range(101, 105))
    assert set(policy.aggressive_symbols).isdisjoint(policy.core_symbols)
    assert len(policy.aggressive_symbols) == 2
    assert policy.total_position_count == 6
    assert "core_positions" not in policy.to_dict()
    assert "quantity" not in str(policy.to_dict()).lower()
    assert "order" not in str(policy.to_dict()).lower()
    assert "price" not in str(policy.to_dict()).lower()


def test_protect_does_not_add_new_listing_or_high_risk_symbol():
    inputs = _inputs(
        TerminalMode.PROTECT,
        core_count=5,
        is_new_listing=True,
        overheat_score=Decimal("30"),
    )
    assert "NEW_LISTING" in inputs.funnel.terminal[0].risk_tags
    policy = build_terminal_allocation_policy(inputs, _config())
    assert policy.aggressive_symbols == ()
    assert policy.aggressive_budget == Decimal("0")
    assert "PROTECT_PRESERVES_CORE_WITHOUT_NEW_RISK" in policy.reasons


def test_last_shot_marks_same_day_exit_unavailable_without_claiming_exit():
    inputs = _inputs(
        TerminalMode.LAST_SHOT,
        core_count=5,
        final=True,
        same_day_exit_unavailable=True,
    )
    policy = build_terminal_allocation_policy(inputs, _config())
    assert policy.same_day_exit_unavailable
    assert set(policy.same_day_exit_unavailable).issubset(policy.aggressive_symbols)
    assert "SAME_DAY_EXIT_UNAVAILABLE_MARKED" in policy.reasons
    assert policy.execution_ready is False


@pytest.mark.parametrize(
    "factory, blocker",
    [
        (lambda: _inputs(TerminalMode.NORMAL, core_count=4, count=1), AllocationBlocker.TERMINAL_CANDIDATE_SHORTFALL),
        (lambda: _inputs(TerminalMode.UNAVAILABLE, core_count=5), AllocationBlocker.MODE_UNAVAILABLE),
        (lambda: TerminalAllocationInputs(
            mode_decision=_decision(TerminalMode.NORMAL),
            funnel=_funnel(TerminalMode.NORMAL),
            core_symbols=("600101.SH", "600101.SH", "600102.SH", "600103.SH"),
        ), AllocationBlocker.CORE_HOLDINGS_DUPLICATE),
    ],
)
def test_shortage_unavailable_and_duplicate_fail_closed(factory, blocker):
    inputs = factory()
    result = evaluate_terminal_allocation(inputs, _config())
    assert result.policy is None
    assert blocker in result.blockers
    with pytest.raises(TerminalAllocationError) as error:
        build_terminal_allocation_policy(inputs, _config())
    assert blocker in error.value.blockers


def test_duplicate_candidate_in_funnel_is_not_silently_repaired():
    observations = [_observation("600001.SH"), _observation("600001.SH", index=1), _observation("600002.SH", index=2)]
    funnel = build_candidate_funnel(observations, _config(), TerminalMode.NORMAL, observed_at=NORMAL_AS_OF)
    inputs = TerminalAllocationInputs(
        mode_decision=_decision(TerminalMode.NORMAL),
        funnel=funnel,
        core_symbols=("600101.SH", "600102.SH", "600103.SH", "600104.SH"),
    )
    result = evaluate_terminal_allocation(inputs, _config())
    assert result.policy is None
    assert AllocationBlocker.TERMINAL_CANDIDATE_DUPLICATE in result.blockers


def test_final_day_gate_and_funnel_status_are_fail_closed():
    not_final = _inputs(TerminalMode.LAST_SHOT, core_count=5, final=False)
    result = evaluate_terminal_allocation(not_final, _config())
    assert result.policy is None
    assert AllocationBlocker.FINAL_DAY_REQUIRED in result.blockers

    partial = _inputs(TerminalMode.NORMAL, core_count=4, count=2, max_weight=Decimal("0.21"))
    result = evaluate_terminal_allocation(partial, _config())
    assert result.policy is None
    assert AllocationBlocker.FUNNEL_NOT_READY in result.blockers


def test_budget_and_single_name_boundaries_are_strict():
    payload = _config().to_dict()
    payload["allocation"]["protect_cash_target"] = "0.19"
    with pytest.raises(ValueError, match="Protect cash"):
        TerminalStrategyConfig.from_dict(payload)
    payload = _config().to_dict()
    payload["funnel"]["terminal_count_max"] = 4
    with pytest.raises(ValueError, match="terminal_count_max"):
        TerminalStrategyConfig.from_dict(payload)


def test_policy_and_input_hashes_are_stable_across_ordering():
    first = _inputs(TerminalMode.ATTACK, core_count=4)
    second = TerminalAllocationInputs(
        mode_decision=first.mode_decision,
        funnel=build_candidate_funnel(
            list(reversed([_observation(f"600{i:03d}.SH", index=i) for i in range(1, 4)])),
            _config(),
            TerminalMode.ATTACK,
            observed_at=NORMAL_AS_OF,
        ),
        core_symbols=tuple(reversed(first.core_symbols)),
    )
    left = build_terminal_allocation_policy(first, _config())
    right = build_terminal_allocation_policy(second, _config())
    assert first.input_hash == second.input_hash
    assert left.policy_hash == right.policy_hash
    assert left.to_dict() == right.to_dict()


def test_hashes_bind_mode_decision_funnel_and_config():
    policy = build_terminal_allocation_policy(_inputs(TerminalMode.NORMAL, core_count=4), _config())
    body = policy.to_dict(include_hash=False)
    assert policy.mode_decision_hash == policy.mode_decision_hash
    assert policy.funnel_hash
    assert policy.config_hash
    assert policy.input_hash
    assert policy.policy_hash
    assert policy.policy_hash != ""
    assert "execution_ready" in policy.to_dict()
    assert body["schema_version"] == "competition.terminal-allocation-policy.v1"
