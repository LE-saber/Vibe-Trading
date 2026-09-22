from datetime import date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from competition_core.terminal_strategy import (
    CandidateBlocker,
    CandidateObservation,
    TerminalMode,
    TerminalStrategyConfig,
    build_candidate_funnel,
    load_terminal_strategy_config,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")
TRADE_DATE = date(2026, 9, 22)
AS_OF = datetime(2026, 9, 22, 13, 15, tzinfo=SHANGHAI)


def _config() -> TerminalStrategyConfig:
    payload = load_terminal_strategy_config().to_dict()
    payload["competition"]["award_rank_cutoff"] = 30
    payload["competition"]["award_rule_source"] = "fixture:rules-v1"
    return TerminalStrategyConfig.from_dict(payload)


def _observation(symbol: str = "600001.SH", *, index: int = 0, **changes) -> CandidateObservation:
    values = {
        "symbol": symbol,
        "as_of": AS_OF,
        "trade_date": TRADE_DATE,
        "relative_strength_1d": Decimal("0.010") + Decimal(index) / Decimal("1000"),
        "relative_strength_3d": Decimal("0.025") + Decimal(index) / Decimal("1000"),
        "relative_strength_5d": Decimal("0.040") + Decimal(index) / Decimal("1000"),
        "benchmark_relative_1d": Decimal("0.005"),
        "benchmark_relative_3d": Decimal("0.010"),
        "benchmark_relative_5d": Decimal("0.015"),
        "amount_cny": Decimal("100000000") + Decimal(index) * Decimal("1000000"),
        "turnover_rate": Decimal("5.0"),
        "volume_ratio": Decimal("1.5"),
        "liquidity_score": Decimal("85"),
        "opening_gap": Decimal("0.010"),
        "close_strength": Decimal("0.85"),
        "intraday_max_drawdown": Decimal("0.020"),
        "trend_continuity": Decimal("0.85"),
        "consecutive_strong_sessions": 3,
        "single_day_pulse": False,
        "industry": "fixture-industry",
        "correlation_cluster": f"cluster-{index}",
        "correlation_to_holdings": {},
        "announcement_risk": False,
        "regulatory_risk": False,
        "reduction_risk": False,
        "earnings_risk": False,
        "risk_facts_complete": True,
        "overheat_score": Decimal("30"),
        "evidence_complete": True,
        "evidence_quality": Decimal("0.90"),
        "evidence_missing": (),
        "evidence_sources": ("fixture.evidence",),
        "quote_as_of": AS_OF,
        "quote_trade_date": TRADE_DATE,
        "quote_age_seconds": Decimal("0"),
        "is_suspended": False,
        "is_tradeable": True,
        "price_locked": False,
        "executable_quote": True,
        "has_valid_bid_ask": True,
        "bid_price": Decimal("9.99"),
        "ask_price": Decimal("10.01"),
        "current_price": Decimal("10.00"),
        "previous_close": Decimal("9.80"),
        "simulated_fill_rate": Decimal("0.80"),
        "simulated_slippage_p95": Decimal("0.01"),
        "simulated_failure_reasons": (),
        "board": "MAIN",
        "listed_sessions": 1000,
        "is_new_listing": False,
        "price_limit_pct": Decimal("0.10"),
        "price_limit_rule_known": True,
        "new_listing_rule_known": True,
        "holding_quantity": 0,
        "sellable_quantity": 0,
        "same_day_exit_unavailable": False,
        "orders_reconciled": True,
        "trigger_price": Decimal("10.00"),
        "invalidation_price": Decimal("9.50"),
        "limit_price": Decimal("10.10"),
        "max_weight": Decimal("0.20"),
        "source_hash": f"fixture-{symbol}",
    }
    values.update(changes)
    return CandidateObservation(**values)


def test_three_tier_funnel_caps_are_explicit_and_terminal_is_aggressive_only():
    observations = [_observation(f"600{i:03d}.SH", index=i) for i in range(1, 13)]
    result = build_candidate_funnel(observations, _config(), TerminalMode.NORMAL, observed_at=AS_OF)
    assert result.status == "SUCCESS"
    assert result.input_count == 12
    assert len(result.watchlist) == 10
    assert len(result.focus) == 5
    assert len(result.terminal) == 3
    assert result.terminal_role == "aggressive_sleeve_candidates_only"
    assert result.execution_ready is False
    assert all(item.max_weight == Decimal("0.20") for item in result.terminal)


def test_stable_symbol_tie_break_and_audit_hash_are_input_order_independent():
    observations = [_observation(f"600{i:03d}.SH") for i in range(1, 13)]
    first = build_candidate_funnel(observations, _config(), TerminalMode.ATTACK, observed_at=AS_OF)
    second = build_candidate_funnel(list(reversed(observations)), _config(), TerminalMode.ATTACK, observed_at=AS_OF)
    assert first.watch_symbols == second.watch_symbols == tuple(sorted(first.watch_symbols))
    assert first.focus_symbols == second.focus_symbols
    assert first.terminal_symbols == second.terminal_symbols
    assert first.input_hash == second.input_hash
    assert first.funnel_hash == second.funnel_hash
    assert all(isinstance(item.score, Decimal) for item in first.watchlist)


@pytest.mark.parametrize(
    "change,blocker",
    [
        (lambda value: value.update(quote_as_of=AS_OF - timedelta(seconds=61)), CandidateBlocker.QUOTE_STALE),
        (lambda value: value.update(is_suspended=True), CandidateBlocker.SUSPENDED),
        (lambda value: value.update(price_locked=True), CandidateBlocker.PRICE_LOCKED),
        (lambda value: value.update(liquidity_score=Decimal("20")), CandidateBlocker.LIQUIDITY_BELOW_MINIMUM),
        (lambda value: value.update(evidence_complete=None), CandidateBlocker.EVIDENCE_MISSING),
        (lambda value: value.update(price_limit_rule_known=None), CandidateBlocker.RULES_MISSING),
        (lambda value: value.update(correlation_to_holdings={"600519.SH": Decimal("0.90")}), CandidateBlocker.CORRELATION_TOO_HIGH),
    ],
)
def test_permanent_hard_gates_never_relax_in_last_shot(change, blocker):
    observation = _observation()
    changes = {field.name: getattr(observation, field.name) for field in observation.__dataclass_fields__.values()}
    change(changes)
    changed = CandidateObservation(**changes)
    for mode in (TerminalMode.NORMAL, TerminalMode.ATTACK, TerminalMode.LAST_SHOT):
        result = build_candidate_funnel([changed], _config(), mode, observed_at=AS_OF)
        assert result.watchlist == ()
        assert blocker in result.rejected[0].blockers


def test_missing_score_facts_are_not_treated_as_good():
    observation = _observation(relative_strength_3d=None, trend_continuity=None, evidence_quality=None)
    result = build_candidate_funnel([observation], _config(), TerminalMode.NORMAL, observed_at=AS_OF)
    assert result.watchlist == ()
    assert CandidateBlocker.RELATIVE_STRENGTH_MISSING in result.rejected[0].blockers
    assert CandidateBlocker.TREND_CONTINUITY_MISSING in result.rejected[0].blockers
    assert CandidateBlocker.EVIDENCE_MISSING in result.rejected[0].blockers


def test_only_last_shot_relaxes_overheat_soft_gate_without_new_listing_bonus():
    overheated = _observation(overheat_score=Decimal("90"), is_new_listing=True, listed_sessions=3)
    normal = build_candidate_funnel([overheated], _config(), TerminalMode.NORMAL, observed_at=AS_OF)
    attack = build_candidate_funnel([overheated], _config(), TerminalMode.ATTACK, observed_at=AS_OF)
    last = build_candidate_funnel([overheated], _config(), TerminalMode.LAST_SHOT, observed_at=AS_OF)
    assert normal.watchlist == () and attack.watchlist == ()
    assert CandidateBlocker.OVERHEATED_SOFT_GATE in normal.rejected[0].blockers
    assert CandidateBlocker.OVERHEATED_SOFT_GATE in attack.rejected[0].blockers
    assert len(last.watchlist) == 1
    assert "OVERHEAT_SOFT_GATE_RELAXED_LAST_SHOT" in last.watchlist[0].warnings
    assert "NEW_LISTING_AND_OVERHEAT_NO_SCORE_BONUS" in last.watchlist[0].warnings

    old = _observation(overheat_score=Decimal("30"), is_new_listing=False, listed_sessions=1000)
    new = _observation("600002.SH", overheat_score=Decimal("30"), is_new_listing=True, listed_sessions=3)
    result = build_candidate_funnel([old, new], _config(), TerminalMode.NORMAL, observed_at=AS_OF)
    scores = {item.symbol: item.score for item in result.watchlist}
    assert scores[old.symbol] == scores[new.symbol]


def test_terminal_requires_machine_checkable_trigger_rules_and_reports_shortfall():
    observations = [
        _observation("600001.SH", trigger_price=None),
        _observation("600002.SH", invalidation_price=None),
        _observation("600003.SH", max_weight=Decimal("0.21")),
    ]
    result = build_candidate_funnel(observations, _config(), TerminalMode.ATTACK, observed_at=AS_OF)
    assert result.terminal == ()
    assert "TERMINAL_CANDIDATE_SHORTFALL" in result.reasons
    assert CandidateBlocker.TERMINAL_RULES_MISSING in result.blockers or CandidateBlocker.TERMINAL_WEIGHT_EXCEEDS_CAP in result.blockers


def test_same_day_exit_is_allowed_only_as_last_shot_softly_ranked_candidate():
    observation = _observation(same_day_exit_unavailable=True)
    for mode in (TerminalMode.PROTECT, TerminalMode.NORMAL, TerminalMode.ATTACK):
        result = build_candidate_funnel([observation], _config(), mode, observed_at=AS_OF)
        assert result.watchlist == ()
        assert CandidateBlocker.SAME_DAY_EXIT_UNAVAILABLE in result.rejected[0].blockers
    result = build_candidate_funnel([observation], _config(), TerminalMode.LAST_SHOT, observed_at=AS_OF)
    assert len(result.watchlist) == 1
    assert "SAME_DAY_EXIT_UNAVAILABLE_LAST_SHOT" in result.watchlist[0].warnings


def test_funnel_weight_and_threshold_configuration_is_strict():
    payload = _config().to_dict()
    payload["funnel"]["scoring_weights"]["relative_strength"] = "0.30"
    with pytest.raises(ValueError, match="weights must sum"):
        TerminalStrategyConfig.from_dict(payload)
    payload = _config().to_dict()
    payload["funnel"]["max_quote_age_seconds"] = 0
    with pytest.raises(ValueError, match="max_quote_age_seconds"):
        TerminalStrategyConfig.from_dict(payload)
