from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from competition_core.terminal_strategy import (
    AwardLineForecast,
    ForecastConfidence,
    LeaderboardEntry,
    TerminalBlocker,
    TerminalMode,
    TerminalSnapshot,
    TerminalStrategyConfig,
    decide_terminal_mode,
    load_terminal_strategy_config,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 9, 22, 13, 15, tzinfo=SHANGHAI)


def _config():
    payload = load_terminal_strategy_config().to_dict()
    payload["competition"]["award_rank_cutoff"] = 30
    payload["competition"]["award_rule_source"] = "fixture:rules-v1"
    return TerminalStrategyConfig.from_dict(payload)


def _snapshot(own=Decimal("1000000.00"), trade_date=date(2026, 9, 22), remaining=None, fresh=True):
    remaining = remaining or (trade_date, date(2026, 9, 23), date(2026, 9, 24), date(2026, 9, 28), date(2026, 9, 29), date(2026, 9, 30))
    as_of = datetime.combine(trade_date, NOW.time(), tzinfo=SHANGHAI)
    rows = tuple(LeaderboardEntry(rank, Decimal("1000000.00") + Decimal(rank) * 100, as_of) for rank in range(1, 31))
    return TerminalSnapshot(
        "synthetic-mode-test", as_of, trade_date, tuple(remaining), own, rows,
        30, "fixture:rules-v1", 34, account_fresh=fresh, leaderboard_fresh=fresh,
    )


def _forecast(target=Decimal("1000000.00"), required=Decimal("0.02"), as_of=NOW):
    return AwardLineForecast(
        available=True, rank_cutoff=30, as_of=as_of,
        current_award_line=Decimal("980000.00"), predicted_award_line=Decimal("990000.00"),
        favorable_award_line=Decimal("985000.00"), base_award_line=Decimal("990000.00"),
        adverse_award_line=Decimal("1000000.00"), safety_buffer=Decimal("0.01"),
        target_asset=target, required_return=required, confidence=ForecastConfidence.LOW,
        method="fixture",
    )


def test_example_gap_is_normal_not_last_shot():
    snapshot = _snapshot(own=Decimal("1067500.00"))
    target = Decimal("1082000.00")
    required = target / snapshot.own_total_assets - 1
    decision = decide_terminal_mode(snapshot, _forecast(target, required), _config(), observed_at=NOW)
    assert decision.mode is TerminalMode.NORMAL
    assert decision.execution_ready is True


def test_protect_and_attack_boundaries_are_deterministic():
    config = _config()
    protect_snapshot = _snapshot(own=Decimal("1030000.00"))
    protect_target = Decimal("1000000.00")
    protect = decide_terminal_mode(
        protect_snapshot,
        _forecast(protect_target, protect_target / protect_snapshot.own_total_assets - 1),
        config,
        observed_at=NOW,
    )
    assert protect.mode is TerminalMode.PROTECT
    attack_snapshot = _snapshot(own=Decimal("950000.00"))
    attack_target = Decimal("1000000.00")
    attack = decide_terminal_mode(
        attack_snapshot,
        _forecast(attack_target, attack_target / attack_snapshot.own_total_assets - 1),
        config,
        observed_at=NOW,
    )
    assert attack.mode is TerminalMode.ATTACK


def test_last_shot_requires_final_trade_session():
    config = _config()
    non_final = _snapshot(own=Decimal("900000.00"))
    target = Decimal("1000000.00")
    decision = decide_terminal_mode(non_final, _forecast(target, target / non_final.own_total_assets - 1), config, observed_at=NOW)
    assert decision.mode is TerminalMode.ATTACK
    assert "LAST_SHOT_DEFERRED_NON_FINAL_SESSION" in decision.reasons
    final_date = date(2026, 9, 30)
    final = _snapshot(own=Decimal("900000.00"), trade_date=final_date, remaining=(final_date,))
    decision = decide_terminal_mode(
        final, _forecast(target, target / final.own_total_assets - 1, as_of=final.as_of), config,
        observed_at=final.as_of,
    )
    assert decision.mode is TerminalMode.LAST_SHOT


def test_downgrade_requires_hysteresis_and_two_confirmations():
    config = _config()
    snapshot = _snapshot(own=Decimal("980000.00"))
    target = Decimal("1000000.00")
    forecast = _forecast(target, target / snapshot.own_total_assets - 1)
    first = decide_terminal_mode(snapshot, forecast, config, previous_mode=TerminalMode.ATTACK, observed_at=NOW)
    assert first.mode is TerminalMode.ATTACK
    assert first.downgrade_confirmation_count == 1
    second = decide_terminal_mode(
        snapshot, forecast, config, previous_mode=TerminalMode.ATTACK,
        downgrade_confirmation_count=first.downgrade_confirmation_count, observed_at=NOW,
    )
    assert second.mode is TerminalMode.NORMAL
    assert second.downgrade_confirmation_count == 0


def test_stale_or_partial_data_is_unavailable_and_never_overridden():
    config = _config()
    snapshot = _snapshot(fresh=False)
    decision = decide_terminal_mode(snapshot, _forecast(), config, observed_at=NOW)
    assert decision.mode is TerminalMode.UNAVAILABLE
    assert TerminalBlocker.INCOMPLETE_ACCOUNT in decision.blockers
    snapshot = _snapshot()
    decision = decide_terminal_mode(snapshot, _forecast(), config, full_snapshot=False, observed_at=NOW)
    assert decision.mode is TerminalMode.UNAVAILABLE
    assert TerminalBlocker.INCOMPLETE_REEVALUATION in decision.blockers
