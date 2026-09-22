from datetime import date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from competition_core.terminal_strategy import (
    AwardLinePoint,
    LeaderboardEntry,
    TerminalBlocker,
    TerminalModelError,
    TerminalSnapshot,
    TerminalStrategyConfig,
    forecast_award_line,
    load_terminal_strategy_config,
    target_asset_for_award_line,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 9, 22, 13, 15, tzinfo=SHANGHAI)


def _config(cutoff=30):
    payload = load_terminal_strategy_config().to_dict()
    payload["competition"]["award_rank_cutoff"] = cutoff
    payload["competition"]["award_rule_source"] = "fixture:rules-v1"
    return TerminalStrategyConfig.from_dict(payload)


def _snapshot(*, cutoff=30, own=Decimal("1067500.00"), trade_date=date(2026, 9, 22), remaining=None,
              line=Decimal("1071300.00"), fresh=True):
    remaining = remaining or (trade_date, date(2026, 9, 23), date(2026, 9, 24), date(2026, 9, 28), date(2026, 9, 29), date(2026, 9, 30))
    rows = tuple(
        LeaderboardEntry(rank, Decimal("1000000.00") + Decimal(rank) * Decimal("1000.00"), NOW)
        for rank in range(1, cutoff)
    ) + (LeaderboardEntry(cutoff, line, NOW),)
    return TerminalSnapshot(
        competition_id="synthetic-terminal-test",
        as_of=NOW,
        trade_date=trade_date,
        remaining_trade_dates=tuple(remaining),
        own_total_assets=own,
        leaderboard=rows,
        award_rank_cutoff=cutoff,
        award_rule_source="fixture:rules-v1",
        own_rank=34,
        account_fresh=fresh,
        leaderboard_fresh=fresh,
    )


def test_target_formula_uses_one_plus_buffer():
    assert target_asset_for_award_line(Decimal("1000000"), Decimal("0.01")) == Decimal("1010000.00")


def test_top30_display_without_confirmed_award_cutoff_fails_closed():
    config = load_terminal_strategy_config()
    snapshot = _snapshot()
    forecast = forecast_award_line(snapshot, config, observed_at=NOW)
    assert forecast.available is False
    assert TerminalBlocker.AWARD_RANK_UNCONFIRMED in forecast.blockers


def test_current_line_without_history_uses_max_buffer_and_low_confidence():
    config = _config()
    snapshot = _snapshot()
    forecast = forecast_award_line(snapshot, config, observed_at=NOW)
    assert forecast.available is True
    assert forecast.confidence.value == "LOW"
    assert forecast.predicted_award_line == Decimal("1071300.00")
    assert forecast.target_asset == Decimal("1087369.50")
    assert forecast.required_return == forecast.target_asset / snapshot.own_total_assets - 1


def test_monotonic_history_produces_conservative_adverse_forecast():
    config = _config()
    snapshot = _snapshot()
    history = [
        AwardLinePoint(NOW - timedelta(days=2), Decimal("1060000.00"), 30),
        AwardLinePoint(NOW - timedelta(days=1), Decimal("1065000.00"), 30),
    ]
    forecast = forecast_award_line(snapshot, config, history, observed_at=NOW)
    assert forecast.available is True
    assert forecast.adverse_award_line >= forecast.base_award_line >= forecast.favorable_award_line
    assert forecast.predicted_award_line == forecast.adverse_award_line
    assert forecast.target_asset == (forecast.predicted_award_line * (Decimal("1") + forecast.safety_buffer)).quantize(Decimal("0.01"))


def test_decreasing_history_or_missing_rank_is_a_hard_blocker():
    config = _config()
    snapshot = _snapshot()
    decreasing = [
        AwardLinePoint(NOW - timedelta(days=2), Decimal("1070000.00"), 30),
        AwardLinePoint(NOW - timedelta(days=1), Decimal("1060000.00"), 30),
    ]
    forecast = forecast_award_line(snapshot, config, decreasing, observed_at=NOW)
    assert forecast.available is False
    assert TerminalBlocker.NON_MONOTONIC_HISTORY in forecast.blockers
    wrong_rank = [AwardLinePoint(NOW - timedelta(days=1), Decimal("1070000.00"), 29)]
    forecast = forecast_award_line(snapshot, config, wrong_rank, observed_at=NOW)
    assert forecast.available is False
    assert TerminalBlocker.RANK_NOT_FOUND in forecast.blockers


def test_stale_snapshot_and_naive_time_are_rejected():
    config = _config()
    snapshot = _snapshot()
    forecast = forecast_award_line(snapshot, config, observed_at=NOW + timedelta(minutes=16))
    assert forecast.available is False
    assert TerminalBlocker.STALE_SNAPSHOT in forecast.blockers
    with pytest.raises(TerminalModelError, match="timezone-aware"):
        LeaderboardEntry(30, Decimal("1071300.00"), datetime(2026, 9, 22, 13, 15))
