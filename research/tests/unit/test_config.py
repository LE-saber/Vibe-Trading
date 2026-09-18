from datetime import date
from decimal import Decimal

import pytest

from competition_core.config import ConfigurationError, load_config


DEFAULT_CONFIG = {
    "competition": {
        "start_date": "2026-08-31",
        "end_date": "2026-09-30",
        "advertised_trading_days": 23,
        "initial_capital": 1000000,
        "trading_market": "CN_A",
        "exchanges": ["SSE", "SZSE"],
        "allowed_boards": ["main", "chinext", "star"],
        "default_cash_ratio": 0.10,
        "snapshot_phase": "postclose",
        "universe_exclusions": {
            "st": True,
            "delisting_risk": True,
            "suspended": True,
            "untradeable": True,
        },
        "leaderboard": {
            "top_n": 30,
            "show_own_rank": True,
            "ranking": [
                {"metric": "total_assets", "direction": "descending"},
                {"metric": "trade_win_rate", "direction": "descending"},
                {"metric": "trade_count", "direction": "descending"},
                {"metric": "max_drawdown", "direction": "ascending"},
            ],
        },
        "board_rules": {
            "main": {
                "normal_price_limit": 0.10,
                "st_price_limit": 0.05,
                "new_listing_special_sessions": 5,
            },
            "chinext": {
                "normal_price_limit": 0.20,
                "st_price_limit": 0.05,
                "new_listing_special_sessions": 5,
            },
            "star": {
                "normal_price_limit": 0.20,
                "st_price_limit": 0.05,
                "new_listing_special_sessions": 5,
            },
        },
        "candidate_limits": {
            "universe": 6000,
            "shortlist": 20,
            "deep_analysis": 8,
        },
        "portfolio": {
            "max_single_weight": 0.20,
            "max_slippage_pct": 0.005,
            "max_quote_age_seconds": 60,
            "execution_mode": "MANUAL_CONFIRM",
        },
    },
    "runtime": {"endpoint": "http://127.0.0.1:8910"},
}


def _write_config(tmp_path, config=None):
    import yaml

    path = tmp_path / "competition.yaml"
    path.write_text(yaml.safe_dump(config or DEFAULT_CONFIG), encoding="utf-8")
    return path


def test_default_config_is_read_and_validated():
    config = load_config()

    assert config.start_date == date(2026, 8, 31)
    assert config.end_date == date(2026, 9, 30)
    assert config.advertised_trading_days == 23
    assert config.initial_capital == Decimal("1000000")
    assert config.trading_market == "CN_A"
    assert config.exchanges == ("SSE", "SZSE")
    assert config.allowed_boards == ("main", "chinext", "star")
    assert config.default_cash_ratio == Decimal("0.1")
    assert config.snapshot_phase == "postclose"
    assert dict(config.universe_exclusions) == {
        "st": True,
        "delisting_risk": True,
        "suspended": True,
        "untradeable": True,
    }
    assert config.leaderboard_top_n == 30
    assert config.show_own_rank is True
    assert config.ranking[0] == ("total_assets", "descending")
    assert config.board_rules["star"]["normal_price_limit"] == Decimal("0.2")
    assert dict(config.candidate_limits) == {
        "universe": 6000,
        "shortlist": 20,
        "deep_analysis": 8,
    }
    assert config.max_single_weight == Decimal("0.2")
    assert config.max_slippage_pct == Decimal("0.005")
    assert config.max_quote_age_seconds == 60
    assert config.execution_mode == "MANUAL_CONFIRM"
    assert config.runtime_endpoint == "http://127.0.0.1:8910"


@pytest.mark.parametrize(
    "start_date,end_date",
    [("2026-09-30", "2026-08-31"), ("2026-09-31", "2026-09-30")],
)
def test_invalid_dates_are_rejected(tmp_path, start_date, end_date):
    config = {**DEFAULT_CONFIG, "competition": {**DEFAULT_CONFIG["competition"], "start_date": start_date, "end_date": end_date}}

    with pytest.raises(ConfigurationError, match="date"):
        load_config(_write_config(tmp_path, config))


@pytest.mark.parametrize("initial_capital", [0, -1, "not-a-number"])
def test_invalid_initial_capital_is_rejected(tmp_path, initial_capital):
    config = {**DEFAULT_CONFIG, "competition": {**DEFAULT_CONFIG["competition"], "initial_capital": initial_capital}}

    with pytest.raises(ConfigurationError, match="initial_capital"):
        load_config(_write_config(tmp_path, config))


@pytest.mark.parametrize("advertised_trading_days", [0, -1, 23.0, "23"])
def test_invalid_advertised_trading_days_are_rejected(tmp_path, advertised_trading_days):
    config = {
        **DEFAULT_CONFIG,
        "competition": {
            **DEFAULT_CONFIG["competition"],
            "advertised_trading_days": advertised_trading_days,
        },
    }

    with pytest.raises(ConfigurationError, match="advertised_trading_days"):
        load_config(_write_config(tmp_path, config))


@pytest.mark.parametrize("cash_ratio", [-0.01, 1.01, "not-a-number"])
def test_invalid_default_cash_ratio_is_rejected(tmp_path, cash_ratio):
    config = {**DEFAULT_CONFIG, "competition": {**DEFAULT_CONFIG["competition"], "default_cash_ratio": cash_ratio}}

    with pytest.raises(ConfigurationError, match="default_cash_ratio"):
        load_config(_write_config(tmp_path, config))


@pytest.mark.parametrize(
    "candidate_limits",
    [{"shortlist": 0}, {"shortlist": -1}, {"shortlist": 1.5}],
)
def test_invalid_candidate_limits_are_rejected(tmp_path, candidate_limits):
    config = {
        **DEFAULT_CONFIG,
        "competition": {
            **DEFAULT_CONFIG["competition"],
            "candidate_limits": candidate_limits,
        },
    }

    with pytest.raises(ConfigurationError, match="candidate_limits"):
        load_config(_write_config(tmp_path, config))


def test_only_runtime_endpoint_environment_override_is_used(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPETITION_RUNTIME_ENDPOINT", "http://127.0.0.1:8920")
    monkeypatch.setenv("COMPETITION_INITIAL_CAPITAL", "1")
    monkeypatch.setenv("COMPETITION_DEFAULT_CASH_RATIO", "1")

    config = load_config(_write_config(tmp_path))

    assert config.runtime_endpoint == "http://127.0.0.1:8920"
    assert config.initial_capital == Decimal("1000000")
    assert config.default_cash_ratio == Decimal("0.1")


def test_invalid_runtime_endpoint_override_is_rejected(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPETITION_RUNTIME_ENDPOINT", "not-an-endpoint")

    with pytest.raises(ConfigurationError, match="runtime endpoint"):
        load_config(_write_config(tmp_path))
