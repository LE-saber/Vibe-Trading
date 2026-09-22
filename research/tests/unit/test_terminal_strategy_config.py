from copy import deepcopy

import pytest
import yaml

from competition_core.terminal_strategy import (
    TerminalStrategyConfig,
    TerminalStrategyConfigError,
    load_terminal_strategy_config,
)


def test_default_config_fails_closed_for_unconfirmed_award_cutoff():
    config = load_terminal_strategy_config()
    assert config.award_rank_cutoff is None
    assert config.competition.award_rule_source is None
    assert config.mode.last_shot_only_on_final_session is True
    assert config.schedule.primary_decision.isoformat(timespec="minutes") == "13:30"
    assert config.config_hash == TerminalStrategyConfig.from_dict(config.to_dict()).config_hash


@pytest.mark.parametrize(
    ("section", "key", "value"),
    [
        ("competition", "award_rank_cutoff", 0),
        ("award_line", "safety_buffer_base", "nan"),
        ("mode", "attack_gap_max", 0.02),
        ("mode", "last_shot_only_on_final_session", False),
        ("funnel", "terminal_count_min", 6),
        ("allocation", "last_shot_cash_target", 0.09),
        ("schedule", "risk_freeze", "15:00"),
    ],
)
def test_invalid_or_unsafe_configuration_is_rejected(section, key, value):
    payload = load_terminal_strategy_config().to_dict()
    payload[section][key] = value
    with pytest.raises(TerminalStrategyConfigError):
        TerminalStrategyConfig.from_dict(payload)


def test_missing_cutoff_is_allowed_only_as_explicit_fail_closed_state(tmp_path):
    payload = load_terminal_strategy_config().to_dict()
    payload["competition"].pop("award_rank_cutoff")
    path = tmp_path / "terminal-strategy.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    assert load_terminal_strategy_config(path).award_rank_cutoff is None


def test_unknown_root_or_nested_fields_are_rejected():
    payload = deepcopy(load_terminal_strategy_config().to_dict())
    payload["unexpected"] = {}
    with pytest.raises(TerminalStrategyConfigError):
        TerminalStrategyConfig.from_dict(payload)
    payload = deepcopy(load_terminal_strategy_config().to_dict())
    payload["mode"]["force_last_shot"] = True
    with pytest.raises(TerminalStrategyConfigError):
        TerminalStrategyConfig.from_dict(payload)
