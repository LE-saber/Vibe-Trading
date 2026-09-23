from copy import deepcopy
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from competition_core.pipeline.artifacts import content_hash
from competition_core.terminal_strategy import (
    TerminalStrategyConfig,
    load_terminal_strategy_config,
    run_terminal_runtime,
)
from competition_core.terminal_strategy import TerminalStrategyConfigError
from competition_core.terminal_strategy.runtime import _derive_remaining_trade_dates

from test_terminal_strategy_pipeline_adapter import _inputs as _candidate_inputs
from test_terminal_strategy_snapshot_adapter import _account_view, _leaderboard_view


SHANGHAI = ZoneInfo("Asia/Shanghai")


def _config(*, cutoff=30, dates=None):
    payload = load_terminal_strategy_config().to_dict()
    payload["competition"]["award_rank_cutoff"] = cutoff
    payload["competition"]["award_rule_source"] = "fixture:rules-v1" if cutoff is not None else None
    if dates is not None:
        payload["competition"]["remaining_trade_dates"] = [item.isoformat() for item in dates]
        payload["competition"]["final_trade_date"] = max(dates).isoformat()
    return TerminalStrategyConfig.from_dict(payload)


def _account():
    view = _account_view(unknown_count=0)
    view["account"]["cash_balance"] = "990000.00"
    view["account"]["available_cash"] = "990000.00"
    view["account"]["current_total_asset"] = "1000000.00"
    view["account"]["positions"] = [{
        "symbol": "600001.SH", "quantity": 100, "sellable_quantity": 50,
        "market_value": "10000.00",
    }]
    return view


def _values(*, config=None, root=None, implementation_hash="a" * 64, **changes):
    values = _candidate_inputs()
    values.update({
        "run_id": "runtime-fixture-run",
        "account_view": _account(),
        "leaderboard_view": _leaderboard_view(),
        "config": config or _config(),
        "persistence_root": root,
        "implementation_hash": implementation_hash,
    })
    values.pop("account_position_facts", None)
    values["core_symbols"] = None
    values.update(changes)
    return values


def test_runtime_persists_read_only_blocked_evaluation(tmp_path):
    result = run_terminal_runtime(**_values(root=tmp_path))
    assert result.evaluation is not None
    assert result.status == "BLOCKED"
    assert result.execution_ready is False
    assert result.path is not None and result.path.exists()
    assert result.evaluation.allocation_policy is None
    assert result.evaluation.blockers
    assert result.diagnostics.core_symbols == ("600001.SH",)


def test_cutoff_null_is_unavailable_but_auditable(tmp_path):
    result = run_terminal_runtime(**_values(root=tmp_path, config=_config(cutoff=None)))
    assert result.evaluation is not None
    assert result.status == "UNAVAILABLE"
    assert result.path is not None
    assert result.evaluation.snapshot.award_rank_cutoff is None


def test_same_input_is_idempotent_and_hash_bound(tmp_path):
    values = _values(root=tmp_path)
    first = run_terminal_runtime(**values)
    second = run_terminal_runtime(**values)
    assert first.evaluation_id == second.evaluation_id
    assert first.to_dict() == second.to_dict()
    assert first.path == second.path


def test_config_and_stage_changes_create_new_evaluation_ids(tmp_path):
    first = run_terminal_runtime(**_values(root=tmp_path))
    changed_config = _config()
    payload = changed_config.to_dict()
    payload["mode"]["hysteresis"] = "0.004"
    second = run_terminal_runtime(**_values(root=tmp_path, config=TerminalStrategyConfig.from_dict(payload)))
    assert first.evaluation_id != second.evaluation_id

    changed = _values(root=tmp_path)
    stage = deepcopy(changed["s4_stage"])
    stage["rows"][0]["features"]["selection_score"] = 81
    body = {key: value for key, value in stage.items() if key != "artifact_hash"}
    stage["artifact_hash"] = content_hash(body)
    changed["s4_stage"] = stage
    third = run_terminal_runtime(**changed)
    assert first.evaluation_id != third.evaluation_id


def test_core_shortfall_is_blocked_but_still_persisted(tmp_path):
    result = run_terminal_runtime(**_values(root=tmp_path))
    assert result.evaluation is not None
    assert len(result.diagnostics.core_symbols) == 1
    assert result.evaluation.allocation_policy is None
    assert result.path is not None


def test_fatal_stage_adapter_error_does_not_write_manifest(tmp_path):
    values = _values(root=tmp_path)
    values["s5_stage"]["artifact_hash"] = "f" * 64
    result = run_terminal_runtime(**values)
    assert result.evaluation is None
    assert result.path is None
    assert "STAGE_HASH_MISMATCH" in result.diagnostics.blockers
    runs = Path(tmp_path) / "runs"
    assert not runs.exists() or not list(runs.rglob("decision-manifest.json"))


def test_config_freezes_explicit_dates_and_final_day_is_sequence_derived(tmp_path):
    config = _config(dates=(date(2026, 9, 22), date(2026, 9, 30)))
    assert config.competition.remaining_trade_dates == (date(2026, 9, 22), date(2026, 9, 30))
    assert config.final_trade_date == max(config.competition.remaining_trade_dates)
    result = run_terminal_runtime(**_values(root=tmp_path, config=config))
    assert result.evaluation is not None
    assert result.evaluation.snapshot.is_final_trade_date is False


def test_config_remaining_dates_reject_duplicates_and_nonmax_final_date():
    payload = load_terminal_strategy_config().to_dict()
    payload["competition"]["remaining_trade_dates"] = ["2026-09-22", "2026-09-22"]
    with pytest.raises(TerminalStrategyConfigError):
        TerminalStrategyConfig.from_dict(payload)
    payload = load_terminal_strategy_config().to_dict()
    payload["competition"]["final_trade_date"] = "2026-09-29"
    with pytest.raises(TerminalStrategyConfigError):
        TerminalStrategyConfig.from_dict(payload)


def test_runtime_merges_explicit_same_day_adapter_blocker(tmp_path):
    result = run_terminal_runtime(**_values(root=tmp_path, same_day_exit_unavailable_symbols=None))
    assert result.evaluation is not None
    assert result.path is not None
    assert result.diagnostics.candidates is not None
    assert "SAME_DAY_INPUT_MISSING" in result.diagnostics.blockers


def test_explicit_implementation_hash_changes_identity_and_no_execution_capability(tmp_path):
    first = run_terminal_runtime(**_values(root=tmp_path, implementation_hash="a" * 64))
    second = run_terminal_runtime(**_values(root=tmp_path, implementation_hash="b" * 64))
    assert first.evaluation_id != second.evaluation_id
    assert first.execution_ready is False
    assert first.evaluation is not None and first.evaluation.execution_ready is False


@pytest.mark.parametrize(
    ("observed_date", "expected"),
    (
        (date(2026, 9, 22), (date(2026, 9, 22), date(2026, 9, 23), date(2026, 9, 24), date(2026, 9, 28), date(2026, 9, 29), date(2026, 9, 30))),
        (date(2026, 9, 23), (date(2026, 9, 23), date(2026, 9, 24), date(2026, 9, 28), date(2026, 9, 29), date(2026, 9, 30))),
        (date(2026, 9, 24), (date(2026, 9, 24), date(2026, 9, 28), date(2026, 9, 29), date(2026, 9, 30))),
        (date(2026, 9, 28), (date(2026, 9, 28), date(2026, 9, 29), date(2026, 9, 30))),
        (date(2026, 9, 30), (date(2026, 9, 30),)),
    ),
)
def test_remaining_sessions_are_derived_from_explicit_observation(observed_date, expected):
    reasons, blockers = [], []
    result = _derive_remaining_trade_dates(
        _config(), datetime.combine(observed_date, datetime.min.time(), tzinfo=SHANGHAI), reasons, blockers,
    )
    assert result == expected
    assert reasons == []
    assert blockers == []


@pytest.mark.parametrize(
    ("observed_date", "reason"),
    (
        (date(2026, 9, 21), "OBSERVED_TRADE_DATE_BEFORE_CALENDAR"),
        (date(2026, 9, 25), "OBSERVED_TRADE_DATE_NOT_CONFIRMED_SESSION"),
        (date(2026, 10, 1), "OBSERVED_TRADE_DATE_AFTER_FINAL_SESSION"),
    ),
)
def test_unconfirmed_observation_date_fails_closed(observed_date, reason):
    reasons, blockers = [], []
    result = _derive_remaining_trade_dates(
        _config(), datetime.combine(observed_date, datetime.min.time(), tzinfo=SHANGHAI), reasons, blockers,
    )
    assert result == ()
    assert blockers == ["TRADE_DATE_NOT_IN_CALENDAR"]
    assert reasons == [reason]


def test_naive_observation_never_uses_wall_clock_for_calendar_derivation():
    reasons, blockers = [], []
    result = _derive_remaining_trade_dates(
        _config(), datetime(2026, 9, 23), reasons, blockers,
    )
    assert result == ()
    assert blockers == ["OBSERVED_AT_INVALID"]
    assert reasons == []


def test_runtime_persists_derived_sessions_after_first_day(tmp_path):
    observed = datetime(2026, 9, 23, 13, 15, tzinfo=SHANGHAI)
    account = _account()
    account["account"]["as_of"] = observed.isoformat()
    leaderboard = _leaderboard_view(as_of=observed)
    values = _values(root=tmp_path, observed_at=observed, account_view=account, leaderboard_view=leaderboard)
    result = run_terminal_runtime(**values)
    assert result.evaluation is not None
    assert result.evaluation.snapshot.remaining_trade_dates == (
        date(2026, 9, 23), date(2026, 9, 24), date(2026, 9, 28),
        date(2026, 9, 29), date(2026, 9, 30),
    )
    assert result.evaluation.snapshot.is_final_trade_date is False
    assert result.evaluation.source_hashes.config_hash == _config().config_hash


def test_calendar_derivation_does_not_change_config_hash():
    first = _config()
    second = TerminalStrategyConfig.from_dict(first.to_dict())
    assert first.config_hash == second.config_hash
