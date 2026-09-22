from datetime import date, datetime
from decimal import Decimal
from dataclasses import replace
from zoneinfo import ZoneInfo

import pytest

from competition_core.pipeline.artifacts import content_hash
from competition_core.terminal_strategy import (
    AwardLinePoint,
    LeaderboardEntry,
    TerminalEvaluationRequest,
    TerminalMode,
    TerminalServiceError,
    TerminalSnapshot,
    TerminalSourceHashes,
    TerminalStrategyConfig,
    TerminalStrategyService,
    load_terminal_strategy_config,
)

from test_terminal_strategy_allocation import _observation


SHANGHAI = ZoneInfo("Asia/Shanghai")
TRADE_DATE = date(2026, 9, 22)
AS_OF = datetime(2026, 9, 22, 13, 15, tzinfo=SHANGHAI)


def _config(*, cutoff=30) -> TerminalStrategyConfig:
    payload = load_terminal_strategy_config().to_dict()
    payload["competition"]["award_rank_cutoff"] = cutoff
    payload["competition"]["award_rule_source"] = "fixture:rules-v1" if cutoff is not None else None
    return TerminalStrategyConfig.from_dict(payload)


def _digest(label: str) -> str:
    return content_hash({"fixture_source": label})


def _snapshot(
    *,
    config: TerminalStrategyConfig,
    own: Decimal = Decimal("1067500.00"),
    trade_date: date = TRADE_DATE,
    as_of: datetime = AS_OF,
    account_hash: str | None = None,
    leaderboard_hash: str | None = None,
) -> TerminalSnapshot:
    cutoff = config.competition.award_rank_cutoff
    rows = tuple(
        LeaderboardEntry(rank, Decimal("1000000.00") + Decimal(rank) * Decimal("1000.00"), as_of)
        for rank in range(1, 30)
    ) + (LeaderboardEntry(30, Decimal("1071300.00"), as_of),)
    return TerminalSnapshot(
        competition_id="terminal-service-fixture",
        as_of=as_of,
        trade_date=trade_date,
        remaining_trade_dates=(trade_date, date(2026, 9, 23), date(2026, 9, 24), date(2026, 9, 28), date(2026, 9, 29), date(2026, 9, 30),),
        own_total_assets=own,
        leaderboard=rows,
        award_rank_cutoff=cutoff,
        award_rule_source="fixture:rules-v1" if cutoff is not None else None,
        own_rank=34,
        account_snapshot_hash=account_hash,
        leaderboard_snapshot_hash=leaderboard_hash,
    )


def _source_hashes(config: TerminalStrategyConfig, *, account: str, leaderboard: str) -> TerminalSourceHashes:
    return TerminalSourceHashes(
        s4_hash=_digest("s4"),
        s5_hash=_digest("s5"),
        s5_5_hash=_digest("s5.5"),
        account_snapshot_hash=account,
        leaderboard_snapshot_hash=leaderboard,
        config_hash=config.config_hash,
        implementation_hash=_digest("terminal-implementation-v1"),
    )


def _request(
    *,
    config: TerminalStrategyConfig | None = None,
    candidates=3,
    core_count=4,
    previous_mode=None,
    account_hash: str | None = None,
    leaderboard_hash: str | None = None,
    trade_date: date = TRADE_DATE,
    as_of: datetime = AS_OF,
    full_snapshot=True,
) -> TerminalEvaluationRequest:
    config = config or _config()
    account_hash = account_hash or _digest("account")
    leaderboard_hash = leaderboard_hash or _digest("leaderboard")
    snapshot = _snapshot(
        config=config,
        trade_date=trade_date,
        as_of=as_of,
        account_hash=account_hash,
        leaderboard_hash=leaderboard_hash,
    )
    observations = tuple(
        _observation(
            f"600{i:03d}.SH",
            index=i,
            as_of=as_of,
            trade_date=trade_date,
        )
        for i in range(1, candidates + 1)
    )
    return TerminalEvaluationRequest(
        run_id="terminal-fixture-run",
        snapshot=snapshot,
        award_line_history=(),
        candidates=observations,
        core_symbols=tuple(f"600{i:03d}.SH" for i in range(101, 101 + core_count)),
        config=config,
        source_hashes=_source_hashes(config, account=account_hash, leaderboard=leaderboard_hash),
        observed_at=as_of,
        previous_mode=previous_mode,
        full_snapshot=full_snapshot,
    )


def test_service_runs_forecast_state_funnel_and_allocation_read_only():
    request = _request()
    result = TerminalStrategyService().evaluate(request)
    assert result.status == "COMPLETE"
    assert result.forecast.available is True
    assert result.mode_decision.mode is TerminalMode.NORMAL
    assert result.mode_decision.execution_ready is False
    assert result.funnel.status == "SUCCESS"
    assert result.allocation_policy is not None
    assert result.allocation_policy.execution_ready is False
    assert result.execution_ready is False
    assert result.evaluation_id == request.evaluation_id
    assert result.request_hash == request.request_hash
    assert "quantity" not in result.to_dict()["allocation_policy"]
    assert "orders" not in result.to_dict()["allocation_policy"]


def test_missing_cutoff_remains_unavailable_but_auditable():
    config = _config(cutoff=None)
    result = TerminalStrategyService().evaluate(_request(config=config, core_count=5))
    assert result.status == "UNAVAILABLE"
    assert result.forecast.available is False
    assert result.mode_decision.mode is TerminalMode.UNAVAILABLE
    assert result.allocation_policy is None
    assert "AWARD_RANK_UNCONFIRMED" in result.blockers
    assert result.execution_ready is False
    assert result.funnel.execution_ready is False


def test_candidate_shortfall_blocks_allocation_without_padding():
    result = TerminalStrategyService().evaluate(_request(candidates=1, core_count=4))
    assert result.status == "BLOCKED"
    assert result.allocation_policy is None
    assert "TERMINAL_CANDIDATE_SHORTFALL" in result.blockers
    assert len(result.funnel.terminal) < 2


def test_same_request_is_idempotent_and_mode_or_config_changes_identity():
    service = TerminalStrategyService()
    first_request = _request()
    first = service.evaluate(first_request)
    second = service.evaluate(first_request)
    assert first.to_dict() == second.to_dict()
    assert first.result_hash == second.result_hash

    changed_previous = service.evaluate(_request(previous_mode=TerminalMode.ATTACK))
    assert changed_previous.evaluation_id != first.evaluation_id
    payload = _config().to_dict()
    payload["mode"]["hysteresis"] = "0.004"
    changed_config = _config()
    changed_config = TerminalStrategyConfig.from_dict(payload)
    changed = service.evaluate(_request(config=changed_config))
    assert changed.evaluation_id != first.evaluation_id
    assert changed.config_hash != first.config_hash


@pytest.mark.parametrize(
    "change",
    [
        lambda payload: payload.update(account_snapshot_hash="not-a-hash"),
        lambda payload: payload.update(config_hash=""),
        lambda payload: payload.update(implementation_hash="sha256:bad"),
    ],
)
def test_missing_or_invalid_source_hashes_fail_closed(change):
    config = _config()
    account = _digest("account")
    leaderboard = _digest("leaderboard")
    values = {
        "s4_hash": _digest("s4"),
        "s5_hash": _digest("s5"),
        "s5_5_hash": _digest("s5.5"),
        "account_snapshot_hash": account,
        "leaderboard_snapshot_hash": leaderboard,
        "config_hash": config.config_hash,
        "implementation_hash": _digest("implementation"),
    }
    change(values)
    with pytest.raises(TerminalServiceError):
        TerminalSourceHashes(**values)


def test_timezone_and_snapshot_bindings_are_strict():
    config = _config()
    account = _digest("account")
    leaderboard = _digest("leaderboard")
    request = _request()
    with pytest.raises(TerminalServiceError, match="timezone-aware"):
        replace(request, observed_at=datetime(2026, 9, 22, 13, 15))
    with pytest.raises(TerminalServiceError, match="account snapshot hash"):
        replace(request, snapshot=replace(request.snapshot, account_snapshot_hash=_digest("other-account")))
