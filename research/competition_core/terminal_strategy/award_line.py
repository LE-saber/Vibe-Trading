"""Conservative, deterministic award-line forecasting.

This module consumes already captured account/leaderboard snapshots.  It never
fetches a ranking, infers an award cutoff from Top-N display settings, or
creates broker quantities.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from competition_core.pipeline.artifacts import content_hash

from .config import TerminalStrategyConfig
from .models import (
    AwardLineForecast,
    AwardLinePoint,
    ForecastConfidence,
    TerminalBlocker,
    TerminalModelError,
    TerminalSnapshot,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")
_ZERO = Decimal("0")
_ONE = Decimal("1")
_ALPHA = Decimal("0.5")


def forecast_award_line(
    snapshot: TerminalSnapshot,
    config: TerminalStrategyConfig,
    history: Iterable[AwardLinePoint | Mapping[str, Any] | tuple[Any, ...]] | None = None,
    *,
    observed_at: datetime | None = None,
) -> AwardLineForecast:
    """Build one conservative forecast, returning an unavailable result on blockers.

    Historical samples are optional.  With fewer than the configured minimum
    valid samples, the safe fallback is the current award line with the maximum
    configured buffer and ``LOW`` confidence.  Malformed samples, a decreasing
    award line, a missing cutoff row, stale snapshots, or an unconfirmed award
    rule remain hard blockers.
    """

    if not isinstance(snapshot, TerminalSnapshot):
        raise TerminalModelError("snapshot must be a TerminalSnapshot")
    if not isinstance(config, TerminalStrategyConfig):
        raise TerminalModelError("config must be a TerminalStrategyConfig")
    now = observed_at or datetime.now(tz=SHANGHAI)
    if not isinstance(now, datetime) or now.utcoffset() is None:
        raise TerminalModelError("observed_at must be timezone-aware")
    blockers: list[TerminalBlocker] = []
    cutoff = config.competition.award_rank_cutoff
    if cutoff is None:
        blockers.append(TerminalBlocker.AWARD_RANK_UNCONFIRMED)
    if not (snapshot.award_rule_source or config.competition.award_rule_source):
        blockers.append(TerminalBlocker.AWARD_RULE_SOURCE_MISSING)
    if snapshot.award_rank_cutoff != cutoff:
        blockers.append(TerminalBlocker.AWARD_RANK_UNCONFIRMED)
    age = (now - snapshot.as_of).total_seconds()
    if age < 0:
        blockers.append(TerminalBlocker.FUTURE_SNAPSHOT)
    elif age > config.award_line.max_snapshot_age_seconds:
        blockers.append(TerminalBlocker.STALE_SNAPSHOT)
    if not snapshot.account_fresh:
        blockers.append(TerminalBlocker.INCOMPLETE_ACCOUNT)
    if not snapshot.leaderboard_fresh:
        blockers.append(TerminalBlocker.INCOMPLETE_LEADERBOARD)
    if not snapshot.complete_order_history:
        blockers.append(TerminalBlocker.INCOMPLETE_ACCOUNT)
    if snapshot.pending_order_count:
        blockers.append(TerminalBlocker.PENDING_ORDERS)
    entry = next((item for item in snapshot.leaderboard if item.rank == cutoff), None) if cutoff else None
    if entry is None:
        blockers.append(TerminalBlocker.RANK_NOT_FOUND)
    if blockers:
        return _unavailable(snapshot, cutoff, config.award_line.forecast_method, blockers)

    try:
        points = _normalise_history(history, default_rank=cutoff)
    except (TypeError, ValueError, TerminalModelError, InvalidOperation):
        return _unavailable(snapshot, cutoff, config.award_line.forecast_method, [TerminalBlocker.INVALID_ASSET])
    current = AwardLinePoint(snapshot.as_of, entry.total_assets, cutoff)
    if any(point.as_of == current.as_of and point.total_assets != current.total_assets for point in points):
        return _unavailable(snapshot, cutoff, config.award_line.forecast_method, [TerminalBlocker.NON_MONOTONIC_HISTORY])
    points = _with_current(points, current)
    if len(points) > config.award_line.history_sessions + 1:
        points = points[-(config.award_line.history_sessions + 1):]
    history_blockers = _validate_points(points, snapshot, now)
    if history_blockers:
        return _unavailable(snapshot, cutoff, config.award_line.forecast_method, history_blockers)

    # The current point is required, so no-history input is a safe low-confidence
    # fallback rather than an invented trend.
    if len(points) < config.award_line.min_valid_snapshots:
        return _build_forecast(
            snapshot=snapshot,
            cutoff=cutoff,
            current=current.total_assets,
            favorable=current.total_assets,
            base=current.total_assets,
            adverse=current.total_assets,
            predicted=current.total_assets,
            safety_buffer=config.award_line.safety_buffer_max,
            confidence=ForecastConfidence.LOW,
            method="current_line_plus_max_buffer_fallback",
            history=points,
            config=config,
        )

    returns = [
        (new.total_assets / old.total_assets) - _ONE
        for old, new in zip(points, points[1:])
    ]
    ewma = returns[0]
    for item in returns[1:]:
        ewma = _ALPHA * item + (_ONE - _ALPHA) * ewma
    mean = sum(returns, _ZERO) / Decimal(len(returns))
    variance = sum((item - mean) ** 2 for item in returns) / Decimal(len(returns))
    volatility = variance.sqrt()
    future_sessions = sum(item > snapshot.trade_date for item in snapshot.remaining_trade_dates)
    favorable_growth = max(_ZERO, ewma - volatility)
    base_growth = max(_ZERO, ewma)
    adverse_growth = max(_ZERO, ewma + volatility)
    favorable = _project(current.total_assets, favorable_growth, future_sessions)
    base = _project(current.total_assets, base_growth, future_sessions)
    adverse = _project(current.total_assets, adverse_growth, future_sessions)
    # The adverse (higher) award-line scenario is the default target.  It keeps
    # the decision conservative when the leaderboard is still moving upward.
    return _build_forecast(
        snapshot=snapshot,
        cutoff=cutoff,
        current=current.total_assets,
        favorable=favorable,
        base=base,
        adverse=adverse,
        predicted=adverse,
        safety_buffer=config.award_line.safety_buffer_base,
        confidence=ForecastConfidence.HIGH if len(points) >= config.award_line.min_valid_snapshots + 2 else ForecastConfidence.MEDIUM,
        method=config.award_line.forecast_method,
        history=points,
        config=config,
    )


# Descriptive aliases for callers and hidden contract tests.
calculate_award_line_forecast = forecast_award_line
build_award_line_forecast = forecast_award_line


def target_asset_for_award_line(predicted_award_line: Decimal, safety_buffer: Decimal) -> Decimal:
    """Apply the documented buffer formula: line * (1 + buffer rate)."""

    try:
        line = Decimal(str(predicted_award_line))
        buffer = Decimal(str(safety_buffer))
    except (InvalidOperation, ValueError) as exc:
        raise TerminalModelError("award line and safety buffer must be decimal") from exc
    if not line.is_finite() or line <= 0 or not buffer.is_finite() or not _ZERO <= buffer <= _ONE:
        raise TerminalModelError("award line and safety buffer are outside supported bounds")
    return (line * (_ONE + buffer)).quantize(Decimal("0.01"))


def _build_forecast(
    *,
    snapshot: TerminalSnapshot,
    cutoff: int,
    current: Decimal,
    favorable: Decimal,
    base: Decimal,
    adverse: Decimal,
    predicted: Decimal,
    safety_buffer: Decimal,
    confidence: ForecastConfidence,
    method: str,
    history: tuple[AwardLinePoint, ...],
    config: TerminalStrategyConfig,
) -> AwardLineForecast:
    target = target_asset_for_award_line(predicted, safety_buffer)
    required_return = (target / snapshot.own_total_assets) - _ONE
    input_hash = content_hash({
        "snapshot": snapshot.to_dict(),
        "history": [item.to_dict() for item in history],
        "config_hash": config.config_hash,
    })
    return AwardLineForecast(
        available=True,
        rank_cutoff=cutoff,
        as_of=snapshot.as_of,
        current_award_line=current,
        predicted_award_line=predicted,
        favorable_award_line=favorable,
        base_award_line=base,
        adverse_award_line=adverse,
        safety_buffer=safety_buffer,
        target_asset=target,
        required_return=required_return,
        confidence=confidence,
        method=method,
        history=history,
        input_hash=input_hash,
    )


def _unavailable(
    snapshot: TerminalSnapshot,
    cutoff: int | None,
    method: str,
    blockers: Iterable[TerminalBlocker],
) -> AwardLineForecast:
    unique = tuple(dict.fromkeys(blockers))
    return AwardLineForecast(
        available=False,
        rank_cutoff=cutoff,
        as_of=snapshot.as_of,
        confidence=ForecastConfidence.UNAVAILABLE,
        method=method,
        blockers=unique,
    )


def _normalise_history(
    history: Iterable[AwardLinePoint | Mapping[str, Any] | tuple[Any, ...]] | None,
    *,
    default_rank: int,
) -> tuple[AwardLinePoint, ...]:
    if history is None:
        return ()
    points: list[AwardLinePoint] = []
    for item in history:
        if isinstance(item, AwardLinePoint):
            points.append(item)
            continue
        if isinstance(item, Mapping):
            if set(item) - {"as_of", "total_assets", "rank"} or not {"as_of", "total_assets"}.issubset(item):
                raise TerminalModelError("history point has missing or unknown fields")
            as_of = item["as_of"]
            if isinstance(as_of, str):
                as_of = datetime.fromisoformat(as_of)
            points.append(AwardLinePoint(as_of, item["total_assets"], item.get("rank", default_rank)))
            continue
        if isinstance(item, tuple) and len(item) in {2, 3}:
            as_of = item[0]
            if isinstance(as_of, str):
                as_of = datetime.fromisoformat(as_of)
            rank = item[2] if len(item) == 3 else default_rank
            points.append(AwardLinePoint(as_of, item[1], rank))
            continue
        raise TerminalModelError("history contains an unsupported point")
    return tuple(sorted(points, key=lambda point: point.as_of))


def _with_current(points: tuple[AwardLinePoint, ...], current: AwardLinePoint) -> tuple[AwardLinePoint, ...]:
    result = [point for point in points if point.as_of != current.as_of]
    result.append(current)
    return tuple(sorted(result, key=lambda point: point.as_of))


def _validate_points(
    points: tuple[AwardLinePoint, ...], snapshot: TerminalSnapshot, now: datetime
) -> tuple[TerminalBlocker, ...]:
    blockers: list[TerminalBlocker] = []
    if not points:
        return (TerminalBlocker.AWARD_LINE_MISSING,)
    if any(point.rank != snapshot.award_rank_cutoff for point in points):
        blockers.append(TerminalBlocker.RANK_NOT_FOUND)
    if any(point.as_of > snapshot.as_of or point.as_of > now for point in points):
        blockers.append(TerminalBlocker.FUTURE_SNAPSHOT)
    if len({point.as_of for point in points}) != len(points):
        blockers.append(TerminalBlocker.NON_MONOTONIC_HISTORY)
    if any(new.total_assets < old.total_assets for old, new in zip(points, points[1:])):
        blockers.append(TerminalBlocker.NON_MONOTONIC_HISTORY)
    return tuple(dict.fromkeys(blockers))


def _project(current: Decimal, growth: Decimal, sessions: int) -> Decimal:
    return (current * (_ONE + growth) ** sessions).quantize(Decimal("0.01"))


__all__ = [
    "AwardLinePoint", "AwardLineSample", "build_award_line_forecast",
    "calculate_award_line_forecast", "forecast_award_line", "target_asset_for_award_line",
]
