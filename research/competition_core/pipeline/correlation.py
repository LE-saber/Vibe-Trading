"""Pairwise qfq-return correlation for the deterministic S4 pipeline."""

from __future__ import annotations

from datetime import date
import json
import math
from typing import Any

import pandas as pd

from competition_core.pipeline.config import PipelineConfig
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.time_context import ResearchContext
from contracts.symbol import CanonicalSymbol, SymbolContractError


CORRELATION_METHOD = "spearman_pairwise_complete"


def build_return_correlation(
    panel: pd.DataFrame,
    *,
    symbols: list[str],
    sessions: list[str],
    context: ResearchContext,
    config: PipelineConfig,
) -> dict[str, Any]:
    """Build deterministic pairwise Spearman correlations from a canonical panel.

    The canonical panel's ``close_qfq`` values are aligned to the supplied
    exchange-session calendar before returns are formed.  Missing or invalid
    price endpoints remove only the affected return; no observations are
    fabricated across a gap.
    """
    _validate_arguments(panel, symbols, sessions, context, config)
    ordered_symbols = _validated_symbols(symbols)
    reference_date = context.market_data_reference_date.isoformat()
    canonical_sessions = _validated_sessions(sessions, reference_date, config)
    risk = config.section("risk")
    window_sessions = int(risk["correlation_sessions"])
    minimum_observations = int(risk["min_pair_observations"])
    threshold = float(risk["corr_threshold"])
    window_dates = canonical_sessions[-(window_sessions + 1):]
    return_dates = window_dates[1:]

    close_by_key = _validated_panel_values(
        panel,
        symbols=ordered_symbols,
        sessions=set(canonical_sessions),
        reference_date=reference_date,
    )
    returns = {
        symbol: _returns_for_symbol(symbol, window_dates, close_by_key)
        for symbol in ordered_symbols
    }
    observed_return_counts = {
        symbol: sum(value is not None for value in values)
        for symbol, values in returns.items()
    }
    return_frame = pd.DataFrame(returns, index=return_dates, dtype="float64")

    correlation_matrix: dict[str, dict[str, float | None]] = {
        symbol: {} for symbol in ordered_symbols
    }
    observation_counts: dict[str, dict[str, int]] = {
        symbol: {} for symbol in ordered_symbols
    }
    high_correlation_pairs: list[dict[str, Any]] = []
    missing_pairs: list[dict[str, Any]] = []
    adjacency = {symbol: set[str]() for symbol in ordered_symbols}

    for left_index, left in enumerate(ordered_symbols):
        for right in ordered_symbols[left_index:]:
            left_values, right_values = _pairwise_values(return_frame, left, right)
            observations = len(left_values)
            observation_counts[left][right] = observations
            observation_counts[right][left] = observations

            correlation: float | None = None
            reason: str | None = None
            if observations < minimum_observations:
                reason = "insufficient_observations"
            elif _is_constant(left_values) or _is_constant(right_values):
                reason = "constant_returns"
            else:
                correlation = _spearman(left_values, right_values)
                if correlation is None:
                    reason = "constant_returns"

            correlation_matrix[left][right] = correlation
            correlation_matrix[right][left] = correlation
            if left == right:
                continue
            if correlation is not None and abs(correlation) >= threshold:
                high_correlation_pairs.append({
                    "left": left,
                    "right": right,
                    "correlation": correlation,
                    "observations": observations,
                })
                adjacency[left].add(right)
                adjacency[right].add(left)
            elif reason is not None:
                missing_pairs.append({
                    "left": left,
                    "right": right,
                    "reason": reason,
                    "observations": observations,
                })

    clusters = _clusters(ordered_symbols, adjacency)
    result: dict[str, Any] = {
        "symbols": ordered_symbols,
        "method": CORRELATION_METHOD,
        "window_sessions": window_sessions,
        "reference_date": reference_date,
        "window_start_date": window_dates[0],
        "window_end_date": window_dates[-1],
        "return_dates": return_dates,
        "correlation_matrix": correlation_matrix,
        "observation_counts": observation_counts,
        "observed_return_counts": observed_return_counts,
        "high_correlation_pairs": high_correlation_pairs,
        "clusters": clusters,
        "missing_pairs": missing_pairs,
    }
    try:
        json.dumps(result, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise PipelineError("STALE_DATA", "correlation result is not JSON-safe") from exc
    return result


def _validate_arguments(
    panel: pd.DataFrame,
    symbols: list[str],
    sessions: list[str],
    context: ResearchContext,
    config: PipelineConfig,
) -> None:
    if not isinstance(panel, pd.DataFrame):
        raise PipelineError("INVALID_ARGUMENTS", "correlation panel must be a DataFrame")
    if not isinstance(context, ResearchContext):
        raise PipelineError("INVALID_ARGUMENTS", "correlation context must be a ResearchContext")
    if not isinstance(config, PipelineConfig):
        raise PipelineError("INVALID_ARGUMENTS", "correlation config must be a PipelineConfig")
    if not isinstance(symbols, list) or not isinstance(sessions, list):
        raise PipelineError("INVALID_ARGUMENTS", "correlation symbols and sessions must be lists")
    if not panel.columns.is_unique:
        raise PipelineError("STALE_DATA", "correlation panel has duplicate columns")
    if not {"symbol", "trade_date", "close_qfq"} <= set(panel.columns):
        raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "correlation panel needs symbol, trade_date and close_qfq")
    if panel.empty:
        raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "correlation panel is empty")


def _validated_symbols(symbols: list[str]) -> list[str]:
    if not symbols:
        raise PipelineError("IDENTITY_CONFLICT", "correlation symbols must be nonempty")
    if any(not isinstance(symbol, str) for symbol in symbols):
        raise PipelineError("IDENTITY_CONFLICT", "correlation symbols must be strings")
    if len(symbols) != len(set(symbols)):
        raise PipelineError("IDENTITY_CONFLICT", "correlation symbols contain duplicates")
    for symbol in symbols:
        try:
            CanonicalSymbol.from_core(symbol)
        except SymbolContractError as exc:
            raise PipelineError("IDENTITY_CONFLICT", "correlation symbol is not canonical", symbol=symbol) from exc
    return sorted(symbols)


def _validated_sessions(
    sessions: list[str], reference_date: str, config: PipelineConfig
) -> list[str]:
    if not sessions:
        raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "correlation sessions are empty")
    if any(not isinstance(session, str) for session in sessions):
        raise PipelineError("STALE_DATA", "correlation sessions must be ISO date strings")
    parsed: list[date] = []
    for session in sessions:
        try:
            value = date.fromisoformat(session)
        except ValueError as exc:
            raise PipelineError("STALE_DATA", "correlation sessions must be ISO dates") from exc
        if value.isoformat() != session:
            raise PipelineError("STALE_DATA", "correlation sessions must be canonical ISO dates")
        parsed.append(value)
    if len(sessions) != len(set(sessions)):
        raise PipelineError("STALE_DATA", "correlation sessions contain duplicates")
    if sessions != sorted(sessions):
        raise PipelineError("STALE_DATA", "correlation sessions must be ascending")
    reference = date.fromisoformat(reference_date)
    if any(value > reference for value in parsed):
        raise PipelineError("STALE_DATA", "correlation sessions contain a future date")
    if sessions[-1] != reference_date:
        raise PipelineError("STALE_DATA", "correlation sessions do not end at the reference date")
    window_sessions = config.section("risk")["correlation_sessions"]
    if len(sessions) < int(window_sessions) + 1:
        raise PipelineError(
            "DATA_COVERAGE_INSUFFICIENT",
            "correlation sessions do not contain the requested 61-session window",
        )
    return list(sessions)


def _validated_panel_values(
    panel: pd.DataFrame,
    *,
    symbols: list[str],
    sessions: set[str],
    reference_date: str,
) -> dict[tuple[str, str], float | None]:
    symbol_set = set(symbols)
    values: dict[tuple[str, str], float | None] = {}
    seen_keys: set[tuple[str, str]] = set()
    validated_symbols: set[str] = set()
    reference = date.fromisoformat(reference_date)
    for symbol, trade_date, raw_close in panel[
        ["symbol", "trade_date", "close_qfq"]
    ].itertuples(index=False, name=None):
        if not isinstance(symbol, str):
            raise PipelineError("IDENTITY_CONFLICT", "correlation panel has a non-string symbol")
        if symbol not in validated_symbols:
            try:
                CanonicalSymbol.from_core(symbol)
            except SymbolContractError as exc:
                raise PipelineError("IDENTITY_CONFLICT", "correlation panel symbol is not canonical", symbol=symbol) from exc
            validated_symbols.add(symbol)
        if not isinstance(trade_date, str):
            raise PipelineError("STALE_DATA", "correlation panel dates must be ISO strings")
        try:
            parsed_date = date.fromisoformat(trade_date)
        except ValueError as exc:
            raise PipelineError("STALE_DATA", "correlation panel has an invalid trade date") from exc
        if parsed_date.isoformat() != trade_date:
            raise PipelineError("STALE_DATA", "correlation panel dates must be canonical ISO dates")
        if parsed_date > reference:
            raise PipelineError("STALE_DATA", "correlation panel contains a future date")
        if trade_date not in sessions:
            raise PipelineError("STALE_DATA", "correlation panel date is outside sessions")
        key = (symbol, trade_date)
        if key in seen_keys:
            raise PipelineError("STALE_DATA", "correlation panel has duplicate symbol/date rows")
        seen_keys.add(key)
        if symbol in symbol_set:
            values[key] = _positive_finite(raw_close)
    return values


def _positive_finite(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def _returns_for_symbol(
    symbol: str,
    window_dates: list[str],
    close_by_key: dict[tuple[str, str], float | None],
) -> list[float | None]:
    closes = [close_by_key.get((symbol, trade_date)) for trade_date in window_dates]
    returns: list[float | None] = []
    for previous, current in zip(closes, closes[1:]):
        if previous is None or current is None:
            returns.append(None)
            continue
        try:
            value = current / previous - 1.0
        except (OverflowError, ZeroDivisionError):
            value = float("nan")
        returns.append(value if math.isfinite(value) else None)
    return returns


def _pairwise_values(
    frame: pd.DataFrame, left: str, right: str
) -> tuple[list[float], list[float]]:
    left_series, right_series = frame[left], frame[right]
    valid = left_series.notna() & right_series.notna()
    return (
        [float(value) for value in left_series.loc[valid].tolist()],
        [float(value) for value in right_series.loc[valid].tolist()],
    )


def _is_constant(values: list[float]) -> bool:
    return not values or min(values) == max(values)


def _spearman(left: list[float], right: list[float]) -> float | None:
    if len(left) != len(right) or _is_constant(left) or _is_constant(right):
        return None
    left_ranks = pd.Series(left, dtype="float64").rank(method="average").to_numpy(dtype=float)
    right_ranks = pd.Series(right, dtype="float64").rank(method="average").to_numpy(dtype=float)
    left_centered = left_ranks - left_ranks.mean()
    right_centered = right_ranks - right_ranks.mean()
    denominator = math.sqrt(float((left_centered ** 2).sum() * (right_centered ** 2).sum()))
    if not math.isfinite(denominator) or denominator == 0:
        return None
    correlation = float((left_centered * right_centered).sum() / denominator)
    if not math.isfinite(correlation):
        return None
    return max(-1.0, min(1.0, correlation))


def _clusters(symbols: list[str], adjacency: dict[str, set[str]]) -> list[dict[str, Any]]:
    components: list[list[str]] = []
    remaining = set(symbols)
    for start in symbols:
        if start not in remaining:
            continue
        stack = [start]
        component: list[str] = []
        while stack:
            symbol = stack.pop()
            if symbol not in remaining:
                continue
            remaining.remove(symbol)
            component.append(symbol)
            stack.extend(sorted(adjacency[symbol], reverse=True))
        components.append(sorted(component))
    components.sort(key=lambda component: component[0])
    return [
        {"cluster_id": cluster_id, "symbols": component}
        for cluster_id, component in enumerate(components, 1)
    ]


__all__ = ["build_return_correlation"]
