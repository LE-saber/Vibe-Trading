"""Calendar-aligned, qfq-price / actual-volume research features; no source calls."""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.time_context import ResearchContext
from contracts.symbol import CanonicalSymbol


def finite(value: Any) -> float | None:
    if isinstance(value, bool):
        return float(value)
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (ValueError, TypeError):
        return None


def session_return(close: pd.Series, periods: int) -> float | None:
    """Exact exchange-calendar endpoints, never N observed non-missing bars."""
    if len(close) <= periods:
        return None
    first, last = finite(close.iloc[-periods - 1]), finite(close.iloc[-1])
    return last / first - 1 if first is not None and first > 0 and last is not None and last > 0 else None


def _complete(series: pd.Series, count: int, *, positive: bool = False) -> bool:
    values = series.iloc[-count:].to_numpy(dtype=float)
    return len(values) == count and bool(np.isfinite(values).all() and (values > 0 if positive else values >= 0).all())


def _drawdown(close: pd.Series, periods: int) -> float | None:
    if not _complete(close, periods + 1, positive=True):
        return None
    values = close.iloc[-periods - 1:]
    return float(-(values / values.cummax() - 1).min())


def _divide(numerator: Any, denominator: Any, *, minus_one: bool = False) -> float | None:
    a, b = finite(numerator), finite(denominator)
    if a is None or b is None or b == 0:
        return None
    return a / b - int(minus_one)


def validate_panel_input(panel: pd.DataFrame, *, context: ResearchContext,
                         sessions: list[str], symbols: list[str]) -> None:
    required = {"symbol", "trade_date", "open_qfq", "high_qfq", "low_qfq", "close_qfq", "close_raw",
                "adj_factor", "volume_raw", "amount_raw", "volume_shares", "amount_cny", "turnover_rate", "volume_ratio"}
    if not required <= set(panel):
        raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "canonical feature input fields missing")
    if not sessions or sessions != sorted(set(sessions)) or sessions[-1] != context.market_data_reference_date.isoformat():
        raise PipelineError("STALE_DATA", "feature calendar differs from historical cutoff")
    context.validate_daily_dates(sessions)
    if any(pd.Timestamp(day).date().isoformat() != day for day in sessions):
        raise PipelineError("STALE_DATA", "feature sessions must use ISO dates")
    if len(symbols) != len(set(symbols)) or not symbols:
        raise PipelineError("IDENTITY_CONFLICT", "feature symbols must be unique and nonempty")
    for symbol in symbols:
        CanonicalSymbol.from_core(symbol)
    if (panel.duplicated(["symbol", "trade_date"]).any() or not set(panel.symbol) <= set(symbols)
            or not set(panel.trade_date) <= set(sessions)):
        raise PipelineError("STALE_DATA", "feature panel has duplicate, unknown or out-of-calendar rows")
    context.validate_daily_dates(panel.trade_date)


def base_features(panel: pd.DataFrame, *, context: ResearchContext, sessions: list[str],
                  symbols: list[str], technical_rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Input has already passed CanonicalDataset.load including source provenance."""
    validate_panel_input(panel, context=context, sessions=sessions, symbols=symbols)
    technical = {row["symbol"]: row for row in technical_rows}
    if len(technical) != len(technical_rows) or set(technical) != set(symbols):
        raise PipelineError("IDENTITY_CONFLICT", "technical batch identities differ from eligible Universe")
    groups = {symbol: frame.set_index("trade_date").reindex(sessions) for symbol, frame in panel.groupby("symbol", sort=False)}
    rows = []
    for symbol in sorted(symbols):
        frame = groups.get(symbol, pd.DataFrame(index=sessions, columns=panel.columns))
        close = pd.to_numeric(frame.close_qfq, errors="coerce").astype(float)
        high = pd.to_numeric(frame.high_qfq, errors="coerce").astype(float)
        low = pd.to_numeric(frame.low_qfq, errors="coerce").astype(float)
        opening = pd.to_numeric(frame.open_qfq, errors="coerce").astype(float)
        amount = pd.to_numeric(frame.amount_cny, errors="coerce").astype(float)
        volume = pd.to_numeric(frame.volume_shares, errors="coerce").astype(float)
        tech = technical[symbol]
        valid = np.isfinite(close) & close.gt(0)
        factor = pd.to_numeric(frame.adj_factor, errors="coerce")
        row: dict[str, Any] = {"symbol": symbol, "latest_close_qfq": finite(close.iloc[-1]),
            "latest_close_raw": finite(frame.close_raw.iloc[-1]), "history_sessions": int(valid.sum()),
            "history_coverage": float(valid.mean()), "missing_ratio": float(1 - valid.mean()),
            "factor_coverage": float((np.isfinite(factor) & factor.gt(0)).mean()),
            "source_confidence": 1.0,  # Only validated, frozen inputs reach this function.
            "latest_amount_cny": finite(amount.iloc[-1]), "latest_volume_raw": finite(frame.volume_raw.iloc[-1]),
            "latest_volume_shares": finite(volume.iloc[-1]), "latest_amount_raw": finite(frame.amount_raw.iloc[-1]),
            "median_amount_20d": float(amount.iloc[-20:].median()) if _complete(amount, 20) else None,
            "turnover_rate": finite(frame.turnover_rate.iloc[-1]), "volume_ratio": finite(frame.volume_ratio.iloc[-1]),
            "technical_contiguous_sessions": tech.get("contiguous_sessions", 0),
            "technical_missing_fields": sorted(tech.get("missing_fields", [])),
        }
        for field in ("sma_5", "sma_10", "sma_20", "sma_20_lag5", "rsi_14", "macd_histogram",
                      "bollinger_upper", "bollinger_middle", "bollinger_lower"):
            row[field] = finite(tech.get(field))
        for periods in (3, 5, 10, 20, 60):
            row[f"momentum_{periods}d"] = session_return(close, periods)
        mas = [row[f"sma_{period}"] for period in (5, 10, 20)]
        row["ma_alignment"] = sum(a > b for a, b in zip(mas, mas[1:])) / 2 if all(v is not None for v in mas) else None
        row["distance_ma20"] = _divide(close.iloc[-1], row["sma_20"], minus_one=True)
        row["ma20_slope_5d"] = _divide(row["sma_20"], row["sma_20_lag5"], minus_one=True)
        prior_5d = session_return(close.iloc[:-5], 5)
        row["acceleration_5d"] = row["momentum_5d"] - prior_5d if row["momentum_5d"] is not None and prior_5d is not None else None
        upper, lower = row["bollinger_upper"], row["bollinger_lower"]
        row["bollinger_position"] = _divide(close.iloc[-1] - lower, upper - lower) if upper is not None and lower is not None else None
        row["atr14_ratio"] = None
        if _complete(close, 15, positive=True) and _complete(high, 14, positive=True) and _complete(low, 14, positive=True):
            previous = close.shift(1)
            ranges = pd.concat([high - low, (high - previous).abs(), (low - previous).abs()], axis=1).max(axis=1)
            row["atr14_ratio"] = float(ranges.iloc[-14:].mean() / close.iloc[-1])
        row["volatility_20d"] = float(close.iloc[-21:].pct_change(fill_method=None).iloc[1:].std(ddof=1)) if _complete(close, 21, positive=True) else None
        row["max_drawdown_5d"] = _drawdown(close, 5)
        row["max_drawdown_20d"] = _drawdown(close, 20)
        row["trend_consistency_20d"] = float(close.iloc[-20:].gt(close.rolling(20, min_periods=20).mean().iloc[-20:]).mean()) if _complete(close, 39, positive=True) else None
        row["breakout_20d"] = _divide(close.iloc[-1], high.iloc[-21:-1].max(), minus_one=True) if _complete(high.iloc[:-1], 20, positive=True) else None
        row["drawdown_recovery_20d"] = None
        if _complete(close, 20, positive=True):
            floor, ceiling = float(close.iloc[-20:].min()), float(close.iloc[-20:].max())
            row["drawdown_recovery_20d"] = (float(close.iloc[-1]) - floor) / (ceiling - floor) if ceiling > floor else 0.5
        row["volume_price_confirmation_5d"] = None
        if _complete(close, 6, positive=True) and _complete(volume, 5) and volume.iloc[-5:].sum() > 0:
            row["volume_price_confirmation_5d"] = float((np.sign(close.diff().iloc[-5:]) * volume.iloc[-5:]).sum() / volume.iloc[-5:].sum())
        row["mean_abs_gap_20d"] = float((opening / close.shift(1) - 1).iloc[-20:].abs().mean()) if _complete(opening, 20, positive=True) and _complete(close, 21, positive=True) else None
        rows.append(row)
    result = pd.DataFrame(rows).set_index("symbol", drop=False)
    for periods in (20, 60):
        momentum = pd.to_numeric(result[f"momentum_{periods}d"], errors="coerce")
        result[f"relative_strength_{periods}d"] = momentum - momentum.median()
    return result


def supplemental_features(features: pd.DataFrame, panel: pd.DataFrame, *, context: ResearchContext,
                          sessions: list[str], benchmark_symbol: str, benchmark: dict[str, Any],
                          limit_batches: list[dict[str, Any]], limit_sessions: int) -> pd.DataFrame:
    """Join independently dated index/price-limit facts. No inferred board rules."""
    result = features.copy(deep=True)
    benchmark_close = pd.Series(index=sessions, dtype=float)
    if benchmark.get("status") == "SUCCESS" and benchmark.get("data"):
        data = pd.DataFrame(benchmark["data"])
        if not {"ts_code", "trade_date", "close"} <= set(data) or not data.ts_code.eq(benchmark_symbol).all():
            raise PipelineError("IDENTITY_CONFLICT", "benchmark response has wrong identity/schema")
        data["trade_date"] = pd.to_datetime(data.trade_date.astype(str), format="mixed").dt.date.astype(str)
        context.validate_daily_dates(data.trade_date)
        if data.trade_date.duplicated().any() or not set(data.trade_date) <= set(sessions):
            raise PipelineError("STALE_DATA", "benchmark dates differ from calendar")
        benchmark_close = pd.to_numeric(data.set_index("trade_date").close, errors="coerce").reindex(sessions)
    for periods in (20, 60):
        benchmark_return = session_return(benchmark_close, periods)
        result[f"benchmark_return_{periods}d"] = benchmark_return
        result[f"benchmark_relative_{periods}d"] = ((1 + result[f"momentum_{periods}d"]) / (1 + benchmark_return) - 1) if benchmark_return is not None else np.nan
        result[f"sector_relative_{periods}d"] = np.nan  # No reliable dated sector membership yet.
    limits: dict[tuple[str, str], dict[str, Any]] = {}
    for batch in limit_batches:
        if batch.get("status") != "SUCCESS":
            continue
        for row in batch.get("data", []):
            day = pd.Timestamp(str(row["trade_date"])).date().isoformat()
            context.validate_daily_dates([day])
            if day not in sessions[-limit_sessions:]:
                raise PipelineError("STALE_DATA", "price-limit date differs from requested window")
            key = (row["ts_code"], day)
            if key in limits:
                raise PipelineError("STALE_DATA", "duplicate price-limit identity/date")
            limits[key] = row
    raw = panel.set_index(["symbol", "trade_date"])
    for name in ("recent_limit_up_count", "recent_limit_down_count", "latest_limit_up", "latest_limit_down"):
        result[name] = np.nan
    for symbol in result.index:
        up_states, down_states = [], []
        for day in sessions[-limit_sessions:]:
            values = limits.get((symbol, day), {})
            upper, lower = finite(values.get("up_limit")), finite(values.get("down_limit"))
            closing = finite(raw.loc[(symbol, day), "close_raw"]) if (symbol, day) in raw.index else None
            known = (upper is not None and lower is not None and closing is not None and 0 < lower < upper
                     and lower - .00500001 <= closing <= upper + .00500001)
            up_states.append(float(abs(closing - upper) <= .00500001) if known else None)
            down_states.append(float(abs(closing - lower) <= .00500001) if known else None)
        result.loc[symbol, "latest_limit_up"] = up_states[-1]
        result.loc[symbol, "latest_limit_down"] = down_states[-1]
        if all(value is not None for value in up_states):
            result.loc[symbol, "recent_limit_up_count"] = sum(up_states)
            result.loc[symbol, "recent_limit_down_count"] = sum(down_states)
    return result
