"""Frozen pure technical-indicator implementations used by research.

Migrated from Vibe's read-only helper without its agent or market-data imports.
The caller supplies the point-in-time qfq close series.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


_RSI_PERIOD = 14
_MACD_FAST = 12
_MACD_SLOW = 26
_MACD_SIGNAL = 9
_BB_PERIOD = 20
_BB_STD = 2.0
_CLOSE_KEYS = ("close", "Close", "CLOSE", "adj_close")
_DATE_KEYS = ("trade_date", "date", "datetime", "timestamp", "time")


def _extract_close_series(payload: Any) -> pd.Series | None:
    if isinstance(payload, list):
        if not payload or not isinstance(payload[0], dict):
            return None
        frame = pd.DataFrame(payload)
    elif isinstance(payload, dict):
        inner = payload.get("data")
        if isinstance(inner, list):
            return _extract_close_series(inner)
        try:
            frame = pd.DataFrame(payload)
        except ValueError:
            return None
    elif isinstance(payload, pd.DataFrame):
        frame = payload.copy()
    else:
        return None
    close_key = next((key for key in _CLOSE_KEYS if key in frame.columns), None)
    if close_key is None:
        return None
    close = pd.to_numeric(frame[close_key], errors="coerce")
    date_key = next((key for key in _DATE_KEYS if key in frame.columns), None)
    if date_key is not None:
        dates = pd.to_datetime(frame[date_key], errors="coerce")
    elif isinstance(frame.index, pd.DatetimeIndex):
        dates = pd.Series(frame.index, index=frame.index)
    elif not isinstance(frame.index, pd.RangeIndex) and not pd.api.types.is_integer_dtype(
        frame.index.dtype
    ):
        dates = pd.Series(pd.to_datetime(frame.index, errors="coerce"), index=frame.index)
    else:
        return close.dropna().reset_index(drop=True).astype(float)
    valid = close.notna() & dates.notna()
    normalized = pd.Series(
        close.loc[valid].to_numpy(dtype=float),
        index=pd.DatetimeIndex(dates.loc[valid]),
        dtype=float,
    )
    return normalized.sort_index(kind="stable")


def _compute_sma(close: pd.Series, period: int) -> float | None:
    if len(close) < period:
        return None
    return float(close.iloc[-period:].mean())


def _compute_ema(close: pd.Series, period: int) -> float | None:
    if len(close) < period:
        return None
    return float(close.ewm(span=period, adjust=False).mean().iloc[-1])


def _compute_rsi(close: pd.Series, period: int = _RSI_PERIOD) -> float | None:
    if len(close) < period + 1:
        return None
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period).mean().iloc[-1]
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period).mean().iloc[-1]
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100.0 - (100.0 / (1.0 + rs)))


def _compute_macd(
    close: pd.Series,
    fast: int = _MACD_FAST,
    slow: int = _MACD_SLOW,
    signal: int = _MACD_SIGNAL,
) -> dict[str, float | None] | None:
    if len(close) < slow + signal:
        return None
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return {
        "macd_line": round(float(macd_line.iloc[-1]), 4),
        "signal_line": round(float(signal_line.iloc[-1]), 4),
        "histogram": round(float(histogram.iloc[-1]), 4),
    }


def _compute_bollinger(
    close: pd.Series,
    period: int = _BB_PERIOD,
    num_std: float = _BB_STD,
) -> dict[str, float | None] | None:
    if len(close) < period:
        return None
    sma = close.rolling(window=period).mean()
    std = close.rolling(window=period).std()
    return {
        "upper": round(float(sma.iloc[-1] + num_std * std.iloc[-1]), 2),
        "middle": round(float(sma.iloc[-1]), 2),
        "lower": round(float(sma.iloc[-1] - num_std * std.iloc[-1]), 2),
    }
