"""Explicit synthetic research data. Never used by a live pipeline command."""

from datetime import date, datetime

import numpy as np
import pandas as pd

from competition_core.pipeline.config import PipelineConfig, load_pipeline_config
from competition_core.pipeline.time_context import ResearchContext


def context():
    return ResearchContext(date(2026, 9, 2), "intraday", date(2026, 9, 1),
                           datetime.fromisoformat("2026-09-02T14:50:00+08:00"))


def synthetic_panel(count=15):
    sessions = pd.bdate_range(end="2026-09-01", periods=120).strftime("%Y-%m-%d").tolist()
    symbols = ["000001.SZ", "300750.SZ", "600519.SH"] + [f"600{i:03d}.SH" for i in range(1, count - 2)]
    symbols = sorted(symbols[:count])
    rows = []
    for number, symbol in enumerate(symbols):
        prices = np.linspace(50, 70 + number, 120) + np.sin(np.arange(120)) * (number + 1) / 20
        for index, day in enumerate(sessions):
            factor = 1.0 if index < 115 else 2.0
            close = prices[index]
            raw = close * 2 / factor
            volume = 1000.0 + index * 2 + number
            row = dict(symbol=symbol, trade_date=day, close_raw=raw, close_qfq=close, adj_factor=factor,
                       volume_raw=volume, volume_shares=volume * 100, amount_raw=volume * raw / 10,
                       amount_cny=volume * raw * 100, turnover_rate=1 + number / 5,
                       volume_ratio=1 + number / 10, total_mv=1e6, circ_mv=8e5)
            for field, scale in (("open", .99), ("high", 1.02), ("low", .98)):
                row[f"{field}_qfq"], row[f"{field}_raw"] = close * scale, raw * scale
            rows.append(row)
    return pd.DataFrame(rows), sessions, symbols


def technical_rows(panel, sessions, symbols):
    """Small indicator fixture for Core math tests; adapter tests use real Vibe."""
    rows = []
    for symbol in symbols:
        close = panel.loc[panel.symbol == symbol].set_index("trade_date").close_qfq.reindex(sessions)
        valid = close.notna() & close.gt(0)
        length = 0
        for present in valid.iloc[::-1]:
            if not present:
                break
            length += 1
        row = {"symbol": symbol, "contiguous_sessions": length, "missing_fields": [], "rsi_14": 95., "macd_histogram": .1}
        for n in (5, 10, 20):
            row[f"sma_{n}"] = float(close.iloc[-n:].mean()) if length >= n else None
        row["sma_20_lag5"] = float(close.iloc[-25:-5].mean()) if close.iloc[-25:-5].notna().all() else None
        for field, sign in (("upper", 2), ("middle", 0), ("lower", -2)):
            row[f"bollinger_{field}"] = float(close.iloc[-20:].mean() + sign * close.iloc[-20:].std()) if length >= 20 else None
        rows.append(row)
    return rows


def small_config():
    payload = load_pipeline_config().to_dict()
    payload["stages"].update(s1_top_n=12, s2_top_n=12, s3_top_n=10, s4_target_n=10)
    return PipelineConfig.from_dict(payload)
