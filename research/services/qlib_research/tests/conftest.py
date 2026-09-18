from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def research_inputs(tmp_path):
    dates = pd.bdate_range("2025-01-02", periods=180)
    symbols = [f"{code:06d}.SZ" for code in range(1, 7)]
    rows = []
    universe = []
    for symbol_index, symbol in enumerate(symbols):
        for date_index, timestamp in enumerate(dates):
            raw_close = 10.0 + symbol_index * 1.5 + date_index * 0.015 + np.sin(date_index / 7 + symbol_index)
            factor = 1.0 + (0.2 if date_index >= 90 and symbol_index == 4 else 0.0)
            volume = float(1_000_000 + symbol_index * 20_000 + date_index * 1_000)
            rows.append(
                {
                    "date": timestamp,
                    "symbol": symbol,
                    "open": raw_close * 0.995,
                    "high": raw_close * 1.01,
                    "low": raw_close * 0.99,
                    "close": raw_close,
                    "open_qfq": raw_close * factor * 0.995,
                    "high_qfq": raw_close * factor * 1.01,
                    "low_qfq": raw_close * factor * 0.99,
                    "close_qfq": raw_close * factor,
                    "volume": volume,
                    "amount": volume * raw_close,
                    "adj_factor": factor,
                    "tradable": 1.0,
                }
            )
            universe.append(
                {"snapshot_date": timestamp, "symbol": symbol, "is_eligible": True}
            )
    market_path = tmp_path / "canonical.parquet"
    universe_path = tmp_path / "universe.parquet"
    pd.DataFrame(rows).to_parquet(market_path, index=False)
    pd.DataFrame(universe).to_parquet(universe_path, index=False)
    return market_path, universe_path, dates, symbols
