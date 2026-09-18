from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from qlib_research.converter import convert_canonical_parquet


def test_converter_preserves_qfq_prices_and_raw_share_volume(research_inputs, tmp_path) -> None:
    market_path, _, dates, _ = research_inputs
    target = tmp_path / "qlib-store"
    cutoff = dates[120].date()

    manifest = convert_canonical_parquet(market_path, target, cutoff)

    source = pd.read_parquet(market_path)
    source = source.loc[
        (source["symbol"] == "000005.SZ") & (source["date"] <= pd.Timestamp(cutoff))
    ].sort_values("date")
    feature_dir = target / "features" / "sz000005"
    close = np.fromfile(feature_dir / "close.day.bin", dtype="<f4")[1:]
    volume = np.fromfile(feature_dir / "volume.day.bin", dtype="<f4")[1:]
    raw_close = np.fromfile(feature_dir / "raw_close.day.bin", dtype="<f4")[1:]

    np.testing.assert_allclose(close, source["close_qfq"], rtol=1e-6)
    np.testing.assert_allclose(raw_close, source["close"], rtol=1e-6)
    np.testing.assert_allclose(volume, source["volume"], rtol=1e-6)
    assert manifest.volume_unit == "shares"
    assert manifest.rows_written < manifest.rows_read
    saved = json.loads((target / "conversion-manifest.json").read_text(encoding="utf-8"))
    assert saved["price_field"] == "qfq"


def test_converter_refuses_duplicate_identity_rows(research_inputs, tmp_path) -> None:
    market_path, _, _, _ = research_inputs
    frame = pd.read_parquet(market_path)
    duplicate = tmp_path / "duplicate.parquet"
    pd.concat([frame, frame.iloc[[0]]]).to_parquet(duplicate, index=False)
    with pytest.raises(ValueError, match="duplicate"):
        convert_canonical_parquet(duplicate, tmp_path / "store", frame["date"].max().date())
