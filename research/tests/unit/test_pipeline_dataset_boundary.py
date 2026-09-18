from datetime import date, datetime

import pandas as pd
import pytest

from competition_core.pipeline.dataset import CanonicalDataset
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.time_context import ResearchContext
from tests.unit.test_pipeline_dataset import (
    SYMBOLS,
    Source,
    context,
    prepare,
    wrapped,
)


def test_same_reference_with_new_as_of_and_snapshot_reuses_daily_cache(tmp_path):
    dataset, first, source = prepare(tmp_path)
    source.calls.clear()
    second_context = ResearchContext(
        date(2026, 9, 2),
        "intraday",
        date(2026, 9, 1),
        datetime.fromisoformat("2026-09-02T10:30:00+08:00"),
    )

    second = dataset.prepare(
        wrapped(source, second_context),
        second_context,
        SYMBOLS,
        snapshot_id="fixture-snapshot-2",
        sessions=3,
    )

    assert second["dataset_id"] != first["dataset_id"]
    assert second["as_of"] == "2026-09-02T10:30:00+08:00"
    assert second["snapshot_id"] == "fixture-snapshot-2"
    assert not any(endpoint == "daily" for endpoint, _ in source.calls)


def test_next_reference_fetches_only_new_session_batches(tmp_path):
    dataset, _, source = prepare(tmp_path)
    source.calls.clear()
    next_context = ResearchContext(
        date(2026, 9, 3),
        "intraday",
        date(2026, 9, 2),
        datetime.fromisoformat("2026-09-03T10:30:00+08:00"),
    )
    next_client = wrapped(source, next_context)
    observed_at = datetime.fromisoformat("2026-09-03T11:00:00+08:00")
    next_client.observed_clock = lambda: observed_at

    manifest = dataset.prepare(
        next_client,
        next_context,
        SYMBOLS,
        snapshot_id="fixture-snapshot-next",
        sessions=3,
    )

    fetched = [
        (endpoint, params["trade_date"])
        for endpoint, params in source.calls
        if endpoint in {"daily", "daily_basic", "adj_factor"}
    ]
    assert set(fetched) == {
        ("daily", "20260902"),
        ("daily_basic", "20260902"),
        ("adj_factor", "20260902"),
    }
    assert manifest["market_data_reference_date"] == "2026-09-02"
    assert manifest["research_date"] == "2026-09-03"


def test_missing_reference_factor_preserves_history_but_nulls_all_qfq(tmp_path):
    source = Source()
    source.missing_factor = True
    reference_context = context(reference=date(2026, 8, 31))
    dataset = CanonicalDataset(tmp_path, "fixture")
    manifest = dataset.prepare(
        wrapped(source, reference_context),
        reference_context,
        SYMBOLS,
        snapshot_id="fixture-snapshot-reference-factor-missing",
        sessions=2,
    )
    panel = dataset.load(manifest["manifest_path"])
    catl = panel.loc[panel.symbol == "300750.SZ"]

    historical = catl.loc[catl.trade_date < "2026-08-31"]
    assert historical.adj_factor.tolist() == [1.0]
    assert pd.isna(catl.loc[catl.trade_date == "2026-08-31", "adj_factor"]).all()
    assert catl[["open_qfq", "high_qfq", "low_qfq", "close_qfq"]].isna().all().all()


def test_raw_ohlc_with_high_below_open_is_rejected(tmp_path):
    source = Source()
    fetch = source.daily

    def with_invalid_high(**params):
        frame = fetch(**params)
        if params["trade_date"] == "20260901":
            mask = frame.ts_code == "000001.SZ"
            frame.loc[mask, "high"] = frame.loc[mask, "open"] - 0.01
        return frame

    source.daily = with_invalid_high
    with pytest.raises(PipelineError, match="inconsistent raw OHLC"):
        prepare(tmp_path, source)
