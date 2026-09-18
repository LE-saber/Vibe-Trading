from __future__ import annotations

import json

import pandas as pd

from qlib_research.models import ResearchRequest, ResearchWindow
from qlib_research.pipeline import QlibResearchPipeline


def test_real_qlib_alpha158_rolling_pipeline_is_pit_and_reproducible(research_inputs, tmp_path) -> None:
    market_path, universe_path, dates, symbols = research_inputs
    request = ResearchRequest(
        run_id="p7-integration",
        canonical_parquet=market_path,
        universe_parquet=universe_path,
        qlib_store=tmp_path / "qlib-store",
        artifact_dir=tmp_path / "artifacts",
        window=ResearchWindow(
            train_start=dates[0].date(),
            train_end=dates[79].date(),
            valid_start=dates[80].date(),
            valid_end=dates[119].date(),
            test_start=dates[120].date(),
            test_end=dates[159].date(),
        ),
        training_cutoff=dates[159].date(),
        inference_as_of=dates[170].date(),
        competition_start=pd.Timestamp("2026-08-31").date(),
        refit_every=20,
    )

    result = QlibResearchPipeline().run(request)

    assert result.framework_version == "0.9.7"
    assert result.alpha_count == 158
    assert result.horizons == [1, 3, 5]
    assert set(result.metrics) == {"1", "3", "5"}
    assert all(result.metrics[key]["coverage"] > 0.9 for key in result.metrics)
    scores = json.loads(open(result.artifacts["competition_scores"], encoding="utf-8").read())
    assert len(scores) == len(symbols) * 3
    assert {item["symbol"] for item in scores} == set(symbols)
    assert {item["horizon"] for item in scores} == {1, 3, 5}
    history = pd.read_parquet(result.artifacts["historical_predictions"])
    for horizon, group in history.groupby("horizon"):
        assert (group["training_end"] < group["datetime"]).all()
        assert group["training_end"].max().date() <= request.training_cutoff
    factor_metrics = pd.read_csv(result.artifacts["alpha_metrics"])
    assert len(factor_metrics) == 158 * 3


def test_pit_universe_exclusion_is_applied_by_sample_date(research_inputs, tmp_path) -> None:
    market_path, universe_path, dates, symbols = research_inputs
    universe = pd.read_parquet(universe_path)
    excluded_date = dates[100]
    excluded_symbol = symbols[0]
    universe.loc[
        (universe["snapshot_date"] == excluded_date) & (universe["symbol"] == excluded_symbol),
        "is_eligible",
    ] = False
    path = tmp_path / "filtered-universe.parquet"
    universe.to_parquet(path, index=False)
    frame = pd.DataFrame(
        {"datetime": [excluded_date, excluded_date], "symbol": [excluded_symbol, symbols[1]]}
    )

    filtered = QlibResearchPipeline._apply_pit_universe(frame, path)

    assert excluded_symbol not in filtered["symbol"].tolist()
    assert symbols[1] in filtered["symbol"].tolist()
