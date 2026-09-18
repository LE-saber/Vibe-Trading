from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from qlib_research.models import ResearchRequest, ResearchWindow
from qlib_research.symbols import canonical_to_qlib, qlib_to_canonical


def test_symbol_round_trip_and_conflict_rejection() -> None:
    assert canonical_to_qlib("600519.SH") == "SH600519"
    assert qlib_to_canonical("SH600519") == "600519.SH"
    assert canonical_to_qlib("000001.SZ") == "SZ000001"
    with pytest.raises(ValueError):
        canonical_to_qlib("600519.SZSE")


def test_research_window_is_ordered_and_non_overlapping() -> None:
    with pytest.raises(ValidationError):
        ResearchWindow(
            train_start=date(2024, 1, 1),
            train_end=date(2024, 6, 30),
            valid_start=date(2024, 6, 30),
            valid_end=date(2024, 8, 1),
            test_start=date(2024, 9, 1),
            test_end=date(2024, 12, 1),
        )


def test_competition_period_cannot_enter_training(tmp_path) -> None:
    with pytest.raises(ValidationError, match="competition-period data"):
        ResearchRequest(
            run_id="lookahead",
            canonical_parquet=tmp_path / "market.parquet",
            universe_parquet=tmp_path / "universe.parquet",
            qlib_store=tmp_path / "store",
            artifact_dir=tmp_path / "artifacts",
            window=ResearchWindow(
                train_start=date(2024, 1, 1),
                train_end=date(2024, 6, 30),
                valid_start=date(2024, 7, 1),
                valid_end=date(2024, 9, 30),
                test_start=date(2024, 10, 1),
                test_end=date(2025, 12, 31),
            ),
            training_cutoff=date(2026, 8, 31),
            inference_as_of=date(2026, 9, 1),
        )
