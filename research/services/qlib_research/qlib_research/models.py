"""Validated temporal contracts for Qlib research runs."""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, model_validator

_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,119}$")


class ResearchWindow(BaseModel):
    train_start: date
    train_end: date
    valid_start: date
    valid_end: date
    test_start: date
    test_end: date

    @model_validator(mode="after")
    def validate_order(self) -> "ResearchWindow":
        if not (
            self.train_start <= self.train_end
            < self.valid_start
            <= self.valid_end
            < self.test_start
            <= self.test_end
        ):
            raise ValueError("train, valid, and test windows must be ordered and non-overlapping")
        return self


class ResearchRequest(BaseModel):
    run_id: str
    canonical_parquet: Path
    universe_parquet: Path
    qlib_store: Path
    artifact_dir: Path
    window: ResearchWindow
    training_cutoff: date
    inference_as_of: date
    competition_start: date = date(2026, 8, 31)
    horizons: tuple[Literal[1, 3, 5], ...] = (1, 3, 5)
    model_version: str = "qlib-alpha158-lgbm-v1"
    refit_every: int = Field(default=20, ge=1, le=63)

    @model_validator(mode="after")
    def validate_temporal_boundary(self) -> "ResearchRequest":
        if not _RUN_ID.fullmatch(self.run_id):
            raise ValueError("run_id contains unsupported characters")
        if len(set(self.horizons)) != len(self.horizons):
            raise ValueError("horizons must be unique")
        if self.window.test_end > self.training_cutoff:
            raise ValueError("historical evaluation must end by training_cutoff")
        if self.training_cutoff >= self.competition_start:
            raise ValueError("competition-period data must not enter model training")
        if self.inference_as_of < self.training_cutoff:
            raise ValueError("inference_as_of cannot precede training_cutoff")
        if not self.model_version.strip():
            raise ValueError("model_version must not be empty")
        return self


class ConversionManifest(BaseModel):
    schema_version: int = 1
    source_sha256: str
    cutoff: date
    rows_read: int
    rows_written: int
    symbols: int
    calendar_days: int
    price_field: str = "qfq"
    volume_unit: Literal["shares"] = "shares"
    qlib_version: str
    symbol_mapping: Literal["canonical_to_qlib"] = "canonical_to_qlib"


class PredictionRecord(BaseModel):
    symbol: str
    prediction: float
    rank: int = Field(ge=1)
    horizon: Literal[1, 3, 5]
    model_version: str
    as_of: date
    training_window: dict[str, str]
    validation_metrics: dict[str, float | int | None]


class ResearchResult(BaseModel):
    run_id: str
    framework: Literal["Microsoft Qlib"] = "Microsoft Qlib"
    framework_version: str
    alpha_count: int
    horizons: list[int]
    historical_window: dict[str, str]
    training_cutoff: date
    inference_as_of: date
    metrics: dict[str, dict]
    artifacts: dict[str, str]
