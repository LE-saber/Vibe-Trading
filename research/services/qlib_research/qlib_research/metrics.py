"""Cross-sectional, point-in-time evaluation metrics."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd


def daily_ic(frame: pd.DataFrame, score: str, label: str, method: str) -> pd.Series:
    def correlation(group: pd.DataFrame) -> float:
        valid = group[[score, label]].dropna()
        if len(valid) < 3 or valid[score].nunique() < 2 or valid[label].nunique() < 2:
            return float("nan")
        return float(valid[score].corr(valid[label], method=method))

    return frame.groupby("datetime", sort=True).apply(correlation, include_groups=False)


def summarize_prediction(frame: pd.DataFrame, score: str, label: str) -> dict[str, float | int | None]:
    ic = daily_ic(frame, score, label, "pearson").dropna()
    rank_ic = daily_ic(frame, score, label, "spearman").dropna()
    total = len(frame)
    covered = int(frame[[score, label]].dropna().shape[0])
    std = float(ic.std(ddof=1)) if len(ic) > 1 else None
    mean = float(ic.mean()) if len(ic) else None
    return {
        "sample_count": total,
        "covered_count": covered,
        "coverage": covered / total if total else 0.0,
        "ic_mean": mean,
        "rank_ic_mean": float(rank_ic.mean()) if len(rank_ic) else None,
        "ic_std": std,
        "icir": mean / std if mean is not None and std not in (None, 0.0) else None,
        "positive_ic_ratio": float((ic > 0).mean()) if len(ic) else None,
    }


def finite_or_none(value):
    if value is None:
        return None
    number = float(value)
    return number if math.isfinite(number) else None
