"""Versioned, deterministic cross-sectional screening; not a return forecast."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from competition_core.pipeline.config import PipelineConfig
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.features import finite

SCORING_VERSION = "p10c.percentile.v1"
# Direction +1 prefers high; -1 prefers low. Available features within each
# component have equal weight, then configured component weights are normalized.
_S1 = {
    "trend": {"ma_alignment": 1, "distance_ma20": 1, "ma20_slope_5d": 1},
    "momentum": {f"momentum_{n}d": 1 for n in (3, 5, 10, 20)},
    "relative_strength": {"relative_strength_20d": 1, "relative_strength_60d": 1},
    "liquidity": {"latest_amount_cny": 1, "median_amount_20d": 1, "turnover_rate": 1, "volume_ratio": 1},
    "volatility_quality": {"atr14_ratio": -1, "volatility_20d": -1, "max_drawdown_5d": -1, "max_drawdown_20d": -1},
    "data_quality": {"history_coverage": 1, "factor_coverage": 1, "source_confidence": 1},
}
_S2_EXTRA = {
    "trend": {"trend_consistency_20d": 1, "breakout_20d": 1},
    "momentum": {},
    "relative_strength": {"benchmark_relative_20d": 1, "benchmark_relative_60d": 1,
                          "sector_relative_20d": 1, "sector_relative_60d": 1},
    "liquidity": {"volume_price_confirmation_5d": 1},
    "volatility_quality": {"mean_abs_gap_20d": -1, "drawdown_recovery_20d": 1},
    "data_quality": {},
}
_OVERHEAT = ("rsi_14", "turnover_rate", "volume_ratio", "acceleration_5d", "distance_ma20")


def percentile(series: pd.Series, direction: int = 1) -> pd.Series:
    """0–100, average ties, constant/single observation neutral50, missing NaN."""
    if direction not in {-1, 1}:
        raise PipelineError("INVALID_ARGUMENTS", "percentile direction must be -1 or 1")
    numeric = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan)
    present = numeric.notna()
    result = pd.Series(np.nan, index=series.index, dtype=float)
    n = int(present.sum())
    if n:
        result.loc[present] = 50.0 if n == 1 or numeric.loc[present].nunique() == 1 else (
            (numeric.loc[present].rank(method="average", ascending=direction == 1) - 1) / (n - 1) * 100
        )
    return result


def feature_groups(stage: str) -> dict[str, dict[str, int]]:
    if stage not in {"S1", "S2"}:
        raise PipelineError("INVALID_ARGUMENTS", "unknown quantitative stage")
    return {name: {**fields, **(_S2_EXTRA[name] if stage == "S2" else {})} for name, fields in _S1.items()}


def score_stage(features: pd.DataFrame, config: PipelineConfig, *, stage: str) -> dict[str, Any]:
    """All inputs retained in audit rows, including rejected/unscorable symbols."""
    groups = feature_groups(stage)
    frame = features.copy(deep=True).set_index("symbol", drop=False)
    if frame.index.duplicated().any() or frame.empty:
        raise PipelineError("IDENTITY_CONFLICT", "screening needs unique nonempty input symbols")
    minimum = config.section("dataset")["minimum_history_sessions"]
    price = pd.to_numeric(frame.latest_close_qfq, errors="coerce")
    enough = frame.history_sessions.ge(minimum) & price.gt(0) & np.isfinite(price)
    peers = frame.loc[enough]
    percentiles = pd.DataFrame(index=frame.index, dtype=float)
    components = pd.DataFrame(index=frame.index, dtype=float)
    for component, fields in groups.items():
        for field, direction in fields.items():
            series = peers[field] if field in peers else pd.Series(np.nan, index=peers.index)
            percentiles[field] = percentile(series, direction).reindex(frame.index)
        components[component] = percentiles[list(fields)].mean(axis=1, skipna=True)
    weights = config.section("score")
    available_weight = sum(components[name].notna().astype(float) * weight for name, weight in weights.items())
    weighted = sum(components[name].fillna(0) * weight for name, weight in weights.items())
    # fillna(0) only masks absent terms in the numerator. Denominator excludes
    # their weights; a missing component itself is always serialized as null.
    scores = weighted.div(available_weight.where(available_weight.gt(0)))
    scoreable = enough & available_weight.ge(config.section("screening")["minimum_available_weight"])
    heat_fields = list(_OVERHEAT) + (["recent_limit_up_count"] if stage == "S2" else [])
    heat = pd.DataFrame({name: percentile(peers[name] if name in peers else pd.Series(np.nan, index=peers.index))
                         for name in heat_fields}, index=frame.index)
    overheat = heat.mean(axis=1, skipna=True)
    top_n = config.section("stages")[f"{stage.lower()}_top_n"]
    ranked = pd.DataFrame({"symbol": frame.index, "quant_score": scores, "scoreable": scoreable}).reset_index(drop=True)
    ordered = ranked.loc[ranked.scoreable].sort_values(["quant_score", "symbol"], ascending=[False, True], kind="stable")
    ranks = {symbol: rank for rank, symbol in enumerate(ordered.symbol, 1)}
    selected = ordered.symbol.head(top_n).tolist()
    selected_set = set(selected)
    rows = []
    for symbol in sorted(frame.index):
        row = frame.loc[symbol]
        missing = sorted(name for name in percentiles if finite(row.get(name)) is None)
        reasons = []
        if finite(row.latest_close_qfq) is None or row.latest_close_qfq <= 0:
            reasons.append("missing_reference_qfq_close")
        if row.history_sessions < minimum:
            reasons.append(f"history_below_{minimum}_sessions")
        if enough.loc[symbol] and not scoreable.loc[symbol]:
            reasons.append("insufficient_available_component_weight")
        if symbol not in selected_set and not reasons:
            reasons.append(f"rank_below_{stage.lower()}_top{top_n}")
        completeness = float(percentiles.loc[symbol].notna().mean())
        denominator = float(available_weight.loc[symbol])
        component_scores = {name: finite(components.loc[symbol, name]) for name in groups}
        rows.append({
            "symbol": symbol, "rank": ranks.get(symbol), "selected": symbol in selected_set,
            "quant_score": finite(scores.loc[symbol]),
            **{f"{name}_score": value for name, value in component_scores.items()},
            "overheat_score": finite(overheat.loc[symbol]),
            "data_confidence": float(row.history_coverage) * denominator * completeness,
            "available_component_weight": denominator, "feature_completeness": completeness,
            "effective_weights": {name: weight / denominator if component_scores[name] is not None and denominator else None
                                  for name, weight in weights.items()},
            "component_scores": component_scores,
            "feature_percentiles": {name: finite(percentiles.loc[symbol, name]) for name in percentiles},
            "overheat_percentiles": {name: finite(heat.loc[symbol, name]) for name in heat},
            "features": {name: (value if isinstance(value, (list, str)) else finite(value))
                         for name, value in row.items() if name != "symbol"},
            "missing_fields": sorted(set(missing + [name for name in heat_fields if finite(row.get(name)) is None]
                                          + list(row.get("technical_missing_fields", [])))),
            "reject_reasons": reasons,
            "keep_reasons": [f"{stage.lower()}_rank_{ranks[symbol]}_within_top{top_n}"] if symbol in selected_set else [],
        })
    return {"stage": stage, "scoring_version": SCORING_VERSION,
            "status": "SUCCESS" if len(selected) == top_n else "PARTIAL",
            "input_symbols": sorted(frame.index), "output_symbols": selected, "rows": rows,
            "summary": {"input_count": len(frame), "scorable_count": len(ordered), "selected_count": len(selected),
                        "rejected_count": len(frame) - len(selected), "requested_count": top_n,
                        "missing_counts": {name: sum(name in row["missing_fields"] for row in rows)
                                           for name in sorted({field for row in rows for field in row["missing_fields"]})},
                        "llm_calls": 0, "agent_tool_calls": 0}}
