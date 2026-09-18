"""Versioned baseline settings, independent of Universe exclusion rules."""

from __future__ import annotations

import json
from dataclasses import dataclass
import math
from pathlib import Path
import re
from typing import Any

import yaml

from competition_core.pipeline.artifacts import content_hash
from competition_core.pipeline.errors import PipelineError

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "config/pipeline-config.yaml"


@dataclass(frozen=True)
class PipelineConfig:
    """Keep a canonical JSON copy so caller-owned dict mutation cannot change a run."""

    _json: str

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PipelineConfig":
        try:
            _validate(payload)
            text = json.dumps(payload, ensure_ascii=False, sort_keys=True, allow_nan=False)
        except (KeyError, TypeError, ValueError) as exc:
            if isinstance(exc, PipelineError):
                raise
            raise PipelineError("NOT_CONFIGURED", "invalid pipeline configuration", reason=str(exc)) from exc
        return cls(text)

    @property
    def version(self) -> str:
        return self.to_dict()["version"]

    @property
    def config_hash(self) -> str:
        return content_hash(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._json)

    def section(self, name: str) -> dict[str, Any]:
        return self.to_dict()[name]


def load_pipeline_config(path: str | Path = DEFAULT_CONFIG) -> PipelineConfig:
    try:
        with Path(path).open(encoding="utf-8") as handle:
            payload = yaml.safe_load(handle)
    except (OSError, yaml.YAMLError) as exc:
        raise PipelineError("NOT_CONFIGURED", "cannot read pipeline config") from exc
    return PipelineConfig.from_dict(payload)


def _validate(config: dict[str, Any]) -> None:
    keys = {"version", "dataset", "stages", "score", "screening", "risk", "ai", "realtime", "portfolio", "shadow"}
    # Old frozen screening runs remain readable, but cannot silently acquire
    # unversioned S3 weights. S3 explicitly requires the evidence section.
    if not isinstance(config, dict) or set(config) not in (keys, keys | {"evidence"}):
        raise ValueError("pipeline config has missing or unknown sections")
    if not isinstance(config["version"], str) or not config["version"].strip():
        raise ValueError("version is required")
    for key in set(config) - {"version"}:
        if not isinstance(config[key], dict):
            raise ValueError(f"{key} must be a mapping")
    expected = {
        "dataset": {"warmup_sessions", "minimum_history_sessions", "minimum_full_history_ratio"},
        "stages": {"s1_top_n", "s2_top_n", "s3_top_n", "s4_target_n"},
        "screening": {"benchmark_symbol", "limit_lookback_sessions", "technical_timeout_seconds", "minimum_available_weight"},
        "risk": {"corr_threshold", "correlation_sessions", "min_pair_observations", "max_positions",
                 "min_positions", "cash_buffer", "single_name_cap", "max_candidates_per_cluster",
                 "max_candidates_per_sector", "max_overheated_candidates", "overheat_threshold"},
        "ai": {"concurrency", "max_attempts", "timeout_seconds", "packet_max_chars"},
        "realtime": {"max_quote_age_seconds", "max_drawdown_from_high", "max_gap"},
        "portfolio": {"core_share", "aggressive_share", "slippage_buffer", "account_max_age_seconds", "execution_mode"},
    }
    for section, fields in expected.items():
        if section == "ai" and set(config[section]) == fields | {"reasoning_effort", "max_output_tokens"}:
            continue  # Legacy frozen screening configs remain readable, not silently upgraded.
        if set(config[section]) != fields:
            raise ValueError(f"{section} has missing or unknown fields")
    if "evidence" in config and set(config["evidence"]) != {
        "lookback_sessions", "quant_weight", "flow_weight", "quality_weight", "minimum_data_confidence"
    }:
        raise ValueError("evidence has missing or unknown fields")

    def number(section: str, key: str, low: float, high: float, *, integer: bool = False):
        value = config[section][key]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError(f"{section}.{key} must be finite numeric")
        if not low <= value <= high or (integer and not isinstance(value, int)):
            raise ValueError(f"{section}.{key} outside supported bounds")
        return value

    warmup = number("dataset", "warmup_sessions", 120, 2000, integer=True)
    number("dataset", "minimum_history_sessions", 20, warmup, integer=True)
    number("dataset", "minimum_full_history_ratio", 0.5, 1)
    sizes = [number("stages", key, 1, 10000, integer=True)
             for key in ("s1_top_n", "s2_top_n", "s3_top_n", "s4_target_n")]
    if sizes != sorted(sizes, reverse=True) or not 10 <= sizes[-1] <= 15:
        raise ValueError("stage sizes must descend to 10–15 finalists")
    components = {"trend", "momentum", "relative_strength", "liquidity", "volatility_quality", "data_quality"}
    if set(config["score"]) != components:
        raise ValueError("unknown/missing score components")
    weights = [number("score", key, 0, 1) for key in components]
    if not math.isclose(sum(weights), 1, abs_tol=1e-10):
        raise ValueError("score weights must sum to one")
    benchmark = config["screening"]["benchmark_symbol"]
    if not isinstance(benchmark, str) or not re.fullmatch(r"(?:000[0-9]{3}\.SH|399[0-9]{3}\.SZ)", benchmark):
        raise ValueError("benchmark_symbol must identify a supported index, not an equity")
    number("screening", "limit_lookback_sessions", 1, 20, integer=True)
    number("screening", "technical_timeout_seconds", 1, 300, integer=True)
    number("screening", "minimum_available_weight", 0.1, 1)
    if "evidence" in config:
        number("evidence", "lookback_sessions", 5, 5, integer=True)
        evidence_weights = [number("evidence", key, 0, 1) for key in ("quant_weight", "flow_weight", "quality_weight")]
        if evidence_weights[0] <= 0 or not math.isclose(sum(evidence_weights), 1, abs_tol=1e-10):
            raise ValueError("evidence weights must sum to one with positive quant weight")
        number("evidence", "minimum_data_confidence", 0.1, 1)
    number("risk", "corr_threshold", 0.1, 1)
    corr = number("risk", "correlation_sessions", 20, warmup - 1, integer=True)
    number("risk", "min_pair_observations", 10, corr, integer=True)
    minimum = number("risk", "min_positions", 5, 8, integer=True)
    maximum = number("risk", "max_positions", minimum, 8, integer=True)
    cash = number("risk", "cash_buffer", 0, 0.9)
    cap = number("risk", "single_name_cap", 0.01, 1)
    if maximum * cap < 1 - cash - 1e-12:
        raise ValueError("cash buffer, position count and per-name cap are incompatible")
    number("risk", "max_candidates_per_cluster", 1, sizes[-1], integer=True)
    number("risk", "max_candidates_per_sector", 1, sizes[-1], integer=True)
    number("risk", "max_overheated_candidates", 0, sizes[-1] - 1, integer=True)
    number("risk", "overheat_threshold", 0, 100)
    number("ai", "concurrency", 1, 4, integer=True)
    number("ai", "max_attempts", 1, 3, integer=True)
    number("ai", "timeout_seconds", 1, 300, integer=True)
    number("ai", "packet_max_chars", 2500, 4000, integer=True)
    if "reasoning_effort" in config["ai"]:
        if config["ai"]["reasoning_effort"] not in {"configured", "none", "low", "high", "max"}:
            raise ValueError("unsupported AI review reasoning effort")
        number("ai", "max_output_tokens", 512, 16384, integer=True)
    number("realtime", "max_quote_age_seconds", 1, 120, integer=True)
    number("realtime", "max_drawdown_from_high", 0, 1)
    number("realtime", "max_gap", 0, 1)
    shares = [number("portfolio", key, 0, 1) for key in ("core_share", "aggressive_share")]
    if not math.isclose(sum(shares), 1, abs_tol=1e-10):
        raise ValueError("core/aggressive shares must sum to one")
    number("portfolio", "slippage_buffer", 0, 0.1)
    number("portfolio", "account_max_age_seconds", 1, 3600, integer=True)
    if config["portfolio"]["execution_mode"] != "MANUAL_CONFIRM":
        raise ValueError("automatic execution is forbidden")
    if set(config["shadow"]) != {"qlib", "trading_agents"} or any(type(v) is not bool for v in config["shadow"].values()):
        raise ValueError("shadow flags must be booleans")
