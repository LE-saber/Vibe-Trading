"""Strict, versioned configuration for the read-only terminal strategy layer.

The terminal strategy is deliberately configured independently from the existing
S0--S6 settings.  In particular, ``leaderboard.top_n`` is only a display
setting; this module never invents an award cutoff when the competition rules
have not supplied one.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, time
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
from typing import Any, Mapping

import yaml

from competition_core.pipeline.artifacts import content_hash


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "terminal-strategy.yaml"


class TerminalStrategyConfigError(ValueError):
    """Raised when terminal strategy configuration is missing or unsafe."""


@dataclass(frozen=True, slots=True)
class TerminalCompetitionConfig:
    """Competition rules needed by the terminal strategy.

    ``award_rank_cutoff`` is intentionally nullable.  A null value is a safe,
    explicit "rules not confirmed" state, not a default to the displayed Top
    30.  The state machine will remain ``UNAVAILABLE`` in that state.
    """

    award_rank_cutoff: int | None
    award_rule_source: str | None
    final_trade_date: date
    remaining_trade_dates: tuple[date, ...]


@dataclass(frozen=True, slots=True)
class AwardLineConfig:
    safety_buffer_min: Decimal
    safety_buffer_base: Decimal
    safety_buffer_max: Decimal
    history_sessions: int
    min_valid_snapshots: int
    max_snapshot_age_seconds: int
    forecast_method: str


@dataclass(frozen=True, slots=True)
class TerminalModeConfig:
    normal_gap_max: Decimal
    attack_gap_max: Decimal
    last_shot_min_gap: Decimal
    protect_lead_min: Decimal
    hysteresis: Decimal
    downgrade_confirmations: int
    last_shot_only_on_final_session: bool


@dataclass(frozen=True, slots=True)
class FunnelConfig:
    watch_count: int
    focus_count: int
    terminal_count_min: int
    terminal_count_max: int
    continuity_sessions: int
    minimum_liquidity_score: Decimal
    maximum_intraday_drawdown: Decimal
    minimum_execution_probability: Decimal
    minimum_evidence_quality: Decimal
    maximum_correlation: Decimal
    maximum_opening_gap: Decimal
    max_quote_age_seconds: int
    overheat_score_threshold: Decimal
    overheat_penalty: Decimal
    scoring_weights: tuple[tuple[str, Decimal], ...]

    @property
    def weight_map(self) -> dict[str, Decimal]:
        return dict(self.scoring_weights)


@dataclass(frozen=True, slots=True)
class AllocationConfig:
    protect_aggressive_cap: Decimal
    normal_aggressive_target: Decimal
    attack_aggressive_min: Decimal
    attack_aggressive_max: Decimal
    last_shot_aggressive_cap: Decimal
    protect_cash_target: Decimal
    normal_cash_target: Decimal
    attack_cash_target: Decimal
    last_shot_cash_target: Decimal
    new_listing_final_day_cap: Decimal


@dataclass(frozen=True, slots=True)
class TerminalScheduleConfig:
    candidate_rank: time
    opening_observation_end: time
    midday_refresh: time
    primary_decision: time
    final_revision: time
    main_order_deadline: time
    risk_freeze: time


@dataclass(frozen=True, slots=True)
class TerminalStrategyConfig:
    """Immutable terminal strategy configuration with a stable content hash."""

    version: str
    competition: TerminalCompetitionConfig
    award_line: AwardLineConfig
    mode: TerminalModeConfig
    funnel: FunnelConfig
    allocation: AllocationConfig
    schedule: TerminalScheduleConfig

    @property
    def award_rank_cutoff(self) -> int | None:
        return self.competition.award_rank_cutoff

    @property
    def final_trade_date(self) -> date:
        return self.competition.final_trade_date

    @property
    def config_hash(self) -> str:
        return content_hash(self.to_dict())

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "TerminalStrategyConfig":
        return _from_mapping(payload)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "competition": {
                "award_rank_cutoff": self.competition.award_rank_cutoff,
                "award_rule_source": self.competition.award_rule_source,
                "final_trade_date": self.competition.final_trade_date.isoformat(),
                "remaining_trade_dates": [
                    item.isoformat() for item in self.competition.remaining_trade_dates
                ],
            },
            "award_line": {
                "safety_buffer_min": str(self.award_line.safety_buffer_min),
                "safety_buffer_base": str(self.award_line.safety_buffer_base),
                "safety_buffer_max": str(self.award_line.safety_buffer_max),
                "history_sessions": self.award_line.history_sessions,
                "min_valid_snapshots": self.award_line.min_valid_snapshots,
                "max_snapshot_age_seconds": self.award_line.max_snapshot_age_seconds,
                "forecast_method": self.award_line.forecast_method,
            },
            "mode": {
                "normal_gap_max": str(self.mode.normal_gap_max),
                "attack_gap_max": str(self.mode.attack_gap_max),
                "last_shot_min_gap": str(self.mode.last_shot_min_gap),
                "protect_lead_min": str(self.mode.protect_lead_min),
                "hysteresis": str(self.mode.hysteresis),
                "downgrade_confirmations": self.mode.downgrade_confirmations,
                "last_shot_only_on_final_session": self.mode.last_shot_only_on_final_session,
            },
            "funnel": {
                "watch_count": self.funnel.watch_count,
                "focus_count": self.funnel.focus_count,
                "terminal_count_min": self.funnel.terminal_count_min,
                "terminal_count_max": self.funnel.terminal_count_max,
                "continuity_sessions": self.funnel.continuity_sessions,
                "minimum_liquidity_score": str(self.funnel.minimum_liquidity_score),
                "maximum_intraday_drawdown": str(self.funnel.maximum_intraday_drawdown),
                "minimum_execution_probability": str(self.funnel.minimum_execution_probability),
                "minimum_evidence_quality": str(self.funnel.minimum_evidence_quality),
                "maximum_correlation": str(self.funnel.maximum_correlation),
                "maximum_opening_gap": str(self.funnel.maximum_opening_gap),
                "max_quote_age_seconds": self.funnel.max_quote_age_seconds,
                "overheat_score_threshold": str(self.funnel.overheat_score_threshold),
                "overheat_penalty": str(self.funnel.overheat_penalty),
                "scoring_weights": {
                    name: str(weight) for name, weight in self.funnel.scoring_weights
                },
            },
            "allocation": {
                "protect_aggressive_cap": str(self.allocation.protect_aggressive_cap),
                "normal_aggressive_target": str(self.allocation.normal_aggressive_target),
                "attack_aggressive_min": str(self.allocation.attack_aggressive_min),
                "attack_aggressive_max": str(self.allocation.attack_aggressive_max),
                "last_shot_aggressive_cap": str(self.allocation.last_shot_aggressive_cap),
                "protect_cash_target": str(self.allocation.protect_cash_target),
                "normal_cash_target": str(self.allocation.normal_cash_target),
                "attack_cash_target": str(self.allocation.attack_cash_target),
                "last_shot_cash_target": str(self.allocation.last_shot_cash_target),
                "new_listing_final_day_cap": str(self.allocation.new_listing_final_day_cap),
            },
            "schedule": {
                key: getattr(self.schedule, key).strftime("%H:%M")
                for key in _SCHEDULE_FIELDS
            },
        }


_ROOT_FIELDS = {"version", "competition", "award_line", "mode", "funnel", "allocation", "schedule"}
_COMPETITION_FIELDS = {
    "award_rank_cutoff", "award_rule_source", "final_trade_date", "remaining_trade_dates",
}
_AWARD_LINE_FIELDS = {
    "safety_buffer_min", "safety_buffer_base", "safety_buffer_max", "history_sessions",
    "min_valid_snapshots", "max_snapshot_age_seconds", "forecast_method",
}
_MODE_FIELDS = {
    "normal_gap_max", "attack_gap_max", "last_shot_min_gap", "protect_lead_min",
    "hysteresis", "downgrade_confirmations", "last_shot_only_on_final_session",
}
_FUNNEL_FIELDS = {
    "watch_count", "focus_count", "terminal_count_min", "terminal_count_max",
    "continuity_sessions", "minimum_liquidity_score", "maximum_intraday_drawdown",
    "minimum_execution_probability", "minimum_evidence_quality", "maximum_correlation",
    "maximum_opening_gap", "max_quote_age_seconds", "overheat_score_threshold",
    "overheat_penalty", "scoring_weights",
}
_ALLOCATION_FIELDS = {
    "protect_aggressive_cap", "normal_aggressive_target", "attack_aggressive_min",
    "attack_aggressive_max", "last_shot_aggressive_cap", "protect_cash_target",
    "normal_cash_target", "attack_cash_target", "last_shot_cash_target",
    "new_listing_final_day_cap",
}
_SCHEDULE_FIELDS = (
    "candidate_rank", "opening_observation_end", "midday_refresh", "primary_decision",
    "final_revision", "main_order_deadline", "risk_freeze",
)


def load_terminal_strategy_config(path: str | Path = DEFAULT_CONFIG_PATH) -> TerminalStrategyConfig:
    """Load and strictly validate one YAML configuration file."""

    try:
        with Path(path).open(encoding="utf-8") as handle:
            payload = yaml.safe_load(handle)
    except (OSError, yaml.YAMLError) as exc:
        raise TerminalStrategyConfigError("cannot read terminal strategy config") from exc
    return TerminalStrategyConfig.from_dict(payload)


# Short alias used by callers that treat all strategy configs uniformly.
load_terminal_config = load_terminal_strategy_config


def _from_mapping(payload: Mapping[str, Any]) -> TerminalStrategyConfig:
    if not isinstance(payload, Mapping) or set(payload) != _ROOT_FIELDS:
        raise TerminalStrategyConfigError("terminal strategy config has missing or unknown sections")
    version = _text(payload["version"], "version")
    competition = _parse_competition(payload["competition"])
    award_line = _parse_award_line(payload["award_line"])
    mode = _parse_mode(payload["mode"])
    funnel = _parse_funnel(payload["funnel"])
    allocation = _parse_allocation(payload["allocation"])
    schedule = _parse_schedule(payload["schedule"])
    return TerminalStrategyConfig(version, competition, award_line, mode, funnel, allocation, schedule)


def _parse_competition(value: Any) -> TerminalCompetitionConfig:
    if (
        not isinstance(value, Mapping)
        or not set(value).issubset(_COMPETITION_FIELDS)
        or "final_trade_date" not in value
        or "remaining_trade_dates" not in value
    ):
        raise TerminalStrategyConfigError("competition has missing or unknown fields")
    cutoff = value.get("award_rank_cutoff")
    if cutoff is not None and (type(cutoff) is not int or not 1 <= cutoff <= 100_000):
        raise TerminalStrategyConfigError("competition.award_rank_cutoff must be a positive integer or null")
    source = value.get("award_rule_source")
    if source is not None:
        source = _text(source, "competition.award_rule_source")
    final_date = _date(value["final_trade_date"], "competition.final_trade_date")
    raw_dates = value["remaining_trade_dates"]
    if not isinstance(raw_dates, (list, tuple)) or not raw_dates:
        raise TerminalStrategyConfigError("competition.remaining_trade_dates must be a non-empty list")
    try:
        remaining = tuple(_date(item, "competition.remaining_trade_dates") for item in raw_dates)
    except TerminalStrategyConfigError:
        raise
    if len(set(remaining)) != len(remaining) or remaining != tuple(sorted(remaining)):
        raise TerminalStrategyConfigError(
            "competition.remaining_trade_dates must be strictly sorted and unique"
        )
    if final_date not in remaining or final_date != max(remaining):
        raise TerminalStrategyConfigError(
            "competition.final_trade_date must be the maximum remaining trade date"
        )
    return TerminalCompetitionConfig(cutoff, source, final_date, remaining)


def _parse_award_line(value: Any) -> AwardLineConfig:
    _exact_fields(value, _AWARD_LINE_FIELDS, "award_line")
    minimum = _rate(value["safety_buffer_min"], "award_line.safety_buffer_min")
    base = _rate(value["safety_buffer_base"], "award_line.safety_buffer_base")
    maximum = _rate(value["safety_buffer_max"], "award_line.safety_buffer_max")
    if not minimum <= base <= maximum:
        raise TerminalStrategyConfigError("award_line safety buffers must be ordered min <= base <= max")
    history = _integer(value["history_sessions"], "award_line.history_sessions", 1, 100)
    minimum_snapshots = _integer(value["min_valid_snapshots"], "award_line.min_valid_snapshots", 1, history)
    max_age = _integer(value["max_snapshot_age_seconds"], "award_line.max_snapshot_age_seconds", 1, 86_400)
    method = _text(value["forecast_method"], "award_line.forecast_method")
    return AwardLineConfig(minimum, base, maximum, history, minimum_snapshots, max_age, method)


def _parse_mode(value: Any) -> TerminalModeConfig:
    _exact_fields(value, _MODE_FIELDS, "mode")
    normal = _rate(value["normal_gap_max"], "mode.normal_gap_max")
    attack = _rate(value["attack_gap_max"], "mode.attack_gap_max")
    last = _rate(value["last_shot_min_gap"], "mode.last_shot_min_gap")
    protect = _rate(value["protect_lead_min"], "mode.protect_lead_min")
    hysteresis = _rate(value["hysteresis"], "mode.hysteresis")
    if not normal < attack <= last:
        raise TerminalStrategyConfigError("mode gaps must satisfy normal < attack <= last_shot")
    if hysteresis >= min(normal, attack, last) and hysteresis != Decimal("0"):
        raise TerminalStrategyConfigError("mode.hysteresis is too large for configured boundaries")
    confirmations = _integer(value["downgrade_confirmations"], "mode.downgrade_confirmations", 1, 10)
    if value["last_shot_only_on_final_session"] is not True:
        raise TerminalStrategyConfigError("last-shot must be restricted to the final trading session")
    return TerminalModeConfig(normal, attack, last, protect, hysteresis, confirmations, True)


def _parse_funnel(value: Any) -> FunnelConfig:
    _exact_fields(value, _FUNNEL_FIELDS, "funnel")
    watch = _integer(value["watch_count"], "funnel.watch_count", 1, 100)
    focus = _integer(value["focus_count"], "funnel.focus_count", 1, watch)
    terminal_min = _integer(value["terminal_count_min"], "funnel.terminal_count_min", 2, focus)
    terminal_max = _integer(value["terminal_count_max"], "funnel.terminal_count_max", terminal_min, 3)
    continuity = _integer(value["continuity_sessions"], "funnel.continuity_sessions", 1, 100)
    liquidity = _number(value["minimum_liquidity_score"], "funnel.minimum_liquidity_score", Decimal("0"), Decimal("100"))
    drawdown = _rate(value["maximum_intraday_drawdown"], "funnel.maximum_intraday_drawdown")
    if drawdown <= Decimal("0"):
        raise TerminalStrategyConfigError("funnel.maximum_intraday_drawdown must be greater than zero")
    execution = _rate(value["minimum_execution_probability"], "funnel.minimum_execution_probability")
    evidence = _rate(value["minimum_evidence_quality"], "funnel.minimum_evidence_quality")
    correlation = _rate(value["maximum_correlation"], "funnel.maximum_correlation")
    opening_gap = _rate(value["maximum_opening_gap"], "funnel.maximum_opening_gap")
    if opening_gap <= Decimal("0"):
        raise TerminalStrategyConfigError("funnel.maximum_opening_gap must be greater than zero")
    max_quote_age = _integer(value["max_quote_age_seconds"], "funnel.max_quote_age_seconds", 1, 3600)
    overheat_threshold = _number(value["overheat_score_threshold"], "funnel.overheat_score_threshold", Decimal("0"), Decimal("100"))
    overheat_penalty = _rate(value["overheat_penalty"], "funnel.overheat_penalty")
    weights = value["scoring_weights"]
    weight_fields = {
        "relative_strength", "trend_continuity", "liquidity_quality",
        "close_intraday_strength", "evidence_quality", "execution_probability",
        "diversification_value",
    }
    if not isinstance(weights, Mapping) or set(weights) != weight_fields:
        raise TerminalStrategyConfigError("funnel.scoring_weights has missing or unknown fields")
    parsed_weights = tuple(sorted(
        (name, _rate(weights[name], f"funnel.scoring_weights.{name}"))
        for name in weight_fields
    ))
    if sum((weight for _, weight in parsed_weights), Decimal("0")) != Decimal("1"):
        raise TerminalStrategyConfigError("funnel scoring weights must sum to one")
    return FunnelConfig(
        watch, focus, terminal_min, terminal_max, continuity, liquidity, drawdown,
        execution, evidence, correlation, opening_gap, max_quote_age, overheat_threshold,
        overheat_penalty, parsed_weights,
    )


def _parse_allocation(value: Any) -> AllocationConfig:
    _exact_fields(value, _ALLOCATION_FIELDS, "allocation")
    names = tuple(_ALLOCATION_FIELDS)
    parsed = {name: _rate(value[name], f"allocation.{name}") for name in names}
    if parsed["protect_aggressive_cap"] > parsed["normal_aggressive_target"]:
        raise TerminalStrategyConfigError("protect aggressive cap cannot exceed normal target")
    if not parsed["normal_aggressive_target"] <= parsed["attack_aggressive_min"] <= parsed["attack_aggressive_max"]:
        raise TerminalStrategyConfigError("allocation aggressive targets are not ordered")
    if parsed["attack_aggressive_max"] > parsed["last_shot_aggressive_cap"]:
        raise TerminalStrategyConfigError("last-shot aggressive cap cannot be below attack cap")
    if parsed["last_shot_aggressive_cap"] > Decimal("0.60"):
        raise TerminalStrategyConfigError("aggressive cap cannot exceed three 20% terminal candidates")
    if parsed["protect_aggressive_cap"] > Decimal("0.10"):
        raise TerminalStrategyConfigError("Protect aggressive cap cannot exceed 10%")
    if not Decimal("0.20") <= parsed["normal_aggressive_target"] <= Decimal("0.40"):
        raise TerminalStrategyConfigError("Normal aggressive target must be between 20% and 40%")
    if not Decimal("0.40") <= parsed["attack_aggressive_min"] <= Decimal("0.60"):
        raise TerminalStrategyConfigError("Attack aggressive minimum must be between 40% and 60%")
    if not Decimal("0.40") <= parsed["attack_aggressive_max"] <= Decimal("0.60"):
        raise TerminalStrategyConfigError("Attack aggressive maximum must be between 40% and 60%")
    if parsed["last_shot_aggressive_cap"] < parsed["attack_aggressive_max"]:
        raise TerminalStrategyConfigError("Last-shot aggressive cap cannot be below Attack maximum")
    if parsed["protect_cash_target"] < Decimal("0.20"):
        raise TerminalStrategyConfigError("Protect cash target must be at least 20%")
    if not Decimal("0.15") <= parsed["normal_cash_target"] <= Decimal("0.20"):
        raise TerminalStrategyConfigError("Normal cash target must be between 15% and 20%")
    if not Decimal("0.10") <= parsed["attack_cash_target"] <= Decimal("0.15"):
        raise TerminalStrategyConfigError("Attack cash target must be between 10% and 15%")
    if parsed["last_shot_cash_target"] < Decimal("0.10"):
        raise TerminalStrategyConfigError("Last-shot cash target must be at least 10%")
    budget_pairs = (
        ("Protect", "protect_aggressive_cap", "protect_cash_target"),
        ("Normal", "normal_aggressive_target", "normal_cash_target"),
        ("Attack", "attack_aggressive_max", "attack_cash_target"),
        ("Last-shot", "last_shot_aggressive_cap", "last_shot_cash_target"),
    )
    for label, aggressive_name, cash_name in budget_pairs:
        if parsed[aggressive_name] + parsed[cash_name] > Decimal("1"):
            raise TerminalStrategyConfigError(f"{label} aggressive and cash budgets exceed 100%")
    if parsed["new_listing_final_day_cap"] > Decimal("0.20"):
        raise TerminalStrategyConfigError("new-listing final-day cap cannot exceed the 20% single-name limit")
    return AllocationConfig(**parsed)


def _parse_schedule(value: Any) -> TerminalScheduleConfig:
    _exact_fields(value, set(_SCHEDULE_FIELDS), "schedule")
    times = {name: _clock(value[name], f"schedule.{name}") for name in _SCHEDULE_FIELDS}
    ordered = tuple(times[name] for name in _SCHEDULE_FIELDS)
    if ordered != tuple(sorted(ordered)) or len(set(ordered)) != len(ordered):
        raise TerminalStrategyConfigError("schedule times must be strictly increasing")
    if not time(8, 30) <= times["candidate_rank"]:
        raise TerminalStrategyConfigError("candidate_rank is too early")
    if times["risk_freeze"] >= time(15, 0):
        raise TerminalStrategyConfigError("risk_freeze must precede the 15:00 market close")
    return TerminalScheduleConfig(**times)


def _exact_fields(value: Any, expected: set[str], label: str) -> None:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise TerminalStrategyConfigError(f"{label} has missing or unknown fields")


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > 512:
        raise TerminalStrategyConfigError(f"{label} must be a non-empty string")
    if any(ord(char) < 32 for char in value):
        raise TerminalStrategyConfigError(f"{label} contains control characters")
    return value.strip()


def _date(value: Any, label: str) -> date:
    if not isinstance(value, str):
        raise TerminalStrategyConfigError(f"{label} must be YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise TerminalStrategyConfigError(f"{label} must be YYYY-MM-DD") from exc


def _clock(value: Any, label: str) -> time:
    text = _text(value, label)
    try:
        parsed = time.fromisoformat(text)
    except ValueError as exc:
        raise TerminalStrategyConfigError(f"{label} must be HH:MM") from exc
    if parsed.second or parsed.microsecond or parsed.tzinfo is not None:
        raise TerminalStrategyConfigError(f"{label} must have minute precision")
    return parsed


def _decimal(value: Any, label: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise TerminalStrategyConfigError(f"{label} must be a finite decimal")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise TerminalStrategyConfigError(f"{label} must be a finite decimal") from exc
    if not parsed.is_finite():
        raise TerminalStrategyConfigError(f"{label} must be a finite decimal")
    return parsed


def _rate(value: Any, label: str) -> Decimal:
    parsed = _decimal(value, label)
    if not Decimal("0") <= parsed <= Decimal("1"):
        raise TerminalStrategyConfigError(f"{label} must be between 0 and 1")
    return parsed


def _number(value: Any, label: str, low: Decimal, high: Decimal) -> Decimal:
    parsed = _decimal(value, label)
    if not low <= parsed <= high:
        raise TerminalStrategyConfigError(f"{label} is outside supported bounds")
    return parsed


def _integer(value: Any, label: str, low: int, high: int) -> int:
    if type(value) is not int or not low <= value <= high:
        raise TerminalStrategyConfigError(f"{label} is outside supported bounds")
    return value


__all__ = [
    "AllocationConfig", "AwardLineConfig", "DEFAULT_CONFIG_PATH", "FunnelConfig",
    "TerminalCompetitionConfig", "TerminalModeConfig", "TerminalScheduleConfig",
    "TerminalStrategyConfig", "TerminalStrategyConfigError", "load_terminal_config",
    "load_terminal_strategy_config",
]
