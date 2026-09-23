"""Validated daily workflow schedule and fail-closed decision state transitions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
from enum import StrEnum
import math
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config/daily-workflow.yaml"


class DailyWorkflowConfigError(ValueError):
    """Raised when the daily workflow policy is incomplete or unsafe."""


class DecisionState(StrEnum):
    DATA_READY = "DATA_READY"
    RESEARCH_READY = "RESEARCH_READY"
    PLAN_DRAFT = "PLAN_DRAFT"
    ACCOUNT_BOUND = "ACCOUNT_BOUND"
    QUOTE_BOUND = "QUOTE_BOUND"
    APPROVED = "APPROVED"
    SENDING = "SENDING"
    RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"
    RECONCILED = "RECONCILED"
    CLOSED = "CLOSED"


_TRANSITIONS = {
    DecisionState.DATA_READY: {DecisionState.RESEARCH_READY},
    DecisionState.RESEARCH_READY: {DecisionState.PLAN_DRAFT},
    DecisionState.PLAN_DRAFT: {DecisionState.ACCOUNT_BOUND},
    DecisionState.ACCOUNT_BOUND: {DecisionState.QUOTE_BOUND},
    DecisionState.QUOTE_BOUND: {DecisionState.APPROVED},
    DecisionState.APPROVED: {DecisionState.SENDING, DecisionState.CLOSED},
    DecisionState.SENDING: {DecisionState.RECONCILIATION_REQUIRED, DecisionState.RECONCILED},
    DecisionState.RECONCILIATION_REQUIRED: {DecisionState.RECONCILED},
    DecisionState.RECONCILED: {DecisionState.ACCOUNT_BOUND, DecisionState.CLOSED},
    DecisionState.CLOSED: set(),
}


@dataclass(frozen=True, slots=True)
class DailyWorkflowConfig:
    version: str
    timezone: str
    preflight_start: time
    preopen_start: time
    preopen_end: time
    intraday_checkpoints: tuple[time, ...]
    execution_target: time
    ordinary_order_cutoff: time
    postclose_start: time
    leaderboard_checkpoints: tuple[str, ...]
    max_positions: int
    max_single_weight: float
    cash_ratio_min: float
    cash_ratio_max: float
    max_sector_weight: float
    max_pair_correlation: float
    max_quote_age_seconds: int
    opening_gap_max: float
    current_change_min: float
    buy_lot: int
    execution_mode: str
    automatic_orders: bool
    unknown_policy: str
    require_complete_order_history: bool
    require_zero_open_orders_before_replan: bool

    @property
    def zone(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)

    def phase_at(self, moment: datetime) -> str:
        if not isinstance(moment, datetime) or moment.utcoffset() is None:
            raise DailyWorkflowConfigError("workflow observation time must be timezone-aware")
        local = moment.astimezone(self.zone).time().replace(tzinfo=None)
        if local < self.preflight_start:
            return "BEFORE_PREFLIGHT"
        if local < self.preopen_start:
            return "PREFLIGHT"
        if local <= self.preopen_end:
            return "PREOPEN"
        if local < time(11, 30):
            return "INTRADAY_AM"
        if local < time(13, 0):
            return "MIDDAY_BREAK"
        if local <= self.ordinary_order_cutoff:
            return "INTRADAY_PM"
        if local < self.postclose_start:
            return "ORDER_CUTOFF"
        return "POSTCLOSE"

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "timezone": self.timezone,
            "schedule": {
                "preflight_start": self.preflight_start.strftime("%H:%M"),
                "preopen_start": self.preopen_start.strftime("%H:%M"),
                "preopen_end": self.preopen_end.strftime("%H:%M"),
                "intraday_checkpoints": [item.strftime("%H:%M") for item in self.intraday_checkpoints],
                "execution_target": self.execution_target.strftime("%H:%M"),
                "ordinary_order_cutoff": self.ordinary_order_cutoff.strftime("%H:%M"),
                "postclose_start": self.postclose_start.strftime("%H:%M"),
                "leaderboard_checkpoints": list(self.leaderboard_checkpoints),
            },
            "risk": {
                "max_positions": self.max_positions,
                "max_single_weight": self.max_single_weight,
                "cash_ratio_min": self.cash_ratio_min,
                "cash_ratio_max": self.cash_ratio_max,
                "max_sector_weight": self.max_sector_weight,
                "max_pair_correlation": self.max_pair_correlation,
                "max_quote_age_seconds": self.max_quote_age_seconds,
                "opening_gap_max": self.opening_gap_max,
                "current_change_min": self.current_change_min,
                "buy_lot": self.buy_lot,
            },
            "execution": {
                "mode": self.execution_mode,
                "automatic_orders": self.automatic_orders,
                "unknown_policy": self.unknown_policy,
                "require_complete_order_history": self.require_complete_order_history,
                "require_zero_open_orders_before_replan": self.require_zero_open_orders_before_replan,
            },
        }


def validate_transition(current: DecisionState | str, target: DecisionState | str) -> DecisionState:
    try:
        before = DecisionState(current)
        after = DecisionState(target)
    except ValueError as exc:
        raise DailyWorkflowConfigError("unknown decision state") from exc
    if after not in _TRANSITIONS[before]:
        raise DailyWorkflowConfigError(f"unsafe decision transition: {before.value} -> {after.value}")
    return after


def load_daily_workflow_config(path: str | Path = DEFAULT_CONFIG_PATH) -> DailyWorkflowConfig:
    try:
        with Path(path).open(encoding="utf-8") as handle:
            payload = yaml.safe_load(handle)
    except (OSError, yaml.YAMLError) as exc:
        raise DailyWorkflowConfigError("cannot read daily workflow config") from exc
    if not isinstance(payload, dict) or set(payload) != {"version", "timezone", "schedule", "risk", "execution"}:
        raise DailyWorkflowConfigError("daily workflow config has missing or unknown sections")
    schedule = _mapping(payload["schedule"], "schedule", {
        "preflight_start", "preopen_start", "preopen_end", "intraday_checkpoints",
        "execution_target", "ordinary_order_cutoff", "postclose_start", "leaderboard_checkpoints",
    })
    risk = _mapping(payload["risk"], "risk", {
        "max_positions", "max_single_weight", "cash_ratio_min", "cash_ratio_max",
        "max_sector_weight", "max_pair_correlation", "max_quote_age_seconds",
        "opening_gap_max", "current_change_min", "buy_lot",
    })
    execution = _mapping(payload["execution"], "execution", {
        "mode", "automatic_orders", "unknown_policy", "require_complete_order_history",
        "require_zero_open_orders_before_replan",
    })
    version = _text(payload["version"], "version")
    timezone = _text(payload["timezone"], "timezone")
    try:
        ZoneInfo(timezone)
    except ZoneInfoNotFoundError as exc:
        raise DailyWorkflowConfigError("daily workflow timezone is invalid") from exc
    preflight_start = _clock(schedule["preflight_start"], "preflight_start")
    preopen_start = _clock(schedule["preopen_start"], "preopen_start")
    preopen_end = _clock(schedule["preopen_end"], "preopen_end")
    checkpoints_raw = schedule["intraday_checkpoints"]
    if not isinstance(checkpoints_raw, list) or not checkpoints_raw:
        raise DailyWorkflowConfigError("intraday_checkpoints must be a non-empty list")
    checkpoints = tuple(_clock(value, "intraday checkpoint") for value in checkpoints_raw)
    if tuple(sorted(set(checkpoints))) != checkpoints:
        raise DailyWorkflowConfigError("intraday checkpoints must be unique and sorted")
    execution_target = _clock(schedule["execution_target"], "execution_target")
    cutoff = _clock(schedule["ordinary_order_cutoff"], "ordinary_order_cutoff")
    postclose = _clock(schedule["postclose_start"], "postclose_start")
    if not time(8, 30) <= preflight_start < preopen_start:
        raise DailyWorkflowConfigError("preflight must start between 08:30 and preopen")
    if not time(9, 0) <= preopen_start < preopen_end < time(9, 30):
        raise DailyWorkflowConfigError("preopen window must be within 09:00-09:30")
    if any(not (time(9, 30) <= item < time(11, 30) or time(13, 0) <= item <= cutoff)
           for item in checkpoints):
        raise DailyWorkflowConfigError("intraday checkpoint is outside the supported session")
    if not checkpoints[-1] < execution_target < cutoff:
        raise DailyWorkflowConfigError("execution target must follow checkpoints and precede order cutoff")
    if not time(14, 30) <= cutoff < time(15, 0) <= postclose:
        raise DailyWorkflowConfigError("order cutoff or postclose start is unsafe")
    leaderboard = schedule["leaderboard_checkpoints"]
    expected_leaderboard = ("preopen", "midday", "tail", "postclose")
    if not isinstance(leaderboard, list) or tuple(leaderboard) != expected_leaderboard:
        raise DailyWorkflowConfigError("leaderboard checkpoints must cover preopen, midday, tail and postclose")
    max_positions = _integer(risk["max_positions"], "max_positions", 1, 8)
    max_single = _number(risk["max_single_weight"], "max_single_weight", 0.01, 0.20)
    cash_min = _number(risk["cash_ratio_min"], "cash_ratio_min", 0, 0.90)
    cash_max = _number(risk["cash_ratio_max"], "cash_ratio_max", cash_min, 0.90)
    if max_positions * max_single + cash_max < 1 - 1e-12:
        raise DailyWorkflowConfigError("position, single-name and cash limits cannot form a full portfolio")
    mode = _text(execution["mode"], "execution.mode")
    automatic = execution["automatic_orders"]
    if mode != "MANUAL_CONFIRM" or automatic is not False:
        raise DailyWorkflowConfigError("daily workflow execution must remain manual-confirm only")
    unknown_policy = _text(execution["unknown_policy"], "execution.unknown_policy")
    if unknown_policy != "AUTHORITATIVE_READBACK_NEVER_RETRY":
        raise DailyWorkflowConfigError("unknown order policy must prohibit retries")
    bool_fields = ("require_complete_order_history", "require_zero_open_orders_before_replan")
    if any(type(execution[name]) is not bool for name in bool_fields):
        raise DailyWorkflowConfigError("execution safety switches must be booleans")
    return DailyWorkflowConfig(
        version=version,
        timezone=timezone,
        preflight_start=preflight_start,
        preopen_start=preopen_start,
        preopen_end=preopen_end,
        intraday_checkpoints=checkpoints,
        execution_target=execution_target,
        ordinary_order_cutoff=cutoff,
        postclose_start=postclose,
        leaderboard_checkpoints=tuple(leaderboard),
        max_positions=max_positions,
        max_single_weight=max_single,
        cash_ratio_min=cash_min,
        cash_ratio_max=cash_max,
        max_sector_weight=_number(risk["max_sector_weight"], "max_sector_weight", max_single, 1),
        max_pair_correlation=_number(risk["max_pair_correlation"], "max_pair_correlation", 0, 1),
        max_quote_age_seconds=_integer(risk["max_quote_age_seconds"], "max_quote_age_seconds", 1, 120),
        opening_gap_max=_number(risk["opening_gap_max"], "opening_gap_max", 0, 0.20),
        current_change_min=_number(risk["current_change_min"], "current_change_min", -0.20, 0),
        buy_lot=_integer(risk["buy_lot"], "buy_lot", 100, 100),
        execution_mode=mode,
        automatic_orders=automatic,
        unknown_policy=unknown_policy,
        require_complete_order_history=execution["require_complete_order_history"],
        require_zero_open_orders_before_replan=execution["require_zero_open_orders_before_replan"],
    )


def _mapping(value: Any, label: str, fields: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise DailyWorkflowConfigError(f"{label} has missing or unknown fields")
    return value


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DailyWorkflowConfigError(f"{label} must be a non-empty string")
    return value.strip()


def _clock(value: Any, label: str) -> time:
    text = _text(value, label)
    try:
        parsed = time.fromisoformat(text)
    except ValueError as exc:
        raise DailyWorkflowConfigError(f"{label} must be HH:MM") from exc
    if parsed.second or parsed.microsecond:
        raise DailyWorkflowConfigError(f"{label} must have minute precision")
    return parsed


def _number(value: Any, label: str, low: float, high: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise DailyWorkflowConfigError(f"{label} must be finite numeric")
    result = float(value)
    if not low <= result <= high:
        raise DailyWorkflowConfigError(f"{label} is outside supported bounds")
    return result


def _integer(value: Any, label: str, low: int, high: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise DailyWorkflowConfigError(f"{label} is outside supported bounds")
    return value


__all__ = [
    "DailyWorkflowConfig",
    "DailyWorkflowConfigError",
    "DecisionState",
    "load_daily_workflow_config",
    "validate_transition",
]
