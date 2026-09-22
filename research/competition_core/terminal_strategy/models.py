"""Immutable domain contracts for terminal strategy evaluation.

These contracts contain research facts and derived read-only decisions only.
They intentionally contain no broker quantities, order payloads, or execution
methods.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from competition_core.pipeline.artifacts import content_hash


SHANGHAI = ZoneInfo("Asia/Shanghai")


class TerminalModelError(ValueError):
    """Raised when a terminal strategy domain value is unsafe."""


class TerminalMode(StrEnum):
    PROTECT = "PROTECT"
    NORMAL = "NORMAL"
    ATTACK = "ATTACK"
    LAST_SHOT = "LAST_SHOT"
    UNAVAILABLE = "UNAVAILABLE"


class TerminalBlocker(StrEnum):
    AWARD_RANK_UNCONFIRMED = "AWARD_RANK_UNCONFIRMED"
    AWARD_RULE_SOURCE_MISSING = "AWARD_RULE_SOURCE_MISSING"
    AWARD_LINE_MISSING = "AWARD_LINE_MISSING"
    RANK_NOT_FOUND = "RANK_NOT_FOUND"
    INVALID_ASSET = "INVALID_ASSET"
    NON_MONOTONIC_HISTORY = "NON_MONOTONIC_HISTORY"
    INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"
    STALE_SNAPSHOT = "STALE_SNAPSHOT"
    FUTURE_SNAPSHOT = "FUTURE_SNAPSHOT"
    SNAPSHOT_DATE_MISMATCH = "SNAPSHOT_DATE_MISMATCH"
    INCOMPLETE_ACCOUNT = "INCOMPLETE_ACCOUNT"
    INCOMPLETE_LEADERBOARD = "INCOMPLETE_LEADERBOARD"
    PENDING_ORDERS = "PENDING_ORDERS"
    CALENDAR_UNAVAILABLE = "CALENDAR_UNAVAILABLE"
    LAST_SHOT_FINAL_DAY_ONLY = "LAST_SHOT_FINAL_DAY_ONLY"
    INCOMPLETE_REEVALUATION = "INCOMPLETE_REEVALUATION"


class ForecastConfidence(StrEnum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class LeaderboardEntry:
    """One ranking row from one coherent leaderboard snapshot."""

    rank: int
    total_assets: Decimal
    as_of: datetime
    participant_ref: str | None = None

    def __post_init__(self) -> None:
        if type(self.rank) is not int or self.rank < 1:
            raise TerminalModelError("leaderboard rank must be a positive integer")
        object.__setattr__(self, "total_assets", _money(self.total_assets, "leaderboard.total_assets"))
        object.__setattr__(self, "as_of", _aware(self.as_of, "leaderboard.as_of"))
        if self.participant_ref is not None:
            object.__setattr__(self, "participant_ref", _text(self.participant_ref, "leaderboard.participant_ref"))

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "rank": self.rank,
            "total_assets": str(self.total_assets),
            "as_of": self.as_of.isoformat(),
        }
        if self.participant_ref is not None:
            result["participant_ref"] = self.participant_ref
        return result


@dataclass(frozen=True, slots=True)
class AwardLinePoint:
    """A historical total-asset observation for the configured award rank."""

    as_of: datetime
    total_assets: Decimal
    rank: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "as_of", _aware(self.as_of, "award_line.as_of"))
        object.__setattr__(self, "total_assets", _money(self.total_assets, "award_line.total_assets"))
        if type(self.rank) is not int or self.rank < 1:
            raise TerminalModelError("award line rank must be a positive integer")

    def to_dict(self) -> dict[str, Any]:
        return {"as_of": self.as_of.isoformat(), "total_assets": str(self.total_assets), "rank": self.rank}


AwardLineSample = AwardLinePoint


@dataclass(frozen=True, slots=True)
class TerminalSnapshot:
    """Coherent account/ranking facts used by the read-only state engine."""

    competition_id: str
    as_of: datetime
    trade_date: date
    remaining_trade_dates: tuple[date, ...]
    own_total_assets: Decimal
    leaderboard: tuple[LeaderboardEntry, ...]
    award_rank_cutoff: int | None = None
    award_rule_source: str | None = None
    own_rank: int | None = None
    account_snapshot_hash: str | None = None
    leaderboard_snapshot_hash: str | None = None
    account_fresh: bool = True
    leaderboard_fresh: bool = True
    complete_order_history: bool = True
    pending_order_count: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "competition_id", _text(self.competition_id, "competition_id"))
        as_of = _aware(self.as_of, "snapshot.as_of")
        object.__setattr__(self, "as_of", as_of)
        if not isinstance(self.trade_date, date) or isinstance(self.trade_date, datetime):
            raise TerminalModelError("snapshot.trade_date must be a date")
        if as_of.astimezone(SHANGHAI).date() != self.trade_date:
            raise TerminalModelError("snapshot.as_of date must match trade_date")
        dates = tuple(self.remaining_trade_dates)
        if not dates or any(not isinstance(item, date) or isinstance(item, datetime) for item in dates):
            raise TerminalModelError("remaining_trade_dates must be non-empty dates")
        if len(set(dates)) != len(dates) or tuple(sorted(dates)) != dates:
            raise TerminalModelError("remaining_trade_dates must be unique and sorted")
        if any(item < self.trade_date for item in dates):
            raise TerminalModelError("remaining_trade_dates cannot precede trade_date")
        object.__setattr__(self, "remaining_trade_dates", dates)
        object.__setattr__(self, "own_total_assets", _money(self.own_total_assets, "own_total_assets"))
        entries = tuple(self.leaderboard)
        if not entries or not all(isinstance(item, LeaderboardEntry) for item in entries):
            raise TerminalModelError("leaderboard must contain at least one typed entry")
        ranks = tuple(item.rank for item in entries)
        if len(set(ranks)) != len(ranks) or ranks != tuple(sorted(ranks)):
            raise TerminalModelError("leaderboard ranks must be unique and sorted")
        if any(item.as_of != as_of for item in entries):
            raise TerminalModelError("leaderboard entries must share snapshot.as_of")
        object.__setattr__(self, "leaderboard", entries)
        if self.award_rank_cutoff is not None and (
            type(self.award_rank_cutoff) is not int or self.award_rank_cutoff < 1
        ):
            raise TerminalModelError("award_rank_cutoff must be a positive integer or null")
        if self.award_rule_source is not None:
            object.__setattr__(self, "award_rule_source", _text(self.award_rule_source, "award_rule_source"))
        if self.own_rank is not None and (type(self.own_rank) is not int or self.own_rank < 1):
            raise TerminalModelError("own_rank must be a positive integer or null")
        for name in ("account_snapshot_hash", "leaderboard_snapshot_hash"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _text(value, name))
        for name in ("account_fresh", "leaderboard_fresh", "complete_order_history"):
            if type(getattr(self, name)) is not bool:
                raise TerminalModelError(f"{name} must be boolean")
        if type(self.pending_order_count) is not int or self.pending_order_count < 0:
            raise TerminalModelError("pending_order_count must be a non-negative integer")

    @property
    def award_line_entry(self) -> LeaderboardEntry | None:
        if self.award_rank_cutoff is None:
            return None
        return next((entry for entry in self.leaderboard if entry.rank == self.award_rank_cutoff), None)

    @property
    def is_final_trade_date(self) -> bool:
        return self.trade_date == max(self.remaining_trade_dates)

    @property
    def snapshot_hash(self) -> str:
        return content_hash(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "competition.terminal-snapshot.v1",
            "competition_id": self.competition_id,
            "as_of": self.as_of.isoformat(),
            "trade_date": self.trade_date.isoformat(),
            "remaining_trade_dates": [item.isoformat() for item in self.remaining_trade_dates],
            "award_rank_cutoff": self.award_rank_cutoff,
            "award_rule_source": self.award_rule_source,
            "own": {"rank": self.own_rank, "total_assets": str(self.own_total_assets)},
            "leaderboard": [entry.to_dict() for entry in self.leaderboard],
            "account_snapshot_hash": self.account_snapshot_hash,
            "leaderboard_snapshot_hash": self.leaderboard_snapshot_hash,
            "account_fresh": self.account_fresh,
            "leaderboard_fresh": self.leaderboard_fresh,
            "complete_order_history": self.complete_order_history,
            "pending_order_count": self.pending_order_count,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "TerminalSnapshot":
        expected = {
            "schema_version", "competition_id", "as_of", "trade_date", "remaining_trade_dates",
            "award_rank_cutoff", "award_rule_source", "own", "leaderboard", "account_snapshot_hash",
            "leaderboard_snapshot_hash", "account_fresh", "leaderboard_fresh", "complete_order_history",
            "pending_order_count",
        }
        if not isinstance(payload, Mapping) or set(payload) != expected or payload["schema_version"] != "competition.terminal-snapshot.v1":
            raise TerminalModelError("invalid terminal snapshot schema")
        own = payload["own"]
        if not isinstance(own, Mapping) or set(own) != {"rank", "total_assets"}:
            raise TerminalModelError("invalid terminal snapshot own section")
        rows = payload["leaderboard"]
        if not isinstance(rows, list):
            raise TerminalModelError("terminal snapshot leaderboard must be a list")
        try:
            as_of = datetime.fromisoformat(payload["as_of"])
            trade_date = date.fromisoformat(payload["trade_date"])
            dates = tuple(date.fromisoformat(item) for item in payload["remaining_trade_dates"])
        except (TypeError, ValueError) as exc:
            raise TerminalModelError("invalid terminal snapshot date") from exc
        entries = []
        for row in rows:
            if not isinstance(row, Mapping) or set(row) - {"rank", "total_assets", "as_of", "participant_ref"}:
                raise TerminalModelError("invalid terminal snapshot leaderboard row")
            try:
                entries.append(LeaderboardEntry(
                    rank=row["rank"], total_assets=row["total_assets"],
                    as_of=datetime.fromisoformat(row["as_of"]), participant_ref=row.get("participant_ref"),
                ))
            except (KeyError, TypeError, ValueError) as exc:
                raise TerminalModelError("invalid terminal snapshot leaderboard row") from exc
        return cls(
            competition_id=payload["competition_id"], as_of=as_of, trade_date=trade_date,
            remaining_trade_dates=dates, own_total_assets=own["total_assets"], leaderboard=tuple(entries),
            award_rank_cutoff=payload["award_rank_cutoff"], award_rule_source=payload["award_rule_source"],
            own_rank=own["rank"], account_snapshot_hash=payload["account_snapshot_hash"],
            leaderboard_snapshot_hash=payload["leaderboard_snapshot_hash"], account_fresh=payload["account_fresh"],
            leaderboard_fresh=payload["leaderboard_fresh"], complete_order_history=payload["complete_order_history"],
            pending_order_count=payload["pending_order_count"],
        )


@dataclass(frozen=True, slots=True)
class AwardLineForecast:
    """Conservative award-line estimate and the exact gap formula inputs."""

    available: bool
    rank_cutoff: int | None = None
    as_of: datetime | None = None
    current_award_line: Decimal | None = None
    predicted_award_line: Decimal | None = None
    favorable_award_line: Decimal | None = None
    base_award_line: Decimal | None = None
    adverse_award_line: Decimal | None = None
    safety_buffer: Decimal | None = None
    target_asset: Decimal | None = None
    required_return: Decimal | None = None
    confidence: ForecastConfidence = ForecastConfidence.UNAVAILABLE
    method: str = ""
    blockers: tuple[TerminalBlocker, ...] = ()
    history: tuple[AwardLinePoint, ...] = ()
    input_hash: str | None = None

    def __post_init__(self) -> None:
        if type(self.available) is not bool:
            raise TerminalModelError("forecast.available must be boolean")
        if self.rank_cutoff is not None and (type(self.rank_cutoff) is not int or self.rank_cutoff < 1):
            raise TerminalModelError("forecast.rank_cutoff must be positive or null")
        if self.as_of is not None:
            object.__setattr__(self, "as_of", _aware(self.as_of, "forecast.as_of"))
        for name in (
            "current_award_line", "predicted_award_line", "favorable_award_line",
            "base_award_line", "adverse_award_line", "target_asset",
        ):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _money(value, f"forecast.{name}"))
        if self.safety_buffer is not None:
            object.__setattr__(self, "safety_buffer", _rate(self.safety_buffer, "forecast.safety_buffer"))
        if self.required_return is not None:
            object.__setattr__(self, "required_return", _finite_decimal(self.required_return, "forecast.required_return"))
        if not isinstance(self.confidence, ForecastConfidence):
            try:
                object.__setattr__(self, "confidence", ForecastConfidence(self.confidence))
            except ValueError as exc:
                raise TerminalModelError("invalid forecast confidence") from exc
        if not isinstance(self.blockers, tuple) or not all(isinstance(item, TerminalBlocker) for item in self.blockers):
            raise TerminalModelError("forecast blockers must be typed")
        if not isinstance(self.history, tuple) or not all(isinstance(item, AwardLinePoint) for item in self.history):
            raise TerminalModelError("forecast history must be typed")
        if self.available:
            required = (
                self.current_award_line, self.predicted_award_line, self.safety_buffer,
                self.target_asset, self.required_return, self.as_of,
            )
            if any(item is None for item in required) or self.confidence is ForecastConfidence.UNAVAILABLE:
                raise TerminalModelError("available forecast is missing required values")
        elif self.confidence is not ForecastConfidence.UNAVAILABLE and not self.blockers:
            raise TerminalModelError("unavailable forecast needs blockers or unavailable confidence")

    @property
    def forecast_hash(self) -> str:
        return content_hash(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "competition.award-line-forecast.v1",
            "available": self.available,
            "rank_cutoff": self.rank_cutoff,
            "as_of": self.as_of.isoformat() if self.as_of else None,
            "current_award_line": _string_or_none(self.current_award_line),
            "predicted_award_line": _string_or_none(self.predicted_award_line),
            "favorable_award_line": _string_or_none(self.favorable_award_line),
            "base_award_line": _string_or_none(self.base_award_line),
            "adverse_award_line": _string_or_none(self.adverse_award_line),
            "safety_buffer": _string_or_none(self.safety_buffer),
            "target_asset": _string_or_none(self.target_asset),
            "required_return": _string_or_none(self.required_return),
            "confidence": self.confidence.value,
            "method": self.method,
            "blockers": [item.value for item in self.blockers],
            "history": [item.to_dict() for item in self.history],
            "input_hash": self.input_hash,
        }


@dataclass(frozen=True, slots=True)
class TerminalModeDecision:
    """Auditable mode decision; no force/override field exists by design."""

    mode: TerminalMode
    as_of: datetime
    trade_date: date
    is_final_trade_date: bool
    target_asset: Decimal | None
    required_return: Decimal | None
    lead_return: Decimal | None
    safety_buffer: Decimal | None
    previous_mode: TerminalMode | None = None
    downgrade_confirmation_count: int = 0
    reasons: tuple[str, ...] = ()
    blockers: tuple[TerminalBlocker, ...] = ()
    execution_ready: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.mode, TerminalMode):
            try:
                object.__setattr__(self, "mode", TerminalMode(self.mode))
            except ValueError as exc:
                raise TerminalModelError("invalid terminal mode") from exc
        object.__setattr__(self, "as_of", _aware(self.as_of, "decision.as_of"))
        if not isinstance(self.trade_date, date) or isinstance(self.trade_date, datetime):
            raise TerminalModelError("decision.trade_date must be a date")
        if type(self.is_final_trade_date) is not bool:
            raise TerminalModelError("decision.is_final_trade_date must be boolean")
        if self.target_asset is not None:
            object.__setattr__(self, "target_asset", _money(self.target_asset, "decision.target_asset"))
        for name in ("required_return", "lead_return"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _finite_decimal(value, f"decision.{name}"))
        if self.safety_buffer is not None:
            object.__setattr__(self, "safety_buffer", _rate(self.safety_buffer, "decision.safety_buffer"))
        if self.previous_mode is not None and not isinstance(self.previous_mode, TerminalMode):
            try:
                object.__setattr__(self, "previous_mode", TerminalMode(self.previous_mode))
            except ValueError as exc:
                raise TerminalModelError("invalid previous terminal mode") from exc
        if type(self.downgrade_confirmation_count) is not int or self.downgrade_confirmation_count < 0:
            raise TerminalModelError("downgrade confirmation count must be non-negative")
        if not isinstance(self.reasons, tuple) or not all(isinstance(item, str) and item for item in self.reasons):
            raise TerminalModelError("decision reasons must be non-empty strings")
        if not isinstance(self.blockers, tuple) or not all(isinstance(item, TerminalBlocker) for item in self.blockers):
            raise TerminalModelError("decision blockers must be typed")
        if type(self.execution_ready) is not bool:
            raise TerminalModelError("decision.execution_ready must be boolean")
        if self.mode is TerminalMode.UNAVAILABLE and self.execution_ready:
            raise TerminalModelError("unavailable decision cannot be execution-ready")

    @property
    def decision_hash(self) -> str:
        return content_hash(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "competition.terminal-mode-decision.v1",
            "mode": self.mode.value,
            "as_of": self.as_of.isoformat(),
            "trade_date": self.trade_date.isoformat(),
            "is_final_trade_date": self.is_final_trade_date,
            "target_asset": _string_or_none(self.target_asset),
            "required_return": _string_or_none(self.required_return),
            "lead_return": _string_or_none(self.lead_return),
            "safety_buffer": _string_or_none(self.safety_buffer),
            "previous_mode": self.previous_mode.value if self.previous_mode else None,
            "downgrade_confirmation_count": self.downgrade_confirmation_count,
            "reasons": list(self.reasons),
            "blockers": [item.value for item in self.blockers],
            "execution_ready": self.execution_ready,
        }


ModeDecision = TerminalModeDecision


def _aware(value: Any, field: str) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise TerminalModelError(f"{field} must be timezone-aware")
    return value


def _money(value: Any, field: str) -> Decimal:
    parsed = _finite_decimal(value, field)
    if parsed <= 0:
        raise TerminalModelError(f"{field} must be greater than zero")
    quantized = parsed.quantize(Decimal("0.01"))
    if parsed != quantized:
        raise TerminalModelError(f"{field} must use cent precision")
    return quantized


def _finite_decimal(value: Any, field: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise TerminalModelError(f"{field} must be a finite decimal")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise TerminalModelError(f"{field} must be a finite decimal") from exc
    if not parsed.is_finite():
        raise TerminalModelError(f"{field} must be a finite decimal")
    return parsed


def _rate(value: Any, field: str) -> Decimal:
    parsed = _finite_decimal(value, field)
    if not Decimal("0") <= parsed <= Decimal("1"):
        raise TerminalModelError(f"{field} must be between 0 and 1")
    return parsed


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > 512:
        raise TerminalModelError(f"{field} must be a non-empty string")
    if any(ord(char) < 32 for char in value):
        raise TerminalModelError(f"{field} contains control characters")
    return value.strip()


def _string_or_none(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


__all__ = [
    "AwardLinePoint", "AwardLineSample", "AwardLineForecast", "ForecastConfidence",
    "LeaderboardEntry", "ModeDecision", "TerminalBlocker", "TerminalMode",
    "TerminalModeDecision", "TerminalModelError", "TerminalSnapshot",
]
