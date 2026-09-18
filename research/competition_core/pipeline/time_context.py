"""Explicit research clocks: wall time never silently selects historical data."""

from dataclasses import asdict, dataclass
from datetime import date, datetime, time
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import pandas as pd

from competition_core.pipeline.errors import PipelineError
from competition_core.universe.service import _market_data_reference_date

SHANGHAI = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True, slots=True)
class ResearchContext:
    research_date: date
    phase: str
    market_data_reference_date: date
    as_of: datetime
    timezone: str = "Asia/Shanghai"

    def __post_init__(self) -> None:
        if self.phase not in {"preopen", "intraday", "postclose"}:
            raise PipelineError("NOT_READY", "unknown research phase")
        if self.timezone != "Asia/Shanghai" or self.as_of.utcoffset() is None:
            raise PipelineError("NOT_READY", "as_of must be timezone-aware; timezone is Asia/Shanghai")
        local = self.as_of.astimezone(SHANGHAI)
        if local.date() != self.research_date:
            raise PipelineError("NOT_READY", "as_of date must equal research_date")
        if self.phase == "postclose":
            if local.time() < time(15) or self.market_data_reference_date != self.research_date:
                raise PipelineError("NOT_READY", "postclose requires completed current-session daily data")
        elif self.market_data_reference_date >= self.research_date:
            raise PipelineError("NOT_READY", "preopen/intraday history must precede research_date")

    @classmethod
    def resolve(
        cls, *, research_date: date, phase: str, as_of: datetime,
        calendar: pd.DataFrame, observed_at: datetime,
        completed_daily_date: date | None = None,
    ) -> "ResearchContext":
        """Resolve calendar, checking the caller's cutoff against an explicit clock.

        completed_daily_date comes from a verified dataset manifest, not from a
        nonempty daily response (which may be a partial publication).
        """
        if as_of.utcoffset() is None or observed_at.utcoffset() is None or as_of > observed_at:
            raise PipelineError("NOT_READY", "research cutoff is naive or in the future")
        reference = _market_data_reference_date(
            calendar, research_date, "postclose" if phase == "postclose" else "preopen"
        )
        if phase == "postclose" and completed_daily_date != research_date:
            raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "current daily publication is not confirmed complete")
        return cls(research_date, phase, reference, as_of)

    @property
    def universe_phase(self) -> str:
        return "postclose" if self.phase == "postclose" else "preopen"

    def validate_daily_dates(self, values: Iterable[Any]) -> None:
        # Numeric YYYYMMDD from source frames must not become nanoseconds in 1970.
        dates = pd.to_datetime(pd.Series(list(values), dtype="object").astype(str), errors="coerce", format="mixed")
        if dates.isna().any() or (dates.dt.date > self.market_data_reference_date).any():
            raise PipelineError("STALE_DATA", "daily dates are invalid or exceed historical cutoff")

    def validate_snapshot(self, snapshot: dict[str, Any]) -> None:
        if snapshot.get("stale") or snapshot.get("fallback_used"):
            raise PipelineError("STALE_DATA", "a degraded Universe cannot seed a production run")
        if (
            snapshot.get("as_of") != self.research_date.isoformat()
            or snapshot.get("snapshot_phase") != self.universe_phase
            or snapshot.get("market_data_reference_date") != self.market_data_reference_date.isoformat()
            or not snapshot.get("snapshot_id")
        ):
            raise PipelineError("STALE_DATA", "Universe snapshot does not match explicit research context")

    def to_dict(self) -> dict[str, str]:
        return {key: value.isoformat() if isinstance(value, (date, datetime)) else value
                for key, value in asdict(self).items()}
