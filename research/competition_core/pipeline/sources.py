"""Strict historical request boundary, leaving original loaders/Universe untouched."""

from datetime import date, datetime, time, timedelta
from typing import Any, Callable

import pandas as pd

from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.time_context import ResearchContext, SHANGHAI

_DAILY = {"daily", "daily_basic", "adj_factor", "moneyflow", "top_list", "margin_detail", "stk_limit", "index_daily"}
_ENDPOINTS = _DAILY | {"trade_cal", "stock_basic", "suspend_d"}


def _date(value: Any) -> date:
    try:
        text = str(value).replace("-", "")
        if len(text) != 8 or not text.isdigit():
            raise ValueError
        return datetime.strptime(text, "%Y%m%d").date()
    except (TypeError, ValueError) as exc:
        raise PipelineError("STALE_DATA", "invalid source date", value=str(value)[:32]) from exc


class ResearchDataClient:
    """Only explicit-date endpoints; validate returned dates, not just parameters.

    A source may ignore a requested cutoff. That is an error, never permission
    to feed later bars into ranking. Current stock_basic cannot reconstruct PIT.
    """

    def __init__(self, client: Any, context: ResearchContext, *,
                 observed_clock: Callable[[], datetime] = lambda: datetime.now(SHANGHAI)) -> None:
        self.client = client
        self.context = context
        self.observed_clock = observed_clock

    def __getattr__(self, endpoint: str):
        if endpoint not in _ENDPOINTS:
            raise AttributeError(endpoint)

        def request(**parameters):
            return self.request(endpoint, **parameters)

        return request

    def historical_suspensions(self, trade_date: str) -> pd.DataFrame:
        """Dated history evidence; never used for current Universe identity."""
        return self.request("suspend_d", _historical_suspension=True, trade_date=trade_date,
                            suspend_type="S", fields="ts_code,trade_date,suspend_type,suspend_timing")

    def request(self, endpoint: str, *, _historical_suspension: bool = False, **parameters) -> pd.DataFrame:
        if endpoint not in _ENDPOINTS:
            raise PipelineError("NOT_READY", "endpoint is outside research source contract", endpoint=endpoint)
        observed = self.observed_clock()
        if observed.utcoffset() is None:
            raise PipelineError("NOT_READY", "source observation clock must be timezone-aware")
        if self.context.as_of > observed:
            raise PipelineError("NOT_READY", "research cutoff exceeds source observation clock")
        if endpoint == "stock_basic":
            if observed.astimezone(SHANGHAI).date() != self.context.research_date:
                raise PipelineError("STALE_DATA", "historical Universe requires a saved PIT snapshot; stock_basic is current metadata")
            start = end = None
        else:
            if "trade_date" in parameters:
                start = end = _date(parameters["trade_date"])
            elif "start_date" in parameters and "end_date" in parameters:
                start, end = _date(parameters["start_date"]), _date(parameters["end_date"])
            else:
                raise PipelineError("NOT_READY", "historical source requires explicit dates", endpoint=endpoint)
            cutoff = self.context.research_date if endpoint in {"trade_cal", "suspend_d"} else self.context.market_data_reference_date
            if start > end or end > cutoff:
                raise PipelineError("STALE_DATA", "source request exceeds permitted date range", endpoint=endpoint)
            if endpoint == "margin_detail":
                # Official margin_detail publication is normally next morning
                # around08:30. A later fetch cannot make it known before then.
                earliest = datetime.combine(end + timedelta(days=1), time(8, 30), SHANGHAI)
                if self.context.as_of < earliest:
                    raise PipelineError("SOURCE_NOT_PUBLISHED", "margin history was not yet published at research cutoff",
                                        endpoint=endpoint, not_before=earliest.isoformat())
            if endpoint == "suspend_d":
                if _historical_suspension:
                    if start != end or end > self.context.market_data_reference_date:
                        raise PipelineError("STALE_DATA", "historical suspension exceeds daily cutoff")
                elif start != self.context.research_date or end != start:
                    raise PipelineError("STALE_DATA", "suspension identity must be for research_date")
        frame = getattr(self.client, endpoint)(**parameters)
        if not isinstance(frame, pd.DataFrame):
            raise PipelineError("SOURCE_UNAVAILABLE", "source did not return a DataFrame", endpoint=endpoint)
        if not frame.empty and start is not None:
            column = "cal_date" if endpoint == "trade_cal" else "trade_date"
            if column not in frame:
                raise PipelineError("STALE_DATA", "source dates are missing", endpoint=endpoint)
            dates = frame[column].map(_date)
            if ((dates < start) | (dates > end)).any():
                raise PipelineError("STALE_DATA", "source returned rows outside requested dates", endpoint=endpoint)
        return frame
