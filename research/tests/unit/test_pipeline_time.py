from datetime import date, datetime

import pandas as pd
import pytest

from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.time_context import ResearchContext


def context(phase="intraday", **overrides):
    params = dict(research_date=date(2026, 9, 2), phase=phase,
                  as_of=datetime.fromisoformat("2026-09-02T10:00:00+08:00"),
                  observed_at=datetime.fromisoformat("2026-09-02T10:01:00+08:00"),
                  calendar=pd.DataFrame({"cal_date": ["20260831", "20260901", "20260902"],
                                         "is_open": [1, 1, 1]}))
    params.update(overrides)
    return ResearchContext.resolve(**params)


@pytest.mark.parametrize("phase", ["preopen", "intraday"])
def test_prior_completed_day_without_current_daily(phase):
    ctx = context(phase)
    assert ctx.market_data_reference_date == date(2026, 9, 1)
    assert ctx.universe_phase == "preopen"
    ctx.validate_daily_dates(["20260901"])
    with pytest.raises(PipelineError):
        ctx.validate_daily_dates(["20260902"])


def test_postclose_needs_time_and_verified_complete_manifest():
    with pytest.raises(PipelineError):
        context("postclose", completed_daily_date=date(2026, 9, 2))
    timestamps = dict(as_of=datetime.fromisoformat("2026-09-02T18:00:00+08:00"),
                      observed_at=datetime.fromisoformat("2026-09-02T18:01:00+08:00"))
    with pytest.raises(PipelineError, match="confirmed complete"):
        context("postclose", **timestamps)
    assert context("postclose", completed_daily_date=date(2026, 9, 2), **timestamps).market_data_reference_date == date(2026, 9, 2)


@pytest.mark.parametrize("cutoff", ["2026-09-02T10:00:00", "2026-09-02T10:02:00+08:00", "2026-09-01T10:00:00+08:00"])
def test_invalid_or_future_clocks_rejected(cutoff):
    with pytest.raises(PipelineError):
        context(as_of=datetime.fromisoformat(cutoff))


def test_snapshot_must_be_matching_not_stale_or_silently_fallback():
    ctx = context()
    snapshot = dict(as_of="2026-09-02", snapshot_phase="preopen",
                    market_data_reference_date="2026-09-01", snapshot_id="example")
    ctx.validate_snapshot(snapshot)
    for change in ({"stale": True}, {"fallback_used": True}, {"as_of": "2026-09-01"},
                   {"market_data_reference_date": "2026-09-02"}, {"snapshot_id": ""}):
        with pytest.raises(PipelineError):
            ctx.validate_snapshot({**snapshot, **change})


def test_previous_trade_day_is_calendar_driven_across_weekend_and_holiday():
    ctx = context(research_date=date(2026, 8, 31),
                  as_of=datetime.fromisoformat("2026-08-31T09:00:00+08:00"),
                  calendar=pd.DataFrame({"cal_date": ["20260828", "20260829", "20260830", "20260831"],
                                         "is_open": [1, 0, 0, 1]}))
    assert ctx.market_data_reference_date == date(2026, 8, 28)


def test_closed_day_and_missing_previous_calendar_rejected():
    with pytest.raises(ValueError, match="not an open"):
        context(calendar=pd.DataFrame({"cal_date": ["20260901", "20260902"], "is_open": [1, 0]}))
    with pytest.raises(RuntimeError, match="no completed"):
        context(calendar=pd.DataFrame({"cal_date": ["20260902"], "is_open": [1]}))


@pytest.mark.parametrize("phase", ["preopen", "intraday", "postclose"])
def test_non_trading_day_has_stable_safe_classification_for_every_phase(phase):
    as_of = datetime.fromisoformat("2026-09-05T18:00:00+08:00")
    with pytest.raises(PipelineError) as raised:
        context(phase, research_date=date(2026, 9, 5), as_of=as_of,
                observed_at=datetime.fromisoformat("2026-09-05T18:01:00+08:00"),
                calendar=pd.DataFrame({"cal_date": ["20260904", "20260905"], "is_open": [1, 0]}))
    assert raised.value.code == "NON_TRADING_DAY"
    assert str(raised.value) == "research date is not an open SSE trading day"


def test_utc_instant_converted_to_exchange_day_without_guessing():
    ctx = context(as_of=datetime.fromisoformat("2026-09-02T02:00:00+00:00"))
    assert ctx.research_date.isoformat() == "2026-09-02"
    assert ctx.to_dict()["as_of"] == "2026-09-02T02:00:00+00:00"


@pytest.mark.parametrize("value", ["not-a-date", None, "20269999"])
def test_invalid_daily_dates_fail_closed(value):
    with pytest.raises(PipelineError):
        context().validate_daily_dates([value])


def test_numeric_source_dates_are_not_interpreted_as_unix_nanoseconds():
    context().validate_daily_dates([20260901])
    with pytest.raises(PipelineError):
        context().validate_daily_dates([20260902])
