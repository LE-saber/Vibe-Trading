from datetime import date, datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.sources import ResearchDataClient
from competition_core.pipeline.time_context import ResearchContext


def source(**endpoints):
    observed = datetime.fromisoformat("2026-09-02T11:00:00+08:00")
    return ResearchDataClient(SimpleNamespace(**endpoints),
                              ResearchContext(date(2026, 9, 2), "intraday", date(2026, 9, 1), observed),
                              observed_clock=lambda: observed)


@pytest.mark.parametrize("endpoint", ["daily", "daily_basic", "adj_factor", "moneyflow", "margin_detail"])
def test_no_implicit_or_current_incomplete_history(endpoint):
    wrapper = source(**{endpoint: lambda **_: pytest.fail("must reject before provider call")})
    with pytest.raises(PipelineError):
        getattr(wrapper, endpoint)()
    with pytest.raises(PipelineError):
        getattr(wrapper, endpoint)(trade_date="20260902")


def test_provider_ignoring_date_cannot_leak_future():
    wrapper = source(daily=lambda **_: pd.DataFrame({"trade_date": ["20260902"], "close": [9]}))
    with pytest.raises(PipelineError, match="outside requested"):
        wrapper.daily(trade_date="20260901")


def test_current_suspension_empty_is_valid_but_none_not_valid():
    assert source(suspend_d=lambda **_: pd.DataFrame()).suspend_d(trade_date="20260902").empty
    with pytest.raises(PipelineError):
        source(suspend_d=lambda **_: None).suspend_d(trade_date="20260902")
    with pytest.raises(PipelineError):
        source(suspend_d=lambda **_: pd.DataFrame()).suspend_d(trade_date="20260901")


def test_historical_suspension_evidence_has_separate_explicit_date_boundary():
    calls = []

    def fetch(**parameters):
        calls.append(parameters)
        return pd.DataFrame([dict(ts_code="600519.SH", trade_date="20260610", suspend_type="S", suspend_timing=None)])

    wrapper = source(suspend_d=fetch)
    assert len(wrapper.historical_suspensions("20260610")) == 1
    assert calls[0]["trade_date"] == "20260610" and calls[0]["suspend_type"] == "S"
    for future in ("20260902", "20260903"):
        with pytest.raises(PipelineError):
            wrapper.historical_suspensions(future)
    assert len(calls) == 1


def test_historical_suspension_evidence_rejects_returned_wrong_date():
    wrapper = source(suspend_d=lambda **_: pd.DataFrame({"trade_date": ["20260611"]}))
    with pytest.raises(PipelineError, match="outside requested"):
        wrapper.historical_suspensions("20260610")


def test_historical_stock_basic_not_mislabeled_pit():
    wrapper = source(stock_basic=lambda **_: pd.DataFrame({"ts_code": ["600519.SH"]}))
    assert len(wrapper.stock_basic(list_status="L")) == 1
    wrapper.observed_clock = lambda: datetime.fromisoformat("2026-09-03T11:00:00+08:00")
    with pytest.raises(PipelineError, match="saved PIT snapshot"):
        wrapper.stock_basic(list_status="L")


def test_direct_dataframes_and_raw_volume_preserved():
    frame = pd.DataFrame({"trade_date": ["20260901"], "vol": [100], "amount": [22]})
    result = source(daily=lambda **_: frame).daily(trade_date="20260901")
    pd.testing.assert_frame_equal(frame, result)


def test_future_cutoff_cannot_bypass_resolver_via_direct_constructor():
    wrapper = source(daily=lambda **_: pytest.fail("must reject before fetching"))
    wrapper.observed_clock = lambda: datetime.fromisoformat("2026-09-02T10:59:59+08:00")
    with pytest.raises(PipelineError, match="cutoff exceeds"):
        wrapper.daily(trade_date="20260901")


@pytest.mark.parametrize("phase,as_of,reference", [
    ("preopen", "2026-09-02T08:29:59+08:00", date(2026, 9, 1)),
    ("postclose", "2026-09-02T17:00:00+08:00", date(2026, 9, 2)),
])
def test_unpublished_margin_cannot_be_obtained_with_a_later_observation_clock(phase, as_of, reference):
    ctx = ResearchContext(date(2026, 9, 2), phase, reference, datetime.fromisoformat(as_of))
    wrapper = ResearchDataClient(SimpleNamespace(margin_detail=lambda **_: pytest.fail("must not fetch unpublished margin")),
                                 ctx, observed_clock=lambda: datetime.fromisoformat("2026-09-03T11:00:00+08:00"))
    with pytest.raises(PipelineError) as error:
        wrapper.margin_detail(trade_date=reference.strftime("%Y%m%d"))
    assert error.value.code == "SOURCE_NOT_PUBLISHED"


def test_margin_at_publication_boundary_retains_explicit_trade_date():
    ctx = ResearchContext(date(2026, 9, 2), "preopen", date(2026, 9, 1), datetime.fromisoformat("2026-09-02T08:30:00+08:00"))
    frame = pd.DataFrame([dict(trade_date="20260901", ts_code="300750.SZ", rzye=0)])
    wrapper = ResearchDataClient(SimpleNamespace(margin_detail=lambda **_: frame), ctx,
                                 observed_clock=lambda: datetime.fromisoformat("2026-09-02T10:00:00+08:00"))
    pd.testing.assert_frame_equal(wrapper.margin_detail(trade_date="20260901"), frame)
