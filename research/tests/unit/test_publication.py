from datetime import date, datetime

import pandas as pd
import pytest

from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.publication import verify_completed_daily, expected_publication_symbols


def proof(**overrides):
    row = dict(ts_code="600519.SH", trade_date="20260902", open=10, high=11, low=9, close=10,
               vol=100, amount=100, adj_factor=1, turnover_rate=1)
    args = dict(reference_date=date(2026, 9, 2), as_of=datetime.fromisoformat("2026-09-02T18:00:00+08:00"),
                observed_at=datetime.fromisoformat("2026-09-02T18:01:00+08:00"),
                expected_symbols=["600519.SH"], daily=pd.DataFrame([row]),
                adj_factor=pd.DataFrame([row]), daily_basic=pd.DataFrame([row]), source="fixture_only")
    args.update(overrides)
    return verify_completed_daily(**args)


def test_nonempty_response_is_not_sufficient_to_confirm_publication():
    with pytest.raises(PipelineError, match="missing expected"):
        proof(expected_symbols=["600519.SH", "300750.SZ"])
    assert proof()["completed_daily_date"] == "2026-09-02"


def test_postclose_requires_complete_factors_and_observation_time():
    with pytest.raises(PipelineError):
        proof(as_of=datetime.fromisoformat("2026-09-02T14:00:00+08:00"))
    with pytest.raises(PipelineError):
        proof(adj_factor=pd.DataFrame([dict(ts_code="600519.SH", trade_date="20260902", adj_factor=0)]))


@pytest.mark.parametrize("change", [
    {"source": ""}, {"source": "   "},
    {"observed_at": datetime.fromisoformat("2026-09-02T17:59:00+08:00")},
    {"observed_at": datetime.fromisoformat("2026-09-02T18:01:00")},
])
def test_publication_requires_provenance_and_explicit_observation(change):
    with pytest.raises(PipelineError):
        proof(**change)


def listed():
    return pd.DataFrame([
        dict(ts_code="600519.SH", exchange="SSE", list_status="L", list_date="20010827"),
        dict(ts_code="300750.SZ", exchange="SZSE", list_status="L", list_date="20180611"),
        dict(ts_code="000001.SZ", exchange="SZSE", list_status="L", list_date="20260903"),
        dict(ts_code="510300.SH", exchange="SSE", list_status="L", list_date="20120528"),
    ])


def test_publication_expected_members_are_independent_of_daily_presence():
    expected = expected_publication_symbols(reference_date=date(2026, 9, 2),
                                            stock_basic=listed(), suspensions=pd.DataFrame())
    assert expected == ["300750.SZ", "600519.SH"]
    # daily only contains 600519: a nonempty response must still fail coverage.
    with pytest.raises(PipelineError, match="missing expected"):
        proof(expected_symbols=expected)


def test_publication_uses_explicit_same_date_suspensions():
    suspensions = pd.DataFrame([dict(ts_code="300750.SZ", trade_date="20260902", suspend_type="S")])
    assert expected_publication_symbols(reference_date=date(2026, 9, 2), stock_basic=listed(),
                                        suspensions=suspensions) == ["600519.SH"]
    suspensions["trade_date"] = "20260901"
    with pytest.raises(PipelineError, match="date mismatch"):
        expected_publication_symbols(reference_date=date(2026, 9, 2), stock_basic=listed(), suspensions=suspensions)


def test_publication_unknown_suspensions_do_not_mean_no_suspensions():
    with pytest.raises(PipelineError, match="successful suspension"):
        expected_publication_symbols(reference_date=date(2026, 9, 2), stock_basic=listed(), suspensions=None)


def test_publication_rejects_inconsistent_identity():
    stocks = listed()
    stocks.loc[0, "exchange"] = "SZSE"
    with pytest.raises(PipelineError, match="exchange conflicts"):
        expected_publication_symbols(reference_date=date(2026, 9, 2), stock_basic=stocks, suspensions=pd.DataFrame())


@pytest.mark.parametrize("symbol", ["301689.SZ", "301699.SZ"])
def test_publication_rejects_epoch_listing_placeholder_with_structured_source(symbol):
    """Synthetic stock_basic regression: an epoch date cannot shrink the denominator."""
    stocks = pd.concat([
        listed().iloc[[0]],
        pd.DataFrame([dict(ts_code=symbol, exchange="SZSE", list_status="L", list_date="19700101")]),
    ], ignore_index=True)

    with pytest.raises(PipelineError) as raised:
        expected_publication_symbols(reference_date=date(2026, 9, 2), stock_basic=stocks,
                                     suspensions=pd.DataFrame())

    error = raised.value
    assert error.code == "DATA_COVERAGE_INSUFFICIENT"
    assert error.details["symbol"] == symbol
    assert error.details["source"] == "stock_basic"
    assert error.details["field"] == "list_date"
    assert error.details["value"] == "19700101"
