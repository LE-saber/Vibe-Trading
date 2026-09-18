from datetime import date, datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from competition_core.pipeline.artifacts import atomic_json, read_json
from competition_core.pipeline.config import PipelineConfig, load_pipeline_config
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.publication import verify_completed_daily
from competition_core.pipeline.sources import ResearchDataClient
from competition_core.pipeline.time_context import ResearchContext


SHANGHAI = ZoneInfo("Asia/Shanghai")


def _context() -> ResearchContext:
    return ResearchContext(
        research_date=date(2026, 9, 2),
        phase="intraday",
        market_data_reference_date=date(2026, 9, 1),
        as_of=datetime(2026, 9, 2, 10, tzinfo=SHANGHAI),
    )


def _source(**endpoints) -> ResearchDataClient:
    observed = datetime(2026, 9, 2, 11, tzinfo=SHANGHAI)
    return ResearchDataClient(
        SimpleNamespace(**endpoints),
        _context(),
        observed_clock=lambda: observed,
    )


def _publication_args(**overrides):
    reference_date = overrides.pop("reference_date", date(2026, 9, 2))
    row = {
        "ts_code": "600519.SH",
        "trade_date": reference_date.strftime("%Y%m%d"),
        "open": 10,
        "high": 11,
        "low": 9,
        "close": 10,
        "vol": 100,
        "amount": 100,
        "adj_factor": 1,
        "turnover_rate": 1,
    }
    args = {
        "reference_date": reference_date,
        "as_of": datetime(2026, 9, 2, 18, tzinfo=SHANGHAI),
        "observed_at": datetime(2026, 9, 2, 18, 1, tzinfo=SHANGHAI),
        "expected_symbols": ["600519.SH"],
        "daily": pd.DataFrame([row]),
        "adj_factor": pd.DataFrame([row]),
        "daily_basic": pd.DataFrame([row]),
        "source": "fixture",
    }
    args.update(overrides)
    return args


@pytest.mark.parametrize(
    "as_of, observed_at",
    [
        (datetime(2026, 9, 2, 10), datetime(2026, 9, 2, 10, 1)),
        (datetime(2026, 9, 2, 10, 2, tzinfo=SHANGHAI), datetime(2026, 9, 2, 10, 1, tzinfo=SHANGHAI)),
    ],
)
def test_context_rejects_naive_or_future_as_of(as_of, observed_at) -> None:
    calendar = pd.DataFrame(
        {"cal_date": ["20260901", "20260902"], "is_open": [1, 1]}
    )

    with pytest.raises(PipelineError):
        ResearchContext.resolve(
            research_date=date(2026, 9, 2),
            phase="intraday",
            as_of=as_of,
            observed_at=observed_at,
            calendar=calendar,
        )


@pytest.mark.parametrize(
    "change",
    [
        {"snapshot_phase": "postclose"},
        {"market_data_reference_date": "2026-09-02"},
        {"as_of": "2026-09-01"},
    ],
)
def test_snapshot_phase_and_reference_mismatch_fail_closed(change) -> None:
    snapshot = {
        "as_of": "2026-09-02",
        "snapshot_phase": "preopen",
        "market_data_reference_date": "2026-09-01",
        "snapshot_id": "snapshot-1",
    }

    with pytest.raises(PipelineError):
        _context().validate_snapshot({**snapshot, **change})


@pytest.mark.parametrize("endpoint", ["daily", "daily_basic", "adj_factor"])
def test_empty_historical_page_is_allowed_for_noninitial_offset(endpoint) -> None:
    calls = []

    def fetch(**parameters):
        calls.append(parameters)
        return pd.DataFrame()

    wrapper = _source(**{endpoint: fetch})
    result = getattr(wrapper, endpoint)(trade_date="20260901", offset=100)

    assert result.empty
    assert calls == [{"trade_date": "20260901", "offset": 100}]


def test_empty_stock_basic_page_is_allowed_for_noninitial_offset() -> None:
    calls = []

    def fetch(**parameters):
        calls.append(parameters)
        return pd.DataFrame()

    wrapper = _source(stock_basic=fetch)
    result = wrapper.stock_basic(list_status="L", offset=100)

    assert result.empty
    assert calls == [{"list_status": "L", "offset": 100}]


def test_postclose_proof_rejects_future_as_of() -> None:
    with pytest.raises(PipelineError):
        verify_completed_daily(
            **_publication_args(
                as_of=datetime(2026, 9, 2, 18, 1, tzinfo=SHANGHAI),
                observed_at=datetime(2026, 9, 2, 18, tzinfo=SHANGHAI),
            )
        )


def test_postclose_proof_requires_nonempty_source_provenance() -> None:
    with pytest.raises(PipelineError):
        verify_completed_daily(**_publication_args(source=""))


def test_immutable_snapshot_content_cannot_be_overwritten(tmp_path) -> None:
    path = tmp_path / "snapshot.json"
    atomic_json(path, {"version": 1}, immutable=True)

    with pytest.raises(PipelineError, match="content conflict"):
        atomic_json(path, {"version": 2}, immutable=True)

    assert read_json(path) == {"version": 1}


def test_pipeline_config_hash_isolated_from_external_mutation() -> None:
    payload = load_pipeline_config().to_dict()
    config = PipelineConfig.from_dict(payload)
    original_hash = config.config_hash

    payload["risk"]["cash_buffer"] = 0.41
    returned = config.to_dict()
    returned["risk"]["cash_buffer"] = 0.42

    assert config.config_hash == original_hash
    assert config.section("risk")["cash_buffer"] == 0.2
