"""Synthetic reference service responses; not live Tushare acceptance."""

from datetime import timedelta
import copy

import pandas as pd
import pytest

from competition_core.pipeline.artifacts import atomic_json, read_json
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.execution_status import collect_execution_status, load_execution_status, FIELDS
from competition_core.pipeline.sources import ResearchDataClient
from tests.pipeline_fixtures import context

SYMBOLS = ["600519.SH", "300750.SZ", "688001.SH"]
RULES = {"MAIN": 5, "CHINEXT": 5, "STAR": 5}


class Source:
    def __init__(self):
        self.calls = []
        self.changes = {}
        self.fail = set()
        self.ignored_offset = False

    def __getattr__(self, endpoint):
        def request(**params):
            self.calls.append((endpoint, params))
            if endpoint in self.fail:
                raise RuntimeError("token=never-copy-this-secret")
            if endpoint == "trade_cal":
                dates = pd.date_range(params["start_date"], params["end_date"])
                rows = [{"exchange": params["exchange"], "cal_date": day.strftime("%Y%m%d"),
                         "is_open": int(day.dayofweek < 5), "pretrade_date": "20260901"} for day in dates]
            elif endpoint == "stock_basic":
                rows = [{"ts_code": symbol, "symbol": symbol[:6], "name": "合成样本", "market": market,
                         "exchange": exchange, "list_status": "L", "list_date": "20150101", "delist_date": None}
                        for symbol, market, exchange in zip(SYMBOLS, ["主板", "创业板", "科创板"], ["SSE", "SZSE", "SSE"])]
                if params["list_status"] != "L":
                    rows = []
            elif endpoint == "suspend_d":
                rows = []
            elif endpoint == "stk_limit":
                rows = [{"ts_code": s, "trade_date": "20260902", "pre_close": 10., "up_limit": 11., "down_limit": 9.} for s in SYMBOLS]
            else:
                pytest.fail("execution source accessed an unapproved endpoint")
            rows = copy.deepcopy(self.changes.get(endpoint, lambda r: r)(rows))
            if params.get("offset") and not self.ignored_offset:
                rows = []
            return pd.DataFrame(rows, columns=FIELDS[endpoint].split(","))
        return request


def collect(tmp_path, source=None, **overrides):
    params = dict(client=source or Source(), symbols=SYMBOLS, context=context(), special_sessions_by_board=RULES,
                  directory=tmp_path / "observation", source_name="explicit.synthetic", clock=lambda: context().as_of)
    return collect_execution_status(**(params | overrides))


def test_whole_market_endpoints_join_canonical_identities_and_keep_proof(tmp_path):
    source = Source()
    report = collect(tmp_path, source)
    assert report["status"] == "READY" and report["usable_symbols"] == SYMBOLS
    assert not report["execution_ready"] and not report["production_accepted"]
    assert report["summary"]["complete_reference_count"] == 3
    assert {endpoint for endpoint, _ in source.calls} == {"trade_cal", "stock_basic", "stk_limit", "suspend_d"}
    assert all(p["list_status"] == "L" for endpoint, p in source.calls if endpoint == "stock_basic")
    assert {p["exchange"] for e, p in source.calls if e == "trade_cal"} == {"SSE", "SZSE"}
    assert load_execution_status(tmp_path / "observation", context=context(), symbols=SYMBOLS,
                                 observed_at=context().as_of, max_age_seconds=60) == report
    for row in report["metadata"].values():
        assert row["up_limit"] == "11.00" and row["down_limit"] == "9.00"
        assert row["suspended"] is False and row["new_listing_special_period"] is False
        assert row["status_scope"] == "daily_reference_not_live_halt_feed"


@pytest.mark.parametrize("endpoint", ["trade_cal", "stock_basic", "suspend_d", "stk_limit"])
def test_failed_source_remains_unknown_without_zero_or_fallback(tmp_path, endpoint):
    source = Source()
    source.fail.add(endpoint)
    report = collect(tmp_path, source)
    assert not report["usable_symbols"] and report["status"] == "PARTIAL"
    assert all(row["missing_fields"] for row in report["metadata"].values())
    assert all("never-copy-this-secret" not in path.read_text(encoding="utf-8") for path in (tmp_path / "observation").rglob("*.json"))


@pytest.mark.parametrize("endpoint", ["stk_limit", "suspend_d"])
def test_ignored_execution_date_is_not_accepted(tmp_path, endpoint):
    source = Source()
    source.changes[endpoint] = lambda rows: [{"ts_code": SYMBOLS[0], "trade_date": "20260901",
        "pre_close": 10, "up_limit": 11, "down_limit": 9, "suspend_type": "S", "suspend_timing": None}]
    assert collect(tmp_path, source)["usable_symbols"] == []


def test_pagination_must_finish_not_repeat_an_ignored_offset(tmp_path):
    source = Source()
    source.ignored_offset = True
    report = collect(tmp_path, source)
    assert report["status"] == "PARTIAL" and not report["usable_symbols"]
    assert len(source.calls) < 20


@pytest.mark.parametrize("timing,expected", [(None, True), ("14:00-15:00", True), ("09:30-10:00", False),
    ("14:49:00-14:51:00", True), ("unknown", None), ("15:00-14:00", None)])
def test_full_day_intraday_and_unknown_suspension_states(tmp_path, timing, expected):
    source = Source()
    source.changes["suspend_d"] = lambda rows: [{"ts_code": SYMBOLS[0], "trade_date": "20260902", "suspend_type": "S", "suspend_timing": timing}]
    report = collect(tmp_path, source)
    assert report["metadata"][SYMBOLS[0]]["suspended"] is expected
    assert set(report["usable_symbols"]) >= set(SYMBOLS[1:])


def test_conflicting_daily_resume_has_no_invented_ordering(tmp_path):
    source = Source()
    source.changes["suspend_d"] = lambda rows: [{"ts_code": SYMBOLS[0], "trade_date": "20260902", "suspend_type": kind, "suspend_timing": None} for kind in ("S", "R")]
    assert collect(tmp_path, source)["metadata"][SYMBOLS[0]]["suspended"] is None


def test_calendar_counts_special_listing_period_and_preserves_exact_limits(tmp_path):
    source = Source()
    def recent(rows):
        rows[0]["list_date"] = "20260901"
        return rows
    source.changes["stock_basic"] = recent
    report = collect(tmp_path, source)
    assert report["metadata"][SYMBOLS[0]]["new_listing_special_period"] is True
    assert report["metadata"][SYMBOLS[0]]["up_limit"] == "11.00"
    assert SYMBOLS[0] not in report["usable_symbols"]


def test_epoch_listing_placeholder_is_unknown_without_affecting_other_synthetic_symbols(tmp_path):
    """Synthetic stock_basic regression: a vendor epoch is not a mature listing."""
    source = Source()
    def epoch(rows):
        rows[0]["list_date"] = "19700101"
        return rows
    source.changes["stock_basic"] = epoch

    report = collect(tmp_path, source)
    placeholder = report["metadata"][SYMBOLS[0]]
    assert placeholder["tradable"] is False
    assert placeholder["new_listing_special_period"] is None
    assert placeholder["list_date"] is None
    assert placeholder["source"] == "explicit.synthetic.execution_reference"
    assert report["metadata"][SYMBOLS[1]]["tradable"] is True
    assert report["metadata"][SYMBOLS[1]]["new_listing_special_period"] is False
    assert report["usable_symbols"] == SYMBOLS[1:]


@pytest.mark.parametrize("column,value", [("name", "*ST合成"), ("name", "退市合成"), ("delist_date", "20260903")])
def test_current_listing_risk_is_metadata_not_universe_mutation(tmp_path, column, value):
    source = Source()
    def change(rows):
        if rows:
            rows[0][column] = value
        return rows
    source.changes["stock_basic"] = change
    report = collect(tmp_path, source)
    assert report["metadata"][SYMBOLS[0]]["tradable"] is False
    assert not report["metadata"][SYMBOLS[0]]["missing_fields"]
    assert "current_listing_or_competition_risk" in report["metadata"][SYMBOLS[0]]["issues"]


def test_missing_calendar_day_or_wrong_exchange_fails_proof(tmp_path):
    source = Source()
    source.changes["trade_cal"] = lambda rows: rows[1:]
    assert collect(tmp_path, source)["status"] == "PARTIAL"


def test_old_context_does_not_fetch_current_identity(tmp_path):
    source = Source()
    with pytest.raises(PipelineError, match="actual current"):
        collect(tmp_path, source, clock=lambda: context().as_of + timedelta(days=1))
    assert not source.calls


def test_observation_is_immutable_expiring_and_source_bound(tmp_path):
    report = collect(tmp_path)
    with pytest.raises(PipelineError, match="already exists"):
        collect(tmp_path)
    with pytest.raises(PipelineError, match="expired"):
        load_execution_status(tmp_path / "observation", context=context(), symbols=SYMBOLS,
                              observed_at=context().as_of + timedelta(seconds=61), max_age_seconds=60)
    receipt = report["source_provenance"][0]
    atomic_json(tmp_path / "observation" / receipt["path"], {"changed": True})
    with pytest.raises(PipelineError, match="receipt changed"):
        load_execution_status(tmp_path / "observation", context=context(), symbols=SYMBOLS,
                              observed_at=context().as_of, max_age_seconds=60)


def test_historical_stk_limit_guard_is_still_unchanged():
    source = Source()
    boundary = ResearchDataClient(source, context(), observed_clock=lambda: context().as_of)
    with pytest.raises(PipelineError, match="date range"):
        boundary.stk_limit(trade_date="20260902")
    assert not source.calls


@pytest.mark.parametrize("interval", ["14:50:30-15:00", "14:00-14:50:30"])
def test_announced_suspension_transition_expires_inside_normal_ttl(tmp_path, interval):
    from competition_core.pipeline.portfolio import _metadata
    source = Source()
    source.changes["suspend_d"] = lambda rows: [{"ts_code": SYMBOLS[0], "trade_date": "20260902",
                                                "suspend_type": "S", "suspend_timing": interval}]
    report = collect(tmp_path, source)
    boundary = context().as_of + timedelta(seconds=30)
    assert report["metadata"][SYMBOLS[0]]["valid_until"] == boundary.isoformat()
    with pytest.raises(PipelineError, match="interval changed"):
        load_execution_status(tmp_path / "observation", context=context(), symbols=SYMBOLS,
                              observed_at=boundary, max_age_seconds=60)
    assert _metadata(report["metadata"][SYMBOLS[0]], symbol=SYMBOLS[0], context=context(),
                     observed_at=boundary, max_age_seconds=60) == "trading_metadata_interval_expired"
