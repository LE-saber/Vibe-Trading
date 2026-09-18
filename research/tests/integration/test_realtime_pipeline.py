"""Synthetic quote observations: no live quote/account acceptance is implied."""

from datetime import timedelta

import pytest

from competition_core.pipeline.artifacts import atomic_json, file_hash, read_json
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.realtime import run_realtime_check
from tests.integration.test_ai_review_pipeline_integration import seed
from tests.pipeline_fixtures import context


def prepared(tmp_path):
    repo, _ = seed(tmp_path)
    s4 = repo.load_stage("S4")
    rows = [row | {"ai_status": "SUCCESS", "ai_verdict": "WATCH", "ai_eligible": True} for row in s4["rows"]]
    repo.start_stage("S5")
    repo.commit_stage("S5", {"stage": "S5", "status": "SUCCESS", "rows": rows,
        "input_symbols": s4["output_symbols"], "output_symbols": s4["output_symbols"],
        "summary": {"explicit_synthetic_fixture": True}},
        metadata=context().to_dict() | {"snapshot_id": s4["snapshot_id"], "config_hash": s4["config_hash"]}, elapsed=0)
    return repo


def quotes(symbols):
    return {"response": {"quotes": [{"symbol": symbol, "source": "mootdx",
            "server_time": context().as_of.isoformat(), "as_of": context().as_of.isoformat(),
            "trade_date": context().research_date.isoformat(), "timestamp_quality": "verified",
            "received_at": context().as_of.isoformat(), "timestamp_source": "explicit.synthetic.fixture",
            "price": 10., "prev_close": 10., "open": 10., "high": 10.1, "low": 9.9,
            "bid1": 9.99, "ask1": 10.01, "volume": 10000., "volume_unit": "lots",
            "amount": 10000000., "amount_unit": "CNY"} for symbol in symbols]}}


def test_observation_preserves_s0_s5_and_does_not_grant_execution(tmp_path):
    repo = prepared(tmp_path)
    before = {stage: file_hash(repo.directory / f"{stage}.json") for stage in ("S0", "S1", "S2", "S3", "S4", "S5")}
    report = run_realtime_check(repository=repo, check_id="synthetic-1", quote_source=quotes, clock=lambda: context().as_of)
    assert len(report["ai_and_quote_eligible"]) == 10 and report["enough_candidates_for_portfolio"]
    assert not report["execution_ready"] and not report["production_accepted"]
    assert all(row["metrics"]["intraday_relative_strength"] == 0 for row in report["result"]["rows"])
    assert repo.state()["completed_through"] == "S5" and repo.load_stage("S5_5") is None
    assert before == {stage: file_hash(repo.directory / f"{stage}.json") for stage in before}
    path = repo.directory / "realtime/synthetic-1/observation.json"
    assert read_json(path)["integrity_hash"]
    with pytest.raises(PipelineError, match="already used"):
        run_realtime_check(repository=repo, check_id="synthetic-1", quote_source=lambda _: pytest.fail("no new request"),
                           clock=lambda: context().as_of)


def test_runtime_failure_retry_is_new_observation_without_screening_or_ai(tmp_path):
    repo = prepared(tmp_path)
    def fail(_):
        raise RuntimeError("token=do-not-log-this")
    first = run_realtime_check(repository=repo, check_id="failure", quote_source=fail, clock=lambda: context().as_of)
    assert first["source_error"]["code"] == "SOURCE_UNAVAILABLE" and not first["ai_and_quote_eligible"]
    assert "do-not-log-this" not in (repo.directory / "realtime/failure/response.json").read_text()
    second = run_realtime_check(repository=repo, check_id="recovered", quote_source=quotes, clock=lambda: context().as_of)
    assert len(second["ai_and_quote_eligible"]) == 10 and repo.state()["last_realtime_check"] == "recovered"
    assert (repo.directory / "realtime/failure/observation.json").exists()


def test_historical_run_never_receives_todays_quotes(tmp_path):
    repo = prepared(tmp_path)
    with pytest.raises(PipelineError) as error:
        run_realtime_check(repository=repo, check_id="historical", clock=lambda: context().as_of + timedelta(days=1),
                           quote_source=lambda _: pytest.fail("historical run must not fetch new market data"))
    assert error.value.code == "RESEARCH_DATE_NOT_CURRENT"
    assert read_json(repo.directory / "realtime/historical/failure.json")["code"] == "RESEARCH_DATE_NOT_CURRENT"
    assert not repo.state()["execution_ready"]


def test_missing_s5_and_changed_manifest_fail_before_fetch(tmp_path):
    repo, _ = seed(tmp_path)
    with pytest.raises(PipelineError, match="through S5"):
        run_realtime_check(repository=repo, quote_source=lambda _: pytest.fail("no fetch"), clock=lambda: context().as_of)
    repo = prepared(tmp_path / "second")
    atomic_json(tmp_path / "second/synthetic-manifest.json", {"corrupt": True})
    with pytest.raises(PipelineError, match="manifest changed"):
        run_realtime_check(repository=repo, quote_source=lambda _: pytest.fail("no fetch"), clock=lambda: context().as_of)


def test_future_clock_and_bad_check_id_do_not_fetch(tmp_path):
    repo = prepared(tmp_path)
    with pytest.raises(PipelineError, match="explicit clock"):
        run_realtime_check(repository=repo, quote_source=lambda _: pytest.fail("no fetch"),
                           clock=lambda: context().as_of - timedelta(seconds=1))
    with pytest.raises(PipelineError, match="identifier"):
        run_realtime_check(repository=repo, check_id="../escape", quote_source=lambda _: pytest.fail("no fetch"))


def test_bad_identity_after_fetch_retains_raw_response_and_allows_new_observation(tmp_path):
    repo = prepared(tmp_path)
    def corrupt(symbols):
        payload = quotes(symbols)
        payload["response"]["quotes"][0]["symbol"] = "300750.SH"
        return payload
    with pytest.raises(PipelineError):
        run_realtime_check(repository=repo, check_id="corrupt", quote_source=corrupt, clock=lambda: context().as_of)
    assert (repo.directory / "realtime/corrupt/response.json").is_file()
    assert (repo.directory / "realtime/corrupt/failure.json").is_file()
    assert repo.state()["status"] == "PARTIAL" and not repo.state()["execution_ready"]
