from types import SimpleNamespace

import pytest

from competition_core.pipeline.artifacts import atomic_json, read_json
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.holding_review_consumer import disposition_for, validate_holding_review_index
from competition_core.pipeline.reviews import run_holding_reviews
from competition_core.pipeline.sources import ResearchDataClient
from tests.unit.test_holding_evidence_builder import batches, panel, SESSIONS
from tests.unit.test_holding_review_attempt import (
    RUNTIME, RepositoryFixture, account, answer, news_context,
)
from tests.unit.test_pipeline_evidence_bundle import _member
from tests.unit.test_pipeline_research_packet import _bundle, _config, _context


@pytest.mark.parametrize("status,verdict,disposition,blocker", [
    ("SUCCESS", "KEEP", "RETAIN", None),
    ("SUCCESS", "WATCH", "RETAIN_WATCH", None),
    ("SUCCESS", "EXCLUDE", "EXIT_REVIEW_REQUIRED", "HOLDING_EXIT_REQUIRES_HUMAN_REVIEW"),
    ("SUCCESS", "REVIEW", "BLOCKED", "HOLDING_REVIEW_UNRESOLVED"),
    ("FAILED", None, "BLOCKED", "MISSING_HOLDING_RESEARCH_COVERAGE"),
])
def test_explicit_non_order_disposition_mapping(status, verdict, disposition, blocker):
    result = disposition_for(symbol="600519.SH", verdict=verdict, attempt_id="holding-" + "a" * 32,
                             status=status)
    assert result.disposition == disposition
    assert (result.blocker is None) if blocker is None else result.blocker.startswith(blocker)


def create_index(repo, snapshot, source_hash, monkeypatch):
    monkeypatch.setattr("competition_core.pipeline.evidence_builder.fetch_supplements",
                        lambda cache, sessions, cache_only: batches())
    return run_holding_reviews(repository=repo, account=snapshot, account_source_hash=source_hash,
        snapshot={"snapshot_id": _bundle().snapshot_id, "members": [_member()]},
        panel=panel(), sessions=SESSIONS, context=_context(), config=_config(),
        source_client=ResearchDataClient(SimpleNamespace(), _context()), source_name="tushare",
        news_context=news_context(), probe=lambda **_: RUNTIME,
        invoker=lambda **kwargs: answer(kwargs["packet"]))


def test_validated_keep_is_retain_only_without_portfolio_authority(tmp_path, monkeypatch):
    repo = RepositoryFixture(tmp_path)
    snapshot, source_hash = account(), "b" * 64
    create_index(repo, snapshot, source_hash, monkeypatch)

    result = validate_holding_review_index(repository=repo, account=snapshot,
                                           account_source_hash=source_hash)

    assert result["blocking_reasons"] == [] and result["portfolio_authority"] is False
    assert result["dispositions"] == [{
        "symbol": "600519.SH", "verdict": "KEEP", "disposition": "RETAIN",
        "attempt_id": result["dispositions"][0]["attempt_id"], "blocker": None,
    }]


def test_missing_index_and_changed_account_remain_fail_closed(tmp_path, monkeypatch):
    repo = RepositoryFixture(tmp_path)
    source_hash = "b" * 64
    missing = validate_holding_review_index(repository=repo, account=account(),
                                            account_source_hash=source_hash)
    assert missing["index_hash"] is None
    assert missing["blocking_reasons"] == ["MISSING_HOLDING_RESEARCH_COVERAGE:600519.SH"]

    create_index(repo, account(), source_hash, monkeypatch)
    with pytest.raises(PipelineError, match="current account/run"):
        validate_holding_review_index(repository=repo, account=account(200),
                                      account_source_hash=source_hash)
    with pytest.raises(PipelineError, match="current account/run"):
        validate_holding_review_index(repository=repo, account=account(),
                                      account_source_hash="c" * 64)


def test_tampered_checkpoint_is_rejected_before_consumption(tmp_path, monkeypatch):
    repo = RepositoryFixture(tmp_path)
    snapshot, source_hash = account(), "b" * 64
    index = create_index(repo, snapshot, source_hash, monkeypatch)
    attempt_id = index["records"][0]["attempt_id"]
    path = repo.directory / "holding-review-attempts" / attempt_id / "reviews" / "600519.SH" / "checkpoint.json"
    checkpoint = read_json(path)
    checkpoint["status"] = "FAILED"
    atomic_json(path, checkpoint)

    with pytest.raises(PipelineError, match="artifact hash mismatch"):
        validate_holding_review_index(repository=repo, account=snapshot,
                                      account_source_hash=source_hash)
