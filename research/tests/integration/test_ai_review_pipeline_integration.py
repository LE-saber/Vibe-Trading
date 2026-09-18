"""Explicit synthetic S4/Evidence inputs; never used for production acceptance."""

from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import threading
import time

import pytest

from competition_core.pipeline.artifacts import atomic_json, content_hash, file_hash
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.persistence import PipelineRepository
from competition_core.pipeline.reviews import run_reviews
from competition_core.storage.evidence import EvidenceRepository
from contracts.evidence import EvidenceBundle, Provenance, SECTION_NAMES
from contracts.symbol import CanonicalSymbol
from tests.pipeline_fixtures import context, small_config

VIBE = Path(__file__).resolve().parents[3] / "Vibe-Trading"
RUNTIME = {"provider": "deepseek", "model": "synthetic-model",
           "generation_parameters": {"extra_body": {"max_tokens": 4096, "thinking": {"type": "disabled"}}, "response_format": {"type": "json_object"}}}


def seed(tmp_path):
    repo = PipelineRepository(tmp_path / "runs", "fixture")
    store = EvidenceRepository(tmp_path / "core/state/core.sqlite", tmp_path / "core/artifacts/evidence")
    config, ctx = small_config(), context()
    manifest = tmp_path / "synthetic-manifest.json"
    atomic_json(manifest, {"explicit_test_fixture": True})
    repo.initialize({"context": ctx.to_dict(), "snapshot_id": "synthetic-snapshot", "config": config.to_dict(),
                     "config_hash": config.config_hash, "manifest_path": str(manifest), "manifest_hash": file_hash(manifest),
                     "vibe_root": str(VIBE)})
    rows = []
    for index in range(10):
        symbol = f"600{index + 1:03d}.SH"
        sections = {section: {} for section in SECTION_NAMES}
        sections["technical"] = {"momentum": .1}
        bundle = EvidenceBundle(symbol=CanonicalSymbol.from_core(symbol), as_of=ctx.as_of, snapshot_id="synthetic-snapshot",
            sections=sections, missing=(), provenance=(Provenance("technical.momentum", "synthetic", ctx.as_of),))
        store.put(bundle)
        atomic_json(repo.directory / "evidence" / f"{bundle.evidence_id}.json", bundle.to_dict(), immutable=True)
        rows.append({"symbol": symbol, "selected": True, "evidence_id": bundle.evidence_id,
                     "quant_score": 80. - index, "selection_score": 70. - index,
                     "keep_reasons": ["synthetic fixture"], "reject_reasons": []})
    for stage in ("S0", "S1", "S2", "S3", "S4"):
        repo.start_stage(stage)
        repo.commit_stage(stage, {"stage": stage, "status": "SUCCESS", "input_symbols": [row["symbol"] for row in rows],
            "output_symbols": [row["symbol"] for row in rows], "rows": rows, "summary": {"llm_calls": 0, "agent_tool_calls": 0}},
            metadata=ctx.to_dict() | {"snapshot_id": "synthetic-snapshot", "config_hash": config.config_hash}, elapsed=0.)
    return repo, store


def packet(**kwargs):
    candidate, bundle = kwargs["candidate"], kwargs["bundle"]
    value = {"schema_version": "p10.research_packet.v1", "symbol": candidate["symbol"], "evidence_id": bundle.evidence_id,
             "fields": {"technical.momentum": .1}, "system_prompt": "Explicit synthetic test fixture. " * 85, "user_prompt": "{}"}
    value["packet_chars"] = len(value["system_prompt"]) + len(value["user_prompt"])
    return value | {"packet_hash": content_hash(value)}


def answer(packet):
    review = {"symbol": packet["symbol"], "verdict": "KEEP", "strengths": ["动量为正[technical.momentum]"], "risks": [],
              "evidence_conflicts": [], "missing_critical_info": [], "summary": "保留研究资格[technical.momentum]"}
    return {"ok": True, "runtime": RUNTIME, "packet_hash": packet["packet_hash"], "tool_calls": [],
            "response_model": "synthetic-model", "llm_calls": 1, "content": json.dumps(review)}


def news_context():
    value = {"schema_version": "ninef.news.v1", "status": "EMPTY_VERIFIED",
             "cutoff": context().as_of.isoformat(), "fetched_at": context().as_of.isoformat(), "items": []}
    return value | {"snapshot_hash": content_hash(value)}


def test_per_stock_failure_isolation_budget_concurrency_and_resume(tmp_path, monkeypatch):
    monkeypatch.setattr("competition_core.pipeline.research_packet.build_research_packet", packet)
    repo, store = seed(tmp_path)
    calls, lock = Counter(), threading.Lock()
    active, peak = 0, 0
    def invoke(**kwargs):
        nonlocal active, peak
        value = kwargs["packet"]
        symbol = value["symbol"]
        with lock:
            calls[symbol] += 1
            number = calls[symbol]
            active += 1
            peak = max(peak, active)
        try:
            time.sleep(.01)
            if symbol == "600002.SH":
                raise PipelineError("TIMEOUT", "explicit synthetic timeout")
            payload = answer(value)
            if symbol == "600001.SH" and number == 1:
                payload["content"] = "not JSON"
            return payload
        finally:
            with lock:
                active -= 1
    state = run_reviews(repository=repo, evidence_store=store, invoker=invoke, probe=lambda **kw: RUNTIME)
    assert state["completed_through"] == "S5" and not state["execution_ready"]
    stage = repo.load_stage("S5")
    assert stage["status"] == "PARTIAL" and stage["summary"]["reviewed_count"] == 9 and stage["summary"]["failed_count"] == 1
    assert peak <= 3 and max(calls.values()) == 2 and sum(calls.values()) == 12
    failed = next(row for row in stage["rows"] if row["symbol"] == "600002.SH")
    assert failed["ai_review"] is None and failed["ai_verdict"] == "REVIEW" and not failed["ai_eligible"]
    assert failed["review_origin"] == "system_failure"
    original = {row["symbol"]: row for row in repo.load_stage("S4")["rows"]}
    assert all(row["quant_score"] == original[row["symbol"]]["quant_score"] for row in stage["rows"])
    run_reviews(repository=repo, evidence_store=store, invoker=lambda **kw: pytest.fail("budget already consumed"), probe=lambda **kw: RUNTIME)


def test_completed_reviews_bind_raw_response_and_frozen_inputs(tmp_path, monkeypatch):
    monkeypatch.setattr("competition_core.pipeline.research_packet.build_research_packet", packet)
    repo, store = seed(tmp_path)
    run_reviews(repository=repo, evidence_store=store, invoker=lambda **kw: answer(kw["packet"]), probe=lambda **kw: RUNTIME)
    assert repo.load_stage("S5")["status"] == "SUCCESS"
    atomic_json(repo.directory / "reviews/600001.SH/attempt-1/response.json", {"explicit_corruption_fixture": True})
    with pytest.raises(PipelineError, match="hash mismatch"):
        run_reviews(repository=repo, evidence_store=store, invoker=lambda **kw: pytest.fail("no model call"), probe=lambda **kw: RUNTIME)


def test_review_attempt_freezes_news_before_any_model_call(tmp_path, monkeypatch):
    seen = []
    def capture(**kwargs):
        seen.append(kwargs["news_context"]["snapshot_hash"])
        return packet(**kwargs)
    monkeypatch.setattr("competition_core.pipeline.research_packet.build_research_packet", capture)
    repo, store = seed(tmp_path)
    news = news_context()

    run_reviews(repository=repo, evidence_store=store, news_context=news,
                invoker=lambda **kw: answer(kw["packet"]), probe=lambda **kw: RUNTIME)

    assert seen == [news["snapshot_hash"]] * 10
    assert json.loads((repo.directory / "news-context.json").read_text(encoding="utf-8")) == news
    assert json.loads((repo.directory / "review-inputs.json").read_text(encoding="utf-8"))["news_context_hash"] == news["snapshot_hash"]
