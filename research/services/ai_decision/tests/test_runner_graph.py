from __future__ import annotations

import pytest

from ai_decision.models import AnalyzeRequest
from ai_decision.offline import OfflineEvidenceLLM
from ai_decision.runner import DecisionRunner
from ai_decision.vendor import EvidenceVendor, install_evidence_vendor
from tests.conftest import evidence_payload


def test_full_tradingagents_graph_runs_on_evidence_only(analyze_request, tmp_path) -> None:
    runner = DecisionRunner(tmp_path)

    output = runner.analyze(analyze_request, offline_llm=OfflineEvidenceLLM())

    assert output.framework == "TradingAgents"
    assert output.framework_version == "0.4.0"
    assert output.symbol == "600519.SH"
    assert output.evidence_id == analyze_request.evidence.evidence_id
    assert output.final_rating == "Hold"
    assert output.decision == "KEEP"
    assert output.execution_mode == "ANALYSIS_ONLY"
    assert output.numeric_audit["status"] == "pass"
    assert output.analyst_reports["market"]
    assert output.analyst_reports["fundamental"]
    assert output.bull_case and output.bear_case and output.risk_assessment


def test_interrupted_graph_resumes_from_sqlite_checkpoint(analyze_request, tmp_path) -> None:
    from tradingagents.graph.checkpointer import checkpoint_step

    runner = DecisionRunner(tmp_path)
    llm = OfflineEvidenceLLM(fail_once_in_debate=True)
    vendor = EvidenceVendor(analyze_request.evidence)
    with install_evidence_vendor(vendor):
        first = runner.build_graph(analyze_request, offline_llm=llm)
        first._resolve_pending_entries = lambda _ticker: None
        with pytest.raises(RuntimeError, match="intentional offline checkpoint"):
            first.propagate("600519.SH", "2026-09-01")

        signature = first._run_signature("stock")
        assert checkpoint_step(first.config["data_cache_dir"], "600519.SH", "2026-09-01", signature)

        resumed = runner.build_graph(analyze_request, offline_llm=llm)
        resumed._resolve_pending_entries = lambda _ticker: None
        state, rating = resumed.propagate("600519.SH", "2026-09-01")

    assert str(getattr(rating, "value", rating)) == "Hold"
    assert state["final_trade_decision"]
    assert checkpoint_step(
        resumed.config["data_cache_dir"], "600519.SH", "2026-09-01", signature
    ) is None


def test_ten_candidate_batch_uses_fresh_graph_and_keeps_identity_isolated(tmp_path) -> None:
    symbols = [f"{code:06d}.SZ" for code in range(1, 11)]
    runner = DecisionRunner(tmp_path)
    outputs = []

    for rank, symbol in enumerate(symbols, start=1):
        evidence = evidence_payload(symbol)
        request = AnalyzeRequest.model_validate(
            {
                "run_id": f"p6-batch-{rank}",
                "depth": "light",
                "candidate": {
                    "symbol": symbol,
                    "rank": rank,
                    "score": 1.0 - rank / 100,
                    "evidence_ref": f"evidence://{evidence['evidence_id']}",
                },
                "evidence": evidence,
            }
        )
        outputs.append(runner.analyze(request, offline_llm=OfflineEvidenceLLM()))

    assert [item.symbol for item in outputs] == symbols
    assert len({item.evidence_id for item in outputs}) == 10
    assert all(item.execution_mode == "ANALYSIS_ONLY" for item in outputs)
    assert all(item.numeric_audit["status"] == "pass" for item in outputs)
