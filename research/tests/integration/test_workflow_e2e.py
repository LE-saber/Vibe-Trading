from __future__ import annotations

import hashlib
from datetime import date, datetime
from zoneinfo import ZoneInfo

from competition_core.storage.workflow import WorkflowRepository
from contracts.portfolio import PortfolioState
from workflows.intraday import run_intraday
from workflows.nightly_research import run_nightly
from workflows.postclose import run_postclose
from workflows.preopen import run_preopen

CHINA = ZoneInfo("Asia/Shanghai")


def _candidates(_date, phase, limit):
    rows = [
        {"symbol": f"{600001 + index:06d}.SH", "amount": float(20 - index)}
        for index in range(limit)
    ]
    return {
        "date": str(_date),
        "phase": phase,
        "snapshot_id": "p8-large-universe",
        "sort_by": "amount",
        "counts": {"candidates": 5217, "eligible": 5006},
        "rows": rows,
        "warnings": [],
    }


def _evidence(candidate):
    evidence_id = hashlib.sha256(candidate.symbol.encode()).hexdigest()
    return {
        "symbol": candidate.symbol,
        "evidence_id": evidence_id,
        "as_of": "2026-09-01T08:30:00+08:00",
    }


def test_5000_to_top20_to_top8_to_target_and_manual_intents(tmp_path) -> None:
    repository = WorkflowRepository(tmp_path / "state.sqlite")

    preopen = run_preopen(
        run_id="20260901-preopen-test",
        snapshot_date=date(2026, 9, 1),
        candidate_source=_candidates,
        evidence_builder=_evidence,
        quant_scores=lambda _date: (_ for _ in ()).throw(RuntimeError("qlib down")),
        ai_analyzer=lambda _c, _e: (_ for _ in ()).throw(RuntimeError("ai down")),
        artifact_root=tmp_path / "artifacts",
        repository=repository,
    )

    assert preopen["manifest"]["counts"] == {"candidates": 5217, "eligible": 5006}
    assert len(preopen["candidates"]) == 20
    assert len(preopen["evidence"]) == 20
    assert len(preopen["watchlist"]) == 8
    assert preopen["manifest"]["fallbacks"] == ["qlib:RuntimeError", "tradingagents:RuntimeError"]

    current = PortfolioState(
        as_of=datetime(2026, 9, 1, 9, 25, tzinfo=CHINA),
        cash=1_000_000,
        positions=(),
        total_assets=1_000_000,
        initial_capital=1_000_000,
        source="competition_account",
    )

    def quotes(symbols):
        now = datetime.now(CHINA).isoformat()
        return [
            {
                "symbol": symbol,
                "price": 10.0 + index,
                "as_of": now,
                "source": "mootdx",
                "tradeable": True,
                "is_limit_up": False,
                "is_limit_down": False,
            }
            for index, symbol in enumerate(symbols)
        ]

    intraday = run_intraday(
        run_id="20260901-intraday-test",
        watchlist=preopen["watchlist"],
        snapshot_id=preopen["manifest"]["snapshot_id"],
        quote_source=quotes,
        current=current,
        artifact_root=tmp_path / "artifacts",
        repository=repository,
    )

    assert len(intraday["target"].weights) == 8
    assert len(intraday["intents"]) == 8
    assert all(item.execution_mode == "MANUAL_CONFIRM" for item in intraday["intents"])
    assert all(item.quantity % 100 == 0 for item in intraday["intents"])
    assert repository.latest_target().target_id == intraday["target"].target_id
    assert len(repository.latest_intents()) == 8


def test_postclose_and_nightly_qlib_failure_are_isolated(tmp_path) -> None:
    repository = WorkflowRepository(tmp_path / "state.sqlite")
    state = run_postclose(
        run_id="20260901-postclose-test",
        cash=500_000,
        positions={"600519.SH": 1000},
        close_prices={"600519.SH": 1500.0},
        initial_capital=1_000_000,
        artifact_root=tmp_path / "artifacts",
        repository=repository,
    )
    assert state.total_assets == 2_000_000
    assert repository.latest_portfolio().total_assets == 2_000_000

    manifest = run_nightly(
        run_id="20260901-nightly-test",
        data_quality=lambda: {"status": "pass"},
        research_refresh=lambda: (_ for _ in ()).throw(RuntimeError("qlib unavailable")),
        artifact_root=tmp_path / "artifacts",
        repository=repository,
    )
    assert manifest["status"] == "complete"
    assert manifest["fallback_used"] is True
