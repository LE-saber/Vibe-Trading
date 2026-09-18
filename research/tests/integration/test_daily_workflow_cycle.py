from datetime import date, datetime
import hashlib

import pytest

from competition_core.pipeline.time_context import SHANGHAI
from competition_core.storage.workflow import WorkflowRepository
from contracts.candidate import CandidateRecord
from contracts.portfolio import PortfolioState
from workflows.intraday import run_intraday
from workflows.postclose import run_postclose
from workflows.preopen import run_preopen


def candidate_source(snapshot_date, phase, limit):
    rows = [{"symbol": "600001.SH", "score": 2}, {"symbol": "000001.SZ", "score": 1}]
    return {
        "date": str(snapshot_date), "phase": phase, "snapshot_id": "daily-cycle-fixture",
        "sort_by": "score", "counts": {"candidates": 2, "eligible": 2},
        "rows": rows[:limit], "warnings": [],
    }


def evidence(candidate):
    return {
        "symbol": candidate.symbol,
        "evidence_id": hashlib.sha256(candidate.symbol.encode()).hexdigest(),
        "as_of": "2026-09-17T09:00:00+08:00",
    }


def test_preopen_starts_at_0900_and_writes_decision_artifacts(tmp_path):
    repository = WorkflowRepository(tmp_path / "state.sqlite")
    with pytest.raises(ValueError, match="09:00-09:25"):
        run_preopen(
            run_id="too-early", snapshot_date=date(2026, 9, 17),
            candidate_source=candidate_source, evidence_builder=evidence,
            artifact_root=tmp_path / "artifacts", repository=repository,
            observed_at=datetime(2026, 9, 17, 8, 59, tzinfo=SHANGHAI),
        )
    result = run_preopen(
        run_id="at-0900", snapshot_date=date(2026, 9, 17),
        candidate_source=candidate_source, evidence_builder=evidence,
        artifact_root=tmp_path / "artifacts", repository=repository,
        observed_at=datetime(2026, 9, 17, 9, 0, tzinfo=SHANGHAI),
        market_context={"regime": "neutral"},
    )
    assert result["manifest"]["preopen_start"] == "09:00"
    assert result["analysis"]["decision_state"] == "RESEARCH_READY"
    assert (tmp_path / "artifacts/at-0900/preopen-analysis.json").is_file()
    assert (tmp_path / "artifacts/at-0900/risk-gates.json").is_file()


def test_intraday_without_material_trigger_emits_no_intents(tmp_path):
    repository = WorkflowRepository(tmp_path / "state.sqlite")
    watchlist = [
        CandidateRecord("600001.SH", "S4", 2, 1, ("fixture",), ()),
        CandidateRecord("000001.SZ", "S4", 1, 2, ("fixture",), ()),
    ]
    current = PortfolioState(
        as_of=datetime(2026, 9, 17, 9, 30, tzinfo=SHANGHAI),
        cash=1_000_000, positions=(), total_assets=1_000_000,
        initial_capital=1_000_000, source="fixture",
    )

    def quotes(symbols):
        return [{
            "symbol": symbol, "price": 10, "open": 10, "pre_close": 10,
            "as_of": "2026-09-17T10:30:00+08:00", "source": "ninef_official",
            "tradeable": True, "is_limit_up": False, "is_limit_down": False,
        } for symbol in symbols]

    result = run_intraday(
        run_id="no-trigger", watchlist=watchlist, snapshot_id="fixture",
        quote_source=quotes, current=current, artifact_root=tmp_path / "artifacts",
        repository=repository, cash_weight=0.20,
        observed_at=datetime(2026, 9, 17, 10, 30, tzinfo=SHANGHAI),
        account_orders_complete=True, open_order_count=0, trigger_reasons=(),
        sector_by_symbol={"600001.SH": "industry-a", "000001.SZ": "industry-b"},
        pair_correlations={"600001.SH": {"000001.SZ": 0.20}},
    )
    assert result["intents"] == ()
    assert result["decision"]["material_trigger"] is False
    assert result["manifest"]["approval_required"] is True


def test_postclose_writes_reconciliation_and_next_day_artifacts(tmp_path):
    repository = WorkflowRepository(tmp_path / "state.sqlite")
    state = run_postclose(
        run_id="postclose", cash=200_000, positions={"600001.SH": 80_000},
        close_prices={"600001.SH": 10}, initial_capital=1_000_000,
        artifact_root=tmp_path / "artifacts", repository=repository,
        observed_at=datetime(2026, 9, 17, 15, 5, tzinfo=SHANGHAI),
        orders_summary={"complete": True, "open_count": 0, "items": []},
        fees_total=25.0, leaderboard_snapshot={"own": {"rank": 12}},
    )
    directory = tmp_path / "artifacts/postclose"
    assert state.total_assets == 1_000_000
    for name in (
        "pnl-attribution.json", "postclose-summary.json", "thesis-tracker.json",
        "next-day-watchlist.json", "daily-manifest.json",
    ):
        assert (directory / name).is_file()
