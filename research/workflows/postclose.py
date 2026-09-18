"""Post-close mark-to-market and attribution input artifact."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from competition_core.daily_workflow import DailyWorkflowConfig, load_daily_workflow_config
from competition_core.storage.workflow import WorkflowRepository
from contracts.portfolio import PortfolioState
from workflows.artifacts import write_json

CHINA = ZoneInfo("Asia/Shanghai")


def run_postclose(
    *,
    run_id: str,
    cash: float,
    positions: dict[str, int],
    close_prices: dict[str, float],
    initial_capital: float,
    artifact_root: str | Path,
    repository: WorkflowRepository,
    observed_at: datetime | None = None,
    orders_summary: dict[str, Any] | None = None,
    fees_total: float | None = None,
    leaderboard_snapshot: dict[str, Any] | None = None,
    prior_portfolio: PortfolioState | None = None,
    decision_log: list[dict[str, Any]] | None = None,
    daily_config: DailyWorkflowConfig | None = None,
) -> PortfolioState:
    missing = sorted(set(positions) - set(close_prices))
    if missing:
        raise ValueError(f"post-close prices missing: {', '.join(missing)}")
    policy = daily_config or load_daily_workflow_config()
    now = observed_at or datetime.now(CHINA)
    if observed_at is not None and policy.phase_at(observed_at) != "POSTCLOSE":
        raise ValueError("postclose workflow must run at or after the configured start time")
    records = tuple(
        (symbol, int(shares), float(close_prices[symbol]))
        for symbol, shares in sorted(positions.items())
    )
    total = cash + sum(shares * price for _symbol, shares, price in records)
    state = PortfolioState(
        as_of=now,
        cash=float(cash),
        positions=records,
        total_assets=float(total),
        initial_capital=float(initial_capital),
        source="postclose_mark_to_market",
    )
    run_dir = Path(artifact_root).resolve() / run_id
    if run_dir.exists():
        raise FileExistsError(f"workflow artifact already exists: {run_dir}")
    run_dir.mkdir(parents=True)
    write_json(run_dir, "portfolio.json", state.to_dict())
    prior = {} if prior_portfolio is None else {
        symbol: (shares, price) for symbol, shares, price in prior_portfolio.positions
    }
    attribution = []
    for symbol, shares, price in records:
        prior_shares, prior_price = prior.get(symbol, (shares, price))
        attribution.append({
            "symbol": symbol,
            "ending_shares": shares,
            "ending_price": price,
            "ending_market_value": shares * price,
            "mark_to_market_change": shares * price - prior_shares * prior_price,
            "note": "includes share changes; not a trade-level realized P&L attribution",
        })
    write_json(run_dir, "pnl-attribution.json", {
        "schema_version": "competition.pnl-attribution.v1",
        "as_of": now.isoformat(),
        "positions": attribution,
        "fees_total": fees_total,
    })
    write_json(run_dir, "postclose-summary.json", {
        "schema_version": "competition.postclose-summary.v1",
        "as_of": now.isoformat(),
        "decision_state": "CLOSED",
        "portfolio": state.to_dict(),
        "orders": orders_summary,
        "fees_total": fees_total,
        "leaderboard": leaderboard_snapshot,
        "decisions": decision_log or [],
        "execution_ready": False,
    })
    write_json(run_dir, "thesis-tracker.json", {
        "schema_version": "competition.thesis-tracker.v1",
        "as_of": now.isoformat(),
        "positions": [{"symbol": symbol, "status": "REUNDERWRITE_NEXT_PREOPEN"}
                      for symbol, _shares, _price in records],
    })
    write_json(run_dir, "next-day-watchlist.json", {
        "schema_version": "competition.next-day-watchlist.v1",
        "created_at": now.isoformat(),
        "symbols": [symbol for symbol, _shares, _price in records],
        "contains_orders": False,
    })
    manifest = {
        "run_id": run_id,
        "phase": "postclose",
        "status": "complete",
        "started_at": now.isoformat(),
        "finished_at": datetime.now(CHINA).isoformat(),
        "fallback_used": False,
        "total_assets": state.total_assets,
        "return_pct": state.to_dict()["return_pct"],
        "decision_state": "CLOSED",
        "execution_ready": False,
        "open_order_count": None if orders_summary is None else orders_summary.get("open_count"),
    }
    write_json(run_dir, "run_manifest.json", manifest)
    write_json(run_dir, "daily-manifest.json", manifest | {
        "required_artifacts": [
            "portfolio.json", "pnl-attribution.json", "postclose-summary.json",
            "thesis-tracker.json", "next-day-watchlist.json", "run_manifest.json",
        ]
    })
    repository.put_portfolio(state)
    repository.put_run(run_id, "postclose", "complete", False, str(run_dir), manifest)
    return state
