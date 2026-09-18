"""Deterministic pre-open shortlist and evidence orchestration."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from competition_core.storage.workflow import WorkflowRepository
from competition_core.daily_workflow import DailyWorkflowConfig, load_daily_workflow_config
from contracts.candidate import CandidateRecord
from workflows.artifacts import content_hash, write_json

CHINA = ZoneInfo("Asia/Shanghai")


def run_preopen(
    *,
    run_id: str,
    snapshot_date: date,
    candidate_source: Callable[[date, str, int], dict[str, Any]],
    evidence_builder: Callable[[CandidateRecord], dict[str, Any]],
    artifact_root: str | Path,
    repository: WorkflowRepository,
    shortlist_limit: int = 20,
    watchlist_limit: int = 8,
    quant_scores: Callable[[date], list[dict[str, Any]]] | None = None,
    ai_analyzer: Callable[[list[CandidateRecord], list[dict[str, Any]]], list[dict[str, Any]]] | None = None,
    observed_at: datetime | None = None,
    account_snapshot: dict[str, Any] | None = None,
    leaderboard_snapshot: dict[str, Any] | None = None,
    market_context: dict[str, Any] | None = None,
    holdings_review: list[dict[str, Any]] | None = None,
    catalyst_calendar: list[dict[str, Any]] | None = None,
    daily_config: DailyWorkflowConfig | None = None,
) -> dict[str, Any]:
    policy = daily_config or load_daily_workflow_config()
    started = observed_at or datetime.now(CHINA)
    if observed_at is not None and policy.phase_at(observed_at) != "PREOPEN":
        raise ValueError("preopen workflow must run within the configured 09:00-09:25 window")
    run_dir = Path(artifact_root).resolve() / run_id
    if run_dir.exists():
        raise FileExistsError(f"workflow artifact already exists: {run_dir}")
    run_dir.mkdir(parents=True)
    fallbacks: list[str] = []

    source = candidate_source(snapshot_date, "preopen", shortlist_limit)
    rows = source.get("rows", [])
    if not rows or len(rows) > shortlist_limit:
        raise ValueError("candidate source must return a bounded non-empty shortlist")
    baseline = [
        CandidateRecord(
            symbol=str(row["symbol"]),
            stage="top20",
            score=float(row.get(source.get("sort_by", "score"), row.get("score", len(rows) - index))),
            rank=index + 1,
            reasons=(str(source.get("sort_by", "deterministic")),),
            risk_flags=(),
        )
        for index, row in enumerate(rows)
    ]

    ranked = baseline
    if quant_scores is not None:
        try:
            qlib = quant_scores(snapshot_date)
            by_symbol = {
                str(item["symbol"]): float(item["prediction"])
                for item in qlib
                if int(item.get("horizon", 1)) == 1
            }
            if by_symbol:
                ranked = sorted(
                    baseline,
                    key=lambda item: (item.symbol in by_symbol, by_symbol.get(item.symbol, item.score), item.symbol),
                    reverse=True,
                )
                ranked = [
                    CandidateRecord(
                        symbol=item.symbol,
                        stage=item.stage,
                        score=by_symbol.get(item.symbol, item.score),
                        rank=index + 1,
                        reasons=item.reasons + (("qlib_1d",) if item.symbol in by_symbol else ()),
                        risk_flags=item.risk_flags,
                    )
                    for index, item in enumerate(ranked)
                ]
        except Exception as exc:  # noqa: BLE001 - explicit deterministic fallback
            fallbacks.append(f"qlib:{type(exc).__name__}")

    evidence: list[dict[str, Any]] = []
    enriched: list[CandidateRecord] = []
    for item in ranked:
        bundle = evidence_builder(item)
        if bundle.get("symbol") != item.symbol or not bundle.get("evidence_id"):
            raise ValueError("evidence identity must match its candidate")
        evidence.append(bundle)
        enriched.append(
            CandidateRecord(
                symbol=item.symbol,
                stage=item.stage,
                score=item.score,
                rank=item.rank,
                reasons=item.reasons,
                risk_flags=item.risk_flags,
                evidence_ref=f"evidence://{bundle['evidence_id']}",
            )
        )

    decisions: list[dict[str, Any]] = []
    watchlist = enriched[:watchlist_limit]
    if ai_analyzer is not None:
        try:
            decisions = ai_analyzer(enriched[:10], evidence[:10])
            score_by_symbol = {
                str(item["symbol"]): float(item["final_score"])
                for item in decisions
                if item.get("execution_mode") == "ANALYSIS_ONLY"
            }
            if score_by_symbol:
                watchlist = sorted(
                    enriched,
                    key=lambda item: (item.symbol in score_by_symbol, score_by_symbol.get(item.symbol, item.score), item.symbol),
                    reverse=True,
                )[:watchlist_limit]
        except Exception as exc:  # noqa: BLE001 - deterministic shortlist remains available
            fallbacks.append(f"tradingagents:{type(exc).__name__}")

    payloads = {
        "universe.json": {
            "snapshot_id": source.get("snapshot_id"),
            "date": source.get("date"),
            "phase": source.get("phase"),
            "counts": source.get("counts", {}),
            "warnings": source.get("warnings", []),
        },
        "candidates.json": [item.to_dict() for item in enriched],
        "evidence.json": [
            {"symbol": item["symbol"], "evidence_id": item["evidence_id"], "as_of": item.get("as_of")}
            for item in evidence
        ],
        "ai_decisions.json": decisions,
        "watchlist.json": [item.to_dict() for item in watchlist],
        "preopen-analysis.json": {
            "schema_version": "competition.preopen-analysis.v1",
            "as_of": started.isoformat(),
            "decision_state": "RESEARCH_READY",
            "schedule": {
                "start": policy.preopen_start.strftime("%H:%M"),
                "end": policy.preopen_end.strftime("%H:%M"),
            },
            "market_context": market_context or {},
            "holdings_review": holdings_review or [],
            "catalyst_calendar": catalyst_calendar or [],
            "account_snapshot": account_snapshot,
            "leaderboard_snapshot": leaderboard_snapshot,
            "watchlist": [item.to_dict() for item in watchlist],
            "execution_ready": False,
            "next_required_state": "PLAN_DRAFT",
        },
        "risk-gates.json": {
            "schema_version": "competition.risk-gates.v1",
            "max_positions": policy.max_positions,
            "max_single_weight": policy.max_single_weight,
            "cash_ratio_range": [policy.cash_ratio_min, policy.cash_ratio_max],
            "max_sector_weight": policy.max_sector_weight,
            "max_pair_correlation": policy.max_pair_correlation,
            "opening_gap_max": policy.opening_gap_max,
            "current_change_min": policy.current_change_min,
            "buy_lot": policy.buy_lot,
            "execution_mode": policy.execution_mode,
            "automatic_orders": policy.automatic_orders,
            "unknown_policy": policy.unknown_policy,
        },
    }
    for name, payload in payloads.items():
        write_json(run_dir, name, payload)
    finished = datetime.now(CHINA)
    manifest = {
        "run_id": run_id,
        "phase": "preopen",
        "status": "complete",
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "fallback_used": bool(fallbacks),
        "fallbacks": fallbacks,
        "snapshot_id": source.get("snapshot_id"),
        "counts": source.get("counts", {}),
        "candidate_count": len(enriched),
        "watchlist_count": len(watchlist),
        "decision_state": "RESEARCH_READY",
        "execution_ready": False,
        "preopen_start": policy.preopen_start.strftime("%H:%M"),
        "content_hashes": {name: content_hash(payload) for name, payload in payloads.items()},
    }
    write_json(run_dir, "run_manifest.json", manifest)
    repository.put_run(run_id, "preopen", "complete", bool(fallbacks), str(run_dir), manifest)
    return {"manifest": manifest, "candidates": enriched, "evidence": evidence,
            "watchlist": watchlist, "analysis": payloads["preopen-analysis.json"]}
