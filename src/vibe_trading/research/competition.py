"""Adapter from the maintained Competition pipeline to ``vibe.research.v1``.

The adapter runs S0-S5 only. It cannot submit, cancel or construct broker
orders; canonical S6 planning remains in :mod:`vibe_trading.planner`.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ..models import ResearchRequest, ResearchResult, Target, digest
from ..news import review_context
from ..ports import Cancelled, Progress
from ..progress import RunCancelled


SHANGHAI = ZoneInfo("Asia/Shanghai")
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RESEARCH_ROOT = REPOSITORY_ROOT / "research"


def _phase(moment: datetime) -> str:
    local = moment.astimezone(SHANGHAI).time().replace(tzinfo=None)
    if local < time(9, 30):
        return "preopen"
    if local < time(15, 0):
        return "intraday"
    return "postclose"


def _research_root() -> Path:
    configured = os.environ.get("VIBE_RESEARCH_ROOT", "").strip()
    root = Path(configured).expanduser().resolve() if configured else DEFAULT_RESEARCH_ROOT.resolve()
    if not (root / "competition_core" / "pipeline" / "runner.py").is_file():
        raise RuntimeError("VIBE_RESEARCH_ROOT does not contain the Competition pipeline")
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    return root


def _news_snapshot(request: ResearchRequest, symbols: set[str], cutoff: datetime) -> tuple[dict[str, Any], str]:
    from competition_core.pipeline.artifacts import content_hash

    selected = review_context(request.news, symbols, request.requested_at)
    items = []
    allowed = sorted(symbols)
    for item in selected["news"]:
        items.append({
            "news_ref": item.news_id,
            "title": item.title,
            "body": item.body,
            "symbols": list(item.symbols) if item.symbols else allowed,
            "published_at": item.published_at.astimezone(SHANGHAI).isoformat(),
            "source": item.source,
            "source_ref": item.source_ref,
        })
    unsigned = {
        "schema_version": "workbench.news.v1",
        "status": "AVAILABLE" if items else "EMPTY_VERIFIED",
        "cutoff": cutoff.isoformat(),
        "fetched_at": request.requested_at.astimezone(SHANGHAI).isoformat(),
        "items": items,
        "source": "canonical_workbench_frozen_news",
        "raw_item_count": len(request.news),
        "malformed_item_count": 0,
    }
    return unsigned | {"snapshot_hash": content_hash(unsigned)}, selected["news_hash"]


def _account_snapshot(request: ResearchRequest):
    from competition_core.pipeline.account import AccountPosition, AccountSnapshot

    account = request.account
    positions = tuple(AccountPosition(
        symbol=row.symbol,
        quantity=row.quantity,
        sellable_quantity=row.sellable_quantity,
        market_value=row.market_value,
    ) for row in account.positions if row.quantity)
    pending = sum(order.is_open for order in account.orders)
    return AccountSnapshot(
        as_of=account.as_of,
        source="canonical_workbench",
        source_kind="competition_api",
        currency="CNY",
        cash_balance=account.available_cash + account.frozen_cash,
        available_cash=account.available_cash,
        current_total_asset=account.total_assets,
        positions=positions,
        pending_order_count=pending,
    )


def _review_reason(row: dict[str, Any], prefix: str = "") -> str:
    review = row.get("ai_review") or row.get("review") or {}
    parts = []
    if prefix:
        parts.append(prefix)
    summary = review.get("summary")
    if isinstance(summary, str) and summary.strip():
        parts.append(summary.strip())
    for label, key in (("优势", "strengths"), ("风险", "risks"), ("缺失", "missing_critical_info")):
        values = review.get(key)
        if isinstance(values, list) and values:
            parts.append(f"{label}: " + "；".join(str(value) for value in values[:3]))
    return (" | ".join(parts) or "量化候选已完成独立复核；组合与交易仍需 S6 校验。")[:1500]


class CompetitionResearchBackend:
    """Synchronous provider used by the workbench's single research worker."""

    def __init__(self, *, research_root: Path | None = None, runs_root: Path | None = None):
        self.root = (research_root or _research_root()).resolve()
        if str(self.root) not in sys.path:
            sys.path.insert(0, str(self.root))
        configured_runs = os.environ.get("VIBE_RESEARCH_RUNS_ROOT", "").strip()
        self.runs_root = (runs_root or (Path(configured_runs).expanduser() if configured_runs
                          else REPOSITORY_ROOT / ".local" / "research-runs")).resolve()

    def research(self, request: ResearchRequest, report: Progress,
                 cancelled: Cancelled) -> ResearchResult:
        from adapters.tushare.client import configured_tushare_client
        from competition_core.pipeline.artifacts import content_hash, read_json
        from competition_core.pipeline.config import load_pipeline_config
        from competition_core.pipeline.dataset import CanonicalDataset
        from competition_core.pipeline.persistence import PipelineRepository
        from competition_core.pipeline.runner import run_screening
        from competition_core.pipeline.reviews import run_holding_reviews, run_reviews
        from competition_core.pipeline.sources import ResearchDataClient
        from competition_core.pipeline.time_context import ResearchContext
        from competition_core.research_worker import prepare_dataset

        def check_cancelled() -> None:
            if cancelled():
                raise RunCancelled()

        local = request.requested_at.astimezone(SHANGHAI)
        phase = _phase(request.requested_at)
        config = load_pipeline_config(self.root / "config" / "pipeline-config.yaml")
        repository = PipelineRepository(self.runs_root, request.request_id)
        task = {
            "research_date": local.date().isoformat(),
            "phase": phase,
            "as_of": local.isoformat(),
            "config": config.to_dict(),
        }
        token_file_value = os.environ.get("TUSHARE_TOKEN_FILE", "").strip()
        token_file = Path(token_file_value).expanduser() if token_file_value else None
        client = configured_tushare_client(token_file=token_file)

        report("PREPARE", 0, 1, "Freezing point-in-time Universe and canonical market data.")
        prepared = prepare_dataset(task, repository.directory, client, lambda: local)
        snapshot = read_json(Path(prepared["snapshot_path"]))
        manifest_path = Path(prepared["manifest_path"])
        manifest = read_json(manifest_path)
        context_data = manifest["context"]
        context = ResearchContext(
            research_date=datetime.fromisoformat(context_data["research_date"]).date(),
            phase=context_data["phase"],
            market_data_reference_date=datetime.fromisoformat(
                context_data["market_data_reference_date"]
            ).date(),
            as_of=datetime.fromisoformat(context_data["as_of"]),
        )
        source = ResearchDataClient(client, context)
        report("PREPARE", 1, 1, "Frozen research inputs are ready.")
        check_cancelled()

        def stage_progress(event: dict[str, Any]) -> None:
            stage = str(event.get("stage", "S0"))
            completed = event.get("completed")
            total = event.get("total")
            count = event.get("count")
            report(stage, int(completed or (1 if count is not None else 0)),
                   int(total) if isinstance(total, int) else None,
                   f"{stage} completed" if count is not None else f"{stage} running")
            check_cancelled()

        run_screening(
            dataset_root=Path(prepared["dataset_root"]), manifest_path=manifest_path,
            snapshot=snapshot, context=context, config=config, repository=repository,
            source_client=source, vibe_root=self.root, through_stage="S4",
            progress=stage_progress,
        )
        s4 = repository.load_stage("S4")
        if s4 is None or s4.get("status") != "SUCCESS":
            raise RuntimeError("S4 did not produce a complete shortlist")
        all_symbols = set(s4["output_symbols"]) | {position.symbol for position in request.account.positions}
        news, canonical_news_hash = _news_snapshot(request, all_symbols, context.as_of)
        check_cancelled()
        report("S5", 0, len(s4["output_symbols"]), "Running isolated per-stock AI reviews.")
        run_reviews(repository=repository, progress=stage_progress, news_context=news)
        s5 = repository.load_stage("S5")
        if s5 is None or s5.get("status") != "SUCCESS":
            raise RuntimeError("S5 did not produce complete validated reviews")

        account = _account_snapshot(request)
        holding_index = None
        if account.positions:
            panel = CanonicalDataset(Path(prepared["dataset_root"]), manifest["source"]).load(manifest_path)
            holding_index = run_holding_reviews(
                repository=repository, account=account,
                account_source_hash=digest(request.account), snapshot=snapshot, panel=panel,
                sessions=manifest["session_dates"], context=context, config=config,
                source_client=source, source_name=manifest["source"], news_context=news,
                progress=stage_progress,
            )
        check_cancelled()
        return self._result(request, s4, s5, holding_index, canonical_news_hash, config)

    def _result(self, request: ResearchRequest, s4: dict[str, Any], s5: dict[str, Any],
                holding_index: dict[str, Any] | None, news_hash: str, config: Any) -> ResearchResult:
        from competition_core.pipeline.artifacts import content_hash

        s5_by = {row["symbol"]: row for row in s5["rows"]}
        identity = {row["symbol"]: row.get("identity", {}) for row in s4["rows"]}
        holdings = {row.symbol: row for row in request.account.positions if row.quantity}
        cash_buffer = Decimal(str(config.section("risk")["cash_buffer"]))
        cap = Decimal(str(config.section("risk")["single_name_cap"]))
        max_positions = config.section("risk")["max_positions"]

        preserved_symbols = {
            symbol for symbol, row in s5_by.items()
            if symbol in holdings and row.get("ai_verdict") != "KEEP"
        } | (set(holdings) - set(s5_by))
        current_weights = {
            symbol: min(Decimal("1"), position.market_value / request.account.total_assets)
            for symbol, position in holdings.items()
        }
        preserved = sum((current_weights[symbol] for symbol in preserved_symbols), Decimal("0"))
        keep = [symbol for symbol in s4["output_symbols"]
                if s5_by[symbol].get("ai_verdict") == "KEEP"][:max_positions]
        allocatable = max(Decimal("0"), Decimal("1") - cash_buffer - preserved)
        keep_weight = min(cap, allocatable / len(keep)) if keep else Decimal("0")
        targets: list[Target] = []
        evidence_ids: set[str] = set()
        for symbol in s4["output_symbols"]:
            row = s5_by[symbol]
            verdict = row.get("ai_verdict", "REVIEW")
            evidence = row.get("evidence_id")
            refs = (evidence,) if isinstance(evidence, str) and evidence else ()
            evidence_ids.update(refs)
            if verdict == "KEEP" and symbol in keep:
                weight, mapped = keep_weight, "KEEP"
            elif symbol in holdings:
                weight, mapped = current_weights[symbol], "WATCH"
            elif verdict == "EXCLUDE":
                weight, mapped = Decimal("0"), "REJECT"
            else:
                weight, mapped = Decimal("0"), "WATCH"
            name = str(identity.get(symbol, {}).get("name") or
                       (holdings[symbol].name if symbol in holdings else symbol))
            targets.append(Target(symbol=symbol, name=name, weight=weight,
                                  verdict=mapped, reason=_review_reason(row), evidence_ids=refs))

        reviewed_holdings: dict[str, dict[str, Any]] = {}
        if holding_index:
            for record in holding_index.get("records", []):
                path = (self.runs_root / request.request_id / "holding-review-attempts" /
                        record["attempt_id"] / "result.json")
                if path.is_file():
                    from competition_core.pipeline.artifacts import read_json
                    reviewed_holdings[record["symbol"]] = read_json(path)
        for symbol in sorted(set(holdings) - set(s5_by)):
            position = holdings[symbol]
            result = reviewed_holdings.get(symbol, {})
            reason = _review_reason(result, "现有持仓补充复核；未授予新增买入权限。")
            targets.append(Target(symbol=symbol, name=position.name or symbol,
                                  weight=current_weights[symbol], verdict="WATCH",
                                  reason=reason, evidence_ids=()))

        binding = {
            "s5": s5["artifact_hash"],
            "holding_index": content_hash(holding_index) if holding_index else None,
        }
        return ResearchResult(
            run_id=request.request_id,
            as_of=request.requested_at,
            candidates=tuple(s4["output_symbols"]),
            targets=tuple(targets),
            evidence_ids=tuple(sorted(evidence_ids)),
            artifact_hash=digest(binding),
            strategy_version=f"competition-{config.to_dict()['version']}-s0-s5",
            news_context_hash=news_hash,
            demo=False,
        )


def create_backend() -> CompetitionResearchBackend:
    return CompetitionResearchBackend()
