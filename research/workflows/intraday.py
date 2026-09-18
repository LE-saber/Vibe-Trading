"""Realtime risk checks, equal-weight target, and manual order intents."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from competition_core.storage.workflow import WorkflowRepository
from competition_core.daily_workflow import DailyWorkflowConfig, load_daily_workflow_config
from contracts.candidate import CandidateRecord
from contracts.order import OrderIntent
from contracts.portfolio import PortfolioState, TargetWeights
from workflows.artifacts import write_json

CHINA = ZoneInfo("Asia/Shanghai")


def run_intraday(
    *,
    run_id: str,
    watchlist: list[CandidateRecord],
    snapshot_id: str,
    quote_source: Callable[[list[str]], list[dict[str, Any]]],
    current: PortfolioState,
    artifact_root: str | Path,
    repository: WorkflowRepository,
    cash_weight: float = 0.10,
    max_single_weight: float = 0.20,
    max_quote_age_seconds: int = 60,
    max_slippage_pct: float = 0.005,
    observed_at: datetime | None = None,
    account_orders_complete: bool | None = None,
    open_order_count: int | None = None,
    trigger_reasons: tuple[str, ...] | None = None,
    leaderboard_snapshot: dict[str, Any] | None = None,
    sector_by_symbol: dict[str, str] | None = None,
    pair_correlations: dict[str, dict[str, float]] | None = None,
    daily_config: DailyWorkflowConfig | None = None,
) -> dict[str, Any]:
    policy = daily_config or load_daily_workflow_config()
    now = observed_at or datetime.now(CHINA)
    if observed_at is not None and policy.phase_at(observed_at) not in {"INTRADAY_AM", "INTRADAY_PM"}:
        raise ValueError("intraday workflow is outside the configured trading window")
    if not watchlist:
        raise ValueError("watchlist must not be empty")
    if len(watchlist) > policy.max_positions:
        raise ValueError("watchlist exceeds configured maximum positions")
    if account_orders_complete is False:
        raise RuntimeError("authoritative order history is incomplete")
    if open_order_count is not None:
        if type(open_order_count) is not int or open_order_count < 0:
            raise ValueError("open order count is invalid")
        if open_order_count and policy.require_zero_open_orders_before_replan:
            raise RuntimeError("open orders must be reconciled before replanning")
    requested_symbols = sorted(
        {item.symbol for item in watchlist} | {symbol for symbol, _shares, _price in current.positions}
    )
    quotes = quote_source(requested_symbols)
    quote_by_symbol = {str(item["symbol"]): item for item in quotes}
    valid: list[tuple[CandidateRecord, dict[str, Any]]] = []
    warnings: list[str] = []
    for candidate in watchlist:
        quote = quote_by_symbol.get(candidate.symbol)
        if quote is None:
            warnings.append(f"{candidate.symbol}:quote_missing")
            continue
        as_of = datetime.fromisoformat(str(quote["as_of"]))
        if as_of.tzinfo is None or now - as_of.astimezone(CHINA) > timedelta(seconds=max_quote_age_seconds):
            warnings.append(f"{candidate.symbol}:quote_stale")
            continue
        if quote.get("source") not in {"mootdx", "vnpy", "ninef_official"} or not quote.get("tradeable", True):
            warnings.append(f"{candidate.symbol}:not_tradeable")
            continue
        if float(quote.get("price", 0)) <= 0:
            warnings.append(f"{candidate.symbol}:invalid_price")
            continue
        if quote.get("open") is not None and quote.get("pre_close") is not None:
            previous = float(quote["pre_close"])
            opened = float(quote["open"])
            price = float(quote["price"])
            if previous <= 0:
                warnings.append(f"{candidate.symbol}:invalid_pre_close")
                continue
            if opened / previous - 1 > policy.opening_gap_max:
                warnings.append(f"{candidate.symbol}:opening_gap_blocked")
                continue
            if price / previous - 1 < policy.current_change_min:
                warnings.append(f"{candidate.symbol}:current_decline_blocked")
                continue
        valid.append((candidate, quote))
    if not valid:
        raise RuntimeError("no fresh tradeable realtime quotes; daily data is not a realtime fallback")

    max_single_weight = min(max_single_weight, policy.max_single_weight)
    if not policy.cash_ratio_min <= cash_weight <= policy.cash_ratio_max:
        raise ValueError("cash weight is outside the configured daily range")
    per_symbol = min(max_single_weight, (1.0 - cash_weight) / len(valid))
    weights = tuple((item.symbol, per_symbol) for item, _ in valid)
    actual_cash = 1.0 - sum(value for _, value in weights)
    if sector_by_symbol is not None:
        sector_weights: dict[str, float] = {}
        for symbol, weight in weights:
            sector = sector_by_symbol.get(symbol)
            if not sector:
                raise ValueError(f"sector evidence is missing for {symbol}")
            sector_weights[sector] = sector_weights.get(sector, 0.0) + weight
        if any(weight > policy.max_sector_weight + 1e-12 for weight in sector_weights.values()):
            raise ValueError("target exceeds configured sector concentration")
    if pair_correlations is not None:
        selected = [symbol for symbol, _weight in weights]
        for index, left in enumerate(selected):
            for right in selected[index + 1:]:
                value = pair_correlations.get(left, {}).get(right)
                if value is None:
                    value = pair_correlations.get(right, {}).get(left)
                if value is None:
                    raise ValueError(f"correlation evidence is missing for {left}/{right}")
                if value > policy.max_pair_correlation:
                    raise ValueError(f"target exceeds correlation cap for {left}/{right}")
    target = TargetWeights(
        as_of=now,
        cash_weight=actual_cash,
        weights=weights,
        max_single_weight=max_single_weight,
        snapshot_id=snapshot_id,
        source="intraday_equal_weight_v1",
    )
    current_shares = {symbol: shares for symbol, shares, _price in current.positions}
    target_map = dict(weights)
    prices = {item.symbol: float(quote["price"]) for item, quote in valid}
    intents: list[OrderIntent] = []
    all_symbols = sorted(set(current_shares) | set(target_map))
    trading_triggered = trigger_reasons is None or bool(trigger_reasons)
    for symbol in all_symbols:
        price = prices.get(symbol)
        if price is None:
            warnings.append(f"{symbol}:cannot_rebalance_without_realtime_price")
            continue
        quote = quote_by_symbol[symbol]
        desired = int(current.total_assets * target_map.get(symbol, 0.0) / price / 100) * 100
        delta = desired - current_shares.get(symbol, 0)
        if delta == 0:
            continue
        side = "BUY" if delta > 0 else "SELL"
        if side == "BUY" and quote.get("is_limit_up"):
            warnings.append(f"{symbol}:limit_up_buy_blocked")
            continue
        if side == "SELL" and quote.get("is_limit_down"):
            warnings.append(f"{symbol}:limit_down_sell_blocked")
            continue
        quantity = abs(delta) // 100 * 100
        if quantity == 0:
            warnings.append(f"{symbol}:below_lot_size")
            continue
        if not trading_triggered:
            warnings.append(f"{symbol}:no_material_trigger")
            continue
        intents.append(
            OrderIntent(
                as_of=now,
                symbol=symbol,
                side=side,
                quantity=quantity,
                target_weight=target_map.get(symbol, 0.0),
                reference_price=price,
                max_slippage_pct=max_slippage_pct,
                target_id=target.target_id,
                reason="rebalance toward validated target weight",
            )
        )

    run_dir = Path(artifact_root).resolve() / run_id
    if run_dir.exists():
        raise FileExistsError(f"workflow artifact already exists: {run_dir}")
    run_dir.mkdir(parents=True)
    write_json(run_dir, "quotes.json", quotes)
    write_json(run_dir, "target_weights.json", target.to_dict())
    write_json(run_dir, "order_intents.json", [item.to_dict() for item in intents])
    decision = {
        "schema_version": "competition.intraday-decision.v1",
        "as_of": now.isoformat(),
        "decision_state": "PLAN_DRAFT",
        "trigger_reasons": list(trigger_reasons or ()),
        "material_trigger": trading_triggered,
        "leaderboard_snapshot": leaderboard_snapshot,
        "account_orders_complete": account_orders_complete,
        "open_order_count": open_order_count,
        "intent_count": len(intents),
        "approval_required": True,
        "execution_ready": False,
        "unknown_policy": policy.unknown_policy,
        "sector_evidence_available": sector_by_symbol is not None,
        "correlation_evidence_available": pair_correlations is not None,
    }
    write_json(run_dir, "intraday-decisions.json", decision)
    write_json(run_dir, "risk-gates.json", {
        "max_positions": policy.max_positions,
        "max_single_weight": policy.max_single_weight,
        "cash_ratio_range": [policy.cash_ratio_min, policy.cash_ratio_max],
        "max_quote_age_seconds": policy.max_quote_age_seconds,
        "opening_gap_max": policy.opening_gap_max,
        "current_change_min": policy.current_change_min,
        "buy_lot": policy.buy_lot,
        "max_sector_weight": policy.max_sector_weight,
        "max_pair_correlation": policy.max_pair_correlation,
        "sector_evidence_available": sector_by_symbol is not None,
        "correlation_evidence_available": pair_correlations is not None,
        "orders_complete": account_orders_complete,
        "open_order_count": open_order_count,
    })
    manifest = {
        "run_id": run_id,
        "phase": "intraday",
        "status": "complete",
        "started_at": now.isoformat(),
        "finished_at": datetime.now(CHINA).isoformat(),
        "fallback_used": bool(warnings),
        "warnings": warnings,
        "fresh_quote_count": len(valid),
        "target_count": len(weights),
        "intent_count": len(intents),
        "execution_mode": "MANUAL_CONFIRM",
        "decision_state": "PLAN_DRAFT",
        "approval_required": True,
        "execution_ready": False,
        "material_trigger": trading_triggered,
    }
    write_json(run_dir, "run_manifest.json", manifest)
    repository.put_target(target)
    repository.put_intents(tuple(intents))
    repository.put_run(run_id, "intraday", "complete", bool(warnings), str(run_dir), manifest)
    return {"manifest": manifest, "target": target, "intents": tuple(intents),
            "decision": decision}
