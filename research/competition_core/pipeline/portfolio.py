"""Transparent, deterministic S6 allocation. Produces drafts, never orders.

Current cash funds buys; projected sell proceeds are disclosed separately and
are not spent before fills. Missing account, fees, status or dated prices yield
an observation-only result without executable quantities.
"""

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from copy import deepcopy
import re

from contracts.symbol import CanonicalSymbol
from competition_core.pipeline.account import AccountSnapshot, assess_account
from competition_core.pipeline.artifacts import content_hash
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.instruments import quantity_rules
from competition_core.pipeline.holding_review_consumer import disposition_for
from competition_core.pipeline.rebalance_quantity import _decimal, plan_quantity
from competition_core.pipeline.realtime_gate import evaluate_realtime_quotes
from competition_core.pipeline.time_context import SHANGHAI

CENT = Decimal("0.01")


def _money(value):
    return format(Decimal(value).quantize(CENT), "f")


def _cluster_key(value):
    # S4 emits positive integer IDs; legacy explicit fixtures use string IDs.
    # Normalize only the counting key, preserving the original S4 metadata.
    if type(value) is int and value > 0:
        return str(value)
    if isinstance(value, str) and value and value == value.strip():
        return value
    raise ValueError("missing cluster")


@dataclass(frozen=True)
class FeeSchedule:
    """User-supplied competition costs, not assumed brokerage/tax defaults."""

    source: str
    as_of: datetime
    valid_through: date
    commission_rate: Decimal
    minimum_commission: Decimal
    buy_other_rate: Decimal
    sell_other_rate: Decimal

    def __post_init__(self):
        if not isinstance(self.source, str) or not self.source.strip() or len(self.source) > 160:
            raise ValueError("fee source is required")
        if not isinstance(self.as_of, datetime) or self.as_of.utcoffset() is None or type(self.valid_through) is not date:
            raise ValueError("fee timestamps must be explicit")
        if self.valid_through < self.as_of.astimezone(SHANGHAI).date():
            raise ValueError("invalid fee validity interval")
        for key in ("commission_rate", "minimum_commission", "buy_other_rate", "sell_other_rate"):
            value = _decimal(getattr(self, key), key)
            if value > (Decimal("10000") if key == "minimum_commission" else Decimal("0.1")):
                raise ValueError("fee outside supported bounds")
            object.__setattr__(self, key, value)

    def charge(self, side, gross):
        if side not in {"BUY", "SELL"}:
            raise ValueError("fee side must be BUY or SELL")
        gross = _decimal(gross, "gross")
        if not gross:
            return Decimal(0)
        # Round each component up; this is a conservative reserve, not a fill.
        commission = max(self.minimum_commission, gross * self.commission_rate).quantize(CENT, rounding=ROUND_CEILING)
        other = (gross * (self.buy_other_rate if side == "BUY" else self.sell_other_rate)).quantize(CENT, rounding=ROUND_CEILING)
        return commission + other

    def to_dict(self):
        return {"source": self.source, "as_of": self.as_of.isoformat(), "valid_through": self.valid_through.isoformat(),
                **{key: str(getattr(self, key)) for key in ("commission_rate", "minimum_commission", "buy_other_rate", "sell_other_rate")}}

    @classmethod
    def from_dict(cls, value):
        fields = {"source", "as_of", "valid_through", "commission_rate", "minimum_commission", "buy_other_rate", "sell_other_rate"}
        if not isinstance(value, dict) or set(value) != fields:
            raise ValueError("explicit fee schedule schema is required")
        return cls(**(value | {"as_of": datetime.fromisoformat(value["as_of"]),
                             "valid_through": date.fromisoformat(value["valid_through"])}))


def _metadata(value, *, symbol, context, observed_at, max_age_seconds):
    """Consume explicit trading metadata; do not reconstruct a backtest engine."""
    if not isinstance(value, dict):
        return "missing_trading_metadata"
    if value.get("symbol", symbol) != symbol:
        return "trading_metadata_identity_conflict"
    try:
        timestamp = datetime.fromisoformat(value["as_of"])
        if (timestamp.utcoffset() is None or timestamp > observed_at
                or (observed_at - timestamp).total_seconds() > max_age_seconds
                or timestamp.astimezone(SHANGHAI).date() != context.research_date
                or value["trade_date"] != context.research_date.isoformat()
                or not isinstance(value["source"], str) or not value["source"].strip()):
            return "unverified_trading_metadata"
        if value.get("valid_until") is not None:
            expires = datetime.fromisoformat(value["valid_until"])
            if expires.utcoffset() is None or observed_at >= expires:
                return "trading_metadata_interval_expired"
        if value["tradable"] is not True or value["suspended"] is not False:
            return "currently_not_tradable"
        if value["new_listing_special_period"] is not False:
            return "new_listing_special_period_requires_review"
        lower, upper = _decimal(value["down_limit"], "down_limit", positive=True), _decimal(value["up_limit"], "up_limit", positive=True)
        if lower >= upper or lower % CENT or upper % CENT:
            return "invalid_price_limits"
    except (KeyError, TypeError, ValueError):
        return "unverified_trading_metadata"
    return None


def _index(rows, *, expected=None):
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise PipelineError("IDENTITY_CONFLICT", "portfolio input must contain explicit rows")
    indexed = {}
    for row in rows:
        symbol = row.get("symbol")
        CanonicalSymbol.from_core(symbol)
        if symbol in indexed:
            raise PipelineError("IDENTITY_CONFLICT", "duplicate portfolio input identity")
        indexed[symbol] = row
    if expected is not None and set(indexed) != set(expected):
        raise PipelineError("IDENTITY_CONFLICT", "portfolio input identity sets differ")
    return indexed


def _holding_dispositions(value, *, account, expected):
    if value is None:
        return {}
    fields = {"schema_version", "account_hash", "account_source_hash", "index_hash",
              "dispositions", "blocking_reasons", "portfolio_authority"}
    if (not isinstance(value, dict) or set(value) != fields
            or value.get("schema_version") != "p10.holding_review_consumption.v1"
            or value.get("portfolio_authority") is not False
            or account is None or value.get("account_hash") != account.snapshot_hash
            or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("account_source_hash")))
            or (value.get("index_hash") is not None
                and not re.fullmatch(r"[0-9a-f]{64}", str(value["index_hash"])))):
        raise PipelineError("STALE_DATA", "holding review consumption is not bound to this account")
    rows = value.get("dispositions")
    indexed = _index(rows, expected=expected)
    blockers = []
    for symbol, row in indexed.items():
        if set(row) != {"symbol", "verdict", "disposition", "attempt_id", "blocker"}:
            raise PipelineError("STALE_DATA", "holding disposition schema is invalid")
        expected_result = disposition_for(symbol=symbol, verdict=row["verdict"],
            attempt_id=row["attempt_id"], status="SUCCESS")
        if row["verdict"] not in {"KEEP", "WATCH", "EXCLUDE", "REVIEW"}:
            expected_result = disposition_for(symbol=symbol, verdict=None,
                attempt_id=row["attempt_id"], status="MISSING")
        if row != expected_result.to_dict():
            raise PipelineError("STALE_DATA", "holding disposition differs from accepted semantics")
        if row["blocker"]:
            blockers.append(row["blocker"])
    if value.get("blocking_reasons") != sorted(blockers):
        raise PipelineError("STALE_DATA", "holding disposition blockers are inconsistent")
    return indexed


def build_portfolio(*, candidates, reviews, quotes, account: AccountSnapshot | None,
                    fees: FeeSchedule | None, trading_metadata, context, config,
                    snapshot_id, observed_at, benchmark_quote=None, holding_dispositions=None):
    """S4-ranked core/aggressive sleeves with inventory-aware cash arithmetic.

    Synthetic accounts can exercise the full math, but never gain production
    eligibility. The caller must bind these inputs to saved stage/source hashes.
    """
    candidates = deepcopy(candidates)
    indexed = _index(candidates)
    if not 10 <= len(indexed) <= 15:
        raise PipelineError("NOT_READY", "portfolio requires 10–15 S4 finalists")
    review_by = _index(reviews, expected=indexed)
    if not isinstance(trading_metadata, dict):
        raise PipelineError("INVALID_ARGUMENTS", "trading metadata must be a mapping")
    assessment = assess_account(account, context=context, config=config, observed_at=observed_at)
    holdings = {p.symbol: p for p in account.positions} if account else {}
    outside_symbols = set(holdings) - set(indexed)
    holding_disposition_by = _holding_dispositions(
        holding_dispositions, account=account, expected=outside_symbols)
    symbols = sorted(set(indexed) | set(holdings))
    gate = evaluate_realtime_quotes(symbols=symbols, quotes=quotes, context=context, config=config,
                                    observed_at=observed_at, snapshot_id=snapshot_id,
                                    benchmark_quote=benchmark_quote)
    quote_by = {row["symbol"]: row for row in gate["rows"]}
    risk, settings = config.section("risk"), config.section("portfolio")
    blockers = [reason for reason in assessment["reasons"] if reason != "SYNTHETIC_ACCOUNT_NOT_PRODUCTION"]
    if fees is None:
        blockers.append("MISSING_COMPETITION_FEE_SCHEDULE")
    elif fees.as_of > observed_at or not fees.as_of.astimezone(SHANGHAI).date() <= context.research_date <= fees.valid_through:
        blockers.append("FEE_SCHEDULE_DATE_MISMATCH")
    if context.phase != "intraday":
        blockers.append("INTRADAY_REQUIRED")
    eligible, retained, rows = [], set(), []
    for symbol in symbols:
        candidate, review, quote = indexed.get(symbol), review_by.get(symbol), quote_by[symbol]
        reasons = []
        if not quote["execution_eligible"]:
            reasons.append("realtime_not_pass")
        meta_problem = _metadata(trading_metadata.get(symbol), symbol=symbol, context=context, observed_at=observed_at,
                                 max_age_seconds=config.section("realtime")["max_quote_age_seconds"])
        if quote["execution_eligible"]:
            try:
                price = _decimal(quote["quote"]["price"], "reference_price", positive=True)
                if price % CENT:
                    raise ValueError("off tick")
                if meta_problem is None and not Decimal(str(trading_metadata[symbol]["down_limit"])) <= price <= Decimal(str(trading_metadata[symbol]["up_limit"])):
                    raise ValueError("reference outside limits")
            except (KeyError, TypeError, ValueError):
                meta_problem = "invalid_reference_price"
        if meta_problem:
            reasons.append(meta_problem)
        if candidate is None:
            disposition = holding_disposition_by.get(symbol)
            if disposition and disposition["disposition"] in {"RETAIN", "RETAIN_WATCH"}:
                retained.add(symbol)
            else:
                reasons.append("holding_outside_s4")
                blockers.append((disposition or {}).get("blocker")
                                or f"MISSING_HOLDING_RESEARCH_COVERAGE:{symbol}")
        else:
            try:
                for key in ("quant_score", "selection_score", "overheat_score"):
                    if _decimal(candidate[key], key) > 100:
                        raise ValueError("score range")
                _cluster_key(candidate.get("correlation_cluster"))
                # An unknown pair must not masquerade as independent returns.
                correlations = candidate.get("correlation_to_selected", {})
                if any(value is None for value in correlations.values()):
                    raise ValueError("unknown correlation")
            except (KeyError, TypeError, ValueError):
                reasons.append("missing_ranking_or_cluster_metadata")
            if (review.get("ai_status") != "SUCCESS" or review.get("ai_eligible") is not True
                    or review.get("ai_verdict") not in {"KEEP", "WATCH"}):
                reasons.append("ai_review_not_eligible")
            if any(review.get(key) != candidate.get(key) for key in ("quant_score", "selection_score")):
                raise PipelineError("STALE_DATA", "AI review changed deterministic scores")
        if symbol in holdings and (not quote["execution_eligible"] or meta_problem):
            blockers.append(f"UNVERIFIED_HELD_INSTRUMENT:{symbol}")
        if symbol in holdings and holdings[symbol].sellable_quantity < holdings[symbol].quantity and reasons:
            blockers.append(f"LOCKED_INELIGIBLE_HOLDING:{symbol}")
        if candidate is not None and not reasons:
            eligible.append(symbol)
        review_body = (review or {}).get("ai_review") or {}
        disposition = holding_disposition_by.get(symbol)
        rows.append({"symbol": symbol, "name": (candidate or {}).get("name"), "selected": False, "rank": None,
                     "current_quantity": holdings[symbol].quantity if symbol in holdings else 0,
                     "sellable_quantity": holdings[symbol].sellable_quantity if symbol in holdings else 0,
                     "target_quantity": None, "delta_quantity": None, "target_weight": None,
                     "reject_reasons": reasons, "keep_reasons": [],
                     "quant_score": (candidate or {}).get("quant_score"),
                     "selection_score": (candidate or {}).get("selection_score"),
                     "ai_verdict": (review or {}).get("ai_verdict") or (disposition or {}).get("verdict"),
                     "holding_disposition": (disposition or {}).get("disposition"),
                     "ai_risks": review_body.get("risks", []),
                     "missing_fields": quote["missing_fields"] + review_body.get("missing_critical_info", []),
                     "realtime_gate": quote["realtime_gate"], "source_provenance": quote["meta"]})
    result = {"stage": "S6", "allocation_version": "inventory_sleeves.v1", "status": "PARTIAL",
              "portfolio_status": "NOT_READY", "input_symbols": symbols, "output_symbols": [], "rows": rows,
              "account_assessment": assessment, "fee_schedule": fees.to_dict() if fees else None,
              "execution_mode": "MANUAL_CONFIRM", "execution_ready": False, "production_accepted": False,
              "meta": context.to_dict() | {"snapshot_id": snapshot_id, "observed_at": observed_at.isoformat()},
              "policy": {"risk": risk, "portfolio": settings, "role_rule": "overheat_threshold",
                         "weight_rule": "equal_within_role_capped_no_cross_role_redistribution",
                         "sell_proceeds_fund_buys": False},
              "risk_checks": {"blocking_reasons": sorted(set(blockers)), "constraints_relaxed": False},
              "summary": {"selected_count": 0, "llm_calls": 0, "agent_tool_calls": 0}}
    if blockers:
        return result
    ordered = sorted(eligible, key=lambda s: (-_decimal(indexed[s]["selection_score"], "score"), s))
    locked = [s for s in ordered if s in holdings and holdings[s].sellable_quantity < holdings[s].quantity]
    ordered = locked + [s for s in ordered if s not in locked]
    selected, clusters, sectors, hot_count = [], Counter(), Counter(), 0
    row_by = {row["symbol"]: row for row in rows}
    for symbol in ordered:
        candidate = indexed[symbol]
        sector = candidate.get("sector_metadata", {})
        sector_key = None
        if sector.get("status") == "available":
            try:
                stamp = datetime.fromisoformat(sector["as_of"])
                if stamp.utcoffset() is None or stamp > context.as_of:
                    raise ValueError("invalid sector time")
                sector_key = tuple(sector[key] for key in ("source", "classification", "value"))
                if not all(isinstance(v, str) and v for v in sector_key):
                    raise ValueError("invalid sector identity")
            except (KeyError, TypeError, ValueError):
                raise PipelineError("STALE_DATA", "invalid verified sector metadata")
        cluster = _cluster_key(candidate["correlation_cluster"])
        hot = _decimal(candidate["overheat_score"], "heat") >= Decimal(str(risk["overheat_threshold"]))
        reasons = row_by[symbol]["reject_reasons"]
        if len(selected) + len(retained) >= risk["max_positions"]:
            reasons.append("position_count_cap")
        if clusters[cluster] >= risk["max_candidates_per_cluster"]:
            reasons.append("correlation_cluster_cap")
        if sector_key and sectors[sector_key] >= risk["max_candidates_per_sector"]:
            reasons.append("verified_sector_cap")
        if hot and hot_count >= risk["max_overheated_candidates"]:
            reasons.append("overheated_style_cap")
        if reasons:
            if symbol in locked:
                blockers.append(f"LOCKED_HOLDING_CONSTRAINT:{symbol}")
            continue
        selected.append(symbol)
        hot_count += int(hot)
        clusters[cluster] += 1
        if sector_key:
            sectors[sector_key] += 1
        row_by[symbol].update(correlation_cluster=candidate["correlation_cluster"], sector_metadata=sector,
                             role="AGGRESSIVE" if hot else "CORE")
    if len(selected) < risk["min_positions"]:
        blockers.append("INSUFFICIENT_ELIGIBLE_POSITIONS")
    if blockers:
        result["risk_checks"]["blocking_reasons"] = sorted(set(blockers))
        return result
    prices = {s: _decimal(quote_by[s]["quote"]["price"], "price", positive=True) for s in set(selected) | set(holdings)}
    marked_asset = account.cash_balance + sum(prices[s] * p.quantity for s, p in holdings.items())
    reserve = (marked_asset * Decimal(str(risk["cash_buffer"]))).quantize(CENT, rounding=ROUND_CEILING)
    investable = marked_asset - reserve
    name_cap = marked_asset * Decimal(str(risk["single_name_cap"]))
    role_counts = Counter(row_by[s]["role"] for s in selected)
    budgets = {}
    for s in selected:
        role = row_by[s]["role"]
        share = Decimal(str(settings["core_share" if role == "CORE" else "aggressive_share"]))
        budgets[s] = min(name_cap, investable * share / role_counts[role]).quantize(CENT, rounding=ROUND_FLOOR)
    # No speculative cash from unfilled sales is used to fund new purchases.
    buy_capacity = max(Decimal(0), min(account.available_cash, account.cash_balance - reserve))
    increments = {s: max(Decimal(0), budgets[s] - prices[s] * (holdings[s].quantity if s in holdings else 0))
                  for s in selected}
    requested_buys = sum(increments.values())
    cash_scale = min(Decimal(1), buy_capacity / requested_buys) if requested_buys else Decimal(0)
    buy_cost = sell_net = costs = Decimal(0)
    plans = {}
    processing = selected + [s for s in sorted(holdings) if s not in selected]
    for symbol in processing:
        current = holdings[symbol].quantity if symbol in holdings else 0
        sellable = holdings[symbol].sellable_quantity if symbol in holdings else 0
        current_value = current * prices[symbol]
        value = current_value if symbol in retained else budgets.get(symbol, Decimal(0))
        if value > current_value:
            increment = min((increments[symbol] * cash_scale).quantize(CENT, rounding=ROUND_FLOOR), buy_capacity)
            fee_reserve = fees.charge("BUY", increment)
            value = current_value + max(Decimal(0), increment - fee_reserve)
        rules = quantity_rules(symbol, context.research_date)
        plan = plan_quantity(current_quantity=current, sellable_quantity=sellable, target_value=value,
                             reference_price=prices[symbol], rules=rules,
                             slippage_buffer=Decimal(str(settings["slippage_buffer"])))
        if "single_order_maximum" in plan["reasons"]:
            blockers.append(f"TARGET_REQUIRES_MULTIPLE_ORDERS:{symbol}")
        gross_buy = Decimal(plan["gross_buy_cash"])
        gross_sell = Decimal(plan["gross_sell_proceeds"])
        fee = fees.charge("BUY", gross_buy) + fees.charge("SELL", gross_sell)
        if plan["delta_quantity"]:
            metadata = trading_metadata[symbol]
            limit = Decimal(plan["order_price"])
            if not Decimal(str(metadata["down_limit"])) <= limit <= Decimal(str(metadata["up_limit"])):
                blockers.append(f"ORDER_PRICE_OUTSIDE_LIMITS:{symbol}")
        if gross_buy:
            buy_capacity -= gross_buy + fee
            buy_cost += gross_buy + fee
        if gross_sell:
            sell_net += gross_sell - fee
        costs += fee
        plan.update(estimated_fees=_money(fee), quantity_rules=rules.to_dict(),
                    allocation_budget=_money(budgets.get(symbol, Decimal(0))),
                    reference_time=quote_by[symbol]["meta"]["server_time"],
                    target_weight=float(Decimal(plan["target_market_value"]) / marked_asset))
        plans[symbol] = plan
        if Decimal(plan["target_market_value"]) > name_cap:
            blockers.append(f"SINGLE_NAME_CAP:{symbol}")
        if symbol not in selected and symbol not in retained and plan["target_quantity"]:
            blockers.append(f"UNREMOVED_HOLDING:{symbol}")
    selected_final = [s for s in selected if plans[s]["target_quantity"] > 0]
    final = selected_final + [s for s in sorted(retained) if plans[s]["target_quantity"] > 0]
    if not risk["min_positions"] <= len(final) <= risk["max_positions"]:
        blockers.append("FINAL_POSITION_COUNT")
    remaining_cash = account.cash_balance - buy_cost + sell_net
    if buy_capacity < 0 or remaining_cash < reserve or account.available_cash - buy_cost < 0:
        blockers.append("CASH_CONSTRAINT")
    target_invested = sum(Decimal(p["target_market_value"]) for p in plans.values())
    role_values = {role: sum(Decimal(plans[s]["target_market_value"]) for s in selected_final if row_by[s]["role"] == role)
                   for role in ("CORE", "AGGRESSIVE")}
    for role, value in role_values.items():
        share = Decimal(str(settings["core_share" if role == "CORE" else "aggressive_share"]))
        if value > investable * share:
            blockers.append(f"{role}_BUDGET_CONSTRAINT")
    result["risk_checks"].update(blocking_reasons=sorted(set(blockers)),
        weight_denominator="current_quote_marked_asset_before_estimated_costs",
        current_account_export_asset=_money(account.current_total_asset), marked_asset=_money(marked_asset),
        mark_to_market_change=_money(marked_asset - account.current_total_asset),
        cash_reserve=_money(reserve), required_buy_cash=_money(buy_cost), projected_net_sell_proceeds=_money(sell_net),
        unfilled_sale_proceeds_spent="0.00", available_buy_scale=float(cash_scale),
        cash_remaining=_money(remaining_cash), available_cash_without_sale_fills=_money(account.available_cash - buy_cost),
        cash_ratio=float(remaining_cash / marked_asset), target_invested=_money(target_invested),
        estimated_fees=_money(costs), max_single_weight=max((p["target_weight"] for p in plans.values()), default=0),
        role_values={k: _money(v) for k, v in role_values.items()}, cluster_counts=dict(clusters),
        sector_counts=[{"source": k[0], "classification": k[1], "value": k[2], "count": v} for k, v in sectors.items()],
        sector_missing_count=sum(row_by[s].get("sector_metadata", {}).get("status") != "available" for s in final))
    if blockers:
        # Keep attempted math for diagnostics, not an executable target/order.
        result["risk_checks"]["attempted_plan_hash"] = content_hash(plans)
        return result
    for rank, symbol in enumerate(final, 1):
        keep_reasons = (["supplemental_holding_review_retain", "inventory_and_cash_checked"]
                        if symbol in retained else
                        ["s4_rank_within_constraints", "ai_and_realtime_pass", "inventory_and_cash_checked"])
        row_by[symbol].update(selected=True, rank=rank, keep_reasons=keep_reasons)
        if symbol in locked:
            row_by[symbol]["keep_reasons"].append("locked_quantity_requires_retention")
    for symbol, plan in plans.items():
        row_by[symbol].update(plan)
    synthetic = account.source_kind == "synthetic_test"
    result.update(status="SUCCESS", portfolio_status="DRY_RUN" if synthetic else "DRAFT_READY",
                  output_symbols=final, execution_ready=not synthetic)
    result["summary"].update(selected_count=len(final), account_hash=account.snapshot_hash,
                             fee_schedule_hash=content_hash(fees.to_dict()), requires_manual_confirmation=True,
                             must_revalidate_before_any_order=True, orders_submitted=0)
    return result
