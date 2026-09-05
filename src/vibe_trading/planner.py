"""Deterministic current -> target -> delta with explicit execution blockers."""
from datetime import datetime, timedelta
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from uuid import uuid4
from zoneinfo import ZoneInfo

from .models import (
    Blocker, FeeSchedule, Plan, Policy, ProposedOrder, ResearchResult,
    Side, TargetPosition, TradingContext, digest,
)
from .reconciliation import account_blockers

ZERO = Decimal(0)
CENT = Decimal("0.01")


def estimate_fee(notional: Decimal, side: Side, fees: FeeSchedule) -> Decimal:
    if not notional:
        return ZERO
    tax = fees.buy_tax_rate if side == Side.BUY else fees.sell_tax_rate
    amount = max(fees.minimum_commission, notional * fees.commission_rate)
    return (amount + notional * (tax + fees.transfer_rate)).quantize(CENT, rounding=ROUND_CEILING)


def validate_market(context: TradingContext, symbols: set[str], now: datetime,
                    policy: Policy) -> list[Blocker]:
    issues: list[Blocker] = []
    def add(code: str, detail: str, symbol: str | None = None) -> None:
        issues.append(Blocker(code=code, detail=detail, symbol=symbol))
    if not context.market_open:
        add("MARKET_CLOSED", "An authoritative open-session observation is required.")
    if context.trading_date != now.astimezone(ZoneInfo("Asia/Shanghai")).date():
        add("WRONG_TRADING_DATE", "Context does not belong to the current Shanghai date.")
    if not context.fees.valid_from <= context.trading_date <= context.fees.valid_until:
        add("EXPIRED_FEES", "No effective verified fee schedule.")
    quotes, rules = {q.symbol: q for q in context.quotes}, {r.symbol: r for r in context.rules}
    for symbol in sorted(symbols):
        q, r = quotes.get(symbol), rules.get(symbol)
        if q is None or r is None:
            add("MISSING_MARKET_INPUT", "Quote and instrument rule are required.", symbol)
            continue
        age = (now - q.as_of).total_seconds()
        if age < -2 or age > policy.max_quote_age_seconds:
            add("STALE_QUOTE", "Quote is stale or in the future.", symbol)
        if not q.tradable:
            add("NOT_TRADABLE", "Instrument is suspended or not tradable.", symbol)
        if not r.valid_from <= context.trading_date <= r.valid_until:
            add("EXPIRED_RULE", "Instrument rule is not effective.", symbol)
    return issues


def build_plan(result: ResearchResult, context: TradingContext, now: datetime,
               policy: Policy | None = None) -> Plan:
    policy = policy or Policy()
    positions = {p.symbol: p for p in context.account.positions if p.quantity}
    targets = {t.symbol: t for t in result.targets}
    symbols = set(positions) | set(targets)
    issues = account_blockers(context.account, now, policy)
    issues.extend(validate_market(context, symbols, now, policy))
    warnings: list[str] = []
    orders: list[ProposedOrder] = []
    target_rows: list[TargetPosition] = []
    def add(code: str, detail: str, symbol: str | None = None) -> None:
        issues.append(Blocker(code=code, detail=detail, symbol=symbol))
    age = (now - result.as_of).total_seconds()
    if age < -2 or age > policy.max_research_age_seconds:
        add("STALE_RESEARCH", "Research must be refreshed.")
    if set(targets) - (set(result.candidates) | set(positions)):
        add("UNREVIEWED_SYMBOL", "A target was not in the candidate/holding universe.")
    for symbol in set(positions) - set(targets):
        add("UNREVIEWED_HOLDING", "Missing holding review; never silently liquidate it.", symbol)
    quotes, rules = {q.symbol: q for q in context.quotes}, {r.symbol: r for r in context.rules}
    nav = context.account.total_assets
    for symbol in sorted(symbols):
        target, position = targets.get(symbol), positions.get(symbol)
        current = position.quantity if position else 0
        if target is None:
            continue
        quote, rule = quotes.get(symbol), rules.get(symbol)
        if quote is None or rule is None:
            continue
        if target.weight > policy.max_position_weight:
            add("POSITION_CAP", "Target exceeds maximum weight.", symbol)
        price = (quote.last / rule.tick).to_integral_value(rounding=ROUND_FLOOR) * rule.tick
        if price <= 0:
            add("INVALID_PRICE", "Tick conversion produced an invalid limit.", symbol)
            continue
        if not quote.limit_down <= price <= quote.limit_up:
            add("PRICE_BAND", "Rounded limit is outside the quote band.", symbol)
        requested = int((nav * target.weight / price / rule.buy_lot)
                        .to_integral_value(rounding=ROUND_FLOOR)) * rule.buy_lot
        planned = requested
        if target.verdict in ("WATCH", "REJECT") and requested > current:
            add("VERDICT_NOT_BUYABLE", "WATCH/REJECT is not permission to buy.", symbol)
        if target.verdict in ("EXIT", "REJECT") and target.weight:
            add("VERDICT_WEIGHT_CONFLICT", "EXIT/REJECT must have zero weight.", symbol)
        if target.verdict == "REDUCE" and requested > current:
            add("VERDICT_WEIGHT_CONFLICT", "REDUCE cannot increase a position.", symbol)
        delta = requested - current
        if delta > 0:
            delta = (delta // rule.buy_lot) * rule.buy_lot
            planned = current + delta
            if delta and not quote.can_buy:
                add("BUY_NOT_ALLOWED", "Provider reports a buy restriction.", symbol)
        elif delta < 0:
            desired_sell = -delta
            if desired_sell > (position.sellable_quantity if position else 0):
                add("INSUFFICIENT_SELLABLE", "T+1/frozen shares prevent this reduction.", symbol)
            if not (requested == 0 and rule.allow_odd_lot_liquidation):
                desired_sell = (desired_sell // rule.sell_lot) * rule.sell_lot
            delta = -desired_sell
            planned = current + delta
            if delta and not quote.can_sell:
                add("SELL_NOT_ALLOWED", "Provider reports a sell restriction.", symbol)
        if planned != requested:
            warnings.append(f"{symbol}: lot rounding leaves {planned} instead of {requested} shares.")
        if Decimal(planned) * quote.last / nav > policy.max_position_weight:
            add("POST_TRADE_POSITION_CAP", "Rounded position exceeds the cap.", symbol)
        target_rows.append(TargetPosition(symbol=symbol, current_quantity=current,
                                         requested_quantity=requested, planned_quantity=planned,
                                         weight=target.weight, reason=target.reason))
        if delta:
            side = Side.BUY if delta > 0 else Side.SELL
            orders.append(ProposedOrder(symbol=symbol, side=side, quantity=abs(delta),
                                        limit_price=price,
                                        estimated_fee=estimate_fee(abs(delta)*price, side, context.fees),
                                        current_quantity=current, target_quantity=planned,
                                        reason=target.reason))
    orders.sort(key=lambda o: (0 if o.side == Side.SELL else 1, o.symbol))
    cash = context.account.available_cash
    for order in orders:
        notional = order.limit_price * order.quantity
        cash += (notional if order.side == Side.SELL else -notional) - order.estimated_fee
    floor = (nav * policy.cash_reserve).quantize(CENT, rounding=ROUND_CEILING)
    if cash < floor:
        add("CASH_RESERVE", "Projected cash is below reserve; reduce targets.")
    if any(o.side == Side.SELL for o in orders):
        warnings.append("Sales are projected only. Buys wait for confirmed fills and reconciled cash.")
    return Plan(plan_id=uuid4().hex, run_id=result.run_id, created_at=now,
                expires_at=now + timedelta(seconds=policy.plan_lifetime_seconds),
                research_hash=digest(result), context=context, policy=policy,
                targets=tuple(target_rows), orders=tuple(orders), blockers=tuple(issues),
                warnings=tuple(warnings), projected_cash=cash, cash_floor=floor, demo=result.demo)
