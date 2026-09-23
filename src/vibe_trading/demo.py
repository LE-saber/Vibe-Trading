"""Deterministic synthetic provider. No network, real AI, real news or ninef.

Format-valid symbols identify fixtures; fees/rules are hypothetical, not market
rules. This tests integration, not alpha quality or actual provider behavior.
"""
from __future__ import annotations

import threading
import time
from collections.abc import Sequence
from datetime import timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from .models import (
    AccountSnapshot, BrokerOrder, Capabilities, FeeSchedule, InstrumentRule,
    Leaderboard, NewsItem, OrderStatus, Position, ProposedOrder, Quote, RankingEntry,
    ResearchRequest, ResearchResult, Side, Target, TradingContext, digest, now_utc,
)
from .news import review_context
from .planner import estimate_fee
from .ports import Cancelled, Progress
from .progress import RunCancelled
from .store import Store

PRICES = {"600001.SH": Decimal("20"), "000001.SZ": Decimal("10"),
          "600002.SH": Decimal("12"), "000002.SZ": Decimal("25")}


class DemoProvider:
    def __init__(self, store: Store, delay: float = 0.025):
        self.store, self.delay = store, delay
        self.lock = threading.RLock()
        try:
            store.get("demo", "account")
        except KeyError:
            store.put("demo", "account", {"cash": "55000", "frozen": "0", "positions": {
                "600001.SH": {"quantity": 1500, "sellable": 1500, "frozen": 0},
                "000001.SZ": {"quantity": 1500, "sellable": 1500, "frozen": 0},
            }})

    def capabilities(self) -> Capabilities:
        return Capabilities(provider="demo", environment="DEMO", account=True, rankings=True,
                            news=True, buy=True, sell=True, cancel=True, verified=True,
                            verification_ref="fixture-v1-NOT-ninef")

    def context(self, symbols: Sequence[str]) -> TradingContext:
        with self.lock:
            state = self.store.get("demo", "account")
            now = now_utc()
            today = now.astimezone(ZoneInfo("Asia/Shanghai")).date()
            positions = tuple(Position(symbol=s, name=f"DEMO {s}", quantity=p["quantity"],
                                       sellable_quantity=p["sellable"], frozen_quantity=p["frozen"],
                                       market_value=PRICES[s]*p["quantity"])
                              for s, p in sorted(state["positions"].items()) if p["quantity"])
            orders = tuple(BrokerOrder.model_validate(item["order"])
                           for item in self.store.list("demo_order", limit=-1))
            cash, frozen = Decimal(state["cash"]), Decimal(state["frozen"])
            nav = cash + frozen + sum((p.market_value for p in positions), Decimal(0))
            fees = FeeSchedule(version="fixture-fees-v1", commission_rate="0.0003",
                               minimum_commission="5", buy_tax_rate="0", sell_tax_rate="0.0005",
                               transfer_rate="0.00001", valid_from=today, valid_until=today,
                               source_ref="DEMO: hypothetical fees, not a market rule", verified=True)
            return TradingContext(
                account=AccountSnapshot(account_id="DEMO_ACCOUNT", provider="demo", as_of=now,
                                        collection_started_at=now, collected_at=now, total_assets=nav,
                                        available_cash=cash, frozen_cash=frozen, positions=positions,
                                        orders=orders, orders_complete=True, consistent=True,
                                        source_ref="DEMO: synthetic local account"),
                quotes=tuple(Quote(symbol=s, as_of=now, last=p, limit_down=p*Decimal("0.9"),
                                   limit_up=p*Decimal("1.1"), tradable=True, can_buy=True,
                                   can_sell=True, source_ref="DEMO: synthetic quote") for s, p in PRICES.items()),
                rules=tuple(InstrumentRule(symbol=s, buy_lot=100, sell_lot=100,
                                           allow_odd_lot_liquidation=True, tick="0.01",
                                           valid_from=today, valid_until=today,
                                           source_ref="DEMO: hypothetical instrument rule", verified=True) for s in PRICES),
                fees=fees, market_open=True, trading_date=today)

    def leaderboard(self) -> Leaderboard:
        return Leaderboard(competition_id="DEMO_COMPETITION", as_of=now_utc(),
                           entries=tuple(RankingEntry(participant_id=f"demo-{i}", display_name=f"DEMO Player {i:02}",
                                                     rank=i, return_pct=Decimal("12.5")-Decimal(i)/10,
                                                     total_assets=Decimal("100000") + Decimal(31-i)*Decimal("1000"))
                                         for i in range(1, 31)),
                           own=RankingEntry(participant_id="demo-owner", display_name="DEMO You", rank=117,
                                            return_pct="2.4", total_assets="100000"),
                           source_ref="DEMO: invented top-30, assets and own rank")

    def news(self) -> tuple[NewsItem, ...]:
        now = now_utc()
        return tuple(NewsItem(news_id=f"fixture-news-{i}", title=title, body=body,
                              symbols=symbols, published_at=now-timedelta(hours=i), fetched_at=now,
                              source="DEMO FIXTURE", source_ref=f"fixture://news/{i}")
                     for i, (title, body, symbols) in enumerate([
                         ("DEMO / market briefing", "Synthetic news for integration testing, not investment information.", ()),
                         ("DEMO / company disclosure", "Fabricated disclosure to test symbol matching and citations.", ("600002.SH",)),
                         ("DEMO / holding review", "Fixture news about a current holding, included beyond new candidates.", ("000001.SZ",)),
                     ], 1))

    def research(self, request: ResearchRequest, report: Progress, cancelled: Cancelled) -> ResearchResult:
        candidates = tuple(PRICES)
        for stage, count in (("S0", 30), ("S1", 20), ("S2", 12), ("S3", 8), ("S4", 4), ("S5", 4), ("S5.5", 4)):
            report(stage, 0, count, f"DEMO fixture work: {stage}")
            for index in range(count):
                if cancelled():
                    raise RunCancelled()
                digest({"stage": stage, "item": index, "symbols": candidates})
                if self.delay:
                    time.sleep(self.delay)
                report(stage, index+1, count, f"DEMO items checked: {index+1}/{count}; not real AI")
        allowed = set(candidates) | {p.symbol for p in request.account.positions}
        news = review_context(request.news, allowed, request.requested_at)
        evidence_ids = ("fixture-quant",) + tuple(n.news_id for n in news["news"])
        return ResearchResult(run_id=request.request_id, as_of=now_utc(), candidates=candidates,
                              targets=(
                                  Target(symbol="600001.SH", name="DEMO 600001.SH", weight="0.20", verdict="REDUCE", reason="DEMO: reduce existing concentration.", evidence_ids=("fixture-quant",)),
                                  Target(symbol="000001.SZ", name="DEMO 000001.SZ", weight="0", verdict="EXIT", reason="DEMO: explicit exit review, not a missing target.", evidence_ids=evidence_ids),
                                  Target(symbol="600002.SH", name="DEMO 600002.SH", weight="0.25", verdict="KEEP", reason="DEMO: candidate with attached news evidence.", evidence_ids=evidence_ids),
                                  Target(symbol="000002.SZ", name="DEMO 000002.SZ", weight="0.20", verdict="KEEP", reason="DEMO: diversify and retain cash reserve.", evidence_ids=("fixture-quant",))),
                              evidence_ids=evidence_ids, artifact_hash=digest({"fixture": "v1", "news": news}),
                              strategy_version="DEMO-NOT-AN-ALPHA-STRATEGY", news_context_hash=news["news_hash"], demo=True)

    def submit(self, order: ProposedOrder, client_order_id: str) -> BrokerOrder:
        with self.lock:
            for old in self.store.list("demo_order", limit=-1):
                if old["order"]["client_order_id"] == client_order_id:
                    return BrokerOrder.model_validate(old["order"])
            context = self.context([order.symbol])
            state = self.store.get("demo", "account")
            cash, frozen = Decimal(state["cash"]), Decimal(state["frozen"])
            notional = order.limit_price*order.quantity
            fee = estimate_fee(notional, order.side, context.fees)
            position = state["positions"].setdefault(order.symbol, {"quantity": 0, "sellable": 0, "frozen": 0})
            if order.side == Side.BUY:
                if cash < notional + fee:
                    raise ValueError("demo insufficient cash")
                state["cash"], state["frozen"] = str(cash-notional-fee), str(frozen+notional+fee)
            else:
                if position["sellable"] < order.quantity:
                    raise ValueError("demo insufficient sellable")
                position["sellable"] -= order.quantity
                position["frozen"] += order.quantity
            receipt = BrokerOrder(order_id=f"demo-{client_order_id}", symbol=order.symbol, side=order.side,
                                  quantity=order.quantity, filled_quantity=0, limit_price=order.limit_price,
                                  status=OrderStatus.PENDING, updated_at=now_utc(), client_order_id=client_order_id)
            self.store.put("demo", "account", state)
            self.store.put("demo_order", receipt.order_id, {"order": receipt, "reads": 0, "reserved_fee": fee})
            return receipt

    def get_order(self, order_id: str) -> BrokerOrder:
        with self.lock:
            record = self.store.get("demo_order", order_id)
            order = BrokerOrder.model_validate(record["order"])
            if not order.is_open:
                return order
            record["reads"] += 1
            filled = order.quantity if record["reads"] >= 2 else max(1, order.quantity//2)
            added = filled - order.filled_quantity
            fees = self.context([order.symbol]).fees
            new_fee = estimate_fee(order.limit_price*filled, order.side, fees)
            fee_increment = new_fee - (order.fee_paid or Decimal(0))
            state = self.store.get("demo", "account")
            p = state["positions"][order.symbol]
            if order.side == Side.BUY:
                p["quantity"] += added  # New shares remain unsellable in this fixture session.
                state["frozen"] = str(Decimal(state["frozen"])-order.limit_price*added-fee_increment)
            else:
                p["quantity"] -= added
                p["frozen"] -= added
                state["cash"] = str(Decimal(state["cash"])+order.limit_price*added-fee_increment)
            order = BrokerOrder.model_validate(order.model_dump() | {
                "filled_quantity": filled, "average_fill_price": order.limit_price, "fee_paid": new_fee,
                "status": OrderStatus.FILLED if filled == order.quantity else OrderStatus.PARTIALLY_FILLED,
                "updated_at": now_utc()})
            record["order"] = order
            self.store.put("demo", "account", state)
            self.store.put("demo_order", order_id, record)
            return order

    def cancel(self, order_id: str) -> None:
        with self.lock:
            record = self.store.get("demo_order", order_id)
            order = BrokerOrder.model_validate(record["order"])
            if not order.is_open:
                return
            state = self.store.get("demo", "account")
            remaining = order.quantity - order.filled_quantity
            if order.side == Side.BUY:
                release = order.limit_price*remaining+Decimal(record["reserved_fee"])-(order.fee_paid or Decimal(0))
                state["cash"] = str(Decimal(state["cash"])+release)
                state["frozen"] = str(Decimal(state["frozen"])-release)
            else:
                p = state["positions"][order.symbol]
                p["sellable"] += remaining
                p["frozen"] -= remaining
            record["order"] = BrokerOrder.model_validate(order.model_dump() | {
                "status": OrderStatus.CANCELLED, "updated_at": now_utc()})
            self.store.put("demo", "account", state)
            self.store.put("demo_order", order_id, record)
