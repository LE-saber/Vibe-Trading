"""Durable approval/dispatch/reconciliation. NOT an exactly-once network protocol.

An uncertain write blocks the account. No blind retries, heuristic matching,
unaudited unlock or arbitrary-order API. Local code/database administrators are
trusted; hashes are integrity identifiers, not a defence against that owner.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

from .models import (
    TERMINAL, Approval, BrokerOrder, OperatorResolution, OrderStatus, Plan, ProposedOrder, Side,
    account_state_hash, canonical, digest, now_utc,
)
from .planner import estimate_fee, validate_market
from .ports import Provider
from .reconciliation import account_blockers
from .store import Store


class GateError(Exception):
    def __init__(self, code: str, detail: str):
        self.code, self.detail = code, detail
        super().__init__(detail)


class ExecutionService:
    def __init__(self, store: Store, provider: Provider, enabled: bool):
        self.store, self.provider, self.enabled = store, provider, enabled

    def _verified_plan(self, plan_id: str, conn: sqlite3.Connection | None = None) -> tuple[Plan, str]:
        if conn is None:
            doc = self.store.get("plan", plan_id)
        else:
            row = conn.execute("SELECT body FROM documents WHERE kind='plan' AND id=?", (plan_id,)).fetchone()
            if not row:
                raise KeyError(plan_id)
            doc = json.loads(row["body"])
        raw_plan, stored_hash = doc["plan"], doc["hash"]
        if digest(raw_plan) != stored_hash:
            raise GateError("PLAN_CORRUPTED", "Plan integrity check failed.")
        return Plan.model_validate(raw_plan), stored_hash

    def _verified_batch_plan(self, conn: sqlite3.Connection, batch_id: str) -> tuple[sqlite3.Row, Approval, Plan, str]:
        batch = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        if not batch:
            raise KeyError(batch_id)
        approval_row = conn.execute(
            "SELECT id,body FROM approvals WHERE id=?", (batch["approval_id"],)
        ).fetchone()
        if not approval_row:
            raise GateError("PLAN_CHANGED", "Approved plan failed integrity validation.")
        approval = Approval.model_validate_json(approval_row["body"])
        plan, stored_hash = self._verified_plan(batch["plan_id"], conn)
        if (approval.approval_id != approval_row["id"] or
                approval.approval_id != batch["approval_id"] or
                approval.plan_id != batch["plan_id"] or
                approval.plan_hash != stored_hash):
            raise GateError("PLAN_CHANGED", "Approved plan failed integrity validation.")
        return batch, approval, plan, stored_hash

    def plan(self, plan_id: str) -> Plan:
        plan, _ = self._verified_plan(plan_id)
        return plan

    def check_enabled(self) -> None:
        cap = self.provider.capabilities()
        if not self.enabled or not cap.verified or not cap.verification_ref or not (cap.buy and cap.sell):
            raise GateError("EXECUTION_DISABLED", "Local writes are disabled or capabilities are unverified.")

    def approve(self, plan_id: str, expected_hash: str, actor: str,
                now: datetime | None = None) -> Approval:
        now = now or now_utc()
        self.check_enabled()
        plan, stored_hash = self._verified_plan(plan_id)
        if stored_hash != expected_hash:
            raise GateError("PLAN_CHANGED", "Displayed plan does not match stored plan.")
        run = self.store.get("run", plan.run_id)
        if run["state"] != "COMPLETED":
            raise GateError("RESEARCH_NOT_COMPLETE", "Only completed research may be approved.")
        if run.get("plan_id") != plan.plan_id:
            raise GateError("PLAN_SUPERSEDED", "Review the latest plan for this research run.")
        if plan.demo != (self.provider.capabilities().environment == "DEMO"):
            raise GateError("MODE_MISMATCH", "Plan and provider environment differ.")
        if plan.blockers or not plan.orders:
            raise GateError("NOT_EXECUTABLE", "Blocked or empty plan cannot be approved.")
        if now >= plan.expires_at:
            raise GateError("PLAN_EXPIRED", "Refresh the plan before approval.")
        approval = Approval(approval_id=uuid4().hex, plan_id=plan_id, plan_hash=stored_hash,
                            actor=actor, approved_at=now, expires_at=plan.expires_at)
        with self.store.transaction() as conn:
            row = conn.execute("SELECT body FROM approvals WHERE plan_id=?", (plan_id,)).fetchone()
            if row:
                existing = Approval.model_validate_json(row[0])
                if existing.plan_id != plan_id or existing.plan_hash != stored_hash:
                    raise GateError("PLAN_CHANGED", "Approved plan failed integrity validation.")
                return existing
            conn.execute("INSERT INTO approvals VALUES(?,?,?)", (approval.approval_id, plan_id, canonical(approval)))
            self.store.event_in(conn, plan_id, "approved", approval)
        return approval

    def start(self, approval_id: str, now: datetime | None = None) -> str:
        now = now or now_utc()
        self.check_enabled()
        with self.store.connect() as conn:
            row = conn.execute("SELECT body FROM approvals WHERE id=?", (approval_id,)).fetchone()
            existing = conn.execute("SELECT id FROM batches WHERE approval_id=?", (approval_id,)).fetchone()
        if existing:
            with self.store.transaction() as conn:
                self._verified_batch_plan(conn, existing["id"])
            return existing[0]
        if not row:
            raise GateError("APPROVAL_REQUIRED", "No authenticated approval exists.")
        approval = Approval.model_validate_json(row[0])
        plan, stored_hash = self._verified_plan(approval.plan_id)
        if self.store.get("run", plan.run_id).get("plan_id") != plan.plan_id:
            raise GateError("PLAN_SUPERSEDED", "This approval belongs to a superseded plan.")
        if stored_hash != approval.plan_hash or now >= approval.expires_at:
            raise GateError("INVALID_APPROVAL", "Approval changed or expired.")
        current = self.provider.context([o.symbol for o in plan.orders])
        now = max(now, now_utc())
        if now >= approval.expires_at:
            raise GateError("INVALID_APPROVAL", "Approval expired during account refresh.")
        issues = account_blockers(current.account, now, plan.policy)
        if issues or account_state_hash(current.account) != account_state_hash(plan.context.account):
            raise GateError("ACCOUNT_CHANGED", "Refresh the plan against the latest reconciled account.")
        batch_id = uuid4().hex
        with self.store.transaction() as conn:
            existing = conn.execute("SELECT id FROM batches WHERE approval_id=?", (approval_id,)).fetchone()
            if existing:
                self._verified_batch_plan(conn, existing["id"])
                return existing[0]
            try:
                conn.execute("INSERT INTO account_locks VALUES(?,?)", (current.account.account_id, batch_id))
            except sqlite3.IntegrityError as exc:
                raise GateError("ACCOUNT_BUSY", "Another batch owns this account; reconcile it first.") from exc
            conn.execute("INSERT INTO batches(id,approval_id,account_id,plan_id,state,created_at) VALUES(?,?,?,?,?,?)",
                         (batch_id, approval_id, current.account.account_id, plan.plan_id, "QUEUED", now.isoformat()))
            for ordinal, order in enumerate(plan.orders):
                conn.execute("INSERT INTO intents(id,batch_id,ordinal,body) VALUES(?,?,?,?)",
                             (f"{batch_id}:{ordinal}", batch_id, ordinal, canonical(order)))
            self.store.event_in(conn, batch_id, "execution_queued", {"plan_id": plan.plan_id})
        return batch_id

    def snapshot(self, batch_id: str) -> dict:
        with self.store.connect() as conn:
            row = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
            intents = conn.execute("SELECT * FROM intents WHERE batch_id=? ORDER BY ordinal", (batch_id,)).fetchall()
        if not row:
            raise KeyError(batch_id)
        return dict(row) | {"intents": [dict(i) | {"body": json.loads(i["body"]),
                           "receipt": json.loads(i["receipt"]) if i["receipt"] else None} for i in intents]}

    def list(self) -> list[dict]:
        with self.store.connect() as conn:
            rows = conn.execute("SELECT id FROM batches ORDER BY rowid DESC LIMIT 30").fetchall()
        return [self.snapshot(row[0]) for row in rows]

    def active(self) -> list[str]:
        with self.store.connect() as conn:
            return [r[0] for r in conn.execute("SELECT batch_id FROM account_locks").fetchall()]

    def stop(self, batch_id: str) -> None:
        self.snapshot(batch_id)
        with self.store.transaction() as conn:
            changed = conn.execute("UPDATE batches SET stop_requested=1 WHERE id=? AND stop_requested=0", (batch_id,)).rowcount
            if changed:
                self.store.event_in(conn, batch_id, "stop_requested", {
                    "detail": "No future dispatch. In-flight/open orders are NOT cancelled."})

    def request_cancel(self, batch_id: str, intent_id: str) -> None:
        self.check_enabled()
        if not self.provider.capabilities().cancel:
            raise GateError("CANCEL_UNAVAILABLE", "Provider cancellation is not verified.")
        with self.store.transaction() as conn:
            self._verified_batch_plan(conn, batch_id)
            row = conn.execute("SELECT * FROM intents WHERE id=? AND batch_id=?", (intent_id, batch_id)).fetchone()
            if not row or not row["broker_order_id"] or row["state"] != "SUBMITTED":
                raise GateError("NOT_CANCELLABLE", "Only a known open order in this batch may be cancelled.")
            order = BrokerOrder.model_validate_json(row["receipt"])
            if not order.is_open:
                raise GateError("NOT_CANCELLABLE", "Order is already terminal.")
            if row["cancel_state"]:
                return
            conn.execute("UPDATE intents SET cancel_state='REQUESTED' WHERE id=?", (intent_id,))
            conn.execute("UPDATE batches SET stop_requested=1 WHERE id=?", (batch_id,))
            self.store.event_in(conn, batch_id, "cancel_approved", {"intent_id": intent_id})

    def resolve_unknown(self, batch_id: str, intent_id: str, broker_order_id: str,
                        evidence_ref: str, operator: str) -> OperatorResolution:
        """Resolve only a stable UNKNOWN intent with an exact authoritative read."""
        batch = self.snapshot(batch_id)
        intent = next((item for item in batch["intents"] if item["id"] == intent_id), None)
        if intent is None or intent["state"] != "UNKNOWN":
            raise GateError("NOT_UNKNOWN", "Only an unresolved submission may be reconciled.")
        if not broker_order_id.strip() or not evidence_ref.strip():
            raise GateError("RESOLUTION_EVIDENCE_REQUIRED", "Exact order ID and evidence reference are required.")
        order = ProposedOrder.model_validate(intent["body"])
        plan, stored_hash = self._verified_plan(batch["plan_id"])
        with self.store.connect() as conn:
            approval_row = conn.execute(
                "SELECT id,body FROM approvals WHERE id=?", (batch["approval_id"],)
            ).fetchone()
        if not approval_row:
            raise GateError("PLAN_CHANGED", "Approved plan failed integrity validation.")
        approval = Approval.model_validate_json(approval_row["body"])
        if (approval.approval_id != approval_row["id"] or
                approval.approval_id != batch["approval_id"] or
                approval.plan_id != batch["plan_id"] or
                approval.plan_id != plan.plan_id or
                approval.plan_hash != stored_hash):
            raise GateError("PLAN_CHANGED", "Approved plan failed integrity validation.")
        ordinal = intent["ordinal"]
        if ordinal >= len(plan.orders) or digest(order) != digest(plan.orders[ordinal]):
            raise GateError(
                "INTENT_CHANGED", "Persisted intent does not match the hash-bound approved plan."
            )
        try:
            receipt = self.provider.get_order(broker_order_id.strip())
        except Exception as exc:
            raise GateError(
                "AUTHORITATIVE_EVIDENCE_UNAVAILABLE",
                "Exact broker evidence could not be read; the account remains blocked.",
            ) from exc
        if receipt.client_order_id != intent_id:
            raise GateError(
                "INTENT_LINKAGE_MISSING",
                "Broker evidence does not authoritatively bind this order to the original intent.",
            )
        self._match(receipt, order)
        if receipt.order_id != broker_order_id.strip():
            raise GateError("ORDER_ID_MISMATCH", "Authoritative receipt returned a different order ID.")
        observed = now_utc()
        payload = {
            "resolution_id": uuid4().hex, "batch_id": batch_id, "intent_id": intent_id,
            "original_intent_hash": digest(order), "broker_order_id": receipt.order_id,
            "receipt_hash": digest(receipt), "evidence_ref": evidence_ref.strip(),
            "operator": operator, "observed_at": observed,
        }
        resolution = OperatorResolution(**payload, record_hash=digest(payload))
        with self.store.transaction() as conn:
            current = conn.execute(
                "SELECT state,body FROM intents WHERE id=? AND batch_id=?", (intent_id, batch_id)
            ).fetchone()
            if current is None or current["state"] != "UNKNOWN":
                raise GateError("RESOLUTION_RACE", "Intent changed while evidence was being read.")
            if digest(ProposedOrder.model_validate_json(current["body"])) != resolution.original_intent_hash:
                raise GateError("INTENT_CHANGED", "Original intent failed integrity validation.")
            conn.execute(
                "UPDATE intents SET state='SUBMITTED',broker_order_id=?,receipt=? WHERE id=?",
                (receipt.order_id, canonical(receipt), intent_id),
            )
            conn.execute(
                "INSERT INTO documents(kind,id,body) VALUES('operator_resolution',?,?)",
                (resolution.resolution_id, canonical(resolution)),
            )
            self.store.event_in(conn, batch_id, "unknown_resolved", resolution)
        self._state(batch_id, "RECONCILING", "Exact broker evidence recorded; no submission was retried.")
        return resolution

    def _state(self, batch_id: str, state: str, reason: str = "", *, release: bool = False) -> None:
        with self.store.transaction() as conn:
            row = conn.execute("SELECT state,reason FROM batches WHERE id=?", (batch_id,)).fetchone()
            conn.execute("UPDATE batches SET state=?,reason=? WHERE id=?", (state, reason, batch_id))
            if release:
                conn.execute("DELETE FROM account_locks WHERE batch_id=?", (batch_id,))
            if tuple(row) != (state, reason):
                self.store.event_in(conn, batch_id, "execution_state", {"state": state, "reason": reason})

    @staticmethod
    def _match(receipt: BrokerOrder, order: ProposedOrder, previous: BrokerOrder | None = None) -> None:
        if (receipt.symbol, receipt.side, receipt.quantity, receipt.limit_price) != (
                order.symbol, order.side, order.quantity, order.limit_price):
            raise GateError("RECEIPT_MISMATCH", "Broker receipt does not match authorized order.")
        if receipt.filled_quantity and receipt.average_fill_price is not None:
            price = receipt.average_fill_price
            if ((order.side == Side.BUY and price > order.limit_price) or
                    (order.side == Side.SELL and price < order.limit_price)):
                raise GateError("FILL_PRICE_VIOLATION", "Fill price violates approved limit.")
        if previous:
            if (previous.fee_paid is not None and receipt.fee_paid is not None
                    and receipt.fee_paid < previous.fee_paid):
                raise GateError("FEE_REGRESSION", "Cumulative fee regressed.")
            if receipt.order_id != previous.order_id or receipt.filled_quantity < previous.filled_quantity:
                raise GateError("RECEIPT_REGRESSION", "Order identity or fill quantity regressed.")
            if previous.status in TERMINAL and receipt.status != previous.status:
                raise GateError("RECEIPT_REGRESSION", "Terminal state changed.")
        if receipt.status == OrderStatus.UNKNOWN:
            raise GateError("UNKNOWN_ORDER_STATUS", "Broker returned unknown order status.")

    def tick(self, batch_id: str, now: datetime | None = None) -> None:
        """One serialized advancement. No database transaction spans network I/O."""
        now = now or now_utc()
        with self.store.transaction() as conn:
            claim = conn.execute("UPDATE batches SET working=1 WHERE id=? AND working=0 AND id IN "
                                 "(SELECT batch_id FROM account_locks)", (batch_id,)).rowcount
        if not claim:
            return
        try:
            self._advance(batch_id, now)
        except GateError as exc:
            self._state(batch_id, "PAUSED", exc.code + ": " + exc.detail)
            self.stop(batch_id)
        except Exception:
            # Provider exception text can contain credentials; never return it.
            self._state(batch_id, "PAUSED_PROVIDER", "Read/validation failed. No new dispatch; inspect locally.")
            self.stop(batch_id)
        finally:
            with self.store.transaction() as conn:
                conn.execute("UPDATE batches SET working=0 WHERE id=?", (batch_id,))

    def _advance(self, batch_id: str, now: datetime) -> None:
        batch = self.snapshot(batch_id)
        plan, stored_hash = self._verified_plan(batch["plan_id"])
        with self.store.connect() as conn:
            row = conn.execute("SELECT body FROM approvals WHERE id=?", (batch["approval_id"],)).fetchone()
        approval = Approval.model_validate_json(row[0])
        if stored_hash != approval.plan_hash or approval.plan_id != plan.plan_id:
            raise GateError("PLAN_CHANGED", "Approved plan failed integrity validation.")
        intents = batch["intents"]
        if len(intents) != len(plan.orders) or any(
            i["ordinal"] != index or digest(ProposedOrder.model_validate(i["body"])) != digest(plan.orders[index])
            for index, i in enumerate(intents)
        ):
            raise GateError("INTENT_CHANGED", "Persisted dispatch intents differ from approved orders.")
        if any(i["state"] in ("UNKNOWN", "SENDING") for i in intents):
            self._state(batch_id, "MANUAL_RECONCILIATION", "Submission outcome unknown. Never retry or match heuristically.")
            return
        for intent in intents:
            if intent["state"] != "SUBMITTED":
                continue
            previous = BrokerOrder.model_validate(intent["receipt"])
            if not previous.is_open:
                continue
            receipt = self.provider.get_order(intent["broker_order_id"])
            self._match(receipt, ProposedOrder.model_validate(intent["body"]), previous)
            with self.store.transaction() as conn:
                conn.execute("UPDATE intents SET receipt=? WHERE id=?", (canonical(receipt), intent["id"]))
                if receipt != previous:
                    self.store.event_in(conn, batch_id, "order_observed", receipt)
            if intent["cancel_state"] == "REQUESTED" and receipt.is_open:
                self.check_enabled()
                if not self.provider.capabilities().cancel:
                    raise GateError("CANCEL_UNAVAILABLE", "Cancellation capability changed.")
                with self.store.transaction() as conn:
                    claimed = conn.execute("UPDATE intents SET cancel_state='SENDING' WHERE id=? AND cancel_state='REQUESTED'", (intent["id"],)).rowcount
                if claimed:
                    try:
                        self.provider.cancel(receipt.order_id)
                        cancellation = "ACKNOWLEDGED"
                    except Exception:
                        cancellation = "UNKNOWN"
                    with self.store.transaction() as conn:
                        conn.execute("UPDATE intents SET cancel_state=? WHERE id=?", (cancellation, intent["id"]))
                        self.store.event_in(conn, batch_id, "cancel_attempt", {"intent_id": intent["id"], "state": cancellation})
                return
        batch = self.snapshot(batch_id)
        intents = batch["intents"]
        receipts = [BrokerOrder.model_validate(i["receipt"]) for i in intents if i["receipt"]]
        live = any(r.is_open for r in receipts)
        now = max(now, now_utc())
        expired = now >= approval.expires_at
        if live:
            state = "EXPIRED_WITH_OPEN_ORDERS" if expired else ("STOPPING" if batch["stop_requested"] else "WAITING_FILL")
            self._state(batch_id, state, "Reconciliation only; no additional order is submitted.")
            return
        if batch["stop_requested"] or expired:
            self._state(batch_id, "EXPIRED" if expired else "STOPPED", "Unsubmitted orders abandoned; replan for further trading.", release=True)
            return
        if any(r.status != OrderStatus.FILLED for r in receipts):
            self._state(batch_id, "REPLAN_REQUIRED", "A prior order was rejected/cancelled/expired.", release=True)
            return
        pending = next((i for i in intents if i["state"] == "PLANNED"), None)
        if pending is None:
            self._validate_resources(plan, receipts, None, now)
            self._state(batch_id, "COMPLETED", "All fills and resulting account reconciled.", release=True)
            return
        self.check_enabled()
        order = ProposedOrder.model_validate(pending["body"])
        self._validate_resources(plan, receipts, order, now)
        # Linearization point: a stop cannot retract a request already SENDING.
        with self.store.transaction() as conn:
            if conn.execute("SELECT stop_requested FROM batches WHERE id=?", (batch_id,)).fetchone()[0]:
                return
            if now_utc() >= approval.expires_at:
                raise GateError("APPROVAL_EXPIRED", "Approval expired before dispatch.")
            sent = conn.execute("UPDATE intents SET state='SENDING' WHERE id=? AND state='PLANNED'", (pending["id"],)).rowcount
            if not sent:
                return
            self.store.event_in(conn, batch_id, "dispatch_started", {"intent_id": pending["id"], "order": order})
        try:
            receipt = self.provider.submit(order, pending["id"])
            self._match(receipt, order)
            with self.store.transaction() as conn:
                conn.execute("UPDATE intents SET state='SUBMITTED',broker_order_id=?,receipt=? WHERE id=?",
                             (receipt.order_id, canonical(receipt), pending["id"]))
                self.store.event_in(conn, batch_id, "order_accepted", receipt)
        except Exception:
            with self.store.transaction() as conn:
                conn.execute("UPDATE intents SET state='UNKNOWN' WHERE id=?", (pending["id"],))
            self._state(batch_id, "MANUAL_RECONCILIATION", "Write outcome unknown. Account locked; no automatic retry.")
            return
        self._state(batch_id, "WAITING_FILL", "Accepted does not mean filled.")

    def _validate_resources(self, plan: Plan, receipts: list[BrokerOrder],
                            order: ProposedOrder | None, now: datetime) -> None:
        current = self.provider.context([o.symbol for o in plan.orders])
        now = max(now, now_utc())
        if order and now >= plan.expires_at:
            raise GateError("PLAN_EXPIRED", "Plan expired during revalidation.")
        if current.account.account_id != plan.context.account.account_id or current.account.provider != plan.context.account.provider:
            raise GateError("ACCOUNT_CHANGED", "Provider/account identity changed.")
        issues = account_blockers(current.account, now, plan.policy, {r.order_id for r in receipts})
        if order:
            issues.extend(validate_market(current, {order.symbol}, now, plan.policy))
        if issues:
            raise GateError(issues[0].code, issues[0].detail)
        owned_receipts = {receipt.order_id: receipt for receipt in receipts}
        for observation in current.account.orders:
            expected = owned_receipts.get(observation.order_id)
            if expected and (observation.status != expected.status or
                             observation.filled_quantity != expected.filled_quantity):
                raise GateError("ORDER_STATE_NOT_RECONCILED", "Account order state differs from authoritative fill observations.")
        if digest(current.fees) != digest(plan.context.fees):
            raise GateError("FEES_CHANGED", "Fee schedule changed; replan.")
        old_rules = {r.symbol: r for r in plan.context.rules}
        new_rules = {r.symbol: r for r in current.rules}
        if order and digest(old_rules[order.symbol]) != digest(new_rules.get(order.symbol)):
            raise GateError("RULE_CHANGED", "Instrument rule changed; replan.")
        quantities = {p.symbol: p.quantity for p in plan.context.account.positions if p.quantity}
        expected_cash = plan.context.account.available_cash
        for receipt in receipts:
            if receipt.average_fill_price is None or receipt.fee_paid is None:
                raise GateError("MISSING_FILL_ACCOUNTING", "Fill lacks authoritative price/fees.")
            sign = 1 if receipt.side == Side.BUY else -1
            quantities[receipt.symbol] = quantities.get(receipt.symbol, 0) + sign * receipt.filled_quantity
            expected_cash -= sign * receipt.average_fill_price * receipt.filled_quantity + receipt.fee_paid
        actual = {p.symbol: p.quantity for p in current.account.positions if p.quantity}
        if actual != {k: v for k, v in quantities.items() if v}:
            raise GateError("POSITION_DRIFT", "Quantities differ from baseline plus known fills.")
        if abs(current.account.available_cash - expected_cash) > Decimal("0.02"):
            raise GateError("CASH_NOT_RECONCILED", "Cash differs from authoritative fills; inspect and replan.")
        if current.account.frozen_cash != plan.context.account.frozen_cash:
            raise GateError("FROZEN_CASH_DRIFT", "Unexpected frozen cash.")
        if not order:
            if current.account.available_cash < plan.cash_floor:
                raise GateError("CASH_RESERVE_BREACH", "Final cash is below approved reserve.")
            return
        quote = next(q for q in current.quotes if q.symbol == order.symbol)
        previous_quote = next(q for q in plan.context.quotes if q.symbol == order.symbol)
        if abs(quote.last / previous_quote.last - 1) > plan.policy.max_price_drift:
            raise GateError("PRICE_DRIFT", "Market moved outside approved drift budget.")
        if not quote.limit_down <= order.limit_price <= quote.limit_up:
            raise GateError("PRICE_BAND", "Approved limit is outside current price limits.")
        if order.side == Side.BUY:
            if not quote.can_buy:
                raise GateError("BUY_NOT_ALLOWED", "Buy-side restriction.")
            cost = order.limit_price * order.quantity
            if current.account.available_cash - cost - estimate_fee(cost, Side.BUY, current.fees) < plan.cash_floor:
                raise GateError("INSUFFICIENT_SETTLED_CASH", "Do not spend projected sales or cash reserve.")
        else:
            if not quote.can_sell:
                raise GateError("SELL_NOT_ALLOWED", "Sell-side restriction.")
            position = next((p for p in current.account.positions if p.symbol == order.symbol), None)
            if position is None or position.sellable_quantity < order.quantity:
                raise GateError("INSUFFICIENT_SELLABLE", "Insufficient currently sellable shares.")
