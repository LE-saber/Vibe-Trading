"""Approval-bound durable dispatch with fail-closed ambiguous-write recovery."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Sequence
from uuid import uuid4

from competition_core.execution.models import (
    Approval,
    BrokerOrder,
    Capabilities,
    OperatorResolution,
    OrderStatus,
    ProposedOrder,
    ProviderContext,
    Side,
    TERMINAL,
    digest,
    validate_w6_plan,
)
from competition_core.execution.ports import ExecutionProvider
from competition_core.execution.store import ExecutionStore


class ExecutionGateError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: datetime, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ExecutionGateError("INVALID_ARGUMENTS", f"{field} must be timezone-aware")
    return value


class ExecutionService:
    """Single controlled path from a W6 plan to provider calls.

    ``writes_enabled`` defaults to false.  Constructing this service, importing
    a plan, reading status and stopping future dispatch do not call a provider
    write.  Tests may opt in with a fake provider; production must separately
    supply verified capabilities.
    """

    def __init__(self, store: ExecutionStore, provider: ExecutionProvider, *, writes_enabled: bool = False) -> None:
        self.store = store
        self.provider = provider
        self.writes_enabled = writes_enabled

    def _capabilities(self, *, cancel: bool = False) -> Capabilities:
        try:
            capability = Capabilities.from_value(self.provider.capabilities())
        except Exception as exc:
            raise ExecutionGateError("EXECUTION_DISABLED", "provider capabilities are unavailable") from exc
        required = capability.cancel if cancel else capability.buy and capability.sell
        if not self.writes_enabled or not capability.verified or not capability.verification_ref or not required:
            raise ExecutionGateError("EXECUTION_DISABLED", "external writes or verified capabilities are disabled")
        return capability

    def register_plan(self, value: dict[str, Any]) -> dict[str, Any]:
        try:
            plan = validate_w6_plan(value)
        except (TypeError, ValueError) as exc:
            raise ExecutionGateError("INVALID_PLAN", "sealed W6 plan validation failed") from exc
        document = {"plan": plan, "plan_hash": plan["plan_hash"], "integrity_hash": plan["integrity_hash"]}
        with self.store.transaction() as connection:
            existing = connection.execute("SELECT body FROM plans WHERE id=?", (plan["plan_id"],)).fetchone()
            if existing is not None:
                if json.loads(existing[0]) != document:
                    raise ExecutionGateError("PLAN_CHANGED", "plan ID already belongs to different content")
                return document
            connection.execute(
                "INSERT INTO plans(id,plan_hash,run_id,seal_hash,body,created_at) VALUES(?,?,?,?,?,?)",
                (plan["plan_id"], plan["plan_hash"], plan["run_id"],
                 plan["source"]["portfolio_seal_hash"], json.dumps(document, ensure_ascii=False, sort_keys=True,
                 separators=(",", ":"), allow_nan=False), _now().isoformat()),
            )
            self.store.event(connection, plan["plan_id"], "plan_registered", {
                "run_id": plan["run_id"], "plan_hash": plan["plan_hash"], "orders": len(plan["orders"])
            })
        return document

    def _plan(self, plan_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
        document = self.store.plan_document(plan_id)
        try:
            plan = validate_w6_plan(document["plan"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ExecutionGateError("PLAN_CORRUPTED", "stored plan failed integrity validation") from exc
        if document != {"plan": plan, "plan_hash": plan["plan_hash"], "integrity_hash": plan["integrity_hash"]}:
            raise ExecutionGateError("PLAN_CORRUPTED", "stored plan wrapper changed")
        return plan, document

    def approve(self, plan_id: str, expected_plan_hash: str, actor: str, *,
                expires_at: datetime, now: datetime | None = None) -> Approval:
        self._capabilities()
        moment = _aware(now or _now(), "approval time")
        expiry = _aware(expires_at, "approval expiry")
        plan, _ = self._plan(plan_id)
        if plan["plan_hash"] != expected_plan_hash:
            raise ExecutionGateError("PLAN_CHANGED", "displayed plan hash differs from stored plan")
        if expiry <= moment or expiry > moment + timedelta(minutes=15):
            raise ExecutionGateError("INVALID_APPROVAL", "approval must expire within fifteen minutes")
        approval = Approval(
            approval_id=uuid4().hex,
            plan_id=plan_id,
            plan_hash=plan["plan_hash"],
            run_id=plan["run_id"],
            seal_hash=plan["source"]["portfolio_seal_hash"],
            actor=actor,
            approved_at=moment,
            expires_at=expiry,
        )
        with self.store.transaction() as connection:
            row = connection.execute("SELECT body FROM approvals WHERE plan_id=?", (plan_id,)).fetchone()
            if row is not None:
                existing = Approval.from_value(json.loads(row[0]))
                if existing.plan_hash != plan["plan_hash"] or existing.seal_hash != approval.seal_hash:
                    raise ExecutionGateError("PLAN_CHANGED", "existing approval has different bindings")
                return existing
            connection.execute("INSERT INTO approvals(id,plan_id,body) VALUES(?,?,?)",
                               (approval.approval_id, plan_id, json.dumps(approval.to_dict(), sort_keys=True,
                                separators=(",", ":"))))
            self.store.event(connection, plan_id, "approved", approval.to_dict())
        return approval

    def _approval(self, approval_id: str) -> Approval:
        with self.store.connect() as connection:
            row = connection.execute("SELECT body FROM approvals WHERE id=?", (approval_id,)).fetchone()
        if row is None:
            raise ExecutionGateError("APPROVAL_REQUIRED", "approval does not exist")
        try:
            return Approval.from_value(json.loads(row[0]))
        except (TypeError, ValueError) as exc:
            raise ExecutionGateError("INVALID_APPROVAL", "approval record is invalid") from exc

    @staticmethod
    def _orders(plan: dict[str, Any]) -> tuple[ProposedOrder, ...]:
        try:
            return tuple(ProposedOrder.from_w6(row) for row in plan["orders"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ExecutionGateError("INVALID_PLAN", "fixed order rows are invalid") from exc

    @staticmethod
    def _preflight(plan: dict[str, Any], context: ProviderContext, orders: Sequence[ProposedOrder],
                   *, exact_account: bool) -> None:
        account = context.account
        binding = plan["account"]
        if account.account_key != binding["account_key"]:
            raise ExecutionGateError("ACCOUNT_CHANGED", "provider account differs from approved account")
        if not account.orders_complete or not account.consistent or account.open_orders:
            raise ExecutionGateError("ACCOUNT_NOT_RECONCILED", "account orders are incomplete, inconsistent or open")
        if exact_account and account.snapshot_hash != binding["snapshot_hash"]:
            raise ExecutionGateError("ACCOUNT_CHANGED", "account snapshot changed after S6")
        if context.fee_schedule_hash != plan["market"]["fee_schedule_hash"]:
            raise ExecutionGateError("FEE_CHANGED", "fee schedule differs from sealed S6")
        quotes = context.quote_by_symbol
        for order in orders:
            if context.quantity_rules_hashes.get(order.symbol) != order.quantity_rules_hash:
                raise ExecutionGateError("RULES_CHANGED", f"quantity rules changed for {order.symbol}")
            quote = quotes.get(order.symbol)
            if quote is None:
                raise ExecutionGateError("QUOTE_MISSING", f"quote is missing for {order.symbol}")
            if order.side is Side.BUY and not quote.can_buy:
                raise ExecutionGateError("NOT_TRADABLE", f"buy is not permitted for {order.symbol}")
            if order.side is Side.SELL and not quote.can_sell:
                raise ExecutionGateError("NOT_TRADABLE", f"sell is not permitted for {order.symbol}")
            if not quote.limit_down <= order.limit_price <= quote.limit_up:
                raise ExecutionGateError("PRICE_BAND_CHANGED", f"approved limit is outside current band for {order.symbol}")

    def start(self, approval_id: str, *, now: datetime | None = None) -> str:
        self._capabilities()
        moment = _aware(now or _now(), "start time")
        approval = self._approval(approval_id)
        plan, _ = self._plan(approval.plan_id)
        if (approval.plan_hash != plan["plan_hash"] or approval.run_id != plan["run_id"]
                or approval.seal_hash != plan["source"]["portfolio_seal_hash"]):
            raise ExecutionGateError("INVALID_APPROVAL", "approval bindings differ from plan")
        if moment >= approval.expires_at:
            raise ExecutionGateError("INVALID_APPROVAL", "approval expired")
        orders = self._orders(plan)
        try:
            context = ProviderContext.from_value(self.provider.context([order.symbol for order in orders]))
        except ExecutionGateError:
            raise
        except Exception as exc:
            raise ExecutionGateError("PROVIDER_UNAVAILABLE", "preflight context is unavailable") from exc
        self._preflight(plan, context, orders, exact_account=True)
        batch_id = uuid4().hex
        with self.store.transaction() as connection:
            existing = connection.execute("SELECT id FROM batches WHERE approval_id=?", (approval_id,)).fetchone()
            if existing is not None:
                self._verify_batch(connection, existing[0])
                return existing[0]
            timestamp = moment.isoformat()
            connection.execute(
                "INSERT INTO batches(id,approval_id,plan_id,account_key,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                (batch_id, approval_id, plan["plan_id"], context.account.account_key, "QUEUED", timestamp, timestamp),
            )
            try:
                connection.execute("INSERT INTO account_locks(account_key,batch_id) VALUES(?,?)",
                                   (context.account.account_key, batch_id))
            except sqlite3.IntegrityError as exc:
                raise ExecutionGateError("ACCOUNT_BUSY", "another batch owns this account") from exc
            for ordinal, order in enumerate(orders):
                intent_id = f"{batch_id}:{ordinal}"
                body = order.to_dict()
                connection.execute(
                    "INSERT INTO intents(id,batch_id,ordinal,state,body,intent_hash) VALUES(?,?,?,?,?,?)",
                    (intent_id, batch_id, ordinal, "PLANNED", json.dumps(body, ensure_ascii=False, sort_keys=True,
                     separators=(",", ":"), allow_nan=False), digest(body)),
                )
            self.store.event(connection, batch_id, "batch_queued", {"plan_id": plan["plan_id"], "orders": len(orders)})
        return batch_id

    def _verify_batch(self, connection: sqlite3.Connection, batch_id: str) -> tuple[sqlite3.Row, Approval, dict[str, Any]]:
        batch = connection.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        if batch is None:
            raise KeyError(batch_id)
        row = connection.execute("SELECT body FROM approvals WHERE id=?", (batch["approval_id"],)).fetchone()
        if row is None:
            raise ExecutionGateError("INVALID_APPROVAL", "batch approval is missing")
        approval = Approval.from_value(json.loads(row[0]))
        plan, _ = self._plan(batch["plan_id"])
        if (approval.approval_id != batch["approval_id"] or approval.plan_id != batch["plan_id"]
                or approval.plan_hash != plan["plan_hash"] or approval.run_id != plan["run_id"]
                or approval.seal_hash != plan["source"]["portfolio_seal_hash"]):
            raise ExecutionGateError("PLAN_CHANGED", "batch, approval and plan bindings differ")
        rows = connection.execute("SELECT * FROM intents WHERE batch_id=? ORDER BY ordinal", (batch_id,)).fetchall()
        orders = self._orders(plan)
        if len(rows) != len(orders):
            raise ExecutionGateError("INTENT_CHANGED", "intent count differs from approved plan")
        for ordinal, (row, order) in enumerate(zip(rows, orders)):
            body = json.loads(row["body"])
            if row["ordinal"] != ordinal or row["intent_hash"] != digest(body) or body != order.to_dict():
                raise ExecutionGateError("INTENT_CHANGED", "persisted intent differs from approved order")
        return batch, approval, plan

    def snapshot(self, batch_id: str) -> dict[str, Any]:
        return self.store.snapshot(batch_id)

    def stop(self, batch_id: str) -> None:
        self.store.snapshot(batch_id)
        with self.store.transaction() as connection:
            changed = connection.execute(
                "UPDATE batches SET stop_requested=1,updated_at=? WHERE id=? AND stop_requested=0",
                (_now().isoformat(), batch_id),
            ).rowcount
            if changed:
                self.store.event(connection, batch_id, "stop_requested", {
                    "cancels_orders": False, "meaning": "no future dispatch"
                })

    def request_cancel(self, batch_id: str, intent_id: str) -> None:
        self._capabilities(cancel=True)
        with self.store.transaction() as connection:
            self._verify_batch(connection, batch_id)
            row = connection.execute("SELECT * FROM intents WHERE id=? AND batch_id=?", (intent_id, batch_id)).fetchone()
            if row is None or row["state"] != "SUBMITTED" or not row["broker_order_id"] or not row["receipt"]:
                raise ExecutionGateError("NOT_CANCELLABLE", "only this batch's known submitted order can be cancelled")
            receipt = BrokerOrder.from_value(json.loads(row["receipt"]))
            if not receipt.is_open:
                raise ExecutionGateError("NOT_CANCELLABLE", "order is terminal")
            if row["cancel_state"]:
                return
            connection.execute("UPDATE intents SET cancel_state='REQUESTED' WHERE id=?", (intent_id,))
            connection.execute("UPDATE batches SET stop_requested=1 WHERE id=?", (batch_id,))
            self.store.event(connection, batch_id, "cancel_requested", {
                "intent_id": intent_id, "broker_order_id": receipt.order_id
            })

    @staticmethod
    def _match(receipt: BrokerOrder, order: ProposedOrder, *, intent_id: str,
               previous: BrokerOrder | None = None) -> None:
        if (receipt.symbol, receipt.side, receipt.quantity, receipt.limit_price) != (
                order.symbol, order.side, order.quantity, order.limit_price):
            raise ExecutionGateError("RECEIPT_MISMATCH", "broker receipt differs from authorized order")
        if receipt.client_order_id != intent_id:
            raise ExecutionGateError("INTENT_LINKAGE_MISSING", "broker receipt lacks exact intent linkage")
        if receipt.filled_quantity and receipt.average_fill_price is not None:
            if ((order.side is Side.BUY and receipt.average_fill_price > order.limit_price)
                    or (order.side is Side.SELL and receipt.average_fill_price < order.limit_price)):
                raise ExecutionGateError("FILL_PRICE_VIOLATION", "fill violates approved limit")
        if receipt.status is OrderStatus.UNKNOWN:
            raise ExecutionGateError("UNKNOWN_ORDER_STATUS", "provider returned unknown order status")
        if previous is not None:
            if receipt.order_id != previous.order_id or receipt.filled_quantity < previous.filled_quantity:
                raise ExecutionGateError("RECEIPT_REGRESSION", "broker order identity or fill quantity regressed")
            if previous.status in TERMINAL and receipt.status != previous.status:
                raise ExecutionGateError("RECEIPT_REGRESSION", "terminal order status changed")
            if (previous.fee_paid is not None and receipt.fee_paid is not None
                    and receipt.fee_paid < previous.fee_paid):
                raise ExecutionGateError("FEE_REGRESSION", "cumulative fee regressed")

    def _set_state(self, batch_id: str, state: str, reason: str = "", *, release: bool = False) -> None:
        with self.store.transaction() as connection:
            current = connection.execute("SELECT state,reason FROM batches WHERE id=?", (batch_id,)).fetchone()
            connection.execute("UPDATE batches SET state=?,reason=?,updated_at=? WHERE id=?",
                               (state, reason, _now().isoformat(), batch_id))
            if release:
                connection.execute("DELETE FROM account_locks WHERE batch_id=?", (batch_id,))
            if current is not None and tuple(current) != (state, reason):
                self.store.event(connection, batch_id, "batch_state", {"state": state, "reason": reason})

    def tick(self, batch_id: str, *, now: datetime | None = None) -> None:
        self._capabilities()
        moment = _aware(now or _now(), "tick time")
        with self.store.transaction() as connection:
            claimed = connection.execute(
                "UPDATE batches SET working=1 WHERE id=? AND working=0 AND id IN (SELECT batch_id FROM account_locks)",
                (batch_id,),
            ).rowcount
        if not claimed:
            return
        try:
            self._advance(batch_id, moment)
        except ExecutionGateError as exc:
            state = "MANUAL_RECONCILIATION" if exc.code in {
                "RECEIPT_MISMATCH", "INTENT_LINKAGE_MISSING", "UNKNOWN_ORDER_STATUS",
                "RECEIPT_REGRESSION", "FILL_PRICE_VIOLATION", "FEE_REGRESSION"
            } else "PAUSED"
            self._set_state(batch_id, state, f"{exc.code}: {exc.message}")
            self.stop(batch_id)
        except Exception:
            self._set_state(batch_id, "PAUSED_PROVIDER", "provider read failed; no new dispatch")
            self.stop(batch_id)
        finally:
            with self.store.transaction() as connection:
                connection.execute("UPDATE batches SET working=0 WHERE id=?", (batch_id,))

    def _advance(self, batch_id: str, moment: datetime) -> None:
        with self.store.transaction() as connection:
            batch, approval, plan = self._verify_batch(connection, batch_id)
        snapshot = self.store.snapshot(batch_id)
        intents = snapshot["intents"]
        if any(row["state"] in {"SENDING", "UNKNOWN"} for row in intents):
            self._set_state(batch_id, "MANUAL_RECONCILIATION", "SUBMISSION_OUTCOME_UNKNOWN: never retry")
            return

        for row in intents:
            if row["state"] != "SUBMITTED" or not row["receipt"]:
                continue
            previous = BrokerOrder.from_value(row["receipt"])
            if not previous.is_open:
                continue
            receipt = BrokerOrder.from_value(self.provider.get_order(row["broker_order_id"]))
            order = ProposedOrder.from_w6(row["body"])
            self._match(receipt, order, intent_id=row["id"], previous=previous)
            with self.store.transaction() as connection:
                connection.execute("UPDATE intents SET receipt=? WHERE id=?",
                                   (json.dumps(receipt.to_dict(), sort_keys=True, separators=(",", ":")), row["id"]))
                self.store.event(connection, batch_id, "order_observed", {
                    "intent_id": row["id"], "status": receipt.status.value,
                    "filled_quantity": receipt.filled_quantity
                })
            if receipt.is_open:
                self._set_state(batch_id, "STOPPED_WITH_OPEN_ORDERS" if snapshot["stop_requested"] else "WAITING_FILL")
                return

        snapshot = self.store.snapshot(batch_id)
        intents = snapshot["intents"]
        if any(row["state"] == "SUBMITTED" and row["receipt"]
               and BrokerOrder.from_value(row["receipt"]).status is not OrderStatus.FILLED for row in intents):
            self._set_state(batch_id, "REPLAN_REQUIRED", "an order ended without full fill", release=True)
            return
        planned = next((row for row in intents if row["state"] == "PLANNED"), None)
        if planned is None:
            self._set_state(batch_id, "STOPPED" if snapshot["stop_requested"] else "COMPLETED", release=True)
            return
        if snapshot["stop_requested"]:
            self._set_state(batch_id, "STOPPED", "future dispatch stopped", release=True)
            return
        if moment >= approval.expires_at:
            self._set_state(batch_id, "EXPIRED", "approval expired before dispatch", release=True)
            return
        order = ProposedOrder.from_w6(planned["body"])
        context = ProviderContext.from_value(self.provider.context([item.symbol for item in self._orders(plan)]))
        # Exact account identity is mandatory before the first write.  Later
        # account changes must be attributable to already observed fills.
        first_dispatch = all(row["state"] == "PLANNED" for row in intents)
        self._preflight(plan, context, (order,), exact_account=first_dispatch)
        if context.account.open_orders:
            raise ExecutionGateError("ACCOUNT_NOT_RECONCILED", "external open orders block dispatch")
        if order.side is Side.BUY:
            required = order.limit_price * order.quantity + order.estimated_fee
            if context.account.available_cash < required:
                raise ExecutionGateError("INSUFFICIENT_CASH", "current available cash cannot fund approved buy")
        else:
            position = context.account.position_by_symbol.get(order.symbol)
            if position is None or position.sellable_quantity < order.quantity:
                raise ExecutionGateError("INSUFFICIENT_SELLABLE", "current sellable quantity changed")

        # This durable state is committed before crossing the provider write boundary.
        with self.store.transaction() as connection:
            changed = connection.execute(
                "UPDATE intents SET state='SENDING' WHERE id=? AND state='PLANNED'", (planned["id"],)
            ).rowcount
            if changed != 1:
                return
            self.store.event(connection, batch_id, "submission_sending", {
                "intent_id": planned["id"], "retry": False
            })
        try:
            receipt = BrokerOrder.from_value(self.provider.submit(order, planned["id"]))
            self._match(receipt, order, intent_id=planned["id"])
        except Exception as exc:
            with self.store.transaction() as connection:
                connection.execute("UPDATE intents SET state='UNKNOWN' WHERE id=? AND state='SENDING'", (planned["id"],))
                self.store.event(connection, batch_id, "submission_unknown", {
                    "intent_id": planned["id"], "retry": False
                })
            self._set_state(batch_id, "MANUAL_RECONCILIATION", "SUBMISSION_OUTCOME_UNKNOWN: exact evidence required")
            return
        with self.store.transaction() as connection:
            changed = connection.execute(
                "UPDATE intents SET state='SUBMITTED',broker_order_id=?,receipt=? WHERE id=? AND state='SENDING'",
                (receipt.order_id, json.dumps(receipt.to_dict(), sort_keys=True, separators=(",", ":")), planned["id"]),
            ).rowcount
            if changed != 1:
                raise ExecutionGateError("SUBMISSION_PERSIST_FAILED", "submission receipt could not bind to intent")
            self.store.event(connection, batch_id, "submitted", {
                "intent_id": planned["id"], "broker_order_id": receipt.order_id
            })
        self._set_state(batch_id, "WAITING_FILL" if receipt.is_open else "QUEUED")

    def process_cancel(self, batch_id: str, intent_id: str) -> None:
        """Perform one separately requested exact cancel; uncertainty is never retried."""
        self._capabilities(cancel=True)
        with self.store.transaction() as connection:
            self._verify_batch(connection, batch_id)
            row = connection.execute("SELECT * FROM intents WHERE id=? AND batch_id=?", (intent_id, batch_id)).fetchone()
            if row is None or row["cancel_state"] != "REQUESTED" or not row["broker_order_id"]:
                raise ExecutionGateError("NOT_CANCELLABLE", "cancel was not explicitly requested")
            connection.execute("UPDATE intents SET cancel_state='SENDING' WHERE id=?", (intent_id,))
        try:
            receipt = BrokerOrder.from_value(self.provider.cancel(row["broker_order_id"]))
            order = ProposedOrder.from_w6(json.loads(row["body"]))
            previous = BrokerOrder.from_value(json.loads(row["receipt"]))
            self._match(receipt, order, intent_id=intent_id, previous=previous)
        except Exception:
            with self.store.transaction() as connection:
                connection.execute("UPDATE intents SET cancel_state='UNKNOWN' WHERE id=?", (intent_id,))
                connection.execute("UPDATE batches SET state='CANCEL_UNKNOWN',reason=? WHERE id=?",
                                   ("CANCEL_OUTCOME_UNKNOWN: exact evidence required", batch_id))
                self.store.event(connection, batch_id, "cancel_unknown", {"intent_id": intent_id, "retry": False})
            return
        with self.store.transaction() as connection:
            connection.execute("UPDATE intents SET cancel_state='ACKED',receipt=? WHERE id=?",
                               (json.dumps(receipt.to_dict(), sort_keys=True, separators=(",", ":")), intent_id))
            self.store.event(connection, batch_id, "cancel_observed", {
                "intent_id": intent_id, "status": receipt.status.value
            })

    def resolve_unknown(self, batch_id: str, intent_id: str, broker_order_id: str,
                        evidence_ref: str, operator: str) -> OperatorResolution:
        snapshot = self.store.snapshot(batch_id)
        row = next((item for item in snapshot["intents"] if item["id"] == intent_id), None)
        if row is None or row["state"] != "UNKNOWN":
            raise ExecutionGateError("NOT_UNKNOWN", "only an UNKNOWN submission can be resolved")
        if not broker_order_id.strip() or not evidence_ref.strip():
            raise ExecutionGateError("RESOLUTION_EVIDENCE_REQUIRED", "exact order ID and evidence reference are required")
        with self.store.transaction() as connection:
            self._verify_batch(connection, batch_id)
        order = ProposedOrder.from_w6(row["body"])
        try:
            receipt = BrokerOrder.from_value(self.provider.get_order(broker_order_id.strip()))
        except Exception as exc:
            raise ExecutionGateError("AUTHORITATIVE_EVIDENCE_UNAVAILABLE", "exact broker order cannot be read") from exc
        if receipt.order_id != broker_order_id.strip():
            raise ExecutionGateError("ORDER_ID_MISMATCH", "provider returned another order ID")
        self._match(receipt, order, intent_id=intent_id)
        payload = {
            "resolution_id": uuid4().hex,
            "batch_id": batch_id,
            "intent_id": intent_id,
            "original_intent_hash": row["intent_hash"],
            "broker_order_id": receipt.order_id,
            "receipt_hash": digest(receipt.to_dict()),
            "evidence_ref": evidence_ref.strip(),
            "operator": operator,
            "observed_at": _now().isoformat(),
        }
        resolution = OperatorResolution(**payload)
        with self.store.transaction() as connection:
            current = connection.execute("SELECT state,intent_hash FROM intents WHERE id=?", (intent_id,)).fetchone()
            if current is None or current["state"] != "UNKNOWN" or current["intent_hash"] != row["intent_hash"]:
                raise ExecutionGateError("RESOLUTION_RACE", "intent changed while evidence was read")
            connection.execute(
                "UPDATE intents SET state='SUBMITTED',broker_order_id=?,receipt=? WHERE id=?",
                (receipt.order_id, json.dumps(receipt.to_dict(), sort_keys=True, separators=(",", ":")), intent_id),
            )
            connection.execute("INSERT INTO resolutions(id,intent_id,body) VALUES(?,?,?)",
                               (resolution.resolution_id, intent_id, json.dumps(resolution.to_dict(), sort_keys=True,
                                separators=(",", ":"))))
            self.store.event(connection, batch_id, "unknown_resolved", resolution.to_dict())
        self._set_state(batch_id, "WAITING_FILL" if receipt.is_open else "QUEUED")
        return resolution


__all__ = ["ExecutionGateError", "ExecutionService"]
