from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from contextlib import contextmanager
import json
import multiprocessing
import sqlite3

import pytest

from competition_core.execution.models import (
    AccountState,
    BrokerOrder,
    Capabilities,
    OrderStatus,
    PositionState,
    ProviderContext,
    QuoteState,
    Side,
)
from competition_core.execution.service import ExecutionGateError, ExecutionService
from competition_core.execution.store import ExecutionStore
from competition_core.pipeline.artifacts import content_hash


NOW = datetime(2026, 9, 7, 2, 0, tzinfo=timezone.utc)
HASH = "a" * 64
RULES = {
    "board": "main", "buy_min_quantity": 100, "buy_step": 100,
    "sell_min_quantity": 100, "sell_step": 100, "max_order_quantity": 1000000,
    "tick_size": "0.01", "odd_lot_sell_allowed": True,
}
RULES_HASH = content_hash(RULES)


def sealed_plan(plan_id: str = "plan-one", *, side: str = "BUY") -> dict:
    current, target = (0, 100) if side == "BUY" else (200, 100)
    order = {
        "symbol": "600000.SH", "side": side, "quantity": 100,
        "current_quantity": current, "sellable_quantity": current,
        "target_quantity": target, "delta_quantity": 100 if side == "BUY" else -100,
        "order_type": "LIMIT", "limit_price": "10.00", "estimated_fee": "5.00",
        "reference_price": "9.98", "gross_value": "1000.00",
        "quantity_rules": RULES, "quantity_rules_hash": RULES_HASH,
        "price_limits": {"down_limit": "9.00", "up_limit": "11.00"},
        "order_ref": f"{plan_id}-order-001",
    }
    body = {
        "schema_version": "p10.execution_plan.v1", "bridge_version": "s6_to_execution_plan.v1",
        "origin": "competition_s6", "run_id": "run-one", "plan_id": plan_id,
        "generated_at": NOW.isoformat(), "research_only": True,
        "external_write_allowed": False, "production_accepted": False, "orders_submitted": 0,
        "source": {"portfolio_seal_hash": HASH, "s6_artifact_hash": "b" * 64,
                   "s6_semantic_hash": "c" * 64},
        "research": {},
        "account": {"account_key": "account-one", "snapshot_hash": "d" * 64,
                    "source_hash": "e" * 64, "pending_order_count": 0},
        "market": {"fee_schedule_hash": "f" * 64},
        "fee_schedule": {}, "quantity_rules": {"600000.SH": RULES_HASH},
        "orders": [order], "holds": [], "economics": {},
        "approval": {"state": "NOT_APPROVED", "required": True, "plan_id": plan_id,
                     "plan_hash": None, "approved_at": None, "expires_at": None},
    }
    plan_hash = content_hash(body)
    body["approval"]["plan_hash"] = plan_hash
    unsigned = body | {"plan_hash": plan_hash}
    return unsigned | {"integrity_hash": content_hash(unsigned)}


class FakeProvider:
    def __init__(self, store: ExecutionStore):
        self.store = store
        self.submit_calls = 0
        self.cancel_calls = 0
        self.orders = {}
        self.account_hash = "d" * 64
        self.cash = Decimal("10000")
        self.positions = ()
        self.submit_error = False
        self.cancel_error = False
        self.fill_on_read = True
        self.partial_on_first_read = False
        self.read_calls = 0

    def capabilities(self):
        return Capabilities(provider="fake", verified=True, verification_ref="test-fixture",
                            buy=True, sell=True, cancel=True)

    def context(self, symbols):
        return ProviderContext(
            account=AccountState(account_key="account-one", snapshot_hash=self.account_hash,
                                 available_cash=self.cash, positions=self.positions,
                                 orders=tuple(self.orders.values()), orders_complete=True,
                                 consistent=True, observed_at=NOW),
            quotes=(QuoteState(symbol="600000.SH", last="10", limit_down="9", limit_up="11",
                               can_buy=True, can_sell=True, as_of=NOW),),
            fee_schedule_hash="f" * 64,
            quantity_rules_hashes={"600000.SH": RULES_HASH}, observed_at=NOW,
        )

    def submit(self, order, client_order_id):
        self.submit_calls += 1
        assert self.store.snapshot(client_order_id.split(":", 1)[0])["intents"][0]["state"] == "SENDING"
        receipt = BrokerOrder(order_id=f"broker-{self.submit_calls}", symbol=order.symbol, side=order.side,
                              quantity=order.quantity, filled_quantity=0, limit_price=order.limit_price,
                              status=OrderStatus.PENDING, updated_at=NOW, client_order_id=client_order_id)
        self.orders[receipt.order_id] = receipt
        if self.submit_error:
            raise TimeoutError("lost response")
        return receipt

    def get_order(self, order_id):
        receipt = self.orders[order_id]
        self.read_calls += 1
        if self.partial_on_first_read and self.read_calls == 1:
            receipt = BrokerOrder(**(receipt.to_dict() | {
                "filled_quantity": receipt.quantity // 2, "status": "PARTIALLY_FILLED",
                "average_fill_price": receipt.limit_price, "fee_paid": "3.00",
            }))
            self.orders[order_id] = receipt
            return receipt
        if self.fill_on_read:
            receipt = BrokerOrder(**(receipt.to_dict() | {
                "filled_quantity": receipt.quantity, "status": "FILLED",
                "average_fill_price": receipt.limit_price, "fee_paid": "5.00",
            }))
            self.orders[order_id] = receipt
        return receipt

    def cancel(self, order_id):
        self.cancel_calls += 1
        if self.cancel_error:
            raise TimeoutError("lost cancel response")
        receipt = self.orders[order_id]
        receipt = BrokerOrder(**(receipt.to_dict() | {"status": "CANCELLED"}))
        self.orders[order_id] = receipt
        return receipt


@pytest.fixture
def bench(tmp_path):
    store = ExecutionStore(tmp_path / "execution.sqlite")
    provider = FakeProvider(store)
    service = ExecutionService(store, provider, writes_enabled=True)
    plan = sealed_plan()
    service.register_plan(plan)
    return store, provider, service, plan


def approve(service, plan):
    return service.approve(plan["plan_id"], plan["plan_hash"], "operator",
                           expires_at=NOW + timedelta(minutes=5), now=NOW)


def start(bench):
    _, _, service, plan = bench
    return service.start(approve(service, plan).approval_id, now=NOW)


def _cross_process_lock_attempt(path: str, result) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute("INSERT INTO account_locks(account_key,batch_id) VALUES('account-one','other-batch')")
        connection.commit()
        result.put("inserted")
    except sqlite3.IntegrityError:
        result.put("blocked")
    finally:
        connection.close()


def test_write_capability_defaults_disabled(tmp_path):
    store = ExecutionStore(tmp_path / "execution.sqlite")
    provider = FakeProvider(store)
    service = ExecutionService(store, provider)
    plan = sealed_plan()
    service.register_plan(plan)
    with pytest.raises(ExecutionGateError, match="EXECUTION_DISABLED"):
        approve(service, plan)
    assert provider.submit_calls == 0


def test_plan_registration_is_idempotent_and_rejects_substitution(bench):
    _, _, service, plan = bench
    assert service.register_plan(plan) == service.register_plan(plan)
    changed = sealed_plan(); changed["orders"][0]["quantity"] = 200
    with pytest.raises(ExecutionGateError):
        service.register_plan(changed)


def test_approval_binds_plan_run_seal_and_short_expiry(bench):
    _, _, service, plan = bench
    approval = approve(service, plan)
    assert (approval.plan_hash, approval.run_id, approval.seal_hash) == (
        plan["plan_hash"], plan["run_id"], plan["source"]["portfolio_seal_hash"])
    assert approve(service, plan) == approval


def test_approval_rejects_displayed_hash_or_long_expiry(bench):
    _, _, service, plan = bench
    with pytest.raises(ExecutionGateError, match="PLAN_CHANGED"):
        service.approve(plan["plan_id"], "0" * 64, "operator", expires_at=NOW + timedelta(minutes=5), now=NOW)
    with pytest.raises(ExecutionGateError, match="INVALID_APPROVAL"):
        service.approve(plan["plan_id"], plan["plan_hash"], "operator", expires_at=NOW + timedelta(minutes=16), now=NOW)


def test_start_revalidates_exact_account_and_is_idempotent(bench):
    _, provider, service, plan = bench
    approval = approve(service, plan)
    first = service.start(approval.approval_id, now=NOW)
    assert service.start(approval.approval_id, now=NOW) == first
    assert provider.submit_calls == 0


def test_changed_account_blocks_before_batch_or_write(bench):
    store, provider, service, plan = bench
    approval = approve(service, plan)
    provider.account_hash = "9" * 64
    with pytest.raises(ExecutionGateError, match="ACCOUNT_CHANGED"):
        service.start(approval.approval_id, now=NOW)
    assert provider.submit_calls == 0
    with store.connect() as connection:
        assert connection.execute("SELECT count(*) FROM batches").fetchone()[0] == 0


def test_account_lock_is_persistent_across_services(tmp_path):
    store = ExecutionStore(tmp_path / "execution.sqlite")
    provider = FakeProvider(store)
    first = ExecutionService(store, provider, writes_enabled=True)
    one = sealed_plan("plan-one"); first.register_plan(one)
    first.start(approve(first, one).approval_id, now=NOW)
    second = ExecutionService(ExecutionStore(store.path), provider, writes_enabled=True)
    two = sealed_plan("plan-two"); second.register_plan(two)
    with pytest.raises(ExecutionGateError, match="ACCOUNT_BUSY"):
        second.start(approve(second, two).approval_id, now=NOW)
    assert provider.submit_calls == 0


def test_account_lock_is_enforced_across_processes(bench):
    store, _, _, _ = bench
    start(bench)
    context = multiprocessing.get_context("spawn")
    result = context.Queue()
    process = context.Process(target=_cross_process_lock_attempt, args=(str(store.path), result))
    process.start(); process.join(10)
    assert process.exitcode == 0
    assert result.get(timeout=2) == "blocked"


def test_sending_is_durable_before_network_and_fill_reconciles(bench):
    _, provider, service, _ = bench
    batch = start(bench)
    service.tick(batch, now=NOW)
    assert provider.submit_calls == 1
    assert service.snapshot(batch)["intents"][0]["state"] == "SUBMITTED"
    service.tick(batch, now=NOW)
    service.tick(batch, now=NOW)
    assert service.snapshot(batch)["state"] == "COMPLETED"


def test_partial_fill_is_observed_before_terminal_fill(bench):
    _, provider, service, _ = bench
    provider.partial_on_first_read = True
    batch = start(bench); service.tick(batch, now=NOW)
    service.tick(batch, now=NOW)
    snapshot = service.snapshot(batch)
    assert snapshot["state"] == "WAITING_FILL"
    assert snapshot["intents"][0]["receipt"]["status"] == "PARTIALLY_FILLED"
    assert snapshot["intents"][0]["receipt"]["filled_quantity"] == 50
    service.tick(batch, now=NOW); service.tick(batch, now=NOW)
    assert service.snapshot(batch)["state"] == "COMPLETED"


def test_ambiguous_submit_becomes_unknown_and_never_retries(bench):
    _, provider, service, _ = bench
    provider.submit_error = True
    batch = start(bench)
    service.tick(batch, now=NOW)
    service.tick(batch, now=NOW)
    assert provider.submit_calls == 1
    assert service.snapshot(batch)["state"] == "MANUAL_RECONCILIATION"
    assert service.snapshot(batch)["intents"][0]["state"] == "UNKNOWN"


def test_receipt_persistence_fault_never_resubmits(bench, monkeypatch):
    store, provider, service, _ = bench
    batch = start(bench)
    original = store.transaction
    faulted = False

    @contextmanager
    def fault_once():
        nonlocal faulted
        if provider.submit_calls == 1 and not faulted:
            faulted = True
            raise OSError("synthetic receipt persistence fault")
        with original() as connection:
            yield connection

    monkeypatch.setattr(store, "transaction", fault_once)
    service.tick(batch, now=NOW)
    assert provider.submit_calls == 1
    assert service.snapshot(batch)["intents"][0]["state"] == "SENDING"
    service.tick(batch, now=NOW)
    assert provider.submit_calls == 1
    assert service.snapshot(batch)["state"] == "MANUAL_RECONCILIATION"


def test_restart_turns_sending_into_unknown_without_dispatch(bench):
    store, provider, service, _ = bench
    batch = start(bench)
    with store.transaction() as connection:
        connection.execute("UPDATE intents SET state='SENDING' WHERE batch_id=?", (batch,))
    ExecutionStore(store.path)
    assert service.snapshot(batch)["intents"][0]["state"] == "UNKNOWN"
    service.tick(batch, now=NOW)
    assert provider.submit_calls == 0


def test_exact_unknown_resolution_requires_authoritative_link(bench):
    _, provider, service, _ = bench
    provider.submit_error = True
    batch = start(bench); service.tick(batch, now=NOW)
    intent = service.snapshot(batch)["intents"][0]
    receipt = next(iter(provider.orders.values()))
    result = service.resolve_unknown(batch, intent["id"], receipt.order_id, "operator-record-1", "operator")
    assert result.broker_order_id == receipt.order_id
    assert service.snapshot(batch)["intents"][0]["state"] == "SUBMITTED"


def test_stop_prevents_future_dispatch_and_does_not_cancel(bench):
    _, provider, service, _ = bench
    batch = start(bench)
    service.stop(batch); service.tick(batch, now=NOW)
    assert service.snapshot(batch)["state"] == "STOPPED"
    assert provider.submit_calls == provider.cancel_calls == 0


def test_cancel_only_targets_known_open_intent(bench):
    _, provider, service, _ = bench
    provider.fill_on_read = False
    batch = start(bench); service.tick(batch, now=NOW)
    with pytest.raises(ExecutionGateError, match="NOT_CANCELLABLE"):
        service.request_cancel(batch, "arbitrary")
    intent = service.snapshot(batch)["intents"][0]
    service.request_cancel(batch, intent["id"]); service.process_cancel(batch, intent["id"])
    assert provider.cancel_calls == 1
    assert service.snapshot(batch)["intents"][0]["receipt"]["status"] == "CANCELLED"


def test_uncertain_cancel_is_not_retried(bench):
    _, provider, service, _ = bench
    provider.fill_on_read = False
    batch = start(bench); service.tick(batch, now=NOW)
    intent = service.snapshot(batch)["intents"][0]
    service.request_cancel(batch, intent["id"])
    provider.cancel_error = True
    service.process_cancel(batch, intent["id"])
    with pytest.raises(ExecutionGateError, match="NOT_CANCELLABLE"):
        service.process_cancel(batch, intent["id"])
    assert provider.cancel_calls == 1
    assert service.snapshot(batch)["state"] == "CANCEL_UNKNOWN"


def test_tampered_intent_stops_before_provider_write(bench):
    store, provider, service, _ = bench
    batch = start(bench)
    with store.transaction() as connection:
        body = json.loads(connection.execute("SELECT body FROM intents WHERE batch_id=?", (batch,)).fetchone()[0])
        body["quantity"] = 200
        connection.execute("UPDATE intents SET body=? WHERE batch_id=?", (json.dumps(body), batch))
    service.tick(batch, now=NOW)
    assert provider.submit_calls == 0
    assert service.snapshot(batch)["state"] == "PAUSED"
