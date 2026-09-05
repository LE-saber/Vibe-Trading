"""Conservative account checks; available cash and sellable shares are NET values.

Never subtract frozen resources twice. Unrelated open orders block new plans;
we do not guess their future fills or automatically cancel them.
"""
from datetime import datetime

from .models import AccountSnapshot, Blocker, OrderStatus, Policy

ALIASES = {
    "PENDING": OrderStatus.PENDING, "NEW": OrderStatus.PENDING,
    "\u5f85\u6210\u4ea4": OrderStatus.PENDING, "\u672a\u6210\u4ea4": OrderStatus.PENDING,
    "\u5df2\u62a5": OrderStatus.PENDING,
    "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
    "\u90e8\u5206\u6210\u4ea4": OrderStatus.PARTIALLY_FILLED,
    "FILLED": OrderStatus.FILLED, "\u5df2\u6210\u4ea4": OrderStatus.FILLED,
    "CANCEL_PENDING": OrderStatus.CANCEL_PENDING, "\u5f85\u64a4": OrderStatus.CANCEL_PENDING,
    "CANCELLED": OrderStatus.CANCELLED, "CANCELED": OrderStatus.CANCELLED,
    "\u5df2\u64a4\u5355": OrderStatus.CANCELLED, "\u5df2\u64a4": OrderStatus.CANCELLED,
    "REJECTED": OrderStatus.REJECTED, "\u5e9f\u5355": OrderStatus.REJECTED,
    "EXPIRED": OrderStatus.EXPIRED,
}


def normalize_status(raw: str) -> OrderStatus:
    return ALIASES.get(raw.strip().upper(), OrderStatus.UNKNOWN)


def account_blockers(account: AccountSnapshot, now: datetime, policy: Policy,
                     owned_order_ids: set[str] | None = None) -> list[Blocker]:
    issues: list[Blocker] = []
    owned_order_ids = owned_order_ids or set()
    def add(code: str, detail: str) -> None:
        issues.append(Blocker(code=code, detail=detail))
    age = (now - account.as_of).total_seconds()
    if age < -2 or age > policy.max_snapshot_age_seconds:
        add("STALE_ACCOUNT", "Account is stale or in the future.")
    if (account.as_of - account.collection_started_at).total_seconds() > policy.max_collection_seconds:
        add("SNAPSHOT_SKEW", "Account collection interval is too wide.")
    if not account.orders_complete:
        add("INCOMPLETE_ORDERS", "All open-order pages must be read.")
    if not account.consistent:
        add("INCONSISTENT_ACCOUNT", "Account/order/position snapshot is not reconciled.")
    for order in account.orders:
        if order.status == OrderStatus.UNKNOWN:
            add("UNKNOWN_ORDER_STATUS", f"Order {order.order_id} requires investigation.")
        elif order.is_open and order.order_id not in owned_order_ids:
            add("EXTERNAL_OPEN_ORDER", f"Resolve open order {order.order_id} before replanning.")
    if not any(o.is_open for o in account.orders) and account.frozen_cash:
        add("UNEXPLAINED_FROZEN_CASH", "Frozen cash without an open order requires reconciliation.")
    return issues
