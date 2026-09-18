"""Narrow provider boundary used by the controlled execution service."""

from __future__ import annotations

from typing import Protocol, Sequence

from competition_core.execution.models import (
    BrokerOrder,
    Capabilities,
    ProposedOrder,
    ProviderContext,
)


class ExecutionProvider(Protocol):
    def capabilities(self) -> Capabilities: ...

    def context(self, symbols: Sequence[str]) -> ProviderContext: ...

    def submit(self, order: ProposedOrder, client_order_id: str) -> BrokerOrder: ...

    def get_order(self, order_id: str) -> BrokerOrder: ...

    def cancel(self, order_id: str) -> BrokerOrder: ...


__all__ = ["ExecutionProvider"]
