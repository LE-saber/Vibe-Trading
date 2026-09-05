"""Only this seam reaches the owner's current local code and credentials.

Implementations require bounded timeouts and no hidden write retries. No guessed
ninef URL or raw credential/header logging belongs in the application kernel.
"""
from collections.abc import Callable, Sequence
from typing import Protocol

from .models import (
    BrokerOrder, Capabilities, Leaderboard, NewsItem, ProposedOrder,
    ResearchRequest, ResearchResult, TradingContext,
)

Progress = Callable[[str, int, int | None, str], None]
Cancelled = Callable[[], bool]


class Provider(Protocol):
    def capabilities(self) -> Capabilities: ...
    def context(self, symbols: Sequence[str]) -> TradingContext:
        """Read all holdings/open-order pages plus requested quotes/rules."""
        ...
    def leaderboard(self) -> Leaderboard: ...
    def news(self) -> tuple[NewsItem, ...]: ...
    def research(self, request: ResearchRequest, report: Progress,
                 cancelled: Cancelled) -> ResearchResult:
        """Reuse actual analysis. Report real completed work, not elapsed timers."""
        ...
    def submit(self, order: ProposedOrder, client_order_id: str) -> BrokerOrder:
        """At most one transport attempt. Acceptance is not assumed fill."""
        ...
    def get_order(self, order_id: str) -> BrokerOrder:
        """Authoritative exact-ID read. Absence/timeout is not cancellation."""
        ...
    def cancel(self, order_id: str) -> None:
        """At most one attempt. Acknowledgment is not terminal cancellation."""
        ...
