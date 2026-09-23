"""Only this seam reaches the owner's current local code and credentials.

Implementations require bounded timeouts and no hidden write retries. No guessed
ninef URL or raw credential/header logging belongs in the application kernel.
"""
from collections.abc import Callable, Sequence
from typing import Protocol, runtime_checkable

from .models import (
    BrokerOrder, Capabilities, Leaderboard, NewsItem, ProposedOrder,
    ResearchRequest, ResearchResult, TradingContext,
)
from .terminal import TerminalProjection, TerminalShadowPlan

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


@runtime_checkable
class TerminalResearchProvider(Protocol):
    """Optional read-only terminal seam; base Provider need not implement it.

    The canonical Workbench supplies only its server-owned run identifier. A
    client cannot choose a mode, symbol, quantity, price, or execution flag
    through this seam. Implementations return bounded research contracts, not
    a canonical ``Plan`` or broker intent.
    """

    def terminal_projection(self, run_id: str) -> TerminalProjection:
        """Read/verify the latest bounded terminal evaluation for one run."""
        ...

    def terminal_shadow_plan(
        self, run_id: str, evaluation_id: str | None = None
    ) -> TerminalShadowPlan:
        """Return a read-only shadow allocation record for one run."""
        ...
