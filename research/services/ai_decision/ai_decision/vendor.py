"""Install an EvidenceBundle as TradingAgents' only A-share data vendor."""

from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any, Iterator

from ai_decision.models import EvidenceInput


class EvidenceVendor:
    """Render one immutable bundle through TradingAgents' vendor signatures."""

    name = "competition_core"

    def __init__(self, evidence: EvidenceInput) -> None:
        self.evidence = evidence
        self._provenance = {item.field: item for item in evidence.provenance}

    def _check_symbol(self, symbol: str) -> None:
        if symbol != self.evidence.symbol:
            raise ValueError(
                f"canonical symbol conflict: requested {symbol}, evidence is {self.evidence.symbol}"
            )

    def _render(self, *sections: str, label: str) -> str:
        values = {name: self.evidence.section(name) for name in sections}
        fields = [
            {
                "field": item.field,
                "source": item.source,
                "as_of": item.as_of.isoformat(),
            }
            for item in self.evidence.provenance
            if item.field.split(".", 1)[0] in sections
        ]
        payload = {
            "label": label,
            "canonical_symbol": self.evidence.symbol,
            "evidence_id": self.evidence.evidence_id,
            "evidence_as_of": self.evidence.as_of.isoformat(),
            "snapshot_id": self.evidence.snapshot_id,
            "sections": values,
            "missing": [
                item for item in self.evidence.missing if item.split(".", 1)[0] in sections
            ],
            "provenance": fields,
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def get_stock_data(self, symbol: str, _start_date: str, _end_date: str) -> str:
        self._check_symbol(symbol)
        return self._render("market", label="Evidence-only market snapshot; no hidden history")

    def get_indicators(
        self, symbol: str, indicator: str, _curr_date: str, _look_back_days: int = 30
    ) -> str:
        self._check_symbol(symbol)
        return self._render("technical", label=f"Evidence-only indicator request: {indicator}")

    def get_fundamentals(self, ticker: str, _curr_date: str) -> str:
        self._check_symbol(ticker)
        return self._render("fundamental", label="Evidence-only fundamentals")

    def get_statement(self, ticker: str, *_args: Any, **_kwargs: Any) -> str:
        self._check_symbol(ticker)
        return self._render("fundamental", label="Evidence-only statement fields")

    def get_news(self, ticker: str, *_args: Any, **_kwargs: Any) -> str:
        self._check_symbol(ticker)
        return self._render("events", label="Evidence-only events/news")

    def get_global_news(self, *_args: Any, **_kwargs: Any) -> str:
        return self._render("events", "risk", label="Evidence-only macro/event context")

    def get_macro(self, *_args: Any, **_kwargs: Any) -> str:
        return self._render("events", "risk", label="Evidence-only macro context")

    def get_prediction(self, *_args: Any, **_kwargs: Any) -> str:
        return self._render("events", "risk", label="Evidence-only prediction context")

    def verified_snapshot(self, symbol: str, _curr_date: str, _look_back_days: int = 30) -> str:
        self._check_symbol(symbol)
        return self._render("market", "technical", label="Verified Competition Core snapshot")

    @property
    def identity(self) -> dict[str, str]:
        values = self.evidence.fundamental
        result = {
            "name": str(values.get("name") or values.get("company_name") or self.evidence.symbol),
            "exchange": "SSE" if self.evidence.symbol.endswith(".SH") else "SZSE",
        }
        for key in ("sector", "industry"):
            if values.get(key):
                result[key] = str(values[key])
        return result


@contextmanager
def install_evidence_vendor(vendor: EvidenceVendor) -> Iterator[None]:
    """Temporarily route every selected TradingAgents data tool to Evidence."""
    from tradingagents.dataflows import interface
    from tradingagents.agents.utils import market_data_validation_tools
    from tradingagents.graph import trading_graph

    bindings = {
        "get_stock_data": vendor.get_stock_data,
        "get_indicators": vendor.get_indicators,
        "get_fundamentals": vendor.get_fundamentals,
        "get_balance_sheet": vendor.get_statement,
        "get_cashflow": vendor.get_statement,
        "get_income_statement": vendor.get_statement,
        "get_news": vendor.get_news,
        "get_global_news": vendor.get_global_news,
        "get_insider_transactions": vendor.get_news,
        "get_macro_indicators": vendor.get_macro,
        "get_prediction_markets": vendor.get_prediction,
    }
    previous = {
        method: dict(interface.VENDOR_METHODS.get(method, {})) for method in bindings
    }
    previous_snapshot = market_data_validation_tools.build_verified_market_snapshot
    previous_identity = trading_graph.resolve_instrument_identity
    try:
        for method, function in bindings.items():
            interface.VENDOR_METHODS.setdefault(method, {})[vendor.name] = function
        market_data_validation_tools.build_verified_market_snapshot = vendor.verified_snapshot
        trading_graph.resolve_instrument_identity = lambda _ticker: vendor.identity
        yield
    finally:
        for method, mapping in previous.items():
            interface.VENDOR_METHODS[method] = mapping
        market_data_validation_tools.build_verified_market_snapshot = previous_snapshot
        trading_graph.resolve_instrument_identity = previous_identity
