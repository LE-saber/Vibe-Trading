"""Deterministic LLM double used only for graph/checkpoint acceptance tests."""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from itertools import count
from typing import Any

from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.runnables import RunnableLambda


class OfflineEvidenceLLM:
    """Exercise the real TradingAgents graph without producing a live decision."""

    def __init__(self, *, fail_once_in_debate: bool = False) -> None:
        self._ids = count(1)
        self.fail_once_in_debate = fail_once_in_debate
        self.failed = False

    def bind_tools(self, tools: list[Any], **_kwargs: Any) -> RunnableLambda:
        return RunnableLambda(lambda value: self._bound_response(value, tools))

    def with_structured_output(self, schema: type, **_kwargs: Any) -> RunnableLambda:
        def invoke(_value: Any) -> Any:
            name = schema.__name__
            if name == "ResearchPlan":
                return schema(
                    recommendation="Hold",
                    rationale="The evidence-only bull and bear cases are balanced.",
                    strategic_actions="Keep the candidate under review without execution.",
                )
            if name == "TraderProposal":
                return schema(
                    action="Hold",
                    reasoning="Evidence is insufficient for an execution proposal.",
                    entry_price=None,
                    stop_loss=None,
                    position_sizing=None,
                )
            if name == "PortfolioDecision":
                return schema(
                    rating="Hold",
                    executive_summary="Retain the candidate for human review only.",
                    investment_thesis="The supplied evidence does not justify directional execution.",
                    price_target=None,
                    time_horizon=None,
                )
            return schema()

        return RunnableLambda(invoke)

    def invoke(self, _value: Any, **_kwargs: Any) -> AIMessage:
        if self.fail_once_in_debate and not self.failed:
            self.failed = True
            raise RuntimeError("intentional offline checkpoint interruption")
        return AIMessage(content="Evidence-only debate response; no new facts introduced.")

    def _bound_response(self, value: Any, tools: list[Any]) -> AIMessage:
        messages = value.to_messages() if hasattr(value, "to_messages") else list(value)
        seen = {
            message.name
            for message in messages
            if isinstance(message, ToolMessage) and message.name
        }
        names = [tool.name for tool in tools]
        symbol, current_date = self._identity(messages)
        for name in names:
            if name in seen:
                continue
            args = self._arguments(name, symbol, current_date)
            return AIMessage(
                content="",
                tool_calls=[
                    {"name": name, "args": args, "id": f"offline-{next(self._ids)}"}
                ],
            )
        return AIMessage(
            content=(
                f"Evidence-only analyst report for {symbol}. All facts come from the "
                "Competition Core EvidenceBundle supplied to the sidecar."
            )
        )

    @staticmethod
    def _identity(messages: list[Any]) -> tuple[str, str]:
        text = "\n".join(str(getattr(message, "content", message)) for message in messages)
        symbol_match = re.search(r"\b\d{6}\.(?:SH|SZ)\b", text)
        date_match = re.search(r"\b20\d{2}-\d{2}-\d{2}\b", text)
        if symbol_match is None or date_match is None:
            raise RuntimeError("offline graph prompt lost canonical identity or date")
        return symbol_match.group(0), date_match.group(0)

    @staticmethod
    def _arguments(name: str, symbol: str, current_date: str) -> dict[str, Any]:
        if name == "get_stock_data":
            end = datetime.fromisoformat(current_date).date()
            return {
                "symbol": symbol,
                "start_date": (end - timedelta(days=30)).isoformat(),
                "end_date": end.isoformat(),
            }
        if name == "get_indicators":
            return {
                "symbol": symbol,
                "indicator": "rsi",
                "curr_date": current_date,
                "look_back_days": 30,
            }
        if name == "get_verified_market_snapshot":
            return {"symbol": symbol, "curr_date": current_date, "look_back_days": 30}
        if name == "get_fundamentals":
            return {"ticker": symbol, "curr_date": current_date}
        if name in {"get_balance_sheet", "get_cashflow", "get_income_statement"}:
            return {"ticker": symbol, "freq": "quarterly", "curr_date": current_date}
        if name == "get_news":
            return {"ticker": symbol, "start_date": current_date, "end_date": current_date}
        if name == "get_global_news":
            return {"curr_date": current_date, "look_back_days": 1, "limit": 1}
        if name == "get_insider_transactions":
            return {"ticker": symbol}
        raise RuntimeError(f"offline LLM has no safe arguments for tool {name}")


class OfflineClient:
    def __init__(self, llm: OfflineEvidenceLLM) -> None:
        self._llm = llm

    def get_llm(self) -> OfflineEvidenceLLM:
        return self._llm
