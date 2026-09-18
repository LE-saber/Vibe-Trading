"""Bounded TradingAgents orchestration over immutable EvidenceBundles."""

from __future__ import annotations

import copy
import importlib.metadata
import os
import re
import threading
from pathlib import Path
from typing import Any

from ai_decision.models import AnalyzeRequest, DecisionOutput
from ai_decision.offline import OfflineClient, OfflineEvidenceLLM
from ai_decision.vendor import EvidenceVendor, install_evidence_vendor

SERVICE_ROOT = Path(__file__).resolve().parents[1]
PLATFORM_ROOT = SERVICE_ROOT.parents[1]
DEFAULT_STATE_ROOT = PLATFORM_ROOT / "storage" / "ai_decision"
_PROVIDER_KEYS = {
    "openai": "OPENAI_API_KEY",
    "deepseek": "DEEPSEEK_API_KEY",
    "google": "GOOGLE_API_KEY",
    "anthropic": "ANTHROPIC_API_KEY",
    "openrouter": "OPENROUTER_API_KEY",
    "qwen": "DASHSCOPE_API_KEY",
    "glm": "ZHIPU_API_KEY",
}
_RATING_SCORE = {
    "Buy": 1.0,
    "Overweight": 0.5,
    "Hold": 0.0,
    "Underweight": -0.5,
    "Sell": -1.0,
    "REVIEW": 0.0,
}


class DecisionConfigurationError(RuntimeError):
    pass


class DecisionRunner:
    """Serialize global TradingAgents vendor installation and graph execution."""

    def __init__(self, state_root: str | Path = DEFAULT_STATE_ROOT) -> None:
        self.state_root = Path(state_root)
        self._lock = threading.Lock()

    def provider_ready(self) -> bool:
        provider = os.getenv("TRADINGAGENTS_LLM_PROVIDER", "openai").strip().lower()
        if provider == "ollama":
            return bool(os.getenv("TRADINGAGENTS_LLM_BACKEND_URL", "").strip())
        key = _PROVIDER_KEYS.get(provider)
        return bool(key and os.getenv(key, "").strip())

    def analyze(
        self,
        request: AnalyzeRequest,
        *,
        offline_llm: OfflineEvidenceLLM | None = None,
    ) -> DecisionOutput:
        if offline_llm is None and not self.provider_ready():
            raise DecisionConfigurationError(
                "TradingAgents LLM provider is not configured; no synthetic live decision was produced"
            )
        vendor = EvidenceVendor(request.evidence)
        with self._lock, install_evidence_vendor(vendor):
            graph = self.build_graph(request, offline_llm=offline_llm)
            # TradingAgents' decision-memory outcome resolver normally consults
            # its own market vendor. A-share facts are Core-owned, so sidecar
            # runs never perform that implicit external lookup.
            graph._resolve_pending_entries = lambda _ticker: None
            state, rating = graph.propagate(
                request.candidate.symbol,
                request.evidence.as_of.date().isoformat(),
                asset_type="stock",
            )
        return self._to_output(request, state, str(getattr(rating, "value", rating)))

    def build_graph(
        self,
        request: AnalyzeRequest,
        *,
        offline_llm: OfflineEvidenceLLM | None = None,
    ) -> Any:
        from tradingagents.default_config import DEFAULT_CONFIG
        from tradingagents.graph import trading_graph
        from tradingagents.graph.trading_graph import TradingAgentsGraph

        config = copy.deepcopy(DEFAULT_CONFIG)
        state_dir = self.state_root / request.run_id
        config.update(
            {
                "data_cache_dir": str(state_dir / "cache"),
                "results_dir": str(state_dir / "results"),
                "memory_log_path": str(state_dir / "memory" / "decisions.md"),
                "checkpoint_enabled": True,
                "output_language": os.getenv("TRADINGAGENTS_OUTPUT_LANGUAGE", "Chinese"),
                "max_debate_rounds": 1,
                "max_risk_discuss_rounds": 1,
                "data_vendors": {
                    category: "competition_core"
                    for category in (
                        "core_stock_apis",
                        "technical_indicators",
                        "fundamental_data",
                        "news_data",
                        "macro_data",
                        "prediction_markets",
                    )
                },
                "tool_vendors": {},
            }
        )
        if offline_llm is None:
            config["llm_provider"] = os.getenv("TRADINGAGENTS_LLM_PROVIDER", "openai")
            config["quick_think_llm"] = os.getenv(
                "TRADINGAGENTS_QUICK_THINK_LLM", config["quick_think_llm"]
            )
            config["deep_think_llm"] = os.getenv(
                "TRADINGAGENTS_DEEP_THINK_LLM", config["deep_think_llm"]
            )
            config["backend_url"] = os.getenv("TRADINGAGENTS_LLM_BACKEND_URL") or None

        analysts = (
            ("market", "fundamentals")
            if request.depth == "light"
            else ("market", "news", "fundamentals")
        )
        if offline_llm is None:
            return TradingAgentsGraph(selected_analysts=analysts, config=config)

        previous_factory = trading_graph.create_llm_client
        trading_graph.create_llm_client = lambda **_kwargs: OfflineClient(offline_llm)
        try:
            return TradingAgentsGraph(selected_analysts=analysts, config=config)
        finally:
            trading_graph.create_llm_client = previous_factory

    def _to_output(
        self,
        request: AnalyzeRequest,
        state: dict[str, Any],
        rating: str,
    ) -> DecisionOutput:
        reports = {
            "market": str(state.get("market_report") or ""),
            "sentiment": str(state.get("sentiment_report") or ""),
            "news": str(state.get("news_report") or ""),
            "fundamental": str(state.get("fundamentals_report") or ""),
        }
        debate = state.get("investment_debate_state") or {}
        risk = state.get("risk_debate_state") or {}
        text_fields = [
            *reports.values(),
            str(debate.get("bull_history") or ""),
            str(debate.get("bear_history") or ""),
            str(risk.get("history") or ""),
            str(state.get("final_trade_decision") or ""),
        ]
        audit = self._numeric_audit(request, text_fields)
        final_rating = rating if rating in _RATING_SCORE else "REVIEW"
        decision = {
            "Buy": "ADVANCE",
            "Overweight": "ADVANCE",
            "Hold": "KEEP",
            "Underweight": "DROP",
            "Sell": "DROP",
            "REVIEW": "REVIEW",
        }[final_rating]
        if audit["untraced_values"]:
            final_rating = "REVIEW"
            decision = "REVIEW"
        return DecisionOutput(
            run_id=request.run_id,
            symbol=request.candidate.symbol,
            evidence_id=request.evidence.evidence_id,
            analyst_reports=reports,
            bull_case=str(debate.get("bull_history") or ""),
            bear_case=str(debate.get("bear_history") or ""),
            risk_assessment=str(risk.get("history") or ""),
            final_score=_RATING_SCORE[final_rating],
            score_derivation=f"deterministic map from final_rating={final_rating}",
            decision=decision,
            confidence=None,
            final_rating=final_rating,
            numeric_audit=audit,
            checkpoint_enabled=True,
            framework_version=importlib.metadata.version("tradingagents"),
        )

    @staticmethod
    def _numeric_audit(request: AnalyzeRequest, texts: list[str]) -> dict[str, Any]:
        allowed: list[float] = []

        def collect(value: Any) -> None:
            if isinstance(value, dict):
                for item in value.values():
                    collect(item)
            elif isinstance(value, list):
                for item in value:
                    collect(item)
            elif isinstance(value, (int, float)) and not isinstance(value, bool):
                allowed.append(float(value))

        for section in (
            request.evidence.market,
            request.evidence.technical,
            request.evidence.flow,
            request.evidence.margin,
            request.evidence.dragon_tiger,
            request.evidence.fundamental,
            request.evidence.events,
            request.evidence.risk,
        ):
            collect(section)
        untraced: set[float] = set()
        for text in texts:
            masked = re.sub(r"[0-9a-f]{64}", " ", text, flags=re.IGNORECASE)
            masked = re.sub(r"\b\d{6}\.(?:SH|SZ)\b", " ", masked)
            masked = re.sub(r"\b20\d{2}-\d{2}-\d{2}(?:T[^\s\"}]+)?", " ", masked)
            for token in re.findall(r"(?<![A-Za-z])[-+]?\d+(?:\.\d+)?", masked):
                value = float(token)
                if not any(math_isclose(value, item) for item in allowed):
                    untraced.add(value)
        return {
            "status": "pass" if not untraced else "review_required",
            "untraced_values": sorted(untraced),
            "allowed_evidence_value_count": len(allowed),
        }


def math_isclose(left: float, right: float) -> bool:
    return abs(left - right) <= max(1e-9, abs(right) * 1e-9)
