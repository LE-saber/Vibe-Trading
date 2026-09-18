from __future__ import annotations

import hashlib
import json

import pytest

from ai_decision.models import AnalyzeRequest


def evidence_payload(symbol: str = "600519.SH") -> dict:
    as_of = "2026-09-01T14:10:00+08:00"
    content = {
        "schema_version": 1,
        "symbol": symbol,
        "as_of": as_of,
        "snapshot_id": "p6-fixture",
        "market": {"price": 1302.36, "amount": 3388857856.0},
        "technical": {"rsi": 58.2, "macd": 3.1},
        "flow": {"main_1d": 1200000.0},
        "margin": {},
        "dragon_tiger": {},
        "fundamental": {"name": "贵州茅台", "sector": "消费"},
        "events": {},
        "risk": {"stale": False},
        "missing": ["events.news"],
        "provenance": [
            {"field": "market.price", "source": "mootdx", "as_of": as_of},
            {"field": "market.amount", "source": "mootdx", "as_of": as_of},
            {"field": "technical.rsi", "source": "vibe", "as_of": as_of},
            {"field": "technical.macd", "source": "vibe", "as_of": as_of},
            {"field": "flow.main_1d", "source": "tushare", "as_of": as_of},
            {"field": "fundamental.name", "source": "tushare", "as_of": as_of},
            {"field": "fundamental.sector", "source": "tushare", "as_of": as_of},
            {"field": "risk.stale", "source": "competition_core", "as_of": as_of},
        ],
    }
    evidence_id = hashlib.sha256(
        json.dumps(content, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return {"evidence_id": evidence_id, **content}


@pytest.fixture
def analyze_request(tmp_path) -> AnalyzeRequest:
    evidence = evidence_payload()
    return AnalyzeRequest.model_validate(
        {
            "run_id": "p6-test-run",
            "depth": "light",
            "candidate": {
                "symbol": evidence["symbol"],
                "rank": 1,
                "score": 0.9,
                "reasons": ["deterministic shortlist"],
                "risk_flags": [],
                "evidence_ref": f"evidence://{evidence['evidence_id']}",
            },
            "evidence": evidence,
        }
    )
