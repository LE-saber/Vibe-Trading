from __future__ import annotations

from copy import deepcopy

import pytest

from adapters.vibe import evidence_from_vibe_result
from contracts.evidence import EvidenceContractError
from contracts.symbol import SymbolContractError


def result() -> dict:
    return {
        "symbol": "300990.SZ",
        "as_of": "2026-09-01T11:29:00+08:00",
        "snapshot_id": "snapshot-1",
        "sections": {
            "market": {
                "values": {"price": 102.07, "amount": 0.0},
                "source": "mootdx",
                "as_of": "2026-09-01T11:29:00+08:00",
            },
            "technical": {
                "values": {"rsi14": 67.27},
                "source": "vibe.technical",
                "as_of": "2026-09-01T11:28:00+08:00",
            },
        },
        "missing": ["margin.financing_balance"],
    }


def test_explicit_section_sources_become_field_provenance() -> None:
    bundle = evidence_from_vibe_result(result())

    assert bundle.symbol.symbol == "300990.SZ"
    assert bundle.sections["market"]["amount"] == 0.0
    assert "market.amount" not in bundle.missing
    assert "margin.financing_balance" in bundle.missing
    assert {item.field: item.source for item in bundle.provenance} == {
        "market.price": "mootdx",
        "market.amount": "mootdx",
        "technical.rsi14": "vibe.technical",
    }


def test_unknown_section_or_top_level_field_is_rejected() -> None:
    payload = result()
    payload["sections"]["predictions"] = {}
    with pytest.raises(EvidenceContractError, match="unsupported section"):
        evidence_from_vibe_result(payload)

    payload = result()
    payload["raw_tool_payload"] = {}
    with pytest.raises(EvidenceContractError, match="unsupported fields"):
        evidence_from_vibe_result(payload)


def test_nonempty_section_without_source_is_rejected() -> None:
    payload = result()
    payload["sections"]["market"]["source"] = ""

    with pytest.raises(EvidenceContractError, match="source"):
        evidence_from_vibe_result(payload)


def test_future_section_timestamp_is_rejected_by_contract() -> None:
    payload = result()
    payload["sections"]["market"]["as_of"] = "2026-09-01T11:30:00+08:00"

    with pytest.raises(EvidenceContractError, match="future provenance"):
        evidence_from_vibe_result(payload)


def test_symbol_exchange_conflict_fails_closed() -> None:
    payload = result()
    payload["symbol"] = "300990.SH"

    with pytest.raises(SymbolContractError):
        evidence_from_vibe_result(payload)


def test_missing_field_cannot_silently_keep_a_value() -> None:
    payload = deepcopy(result())
    payload["missing"].append("market.price")

    with pytest.raises(EvidenceContractError, match="missing field has a value"):
        evidence_from_vibe_result(payload)
