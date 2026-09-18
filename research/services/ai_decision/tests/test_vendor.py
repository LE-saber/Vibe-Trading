from __future__ import annotations

import json

import pytest

from ai_decision.vendor import EvidenceVendor, install_evidence_vendor


def test_vendor_returns_only_bundle_values_and_provenance(analyze_request) -> None:
    vendor = EvidenceVendor(analyze_request.evidence)

    market = json.loads(vendor.get_stock_data("600519.SH", "2026-08-01", "2026-09-01"))
    technical = json.loads(vendor.get_indicators("600519.SH", "rsi", "2026-09-01"))

    assert market["evidence_id"] == analyze_request.evidence.evidence_id
    assert market["sections"]["market"]["price"] == 1302.36
    assert {item["source"] for item in technical["provenance"]} == {"vibe"}


def test_vendor_symbol_conflict_fails_closed(analyze_request) -> None:
    vendor = EvidenceVendor(analyze_request.evidence)
    with pytest.raises(ValueError, match="canonical symbol conflict"):
        vendor.get_stock_data("600519.SZ", "2026-08-01", "2026-09-01")


def test_install_restores_upstream_vendor_registry(analyze_request) -> None:
    from tradingagents.dataflows import interface

    before = dict(interface.VENDOR_METHODS["get_stock_data"])
    with install_evidence_vendor(EvidenceVendor(analyze_request.evidence)):
        assert "competition_core" in interface.VENDOR_METHODS["get_stock_data"]
    assert interface.VENDOR_METHODS["get_stock_data"] == before
