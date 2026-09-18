from __future__ import annotations

import pytest
from pydantic import ValidationError

from ai_decision.models import AnalyzeRequest, BatchAnalyzeRequest, EvidenceInput
from tests.conftest import evidence_payload


def test_evidence_hash_provenance_and_pair_are_strict() -> None:
    payload = evidence_payload()
    evidence = EvidenceInput.model_validate(payload)

    assert evidence.symbol == "600519.SH"

    tampered = {**payload, "market": {"price": 1.0}}
    with pytest.raises(ValidationError, match="provenance|evidence_id"):
        EvidenceInput.model_validate(tampered)


def test_candidate_evidence_symbol_conflict_fails_closed() -> None:
    payload = evidence_payload()
    with pytest.raises(ValidationError, match="symbol conflict"):
        AnalyzeRequest.model_validate(
            {
                "run_id": "conflict",
                "candidate": {
                    "symbol": "300750.SZ",
                    "rank": 1,
                    "evidence_ref": f"evidence://{payload['evidence_id']}",
                },
                "evidence": payload,
            }
        )


def test_batch_is_bounded_to_ten_unique_symbols(analyze_request) -> None:
    with pytest.raises(ValidationError):
        BatchAnalyzeRequest(items=[analyze_request] * 11)
    with pytest.raises(ValidationError, match="unique"):
        BatchAnalyzeRequest(items=[analyze_request, analyze_request])
