from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from contracts.evidence import EvidenceBundle, EvidenceContractError, Provenance
from contracts.symbol import CanonicalSymbol

AS_OF = datetime(2026, 9, 1, 11, 29, tzinfo=timezone(timedelta(hours=8)))


def bundle(**overrides) -> EvidenceBundle:
    sections = {
        "market": {"price": 102.07, "amount": 574_235_500.0},
        "technical": {"rsi14": 67.27},
        "flow": {"main_1d": 0.0},
        "margin": {},
        "dragon_tiger": {},
        "fundamental": {},
        "events": {},
        "risk": {},
    }
    provenance = tuple(
        Provenance(field=field, source="fixture", as_of=AS_OF)
        for field in ("market.price", "market.amount", "technical.rsi14", "flow.main_1d")
    )
    values = {
        "symbol": CanonicalSymbol.from_core("300990.SZ"),
        "as_of": AS_OF,
        "snapshot_id": "snapshot-1",
        "sections": sections,
        "missing": ("margin.financing_balance",),
        "provenance": provenance,
    }
    values.update(overrides)
    return EvidenceBundle(**values)


def test_bundle_is_deterministic_json_and_round_trips() -> None:
    first = bundle()
    second = EvidenceBundle.from_dict(json.loads(first.to_json()))

    assert second.evidence_id == first.evidence_id
    assert second.to_dict() == first.to_dict()
    assert second.sections["flow"]["main_1d"] == 0.0
    assert "flow.main_1d" not in second.missing


def test_missing_and_zero_are_distinct() -> None:
    result = bundle()

    assert "margin.financing_balance" in result.missing
    assert result.sections["flow"]["main_1d"] == 0.0


def test_missing_field_cannot_have_a_value() -> None:
    with pytest.raises(EvidenceContractError, match="missing field has a value"):
        bundle(missing=("market.price",))


def test_every_non_missing_leaf_requires_provenance() -> None:
    with pytest.raises(EvidenceContractError, match="require provenance"):
        bundle(provenance=())


def test_future_provenance_is_rejected() -> None:
    provenance = list(bundle().provenance)
    provenance[0] = Provenance(
        field="market.price", source="fixture", as_of=AS_OF + timedelta(seconds=1)
    )

    with pytest.raises(EvidenceContractError, match="future provenance"):
        bundle(provenance=tuple(provenance))


def test_naive_timestamp_and_nan_are_rejected() -> None:
    with pytest.raises(EvidenceContractError, match="timezone"):
        bundle(as_of=datetime(2026, 9, 1, 11, 29))
    sections = dict(bundle().sections)
    sections["market"] = {"price": float("nan")}
    with pytest.raises(EvidenceContractError, match="NaN"):
        bundle(sections=sections)


def test_tampered_evidence_id_is_rejected() -> None:
    payload = bundle().to_dict()
    payload["evidence_id"] = "tampered"

    with pytest.raises(EvidenceContractError, match="does not match"):
        EvidenceBundle.from_dict(payload)
