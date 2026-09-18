from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from competition_core.storage import EvidenceRepository, RepositoryError
from contracts.evidence import EvidenceBundle, Provenance, SECTION_NAMES
from contracts.symbol import CanonicalSymbol

TZ = timezone(timedelta(hours=8))


def make_bundle(sequence: int, symbol: str = "300990.SZ") -> EvidenceBundle:
    as_of = datetime(2026, 9, 1, 9, 30, sequence, tzinfo=TZ)
    sections = {name: {} for name in SECTION_NAMES}
    sections["market"] = {"price": 100.0 + sequence, "zero_value": 0.0}
    sections["technical"] = {"rsi14": 60.0 + sequence, "macd_hist": 0.1 + sequence}
    sections["flow"] = {"main_1d": 1_000_000.0 + sequence}
    return EvidenceBundle(
        symbol=CanonicalSymbol.from_core(symbol),
        as_of=as_of,
        snapshot_id="snapshot-1",
        sections=sections,
        missing=("margin.financing_balance",),
        provenance=(
            Provenance("market.price", "fixture", as_of),
            Provenance("market.zero_value", "fixture", as_of),
            Provenance("technical.rsi14", "fixture", as_of),
            Provenance("technical.macd_hist", "fixture", as_of),
            Provenance("flow.main_1d", "fixture", as_of),
        ),
    )


def repository(tmp_path) -> EvidenceRepository:
    return EvidenceRepository(tmp_path / "state.sqlite", tmp_path / "bodies")


def test_put_get_is_idempotent_and_registers_symbol(tmp_path) -> None:
    repo = repository(tmp_path)
    bundle = make_bundle(1)

    first = repo.put(bundle)
    second = repo.put(bundle)

    assert first == second == bundle.evidence_id
    assert repo.get(first).to_dict() == bundle.to_dict()
    assert repo.get_symbol("300990.SZ") == bundle.symbol


def test_latest_and_latest_many_are_ordered_by_as_of(tmp_path) -> None:
    repo = repository(tmp_path)
    repo.put(make_bundle(1))
    newest = make_bundle(2)
    repo.put(newest)
    other = make_bundle(1, "600000.SH")
    repo.put(other)

    assert repo.latest("300990.SZ").evidence_id == newest.evidence_id
    assert set(repo.latest_many(["300990.SZ", "600000.SH", "300750.SZ"])) == {
        "300990.SZ", "600000.SH"
    }


def test_more_than_thirty_raw_results_can_be_cleared_and_recovered(tmp_path) -> None:
    repo = repository(tmp_path)
    raw_results = []
    ids = []
    for sequence in range(35):
        bundle = make_bundle(sequence)
        raw_results.append(bundle.to_dict())
        ids.append(repo.put(bundle))

    raw_results.clear()

    assert not raw_results
    assert repo.get(ids[0]).sections["market"]["price"] == 100.0
    assert repo.get(ids[-1]).sections["market"]["price"] == 134.0
    assert repo.get(ids[-1]).sections["market"]["zero_value"] == 0.0
    assert repo.get(ids[-1]).sections["technical"]["rsi14"] == 94.0
    assert repo.get(ids[-1]).sections["technical"]["macd_hist"] == 34.1
    assert repo.get(ids[-1]).sections["flow"]["main_1d"] == 1_000_034.0
    assert "margin.financing_balance" in repo.get(ids[-1]).missing


def test_corrupt_or_missing_body_fails_loudly(tmp_path) -> None:
    repo = repository(tmp_path)
    bundle = make_bundle(1)
    repo.put(bundle)
    body = next((tmp_path / "bodies").glob("*.json"))
    body.write_text("{}", encoding="utf-8")

    with pytest.raises(RepositoryError, match="cannot read evidence body"):
        repo.get(bundle.evidence_id)
    with pytest.raises(KeyError):
        repo.get("missing")
