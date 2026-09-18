from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from competition_core.api import app, get_evidence_repository
from competition_core.storage import EvidenceRepository
from contracts.evidence import EvidenceBundle, Provenance, SECTION_NAMES
from contracts.symbol import CanonicalSymbol


def make_bundle(symbol: str = "300990.SZ") -> EvidenceBundle:
    as_of = datetime(2026, 9, 1, 11, 29, tzinfo=timezone(timedelta(hours=8)))
    sections = {name: {} for name in SECTION_NAMES}
    sections["technical"] = {"rsi14": 67.27}
    return EvidenceBundle(
        symbol=CanonicalSymbol.from_core(symbol),
        as_of=as_of,
        snapshot_id="snapshot-1",
        sections=sections,
        missing=("margin.financing_balance",),
        provenance=(Provenance("technical.rsi14", "fixture", as_of),),
    )


def client_for(tmp_path) -> tuple[TestClient, EvidenceRepository]:
    repository = EvidenceRepository(tmp_path / "state.sqlite", tmp_path / "bodies")
    app.dependency_overrides[get_evidence_repository] = lambda: repository
    return TestClient(app), repository


def test_post_get_latest_and_batch_round_trip(tmp_path) -> None:
    client, repository = client_for(tmp_path)
    bundle = make_bundle()

    with client:
        created = client.post("/evidence", json=bundle.to_dict())
        by_id = client.get(f"/evidence/{bundle.evidence_id}")
        latest = client.get("/evidence/latest/300990.SZ")
        batch = client.get("/evidence/latest", params={"symbols": "300990.SZ,600000.SH"})
    app.dependency_overrides.clear()

    assert created.status_code == 201
    assert created.json() == {"evidence_id": bundle.evidence_id}
    assert by_id.json() == latest.json() == bundle.to_dict()
    assert batch.json() == {"evidence": [bundle.to_dict()]}
    assert repository.get(bundle.evidence_id).to_dict() == bundle.to_dict()


def test_invalid_or_missing_evidence_is_explicit(tmp_path) -> None:
    client, _ = client_for(tmp_path)
    payload = make_bundle().to_dict()
    payload["technical"]["rsi14"] = 99.0

    with client:
        invalid = client.post("/evidence", json=payload)
        missing = client.get("/evidence/not-found")
    app.dependency_overrides.clear()

    assert invalid.status_code == 422
    assert missing.status_code == 404
