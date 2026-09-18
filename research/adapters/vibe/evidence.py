"""Convert a source-explicit Vibe result envelope into EvidenceBundle."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from contracts.evidence import (
    EvidenceBundle,
    EvidenceContractError,
    Provenance,
    SECTION_NAMES,
)
from contracts.symbol import CanonicalSymbol

_TOP_LEVEL = frozenset({"symbol", "as_of", "snapshot_id", "sections", "missing"})
_SECTION_ENVELOPE = frozenset({"values", "source", "as_of"})


def evidence_from_vibe_result(result: Mapping[str, Any]) -> EvidenceBundle:
    """Ingest one compact direct-runner result without guessing provenance.

    Every non-empty section is an envelope containing ``values``, ``source``,
    and a timezone-aware ``as_of``. This adapter deliberately does not accept
    arbitrary raw tool output: the direct runner must first choose the fields
    that belong in durable evidence and identify their source.
    """

    if not isinstance(result, Mapping):
        raise EvidenceContractError("Vibe evidence result must be a mapping")
    if set(result) != _TOP_LEVEL:
        raise EvidenceContractError("Vibe evidence result has missing or unsupported fields")
    raw_sections = result["sections"]
    if not isinstance(raw_sections, Mapping) or set(raw_sections) - set(SECTION_NAMES):
        raise EvidenceContractError("Vibe evidence sections contain an unsupported section")

    bundle_as_of = _datetime(result["as_of"], "as_of")
    snapshot_id = result["snapshot_id"]
    if not isinstance(snapshot_id, str) or not snapshot_id.strip():
        raise EvidenceContractError("snapshot_id must not be empty")
    missing = result["missing"]
    if not isinstance(missing, (list, tuple)):
        raise EvidenceContractError("missing must be a list")
    sections: dict[str, Mapping[str, Any]] = {name: {} for name in SECTION_NAMES}
    provenance: list[Provenance] = []
    for section_name, envelope in raw_sections.items():
        if not isinstance(envelope, Mapping) or set(envelope) != _SECTION_ENVELOPE:
            raise EvidenceContractError(
                f"{section_name} must contain values, source, and as_of"
            )
        values = envelope["values"]
        if not isinstance(values, Mapping):
            raise EvidenceContractError(f"{section_name}.values must be a mapping")
        source = envelope["source"]
        if not isinstance(source, str) or not source.strip():
            raise EvidenceContractError(f"{section_name}.source must not be empty")
        section_as_of = _datetime(envelope["as_of"], f"{section_name}.as_of")
        sections[section_name] = dict(values)
        for path in _leaf_paths(section_name, values):
            provenance.append(Provenance(path, source.strip(), section_as_of))

    return EvidenceBundle(
        symbol=CanonicalSymbol.from_core(result["symbol"]),
        as_of=bundle_as_of,
        snapshot_id=snapshot_id,
        sections=sections,
        missing=tuple(str(item) for item in missing),
        provenance=tuple(provenance),
    )


def _datetime(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise EvidenceContractError(f"{field} must be an ISO datetime")
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise EvidenceContractError(f"{field} must be an ISO datetime") from exc


def _leaf_paths(prefix: str, values: Mapping[str, Any]) -> list[str]:
    paths: list[str] = []
    for key, value in values.items():
        path = f"{prefix}.{key}"
        if isinstance(value, Mapping):
            paths.extend(_leaf_paths(path, value))
        elif value is not None:
            paths.append(path)
    return paths
