"""Versioned EvidenceBundle contract persisted outside model context."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Any, Mapping

from contracts.symbol import CanonicalSymbol

SECTION_NAMES = (
    "market",
    "technical",
    "flow",
    "margin",
    "dragon_tiger",
    "fundamental",
    "events",
    "risk",
)


class EvidenceContractError(ValueError):
    """Raised when evidence is ambiguous, non-causal, or non-serializable."""


@dataclass(frozen=True, slots=True)
class Provenance:
    field: str
    source: str
    as_of: datetime

    def __post_init__(self) -> None:
        if not self.field or "." not in self.field:
            raise EvidenceContractError("provenance field must be a section-qualified path")
        if self.field.split(".", 1)[0] not in SECTION_NAMES:
            raise EvidenceContractError(f"unsupported provenance field: {self.field}")
        if not self.source.strip():
            raise EvidenceContractError("provenance source must not be empty")
        _require_aware(self.as_of, "provenance.as_of")

    def to_dict(self) -> dict[str, str]:
        return {
            "field": self.field,
            "source": self.source,
            "as_of": self.as_of.isoformat(),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "Provenance":
        if set(value) != {"field", "source", "as_of"}:
            raise EvidenceContractError("provenance must contain field, source, and as_of")
        return cls(
            field=str(value["field"]),
            source=str(value["source"]),
            as_of=_parse_datetime(value["as_of"], "provenance.as_of"),
        )


@dataclass(frozen=True, slots=True)
class EvidenceBundle:
    symbol: CanonicalSymbol
    as_of: datetime
    snapshot_id: str
    sections: Mapping[str, Mapping[str, Any]]
    missing: tuple[str, ...]
    provenance: tuple[Provenance, ...]
    schema_version: int = 1

    def __post_init__(self) -> None:
        _require_aware(self.as_of, "as_of")
        if not self.snapshot_id.strip():
            raise EvidenceContractError("snapshot_id must not be empty")
        if self.schema_version != 1:
            raise EvidenceContractError("unsupported EvidenceBundle schema_version")
        if set(self.sections) != set(SECTION_NAMES):
            raise EvidenceContractError("sections must define every EvidenceBundle section")

        frozen_sections: dict[str, Mapping[str, Any]] = {}
        leaves: dict[str, Any] = {}
        for section_name in SECTION_NAMES:
            section = self.sections[section_name]
            if not isinstance(section, Mapping):
                raise EvidenceContractError(f"{section_name} must be a mapping")
            plain = _json_copy(dict(section), section_name)
            frozen_sections[section_name] = MappingProxyType(plain)
            _flatten(section_name, plain, leaves)
        object.__setattr__(self, "sections", MappingProxyType(frozen_sections))

        if len(set(self.missing)) != len(self.missing):
            raise EvidenceContractError("missing fields must be unique")
        for field in self.missing:
            if field.split(".", 1)[0] not in SECTION_NAMES or "." not in field:
                raise EvidenceContractError(f"invalid missing field: {field}")
            if field in leaves and leaves[field] is not None:
                raise EvidenceContractError(f"missing field has a value: {field}")

        provenance_by_field = {item.field: item for item in self.provenance}
        if len(provenance_by_field) != len(self.provenance):
            raise EvidenceContractError("provenance fields must be unique")
        for item in self.provenance:
            if item.as_of > self.as_of:
                raise EvidenceContractError(f"future provenance is not allowed: {item.field}")
        unproven = sorted(
            field
            for field, value in leaves.items()
            if value is not None and field not in provenance_by_field
        )
        if unproven:
            raise EvidenceContractError(
                f"non-missing evidence fields require provenance: {', '.join(unproven)}"
            )

    @property
    def evidence_id(self) -> str:
        payload = json.dumps(
            self._content_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {"evidence_id": self.evidence_id, **self._content_dict()}

    def to_json(self) -> str:
        return json.dumps(
            self.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )

    def _content_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "symbol": self.symbol.symbol,
            "as_of": self.as_of.isoformat(),
            "snapshot_id": self.snapshot_id,
            **{name: _thaw(self.sections[name]) for name in SECTION_NAMES},
            "missing": list(self.missing),
            "provenance": [item.to_dict() for item in self.provenance],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "EvidenceBundle":
        allowed = {
            "evidence_id",
            "schema_version",
            "symbol",
            "as_of",
            "snapshot_id",
            "missing",
            "provenance",
            *SECTION_NAMES,
        }
        if set(value) - allowed:
            raise EvidenceContractError("unsupported EvidenceBundle fields")
        try:
            bundle = cls(
                symbol=CanonicalSymbol.from_core(value["symbol"]),
                as_of=_parse_datetime(value["as_of"], "as_of"),
                snapshot_id=str(value["snapshot_id"]),
                sections={name: value.get(name, {}) for name in SECTION_NAMES},
                missing=tuple(str(item) for item in value.get("missing", ())),
                provenance=tuple(
                    Provenance.from_dict(item) for item in value.get("provenance", ())
                ),
                schema_version=int(value.get("schema_version", 1)),
            )
        except KeyError as exc:
            raise EvidenceContractError(f"missing EvidenceBundle field: {exc.args[0]}") from exc
        supplied_id = value.get("evidence_id")
        if supplied_id is not None and supplied_id != bundle.evidence_id:
            raise EvidenceContractError("evidence_id does not match bundle content")
        return bundle


def _parse_datetime(value: Any, name: str) -> datetime:
    if not isinstance(value, str):
        raise EvidenceContractError(f"{name} must be an ISO datetime")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise EvidenceContractError(f"{name} must be an ISO datetime") from exc
    _require_aware(parsed, name)
    return parsed


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise EvidenceContractError(f"{name} must include a timezone")


def _json_copy(value: Any, path: str) -> Any:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) or not key for key in value):
            raise EvidenceContractError(f"{path} keys must be non-empty strings")
        return {key: _json_copy(item, f"{path}.{key}") for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_copy(item, path) for item in value]
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise EvidenceContractError(f"{path} must not contain NaN or infinity")
        return value
    raise EvidenceContractError(f"{path} contains a non-JSON value")


def _flatten(prefix: str, value: Mapping[str, Any], target: dict[str, Any]) -> None:
    for key, item in value.items():
        path = f"{prefix}.{key}"
        if isinstance(item, Mapping):
            _flatten(path, item, target)
        else:
            target[path] = item


def _thaw(value: Mapping[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(dict(value), ensure_ascii=False))


__all__ = [
    "EvidenceBundle",
    "EvidenceContractError",
    "Provenance",
    "SECTION_NAMES",
]
