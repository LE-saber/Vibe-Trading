"""HTTP and runner contracts for bounded AI decision analysis."""

from __future__ import annotations

import math
import re
import hashlib
import json
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

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
_SYMBOL = re.compile(r"^\d{6}\.(?:SH|SZ)$")
_EVIDENCE_ID = re.compile(r"^[0-9a-f]{64}$")


class ProvenanceInput(BaseModel):
    field: str
    source: str
    as_of: datetime

    @model_validator(mode="after")
    def validate_provenance(self) -> "ProvenanceInput":
        if "." not in self.field or self.field.split(".", 1)[0] not in SECTION_NAMES:
            raise ValueError("provenance field must be section-qualified")
        if not self.source.strip():
            raise ValueError("provenance source must not be empty")
        if self.as_of.tzinfo is None or self.as_of.utcoffset() is None:
            raise ValueError("provenance as_of must include a timezone")
        return self


class EvidenceInput(BaseModel):
    evidence_id: str
    schema_version: int = 1
    symbol: str
    as_of: datetime
    snapshot_id: str
    market: dict[str, Any] = Field(default_factory=dict)
    technical: dict[str, Any] = Field(default_factory=dict)
    flow: dict[str, Any] = Field(default_factory=dict)
    margin: dict[str, Any] = Field(default_factory=dict)
    dragon_tiger: dict[str, Any] = Field(default_factory=dict)
    fundamental: dict[str, Any] = Field(default_factory=dict)
    events: dict[str, Any] = Field(default_factory=dict)
    risk: dict[str, Any] = Field(default_factory=dict)
    missing: list[str] = Field(default_factory=list)
    provenance: list[ProvenanceInput] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_evidence(self) -> "EvidenceInput":
        if not _EVIDENCE_ID.fullmatch(self.evidence_id):
            raise ValueError("evidence_id must be a lowercase SHA-256 digest")
        if not _SYMBOL.fullmatch(self.symbol):
            raise ValueError("symbol must use canonical six-digit .SH/.SZ format")
        if self.as_of.tzinfo is None or self.as_of.utcoffset() is None:
            raise ValueError("evidence as_of must include a timezone")
        if self.schema_version != 1 or not self.snapshot_id.strip():
            raise ValueError("unsupported or incomplete EvidenceBundle")
        if len(set(self.missing)) != len(self.missing):
            raise ValueError("missing fields must be unique")
        sources = {item.field: item for item in self.provenance}
        if len(sources) != len(self.provenance):
            raise ValueError("provenance fields must be unique")
        for item in self.provenance:
            if item.as_of > self.as_of:
                raise ValueError("future provenance is not allowed")
        leaves: dict[str, Any] = {}
        for section in SECTION_NAMES:
            value = getattr(self, section)
            self._validate_json(value, section)
            self._flatten(value, section, leaves)
        for field in self.missing:
            if "." not in field or field.split(".", 1)[0] not in SECTION_NAMES:
                raise ValueError("missing fields must be section-qualified")
            if field in leaves and leaves[field] is not None:
                raise ValueError("missing field has a value")
        unproven = sorted(
            field for field, value in leaves.items() if value is not None and field not in sources
        )
        if unproven:
            raise ValueError(f"non-missing evidence fields require provenance: {', '.join(unproven)}")
        content = {
            "schema_version": self.schema_version,
            "symbol": self.symbol,
            "as_of": self.as_of.isoformat(),
            "snapshot_id": self.snapshot_id,
            **{name: getattr(self, name) for name in SECTION_NAMES},
            "missing": self.missing,
            "provenance": [
                {
                    "field": item.field,
                    "source": item.source,
                    "as_of": item.as_of.isoformat(),
                }
                for item in self.provenance
            ],
        }
        expected = hashlib.sha256(
            json.dumps(
                content, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        ).hexdigest()
        if self.evidence_id != expected:
            raise ValueError("evidence_id does not match EvidenceBundle content")
        return self

    @classmethod
    def _validate_json(cls, value: Any, path: str) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                if not isinstance(key, str) or not key:
                    raise ValueError(f"{path} keys must be non-empty strings")
                cls._validate_json(item, f"{path}.{key}")
            return
        if isinstance(value, list):
            for item in value:
                cls._validate_json(item, path)
            return
        if value is None or isinstance(value, (str, bool, int)):
            return
        if isinstance(value, float) and math.isfinite(value):
            return
        raise ValueError(f"{path} contains a non-finite or non-JSON value")

    @classmethod
    def _flatten(cls, value: Any, path: str, target: dict[str, Any]) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                cls._flatten(item, f"{path}.{key}", target)
            return
        target[path] = value

    def section(self, name: str) -> dict[str, Any]:
        if name not in SECTION_NAMES:
            raise KeyError(name)
        return getattr(self, name)


class CandidateInput(BaseModel):
    symbol: str
    rank: int = Field(ge=1)
    score: float | None = None
    reasons: list[str] = Field(default_factory=list)
    risk_flags: list[str] = Field(default_factory=list)
    evidence_ref: str

    @model_validator(mode="after")
    def validate_candidate(self) -> "CandidateInput":
        if not _SYMBOL.fullmatch(self.symbol):
            raise ValueError("candidate symbol must be canonical")
        expected = f"evidence://{self.evidence_ref.removeprefix('evidence://')}"
        evidence_id = expected.removeprefix("evidence://")
        if not _EVIDENCE_ID.fullmatch(evidence_id):
            raise ValueError("evidence_ref must contain a SHA-256 evidence ID")
        self.evidence_ref = expected
        return self


class AnalyzeRequest(BaseModel):
    candidate: CandidateInput
    evidence: EvidenceInput
    depth: Literal["light", "full"] = "full"
    run_id: str = Field(min_length=1, max_length=120)

    @model_validator(mode="after")
    def validate_pair(self) -> "AnalyzeRequest":
        if self.candidate.symbol != self.evidence.symbol:
            raise ValueError("candidate/evidence symbol conflict")
        if self.candidate.evidence_ref != f"evidence://{self.evidence.evidence_id}":
            raise ValueError("candidate evidence_ref does not match EvidenceBundle")
        return self


class BatchAnalyzeRequest(BaseModel):
    items: list[AnalyzeRequest] = Field(min_length=1, max_length=10)

    @model_validator(mode="after")
    def validate_batch(self) -> "BatchAnalyzeRequest":
        symbols = [item.candidate.symbol for item in self.items]
        if len(set(symbols)) != len(symbols):
            raise ValueError("batch symbols must be unique")
        return self


class DecisionOutput(BaseModel):
    run_id: str
    symbol: str
    evidence_id: str
    analyst_reports: dict[str, str]
    bull_case: str
    bear_case: str
    risk_assessment: str
    final_score: float
    score_derivation: str
    decision: Literal["ADVANCE", "KEEP", "DROP", "REVIEW"]
    confidence: float | None = None
    final_rating: str
    execution_mode: Literal["ANALYSIS_ONLY"] = "ANALYSIS_ONLY"
    numeric_audit: dict[str, Any] = Field(default_factory=dict)
    checkpoint_enabled: bool
    framework: str = "TradingAgents"
    framework_version: str
