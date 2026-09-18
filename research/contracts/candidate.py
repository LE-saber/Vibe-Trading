"""Stable candidate record crossing deterministic and AI research stages."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from contracts.symbol import CanonicalSymbol, SymbolContractError


class CandidateContractError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class CandidateRecord:
    symbol: str
    stage: str
    score: float
    rank: int
    reasons: tuple[str, ...]
    risk_flags: tuple[str, ...]
    evidence_ref: str | None = None

    def __post_init__(self) -> None:
        try:
            CanonicalSymbol.from_core(self.symbol)
        except SymbolContractError as exc:
            raise CandidateContractError(str(exc)) from exc
        if not self.stage.strip() or self.rank < 1:
            raise CandidateContractError("stage must be non-empty and rank positive")
        if not isinstance(self.score, (int, float)):
            raise CandidateContractError("score must be numeric")
        if self.evidence_ref is not None and not self.evidence_ref.startswith("evidence://"):
            raise CandidateContractError("evidence_ref must use evidence://")

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "stage": self.stage,
            "score": float(self.score),
            "rank": self.rank,
            "reasons": list(self.reasons),
            "risk_flags": list(self.risk_flags),
            "evidence_ref": self.evidence_ref,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "CandidateRecord":
        return cls(
            symbol=str(payload["symbol"]),
            stage=str(payload["stage"]),
            score=float(payload["score"]),
            rank=int(payload["rank"]),
            reasons=tuple(str(item) for item in payload.get("reasons", [])),
            risk_flags=tuple(str(item) for item in payload.get("risk_flags", [])),
            evidence_ref=payload.get("evidence_ref"),
        )
