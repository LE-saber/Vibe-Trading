"""Serializable domain records for daily Competition Universe snapshots."""

from __future__ import annotations

import hashlib
import json
import tempfile
from dataclasses import dataclass
from datetime import date
from enum import Enum
from pathlib import Path
from typing import Any


class Board(str, Enum):
    MAIN = "main_board"
    CHINEXT = "chinext"
    STAR = "star_market"


@dataclass(frozen=True, slots=True)
class UniverseSource:
    endpoint: str
    fields: tuple[str, ...]
    parameters: tuple[tuple[str, str], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "endpoint": self.endpoint,
            "fields": list(self.fields),
            "parameters": dict(self.parameters),
        }


@dataclass(frozen=True, slots=True)
class UniverseMember:
    ts_code: str
    name: str
    exchange: str
    board: Board
    list_date: date
    delist_date: date | None
    list_status: str
    is_st: bool
    is_delisting_risk: bool
    is_suspended: bool
    is_tradeable: bool
    is_eligible: bool
    exclusion_reasons: tuple[str, ...]
    recent_amount_cny: float | None
    recent_volume_lots: float | None
    liquidity_date: date | None
    trading_rule_key: str
    normal_price_limit: float
    new_listing_special_sessions: int
    field_sources: tuple[tuple[str, str], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ts_code": self.ts_code,
            "name": self.name,
            "exchange": self.exchange,
            "board": self.board.value,
            "list_date": self.list_date.isoformat(),
            "delist_date": self.delist_date.isoformat() if self.delist_date else None,
            "list_status": self.list_status,
            "is_st": self.is_st,
            "is_delisting_risk": self.is_delisting_risk,
            "is_suspended": self.is_suspended,
            "is_tradeable": self.is_tradeable,
            "is_eligible": self.is_eligible,
            "exclusion_reasons": list(self.exclusion_reasons),
            "recent_amount_cny": self.recent_amount_cny,
            "recent_volume_lots": self.recent_volume_lots,
            "liquidity_date": self.liquidity_date.isoformat() if self.liquidity_date else None,
            "trading_rule_key": self.trading_rule_key,
            "normal_price_limit": self.normal_price_limit,
            "new_listing_special_sessions": self.new_listing_special_sessions,
            "field_sources": dict(self.field_sources),
        }


@dataclass(frozen=True, slots=True)
class CompetitionUniverseSnapshot:
    competition_id: str
    as_of: date
    snapshot_phase: str
    market_data_reference_date: date
    members: tuple[UniverseMember, ...]
    sources: tuple[UniverseSource, ...]
    warnings: tuple[str, ...] = ()
    schema_version: int = 1

    @property
    def eligible_members(self) -> tuple[UniverseMember, ...]:
        return tuple(member for member in self.members if member.is_eligible)

    @property
    def snapshot_id(self) -> str:
        payload = json.dumps(
            self._content_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def counts(self) -> dict[str, int]:
        result = {
            "candidates": len(self.members),
            "eligible": len(self.eligible_members),
            "excluded": len(self.members) - len(self.eligible_members),
            "st": sum(member.is_st for member in self.members),
            "delisting_risk": sum(member.is_delisting_risk for member in self.members),
            "suspended": sum(member.is_suspended for member in self.members),
            "untradeable": sum(not member.is_tradeable for member in self.members),
        }
        for board in Board:
            result[board.value] = sum(member.board == board for member in self.members)
            result[f"eligible_{board.value}"] = sum(
                member.board == board and member.is_eligible for member in self.members
            )
        return result

    def to_dict(self) -> dict[str, Any]:
        return {"snapshot_id": self.snapshot_id, **self._content_dict()}

    def write_json(self, directory: str | Path) -> Path:
        target_dir = Path(directory)
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / f"universe-{self.as_of.isoformat()}-{self.snapshot_phase}.json"
        payload = json.dumps(
            self.to_dict(), ensure_ascii=False, sort_keys=True, indent=2
        ) + "\n"
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=target_dir, delete=False
        ) as handle:
            handle.write(payload)
            temporary = Path(handle.name)
        temporary.replace(target)
        return target

    def _content_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "competition_id": self.competition_id,
            "as_of": self.as_of.isoformat(),
            "snapshot_phase": self.snapshot_phase,
            "market_data_reference_date": self.market_data_reference_date.isoformat(),
            "members": [member.to_dict() for member in self.members],
            "sources": [source.to_dict() for source in self.sources],
            "warnings": list(self.warnings),
        }
