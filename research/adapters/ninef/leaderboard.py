"""Strict read-only projection of the official Ninef competition leaderboard."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib
import os
from typing import Any, Callable, Mapping

from adapters.ninef.account import NinefAccountAdapterError, _clean_token, _request_json
from competition_core.pipeline.time_context import SHANGHAI


Requester = Callable[[str, str, Mapping[str, str] | None], dict[str, Any]]


def _positive_integer(value: Any, field: str) -> int:
    if type(value) is not int or value <= 0:
        raise NinefAccountAdapterError(f"Ninef {field} is invalid")
    return value


def _percentage(value: Any) -> Decimal:
    text = str(value or "").strip().removesuffix("%").strip()
    try:
        result = Decimal(text)
    except InvalidOperation as exc:
        raise NinefAccountAdapterError("Ninef ranking return is invalid") from exc
    if not result.is_finite() or abs(result) > Decimal("1000000"):
        raise NinefAccountAdapterError("Ninef ranking return is invalid")
    return result


def _optional_decimal(value: Any, field: str, *, nonnegative: bool = False) -> Decimal | None:
    if value in (None, ""):
        return None
    text = str(value).strip().replace(",", "").removesuffix("%").strip()
    try:
        result = Decimal(text)
    except InvalidOperation as exc:
        raise NinefAccountAdapterError(f"Ninef {field} is invalid") from exc
    if not result.is_finite() or (nonnegative and result < 0):
        raise NinefAccountAdapterError(f"Ninef {field} is invalid")
    return result


def _optional_integer(value: Any, field: str) -> int | None:
    if value in (None, ""):
        return None
    if type(value) is not int or value < 0:
        raise NinefAccountAdapterError(f"Ninef {field} is invalid")
    return value


def _first(value: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in value:
            return value[key]
    return None


def _observed_at(value: Any, fallback: datetime) -> datetime:
    if value in (None, ""):
        return fallback.astimezone(SHANGHAI)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise NinefAccountAdapterError("Ninef leaderboard timestamp is invalid") from exc
    if parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=SHANGHAI)
    return parsed.astimezone(SHANGHAI)


@dataclass(frozen=True, slots=True)
class RankingEntry:
    participant_ref: str
    display_name: str
    rank: int
    return_pct: Decimal
    total_assets: Decimal | None = None
    day_return_pct: Decimal | None = None
    trade_win_rate: Decimal | None = None
    trade_count: int | None = None
    max_drawdown: Decimal | None = None

    def to_dict(self):
        return {"participant_ref": self.participant_ref, "display_name": self.display_name,
                "rank": self.rank, "return_pct": str(self.return_pct),
                "total_assets": None if self.total_assets is None else str(self.total_assets),
                "day_return_pct": None if self.day_return_pct is None else str(self.day_return_pct),
                "trade_win_rate": None if self.trade_win_rate is None else str(self.trade_win_rate),
                "trade_count": self.trade_count,
                "max_drawdown": None if self.max_drawdown is None else str(self.max_drawdown)}


@dataclass(frozen=True, slots=True)
class NinefLeaderboard:
    competition_id: str
    as_of: datetime
    entries: tuple[RankingEntry, ...]
    own: RankingEntry | None
    total_participants: int | None = None
    source: str = "ninef.competition_api.readonly"

    def to_dict(self):
        return {"schema_version": "ninef.leaderboard.v1", "competition_id": self.competition_id,
                "as_of": self.as_of.isoformat(), "entries": [item.to_dict() for item in self.entries],
                "own": self.own.to_dict() if self.own else None, "source": self.source,
                "top_limit": 30, "entry_count": len(self.entries),
                "total_participants": self.total_participants,
                "collection_consistent": True, "external_ranking_authoritative": True}


def _entry(value: Mapping[str, Any]) -> RankingEntry:
    rank = _positive_integer(value.get("rank"), "ranking rank")
    identity = value.get("userId") or value.get("displayName") or rank
    participant_ref = "ninef-player-" + hashlib.sha256(str(identity).encode("utf-8")).hexdigest()[:24]
    name = str(value.get("displayName") or "").strip()[:80]
    return RankingEntry(
        participant_ref=participant_ref,
        display_name=name,
        rank=rank,
        return_pct=_percentage(value.get("metricText")),
        total_assets=_optional_decimal(
            _first(value, "totalAssets", "totalAsset", "assetValue", "total_assets"),
            "ranking total assets", nonnegative=True,
        ),
        day_return_pct=_optional_decimal(
            _first(value, "dayReturnPct", "dailyReturnPct", "todayReturnPct", "day_return_pct"),
            "ranking daily return",
        ),
        trade_win_rate=_optional_decimal(
            _first(value, "tradeWinRate", "winRate", "trade_win_rate"),
            "ranking trade win rate", nonnegative=True,
        ),
        trade_count=_optional_integer(
            _first(value, "tradeCount", "trade_count"), "ranking trade count"
        ),
        max_drawdown=_optional_decimal(
            _first(value, "maxDrawdown", "max_drawdown"), "ranking maximum drawdown"
        ),
    )


def fetch_ninef_leaderboard(token: str, *, competition_id: str | None = None,
                            requester: Requester | None = None,
                            clock: Callable[[], datetime] | None = None) -> NinefLeaderboard:
    credential = _clean_token(token)
    read = requester or (lambda supplied, name, params=None: _request_json(supplied, name, params))
    selected = (competition_id or os.getenv("NINEF_COMPETITION_ID", "")).strip()
    if selected and (len(selected) > 160 or any(ord(char) < 32 for char in selected)):
        raise NinefAccountAdapterError("Ninef competition identifier is invalid")
    if not selected:
        data = read(credential, "competitions", None).get("data")
        items = data.get("items") if isinstance(data, dict) else None
        available = [str(item["competitionId"]) for item in (items or [])
                     if isinstance(item, dict) and item.get("competitionId")]
        if len(available) != 1:
            raise NinefAccountAdapterError("NINEF_COMPETITION_ID is required when competition selection is ambiguous")
        selected = available[0]
    payload = read(credential, "competition_status", {"competitionId": selected})
    status = payload.get("data")
    board = status.get("leaderboard") if isinstance(status, dict) else None
    rows = (board.get("items") or board.get("top50")) if isinstance(board, dict) else None
    if not isinstance(board, dict) or not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise NinefAccountAdapterError("Ninef leaderboard response is invalid")
    entries = tuple(_entry(row) for row in rows[:30])
    if len({item.participant_ref for item in entries}) != len(entries):
        raise NinefAccountAdapterError("Ninef leaderboard contains duplicate participants")
    own_raw = board.get("currentUser") or board.get("myRank")
    own = _entry(own_raw) if isinstance(own_raw, dict) else None
    total_raw = _first(board, "totalParticipants", "participantCount", "total")
    total_participants = _optional_integer(total_raw, "participant count")
    if total_participants is not None:
        observed_max_rank = max((item.rank for item in entries), default=0)
        if own is not None:
            observed_max_rank = max(observed_max_rank, own.rank)
        if total_participants < observed_max_rank:
            raise NinefAccountAdapterError("Ninef participant count contradicts ranking")
    now = (clock or (lambda: datetime.now(SHANGHAI)))()
    if not isinstance(now, datetime) or now.utcoffset() is None:
        raise NinefAccountAdapterError("Ninef observation clock must be timezone-aware")
    observed = _observed_at(board.get("updatedAt"), now)
    if observed > now.astimezone(SHANGHAI):
        raise NinefAccountAdapterError("Ninef leaderboard timestamp is in the future")
    return NinefLeaderboard(competition_id=selected, as_of=observed,
                            entries=entries, own=own, total_participants=total_participants)


__all__ = ["NinefLeaderboard", "RankingEntry", "fetch_ninef_leaderboard"]
