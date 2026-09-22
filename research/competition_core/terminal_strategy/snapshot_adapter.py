"""Pure read-only account/leaderboard to :class:`TerminalSnapshot` adapter.

The adapter consumes already captured API views.  It performs no I/O and never
uses the displayed leaderboard limit as a competition award rule.  A snapshot
may be returned with ``fresh=False`` when it is useful for audit, but any
diagnostic blocker makes it ineligible for a full terminal evaluation.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
import re
from typing import Any
from zoneinfo import ZoneInfo

from competition_core.pipeline.account import AccountSnapshot
from competition_core.pipeline.artifacts import content_hash
from competition_core.pipeline.errors import PipelineError

from .config import TerminalStrategyConfig
from .models import LeaderboardEntry, TerminalModelError, TerminalSnapshot


SHANGHAI = ZoneInfo("Asia/Shanghai")
_SHA256 = re.compile(r"^[0-9a-fA-F]{64}$")


class SnapshotAdapterBlocker(StrEnum):
    OBSERVED_AT_INVALID = "OBSERVED_AT_INVALID"
    CONFIG_INVALID = "CONFIG_INVALID"
    ACCOUNT_VIEW_INVALID = "ACCOUNT_VIEW_INVALID"
    ACCOUNT_SCHEMA_INVALID = "ACCOUNT_SCHEMA_INVALID"
    ACCOUNT_NOT_READ_ONLY = "ACCOUNT_NOT_READ_ONLY"
    ACCOUNT_COLLECTION_INCONSISTENT = "ACCOUNT_COLLECTION_INCONSISTENT"
    ACCOUNT_STALE = "ACCOUNT_STALE"
    ACCOUNT_FUTURE = "ACCOUNT_FUTURE"
    ACCOUNT_HASH_INVALID = "ACCOUNT_HASH_INVALID"
    LEADERBOARD_VIEW_INVALID = "LEADERBOARD_VIEW_INVALID"
    LEADERBOARD_SCHEMA_INVALID = "LEADERBOARD_SCHEMA_INVALID"
    LEADERBOARD_NOT_READ_ONLY = "LEADERBOARD_NOT_READ_ONLY"
    LEADERBOARD_NOT_AUTHORITATIVE = "LEADERBOARD_NOT_AUTHORITATIVE"
    LEADERBOARD_COLLECTION_INCONSISTENT = "LEADERBOARD_COLLECTION_INCONSISTENT"
    LEADERBOARD_STALE = "LEADERBOARD_STALE"
    LEADERBOARD_FUTURE = "LEADERBOARD_FUTURE"
    LEADERBOARD_HASH_INVALID = "LEADERBOARD_HASH_INVALID"
    COMPETITION_ID_INVALID = "COMPETITION_ID_INVALID"
    LEADERBOARD_ENTRIES_INVALID = "LEADERBOARD_ENTRIES_INVALID"
    LEADERBOARD_RANK_INVALID = "LEADERBOARD_RANK_INVALID"
    LEADERBOARD_ASSET_MISSING = "LEADERBOARD_ASSET_MISSING"
    LEADERBOARD_ASSET_INVALID = "LEADERBOARD_ASSET_INVALID"
    OWN_MISSING = "OWN_MISSING"
    OWN_RANK_INVALID = "OWN_RANK_INVALID"
    OWN_ASSET_MISSING = "OWN_ASSET_MISSING"
    OWN_ASSET_INVALID = "OWN_ASSET_INVALID"
    ASSET_MISMATCH = "ASSET_MISMATCH"
    ORDERS_SCHEMA_INVALID = "ORDERS_SCHEMA_INVALID"
    ORDERS_INCOMPLETE = "ORDERS_INCOMPLETE"
    OPEN_ORDER_COUNT_INVALID = "OPEN_ORDER_COUNT_INVALID"
    OPEN_ORDER_COUNT_MISMATCH = "OPEN_ORDER_COUNT_MISMATCH"
    PENDING_ORDERS = "PENDING_ORDERS"
    UNKNOWN_ORDER_STATE_UNAVAILABLE = "UNKNOWN_ORDER_STATE_UNAVAILABLE"
    UNKNOWN_ORDER_STATE = "UNKNOWN_ORDER_STATE"
    ORDERS_SUBMITTED = "ORDERS_SUBMITTED"
    REMAINING_DATES_INVALID = "REMAINING_DATES_INVALID"
    SNAPSHOT_UNAVAILABLE = "SNAPSHOT_UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class SnapshotAdapterDiagnostics:
    blockers: tuple[str, ...] = ()
    account_as_of: str | None = None
    leaderboard_as_of: str | None = None
    account_age_seconds: Decimal | None = None
    leaderboard_age_seconds: Decimal | None = None
    freshness_threshold_seconds: Decimal | None = None
    account_fresh: bool = False
    leaderboard_fresh: bool = False
    full_snapshot: bool = False
    asset_difference: Decimal | None = None
    asset_tolerance: Decimal | None = None
    account_hash: str | None = None
    leaderboard_hash: str | None = None

    @property
    def ready(self) -> bool:
        return self.full_snapshot and not self.blockers

    @property
    def blocked(self) -> bool:
        return bool(self.blockers)

    def to_dict(self) -> dict[str, Any]:
        def money(value: Decimal | None) -> str | None:
            return None if value is None else str(value)

        return {
            "blockers": list(self.blockers),
            "account_as_of": self.account_as_of,
            "leaderboard_as_of": self.leaderboard_as_of,
            "account_age_seconds": money(self.account_age_seconds),
            "leaderboard_age_seconds": money(self.leaderboard_age_seconds),
            "freshness_threshold_seconds": money(self.freshness_threshold_seconds),
            "account_fresh": self.account_fresh,
            "leaderboard_fresh": self.leaderboard_fresh,
            "full_snapshot": self.full_snapshot,
            "asset_difference": money(self.asset_difference),
            "asset_tolerance": money(self.asset_tolerance),
            "account_hash": self.account_hash,
            "leaderboard_hash": self.leaderboard_hash,
        }


@dataclass(frozen=True, slots=True)
class SnapshotAdapterResult:
    snapshot: TerminalSnapshot | None
    diagnostics: SnapshotAdapterDiagnostics
    account_position_facts: Mapping[str, Mapping[str, Any]]
    account_hash: str | None
    leaderboard_hash: str | None
    execution_ready: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot": None if self.snapshot is None else self.snapshot.to_dict(),
            "diagnostics": self.diagnostics.to_dict(),
            "account_position_facts": {
                key: dict(value) for key, value in sorted(self.account_position_facts.items())
            },
            "account_hash": self.account_hash,
            "leaderboard_hash": self.leaderboard_hash,
            "execution_ready": False,
        }


SnapshotAdapterError = ValueError


@dataclass(slots=True)
class _Collector:
    blockers: list[str]

    def __init__(self) -> None:
        self.blockers = []

    def add(self, value: SnapshotAdapterBlocker | str) -> None:
        reason = value.value if isinstance(value, SnapshotAdapterBlocker) else str(value)
        if reason not in self.blockers:
            self.blockers.append(reason)


def adapt_account_leaderboard_snapshot(
    account_view: Mapping[str, Any],
    leaderboard_view: Mapping[str, Any],
    remaining_trade_dates: Iterable[date],
    observed_at: datetime,
    config: TerminalStrategyConfig,
    freshness_threshold_seconds: int | Decimal | None = None,
) -> SnapshotAdapterResult:
    """Adapt two captured read-only views into a provider-neutral snapshot."""

    collector = _Collector()
    observed = _aware(observed_at)
    if observed is None:
        collector.add(SnapshotAdapterBlocker.OBSERVED_AT_INVALID)
    if not isinstance(config, TerminalStrategyConfig):
        collector.add(SnapshotAdapterBlocker.CONFIG_INVALID)
    threshold = _decimal(
        config.award_line.max_snapshot_age_seconds if isinstance(config, TerminalStrategyConfig)
        and freshness_threshold_seconds is None else freshness_threshold_seconds,
    )
    if threshold is None or threshold < 0:
        collector.add(SnapshotAdapterBlocker.CONFIG_INVALID)

    dates = _remaining_dates(remaining_trade_dates, observed, collector)
    account, account_hash, account_facts, account_state = _parse_account(account_view, observed, threshold, collector)
    leaderboard, leaderboard_hash, leaderboard_state = _parse_leaderboard(leaderboard_view, observed, threshold, collector)

    difference = tolerance = None
    if account is not None and leaderboard_state["own_assets"] is not None:
        own_assets = leaderboard_state["own_assets"]
        difference = abs(account.current_total_asset - own_assets)
        tolerance = max(Decimal("1.00"), account.current_total_asset * Decimal("0.001"))
        if difference > tolerance:
            collector.add(SnapshotAdapterBlocker.ASSET_MISMATCH)

    snapshot: TerminalSnapshot | None = None
    if observed is not None and dates is not None and account is not None and leaderboard is not None:
        entries = leaderboard_state["entries"]
        if entries:
            try:
                snapshot = TerminalSnapshot(
                    competition_id=leaderboard_state["competition_id"],
                    as_of=observed,
                    trade_date=observed.astimezone(SHANGHAI).date(),
                    remaining_trade_dates=dates,
                    own_total_assets=account.current_total_asset,
                    leaderboard=tuple(entries),
                    award_rank_cutoff=config.competition.award_rank_cutoff,
                    award_rule_source=config.competition.award_rule_source,
                    own_rank=leaderboard_state["own_rank"],
                    account_snapshot_hash=account_hash,
                    leaderboard_snapshot_hash=leaderboard_hash,
                    account_fresh=bool(account_state["fresh"] and not any(
                        reason in collector.blockers for reason in (
                            SnapshotAdapterBlocker.ACCOUNT_COLLECTION_INCONSISTENT.value,
                            SnapshotAdapterBlocker.ACCOUNT_NOT_READ_ONLY.value,
                            SnapshotAdapterBlocker.ACCOUNT_STALE.value,
                            SnapshotAdapterBlocker.ACCOUNT_FUTURE.value,
                        )
                    )),
                    leaderboard_fresh=bool(leaderboard_state["fresh"] and not any(
                        reason in collector.blockers for reason in (
                            SnapshotAdapterBlocker.LEADERBOARD_COLLECTION_INCONSISTENT.value,
                            SnapshotAdapterBlocker.LEADERBOARD_NOT_READ_ONLY.value,
                            SnapshotAdapterBlocker.LEADERBOARD_NOT_AUTHORITATIVE.value,
                            SnapshotAdapterBlocker.LEADERBOARD_STALE.value,
                            SnapshotAdapterBlocker.LEADERBOARD_FUTURE.value,
                        )
                    )),
                    complete_order_history=bool(account_state["orders_reconciled"] is not None),
                    pending_order_count=account.pending_order_count,
                )
            except (TerminalModelError, TypeError, ValueError):
                collector.add(SnapshotAdapterBlocker.SNAPSHOT_UNAVAILABLE)
                snapshot = None
        else:
            collector.add(SnapshotAdapterBlocker.SNAPSHOT_UNAVAILABLE)
    else:
        collector.add(SnapshotAdapterBlocker.SNAPSHOT_UNAVAILABLE)

    full = snapshot is not None and not collector.blockers
    diagnostics = SnapshotAdapterDiagnostics(
        blockers=tuple(collector.blockers),
        account_as_of=account_state["as_of"],
        leaderboard_as_of=leaderboard_state["as_of"],
        account_age_seconds=account_state["age"],
        leaderboard_age_seconds=leaderboard_state["age"],
        freshness_threshold_seconds=threshold,
        account_fresh=bool(account_state["fresh"]),
        leaderboard_fresh=bool(leaderboard_state["fresh"]),
        full_snapshot=full,
        asset_difference=difference,
        asset_tolerance=tolerance,
        account_hash=account_hash,
        leaderboard_hash=leaderboard_hash,
    )
    return SnapshotAdapterResult(snapshot, diagnostics, account_facts, account_hash, leaderboard_hash, False)


adapt_pipeline_snapshot = adapt_account_leaderboard_snapshot
build_terminal_snapshot = adapt_account_leaderboard_snapshot


def _parse_account(account_view: Mapping[str, Any], observed: datetime | None, threshold: Decimal | None,
                   collector: _Collector) -> tuple[AccountSnapshot | None, str | None, dict[str, Mapping[str, Any]], dict[str, Any]]:
    state: dict[str, Any] = {"as_of": None, "age": None, "fresh": False, "orders_reconciled": None}
    if not isinstance(account_view, Mapping):
        collector.add(SnapshotAdapterBlocker.ACCOUNT_VIEW_INVALID)
        return None, None, {}, state
    required = {"schema_version", "account", "frozen_cash", "orders", "collection_consistent",
                "fee_evidence_available", "orders_submitted", "read_only"}
    allowed = required | {"account_snapshot_hash"}
    if set(account_view) - allowed or not required <= set(account_view):
        collector.add(SnapshotAdapterBlocker.ACCOUNT_VIEW_INVALID)
        return None, None, {}, state
    if account_view.get("schema_version") != "competition.account-view.v1":
        collector.add(SnapshotAdapterBlocker.ACCOUNT_VIEW_INVALID)
    if account_view.get("read_only") is not True:
        collector.add(SnapshotAdapterBlocker.ACCOUNT_NOT_READ_ONLY)
    if account_view.get("collection_consistent") is not True:
        collector.add(SnapshotAdapterBlocker.ACCOUNT_COLLECTION_INCONSISTENT)
    if account_view.get("orders_submitted") != 0:
        collector.add(SnapshotAdapterBlocker.ORDERS_SUBMITTED)
    try:
        account = AccountSnapshot.from_dict(account_view.get("account"))
    except (PipelineError, TypeError, ValueError, KeyError):
        collector.add(SnapshotAdapterBlocker.ACCOUNT_SCHEMA_INVALID)
        return None, None, {}, state
    account_hash = account.snapshot_hash
    supplied_hash = account_view.get("account_snapshot_hash")
    if supplied_hash is not None:
        if not isinstance(supplied_hash, str) or _SHA256.fullmatch(supplied_hash) is None or supplied_hash.lower() != account_hash.lower():
            collector.add(SnapshotAdapterBlocker.ACCOUNT_HASH_INVALID)
    state["as_of"] = account.as_of.isoformat()
    if observed is not None:
        age = _age(observed, account.as_of)
        state["age"] = age
        if age < 0:
            collector.add(SnapshotAdapterBlocker.ACCOUNT_FUTURE)
        elif threshold is not None and age > threshold:
            collector.add(SnapshotAdapterBlocker.ACCOUNT_STALE)
        else:
            state["fresh"] = True
    orders_reconciled, order_complete, unknown_count = _parse_orders(
        account_view.get("orders"), account.pending_order_count, collector,
    )
    state["orders_reconciled"] = orders_reconciled
    state["order_complete"] = order_complete
    state["unknown_count"] = unknown_count
    facts = {
        position.symbol: {
            "holding_quantity": position.quantity,
            "sellable_quantity": position.sellable_quantity,
            "orders_reconciled": orders_reconciled,
        }
        for position in account.positions
    }
    return account, account_hash, facts, state


def _parse_orders(orders: Any, pending: int, collector: _Collector) -> tuple[bool | None, bool, int | None]:
    if not isinstance(orders, Mapping):
        collector.add(SnapshotAdapterBlocker.ORDERS_SCHEMA_INVALID)
        return None, False, None
    required = {"complete", "count", "open_count", "items"}
    if not required <= set(orders) or set(orders) - required - {"unknown_count", "unknown_order_count", "unknown"}:
        collector.add(SnapshotAdapterBlocker.ORDERS_SCHEMA_INVALID)
        return None, False, None
    complete = orders.get("complete")
    count = orders.get("count")
    open_count = orders.get("open_count")
    if type(complete) is not bool or type(count) is not int or count < 0 or not isinstance(orders.get("items"), list):
        collector.add(SnapshotAdapterBlocker.ORDERS_SCHEMA_INVALID)
        return None, False, None
    if type(open_count) is not int or open_count < 0:
        collector.add(SnapshotAdapterBlocker.OPEN_ORDER_COUNT_INVALID)
        return None, False, None
    if open_count != pending:
        collector.add(SnapshotAdapterBlocker.OPEN_ORDER_COUNT_MISMATCH)
    if open_count:
        collector.add(SnapshotAdapterBlocker.PENDING_ORDERS)
    if complete is not True:
        collector.add(SnapshotAdapterBlocker.ORDERS_INCOMPLETE)
    unknown_key = next((key for key in ("unknown_count", "unknown_order_count", "unknown") if key in orders), None)
    if unknown_key is None:
        collector.add(SnapshotAdapterBlocker.UNKNOWN_ORDER_STATE_UNAVAILABLE)
        return (False if open_count else None), complete, None
    unknown = orders[unknown_key]
    if type(unknown) is bool:
        unknown_count = int(unknown)
    elif type(unknown) is int and unknown >= 0:
        unknown_count = unknown
    else:
        collector.add(SnapshotAdapterBlocker.UNKNOWN_ORDER_STATE)
        return None, complete, None
    if unknown_count:
        collector.add(SnapshotAdapterBlocker.UNKNOWN_ORDER_STATE)
        return None, complete, unknown_count
    if complete and open_count == 0:
        return True, True, 0
    return False if open_count else None, complete, 0


def _parse_leaderboard(view: Mapping[str, Any], observed: datetime | None, threshold: Decimal | None,
                       collector: _Collector) -> tuple[dict[str, Any] | None, str | None, dict[str, Any]]:
    state: dict[str, Any] = {"as_of": None, "age": None, "fresh": False, "entries": [], "own_assets": None,
                             "own_rank": None, "competition_id": ""}
    if not isinstance(view, Mapping):
        collector.add(SnapshotAdapterBlocker.LEADERBOARD_VIEW_INVALID)
        return None, None, state
    required = {"schema_version", "competition_id", "as_of", "entries", "own", "source", "top_limit",
                "entry_count", "total_participants", "collection_consistent", "external_ranking_authoritative",
                "read_only", "orders_submitted"}
    allowed = required | {"leaderboard_snapshot_hash", "snapshot_hash"}
    if set(view) - allowed or not required <= set(view):
        collector.add(SnapshotAdapterBlocker.LEADERBOARD_VIEW_INVALID)
        return None, None, state
    if view.get("schema_version") != "ninef.leaderboard.v1":
        collector.add(SnapshotAdapterBlocker.LEADERBOARD_SCHEMA_INVALID)
    competition_id = view.get("competition_id")
    if not isinstance(competition_id, str) or not competition_id.strip():
        collector.add(SnapshotAdapterBlocker.COMPETITION_ID_INVALID)
        return None, None, state
    source_as_of = _aware(view.get("as_of"))
    if source_as_of is None:
        collector.add(SnapshotAdapterBlocker.LEADERBOARD_SCHEMA_INVALID)
        return None, None, state
    state["competition_id"] = competition_id.strip()
    state["as_of"] = source_as_of.isoformat()
    if observed is not None:
        age = _age(observed, source_as_of)
        state["age"] = age
        if age < 0:
            collector.add(SnapshotAdapterBlocker.LEADERBOARD_FUTURE)
        elif threshold is not None and age > threshold:
            collector.add(SnapshotAdapterBlocker.LEADERBOARD_STALE)
        else:
            state["fresh"] = True
    if view.get("read_only") is not True:
        collector.add(SnapshotAdapterBlocker.LEADERBOARD_NOT_READ_ONLY)
    if view.get("collection_consistent") is not True:
        collector.add(SnapshotAdapterBlocker.LEADERBOARD_COLLECTION_INCONSISTENT)
    if view.get("external_ranking_authoritative") is not True:
        collector.add(SnapshotAdapterBlocker.LEADERBOARD_NOT_AUTHORITATIVE)
    if view.get("orders_submitted") != 0:
        collector.add(SnapshotAdapterBlocker.ORDERS_SUBMITTED)

    raw_entries = view.get("entries")
    if not isinstance(raw_entries, list) or view.get("entry_count") != len(raw_entries):
        collector.add(SnapshotAdapterBlocker.LEADERBOARD_ENTRIES_INVALID)
        raw_entries = raw_entries if isinstance(raw_entries, list) else []
    ranks: list[int] = []
    entries: list[LeaderboardEntry] = []
    for raw in raw_entries:
        if not isinstance(raw, Mapping) or not isinstance(raw.get("rank"), int) or raw.get("rank") < 1:
            collector.add(SnapshotAdapterBlocker.LEADERBOARD_RANK_INVALID)
            continue
        rank = raw["rank"]
        ranks.append(rank)
        assets = raw.get("total_assets")
        if assets is None:
            collector.add(SnapshotAdapterBlocker.LEADERBOARD_ASSET_MISSING)
            continue
        parsed = _money(assets)
        if parsed is None or parsed <= 0:
            collector.add(SnapshotAdapterBlocker.LEADERBOARD_ASSET_INVALID)
            continue
        participant = raw.get("participant_ref")
        if participant is not None and not isinstance(participant, str):
            collector.add(SnapshotAdapterBlocker.LEADERBOARD_ENTRIES_INVALID)
            participant = None
        entries.append(LeaderboardEntry(rank, parsed, observed or source_as_of, participant))
    if len(ranks) != len(set(ranks)) or ranks != sorted(ranks):
        collector.add(SnapshotAdapterBlocker.LEADERBOARD_RANK_INVALID)
    state["entries"] = sorted(entries, key=lambda entry: entry.rank)
    own = view.get("own")
    if own is None:
        collector.add(SnapshotAdapterBlocker.OWN_MISSING)
    elif not isinstance(own, Mapping):
        collector.add(SnapshotAdapterBlocker.OWN_MISSING)
    else:
        own_rank = own.get("rank")
        if type(own_rank) is not int or own_rank < 1:
            collector.add(SnapshotAdapterBlocker.OWN_RANK_INVALID)
        else:
            state["own_rank"] = own_rank
        own_assets = own.get("total_assets")
        if own_assets is None:
            collector.add(SnapshotAdapterBlocker.OWN_ASSET_MISSING)
        else:
            parsed_own = _money(own_assets)
            if parsed_own is None or parsed_own <= 0:
                collector.add(SnapshotAdapterBlocker.OWN_ASSET_INVALID)
            else:
                state["own_assets"] = parsed_own
    supplied_hash = view.get("leaderboard_snapshot_hash", view.get("snapshot_hash"))
    try:
        computed_hash = content_hash(_json_safe(view))
    except (TypeError, ValueError):
        collector.add(SnapshotAdapterBlocker.LEADERBOARD_HASH_INVALID)
        computed_hash = None
    if supplied_hash is not None:
        if not isinstance(supplied_hash, str) or _SHA256.fullmatch(supplied_hash) is None:
            collector.add(SnapshotAdapterBlocker.LEADERBOARD_HASH_INVALID)
        else:
            computed_hash = supplied_hash.lower()
    state["source_hash"] = computed_hash
    return state, computed_hash, state


def _remaining_dates(values: Iterable[date], observed: datetime | None, collector: _Collector) -> tuple[date, ...] | None:
    if observed is None or isinstance(values, (str, bytes)):
        collector.add(SnapshotAdapterBlocker.REMAINING_DATES_INVALID)
        return None
    try:
        dates = tuple(values)
    except TypeError:
        collector.add(SnapshotAdapterBlocker.REMAINING_DATES_INVALID)
        return None
    trade_date = observed.astimezone(SHANGHAI).date()
    if not dates or any(not isinstance(item, date) or isinstance(item, datetime) for item in dates):
        collector.add(SnapshotAdapterBlocker.REMAINING_DATES_INVALID)
        return None
    if len(set(dates)) != len(dates) or dates != tuple(sorted(dates)) or trade_date not in dates or any(item < trade_date for item in dates):
        collector.add(SnapshotAdapterBlocker.REMAINING_DATES_INVALID)
        return None
    return dates


def _aware(value: Any) -> datetime | None:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return None
    if not isinstance(value, datetime) or value.utcoffset() is None:
        return None
    return value.astimezone(SHANGHAI)


def _age(observed: datetime, source: datetime) -> Decimal:
    return Decimal(str((observed - source).total_seconds()))


def _decimal(value: Any) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _money(value: Any) -> Decimal | None:
    parsed = _decimal(value)
    if parsed is None or parsed != parsed.quantize(Decimal("0.01")):
        return None
    return parsed.quantize(Decimal("0.01"))


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (set, frozenset)):
        return sorted((_json_safe(item) for item in value), key=repr)
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


__all__ = [
    "SnapshotAdapterBlocker",
    "SnapshotAdapterDiagnostics",
    "SnapshotAdapterError",
    "SnapshotAdapterResult",
    "adapt_account_leaderboard_snapshot",
    "adapt_pipeline_snapshot",
    "build_terminal_snapshot",
]
