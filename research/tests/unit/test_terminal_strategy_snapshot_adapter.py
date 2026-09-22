from datetime import date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from competition_core.pipeline.artifacts import content_hash
from competition_core.terminal_strategy import (
    SnapshotAdapterBlocker,
    TerminalStrategyConfig,
    adapt_account_leaderboard_snapshot,
    load_terminal_strategy_config,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")
AS_OF = datetime(2026, 9, 22, 13, 15, tzinfo=SHANGHAI)
TRADE_DATE = date(2026, 9, 22)


def _config(*, cutoff=30):
    payload = load_terminal_strategy_config().to_dict()
    payload["competition"]["award_rank_cutoff"] = cutoff
    payload["competition"]["award_rule_source"] = "fixture:rules-v1" if cutoff is not None else None
    return TerminalStrategyConfig.from_dict(payload)


def _account_view(*, as_of=AS_OF, pending=0, complete=True, open_count=None, unknown_count=0,
                  collection=True, read_only=True, orders_submitted=0):
    open_count = pending if open_count is None else open_count
    account = {
        "schema_version": "competition.account.v1",
        "as_of": as_of.isoformat(), "source": "fixture.account", "source_kind": "competition_api",
        "currency": "CNY", "cash_balance": "1000000.00", "available_cash": "1000000.00",
        "current_total_asset": "1000000.00", "positions": [], "pending_order_count": pending,
    }
    return {
        "schema_version": "competition.account-view.v1", "account": account, "frozen_cash": "0.00",
        "orders": {"complete": complete, "count": pending, "open_count": open_count, "items": [], "unknown_count": unknown_count},
        "collection_consistent": collection, "fee_evidence_available": False,
        "orders_submitted": orders_submitted, "read_only": read_only,
    }


def _leaderboard_view(*, as_of=AS_OF, own=True, own_assets="1000000.00", missing_rank=None,
                      collection=True, authoritative=True, read_only=True, orders_submitted=0):
    entries = [
        {"rank": rank, "participant_ref": f"participant-{rank}",
         "total_assets": None if rank == missing_rank else f"{1000000 + rank * 1000:.2f}"}
        for rank in range(1, 31)
    ]
    return {
        "schema_version": "ninef.leaderboard.v1", "competition_id": "fixture-competition",
        "as_of": as_of.isoformat(), "entries": entries,
        "own": None if not own else {"rank": 34, "total_assets": own_assets},
        "source": "fixture.leaderboard", "top_limit": 30, "entry_count": 30,
        "total_participants": 34, "collection_consistent": collection,
        "external_ranking_authoritative": authoritative, "read_only": read_only,
        "orders_submitted": orders_submitted,
    }


def _adapt(**changes):
    values = {
        "account_view": _account_view(),
        "leaderboard_view": _leaderboard_view(),
        "remaining_trade_dates": (TRADE_DATE, date(2026, 9, 23)),
        "observed_at": AS_OF,
        "config": _config(),
        "freshness_threshold_seconds": 60,
    }
    values.update(changes)
    return adapt_account_leaderboard_snapshot(**values)


def test_normal_snapshot_uses_account_assets_and_preserves_hashes():
    result = _adapt()
    assert result.snapshot is not None
    assert result.diagnostics.ready
    assert result.snapshot.own_total_assets == Decimal("1000000.00")
    assert result.snapshot.award_rank_cutoff == 30
    assert result.snapshot.award_line_entry is not None
    assert result.snapshot.award_line_entry.as_of == AS_OF
    assert result.account_hash and len(result.account_hash) == 64
    assert result.leaderboard_hash and len(result.leaderboard_hash) == 64
    assert result.execution_ready is False


def test_cutoff_null_does_not_infer_top30():
    result = _adapt(config=_config(cutoff=None))
    assert result.snapshot is not None
    assert result.snapshot.award_rank_cutoff is None
    assert result.snapshot.award_line_entry is None


def test_missing_rank_asset_is_explicit_and_not_filled():
    result = _adapt(leaderboard_view=_leaderboard_view(missing_rank=30))
    assert result.snapshot is not None
    assert result.snapshot.award_line_entry is None
    assert SnapshotAdapterBlocker.LEADERBOARD_ASSET_MISSING in result.diagnostics.blockers
    assert result.diagnostics.full_snapshot is False


def test_missing_or_inconsistent_own_is_blocked():
    missing = _adapt(leaderboard_view=_leaderboard_view(own=False))
    assert missing.snapshot is not None
    assert SnapshotAdapterBlocker.OWN_MISSING in missing.diagnostics.blockers
    mismatch = _adapt(leaderboard_view=_leaderboard_view(own_assets="1005000.00"))
    assert SnapshotAdapterBlocker.ASSET_MISMATCH in mismatch.diagnostics.blockers
    assert mismatch.snapshot.own_total_assets == Decimal("1000000.00")


def test_orders_incomplete_and_open_count_mismatch_are_not_reconciled():
    incomplete = _adapt(account_view=_account_view(complete=False, unknown_count=0))
    assert SnapshotAdapterBlocker.ORDERS_INCOMPLETE in incomplete.diagnostics.blockers
    assert incomplete.snapshot is not None and incomplete.snapshot.complete_order_history is False
    mismatch = _adapt(account_view=_account_view(pending=1, open_count=0, unknown_count=0))
    assert SnapshotAdapterBlocker.OPEN_ORDER_COUNT_MISMATCH in mismatch.diagnostics.blockers
    assert mismatch.snapshot is not None and mismatch.snapshot.pending_order_count == 1


def test_unknown_order_state_is_never_guessed_as_zero():
    view = _account_view()
    del view["orders"]["unknown_count"]
    result = _adapt(account_view=view)
    assert SnapshotAdapterBlocker.UNKNOWN_ORDER_STATE_UNAVAILABLE in result.diagnostics.blockers
    assert result.snapshot is not None
    assert result.snapshot.complete_order_history is False
    assert result.account_position_facts == {}


def test_orders_reconciled_facts_are_explicit_for_positions():
    view = _account_view()
    view["account"]["cash_balance"] = "990000.00"
    view["account"]["available_cash"] = "990000.00"
    view["account"]["current_total_asset"] = "1000000.00"
    view["account"]["positions"] = [{"symbol": "600001.SH", "quantity": 100,
                                      "sellable_quantity": 50, "market_value": "10000.00"}]
    result = _adapt(account_view=view)
    assert result.account_position_facts["600001.SH"]["holding_quantity"] == 100
    assert result.account_position_facts["600001.SH"]["sellable_quantity"] == 50
    assert result.account_position_facts["600001.SH"]["orders_reconciled"] is True


def test_stale_and_future_sources_mark_freshness_and_block():
    stale = _adapt(
        account_view=_account_view(as_of=AS_OF - timedelta(seconds=61)),
        leaderboard_view=_leaderboard_view(as_of=AS_OF - timedelta(seconds=61)),
    )
    assert SnapshotAdapterBlocker.ACCOUNT_STALE in stale.diagnostics.blockers
    assert SnapshotAdapterBlocker.LEADERBOARD_STALE in stale.diagnostics.blockers
    assert stale.snapshot is not None
    assert stale.snapshot.account_fresh is False and stale.snapshot.leaderboard_fresh is False
    future = _adapt(leaderboard_view=_leaderboard_view(as_of=AS_OF + timedelta(seconds=1)))
    assert SnapshotAdapterBlocker.LEADERBOARD_FUTURE in future.diagnostics.blockers


def test_collection_and_authority_flags_are_not_downgraded():
    result = _adapt(
        account_view=_account_view(collection=False, read_only=False),
        leaderboard_view=_leaderboard_view(collection=False, authoritative=False, read_only=False),
    )
    assert SnapshotAdapterBlocker.ACCOUNT_COLLECTION_INCONSISTENT in result.diagnostics.blockers
    assert SnapshotAdapterBlocker.ACCOUNT_NOT_READ_ONLY in result.diagnostics.blockers
    assert SnapshotAdapterBlocker.LEADERBOARD_COLLECTION_INCONSISTENT in result.diagnostics.blockers
    assert SnapshotAdapterBlocker.LEADERBOARD_NOT_AUTHORITATIVE in result.diagnostics.blockers
    assert result.snapshot is not None and result.snapshot.account_fresh is False


def test_remaining_dates_are_explicit_and_final_session_comes_from_sequence():
    result = _adapt(remaining_trade_dates=(TRADE_DATE, date(2026, 9, 30)))
    assert result.snapshot is not None and result.snapshot.is_final_trade_date is False
    final = _adapt(remaining_trade_dates=(TRADE_DATE,))
    assert final.snapshot is not None and final.snapshot.is_final_trade_date is True
    invalid = _adapt(remaining_trade_dates=(TRADE_DATE, TRADE_DATE))
    assert invalid.snapshot is None
    assert SnapshotAdapterBlocker.REMAINING_DATES_INVALID in invalid.diagnostics.blockers


def test_naive_observation_and_invalid_account_are_fail_closed():
    naive = _adapt(observed_at=datetime(2026, 9, 22, 13, 15))
    assert naive.snapshot is None
    assert SnapshotAdapterBlocker.OBSERVED_AT_INVALID in naive.diagnostics.blockers
    invalid = _adapt(account_view={"schema_version": "competition.account-view.v1"})
    assert invalid.snapshot is None
    assert SnapshotAdapterBlocker.ACCOUNT_VIEW_INVALID in invalid.diagnostics.blockers


def test_hashes_and_projection_are_stable():
    first = _adapt()
    second = _adapt()
    assert first.account_hash == second.account_hash
    assert first.leaderboard_hash == second.leaderboard_hash
    assert first.to_dict() == second.to_dict()
    assert content_hash(first.snapshot.to_dict()) == content_hash(second.snapshot.to_dict())
