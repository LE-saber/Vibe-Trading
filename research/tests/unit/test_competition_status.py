from datetime import datetime
from decimal import Decimal

from adapters.ninef.leaderboard import NinefLeaderboard, RankingEntry
from competition_core.pipeline.account import AccountPosition, AccountSnapshot
from competition_core.pipeline.time_context import SHANGHAI
from workflows.competition_status import build_competition_status, write_competition_status


NOW = datetime(2026, 9, 17, 10, 30, tzinfo=SHANGHAI)


def account():
    return AccountSnapshot(
        as_of=datetime(2026, 9, 17, 10, 29, tzinfo=SHANGHAI),
        source="ninef.competition_api.readonly",
        source_kind="competition_api",
        currency="CNY",
        cash_balance=Decimal("200000"),
        available_cash=Decimal("200000"),
        current_total_asset=Decimal("1000000"),
        positions=(AccountPosition(
            symbol="600519.SH", quantity=500, sellable_quantity=500,
            market_value=Decimal("800000"),
        ),),
        pending_order_count=0,
    )


def leaderboard():
    own = RankingEntry(
        participant_ref="ninef-player-owner",
        display_name="本人",
        rank=12,
        return_pct=Decimal("0"),
        total_assets=Decimal("1000000"),
        day_return_pct=Decimal("1.2"),
    )
    return NinefLeaderboard(
        competition_id="fixture",
        as_of=datetime(2026, 9, 17, 10, 29, tzinfo=SHANGHAI),
        entries=(own,),
        own=own,
        total_participants=100,
    )


def receipt():
    return {
        "orders": {"complete": True, "open_count": 0, "count": 5, "items": []},
        "account_snapshot_hash": "a" * 64,
    }


def test_status_combines_rank_and_current_account_assets_without_orders():
    result = build_competition_status(
        checkpoint="midday",
        account=account(),
        account_receipt=receipt(),
        leaderboard=leaderboard(),
        observed_at=NOW,
    )
    assert result["summary"]["own_rank"] == 12
    assert result["summary"]["account_total_assets"] == "1000000.00"
    assert result["summary"]["asset_crosscheck"] == "MATCH"
    assert result["orders_submitted"] == 0 and result["read_only"] is True


def test_status_writer_creates_auditable_checkpoint(tmp_path):
    result = write_competition_status(
        run_id="20260917-midday-103000",
        checkpoint="midday",
        account=account(),
        account_receipt=receipt(),
        leaderboard=leaderboard(),
        artifact_root=tmp_path,
        observed_at=NOW,
    )
    directory = tmp_path / "20260917-midday-103000"
    assert result["summary"]["own_rank"] == 12
    assert {path.name for path in directory.iterdir()} == {
        "account.json", "leaderboard.json", "competition-status.json", "run_manifest.json"
    }
