from __future__ import annotations

import json

import pytest

from competition_core.config import load_config
from competition_core.universe import CompetitionUniverse
from tests.fakes import FakeTushare


def test_filters_non_a_share_and_preserves_exclusion_evidence() -> None:
    snapshot = CompetitionUniverse(FakeTushare(), load_config()).build("2026-08-31")

    assert [item.ts_code for item in snapshot.eligible_members] == [
        "300750.SZ", "600000.SH", "688001.SH"
    ]
    assert "920001.BJ" not in {item.ts_code for item in snapshot.members}
    assert "510300.SH" not in {item.ts_code for item in snapshot.members}
    members = {item.ts_code: item for item in snapshot.members}
    assert members["000001.SZ"].exclusion_reasons == ("st",)
    assert members["000002.SZ"].exclusion_reasons == ("delisting_risk",)
    assert members["000003.SZ"].exclusion_reasons == ("suspended", "untradeable")
    assert members["000004.SZ"].exclusion_reasons == ("untradeable",)


def test_liquidity_units_sources_and_board_rule_are_explicit() -> None:
    snapshot = CompetitionUniverse(FakeTushare(), load_config()).build("2026-08-31")
    member = next(item for item in snapshot.members if item.ts_code == "300750.SZ")

    assert member.recent_amount_cny == 200_000_000.0
    assert member.recent_volume_lots == 600_000.0
    assert member.normal_price_limit == pytest.approx(0.20)
    assert member.new_listing_special_sessions == 5
    assert dict(member.field_sources)["liquidity"] == "tushare.daily"


def test_preopen_uses_previous_completed_day_and_current_suspension() -> None:
    client = FakeTushare()
    snapshot = CompetitionUniverse(client, load_config()).build("2026-08-31", "preopen")

    assert snapshot.market_data_reference_date.isoformat() == "2026-08-28"
    assert client.daily_trade_dates == ["20260828"]
    assert client.suspend_trade_dates == ["20260831"]
    assert next(item for item in snapshot.members if item.ts_code == "300750.SZ").is_tradeable


def test_preopen_requires_current_suspension_evidence() -> None:
    universe = CompetitionUniverse(
        FakeTushare(suspend_error=RuntimeError("unsupported")), load_config()
    )

    with pytest.raises(RuntimeError, match="required for a preopen"):
        universe.build("2026-08-31", "preopen")


def test_postclose_suspension_degradation_is_recorded_and_fail_closed() -> None:
    snapshot = CompetitionUniverse(
        FakeTushare(suspend_error=RuntimeError("unsupported")), load_config()
    ).build("2026-08-31", "postclose")
    missing_daily = next(item for item in snapshot.members if item.ts_code == "000004.SZ")

    assert snapshot.warnings
    assert missing_daily.is_suspended is True
    assert dict(missing_daily.field_sources)["suspension"] == "tushare.daily_presence"


def test_snapshot_hash_and_file_are_reproducible(tmp_path) -> None:
    universe = CompetitionUniverse(FakeTushare(), load_config())
    first = universe.build("2026-08-31")
    second = universe.build("2026-08-31")

    assert first.snapshot_id == second.snapshot_id
    path = first.write_json(tmp_path)
    first_bytes = path.read_bytes()
    second.write_json(tmp_path)
    assert path.read_bytes() == first_bytes
    assert json.loads(first_bytes)["snapshot_id"] == first.snapshot_id


def test_closed_or_out_of_period_dates_are_rejected() -> None:
    with pytest.raises(ValueError, match="outside"):
        CompetitionUniverse(FakeTushare(), load_config()).build("2026-08-30")
