from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from market_runtime.provider import MootdxProvider, normalize_bars
from market_runtime.symbols import MarketSymbol

SHANGHAI = ZoneInfo("Asia/Shanghai")


class FakeClient:
    def __init__(self, *, servertime="11:29:00", **overrides):
        self.servertime = servertime
        self.overrides = overrides

    def quotes(self, symbol):
        return pd.DataFrame([{
            "market": 1, "code": "600519", "price": 1301.2,
            "last_close": 1299.52, "open": 1295.0, "high": 1307.99,
            "low": 1286.1, "servertime": self.servertime, "bid1": 1301.18,
            "ask1": 1301.57, "volume": 25779, "amount": 3346555136.0,
            **self.overrides,
        }])

    def bars(self, **_kwargs):
        timestamps = [
            "2026-09-01 11:29:00", "2026-09-01 13:00:00",
            "2026-09-01 13:01:00", "2026-09-01 13:05:00",
        ]
        return pd.DataFrame(
            {
                "datetime": timestamps,
                "open": [1.0] * 4, "high": [2.0] * 4, "low": [0.5] * 4,
                "close": [1.5] * 4, "volume": [100.0] * 4, "amount": [150.0] * 4,
            }
        )


def test_quote_contract_is_compact_and_exchange_checked() -> None:
    rows = MootdxProvider(FakeClient()).quotes([MarketSymbol.parse("600519.SH")])

    assert rows[0]["symbol"] == "600519.SH"
    assert rows[0]["prev_close"] == 1299.52
    assert rows[0]["volume_unit"] == "lots"
    assert rows[0]["server_time"] == "11:29:00"
    assert rows[0]["trade_date"] is None
    assert rows[0]["source"] == "mootdx"
    assert rows[0]["timestamp_quality"] == "time_only"
    assert rows[0]["timestamp_source"] == "mootdx.servertime"
    assert rows[0]["raw_server_time"] == "11:29:00"
    assert rows[0]["received_at"]
    assert rows[0]["as_of"] is None
    assert "reversed_bytes0" not in rows[0]


def test_full_quote_timestamp_preserves_as_of_and_rejects_future() -> None:
    observed_at = datetime(2026, 9, 2, 10, 0, tzinfo=SHANGHAI)
    current = MootdxProvider(
        FakeClient(servertime="2026-09-02T09:59:59+08:00"),
        clock=lambda: observed_at,
    ).quotes([MarketSymbol.parse("600519.SH")])[0]
    future = MootdxProvider(
        FakeClient(servertime="2026-09-02T10:00:01+08:00"),
        clock=lambda: observed_at,
    ).quotes([MarketSymbol.parse("600519.SH")])[0]

    assert current["timestamp_quality"] == "verified"
    assert current["trade_date"] == "2026-09-02"
    assert current["server_time"] == current["as_of"]
    assert future["timestamp_quality"] == "future"
    assert future["trade_date"] == "2026-09-02"
    assert future["as_of"] is None


def test_matching_full_datetime_and_row_trade_date_are_verified() -> None:
    observed_at = datetime(2026, 9, 2, 10, 0, tzinfo=SHANGHAI)
    row = MootdxProvider(
        FakeClient(
            servertime="2026-09-02T09:59:59+08:00",
            trade_date="2026-09-02",
        ),
        clock=lambda: observed_at,
    ).quotes([MarketSymbol.parse("600519.SH")])[0]

    assert row["timestamp_quality"] == "verified"
    assert row["trade_date"] == "2026-09-02"
    assert row["raw_server_time"] == "2026-09-02T09:59:59+08:00"
    assert row["received_at"] == observed_at.isoformat()
    assert row["as_of"] == row["server_time"]


def test_time_only_with_explicit_row_trade_date_can_be_verified() -> None:
    observed_at = datetime(2026, 9, 2, 10, 0, tzinfo=SHANGHAI)
    row = MootdxProvider(
        FakeClient(servertime="09:30:00", trade_date="20260902"),
        clock=lambda: observed_at,
    ).quotes([MarketSymbol.parse("600519.SH")])[0]

    assert row["timestamp_quality"] == "verified"
    assert row["timestamp_source"] == "mootdx.servertime+row.trade_date"
    assert row["server_time"] == "2026-09-02T09:30:00+08:00"
    assert row["trade_date"] == "2026-09-02"
    assert row["as_of"] == row["server_time"]


def test_non_finite_quote_numbers_are_missing() -> None:
    row = MootdxProvider(
        FakeClient(price=float("inf"), amount=float("-inf"), bid1=float("nan"))
    ).quotes([MarketSymbol.parse("600519.SH")])[0]

    assert row["price"] is None
    assert row["amount"] is None
    assert row["bid1"] is None


def test_missing_quote_timestamp_is_explicit_and_not_now() -> None:
    row = MootdxProvider(FakeClient(servertime=None)).quotes(
        [MarketSymbol.parse("600519.SH")]
    )[0]

    assert row["server_time"] is None
    assert row["trade_date"] is None
    assert row["timestamp_quality"] == "missing"
    assert row["timestamp_source"] == "mootdx.servertime"
    assert row["as_of"] is None


def test_noon_boundary_normalization_is_conditional() -> None:
    frame = FakeClient().bars()
    result = normalize_bars(frame)
    times = [item.strftime("%H:%M") for item in result.index]

    assert times == ["11:29", "11:30", "13:01", "13:05"]


def test_noon_normalization_is_per_day_and_idempotent() -> None:
    frame = pd.DataFrame({
        "datetime": [
            "2026-09-01 11:29:00", "2026-09-01 13:00:00",
            "2026-09-02 13:00:00", "2026-09-02 13:01:00",
        ]
    })

    once = normalize_bars(frame)
    twice = normalize_bars(once)

    assert [item.strftime("%Y-%m-%d %H:%M") for item in once.index] == [
        "2026-09-01 11:29", "2026-09-01 11:30",
        "2026-09-02 13:00", "2026-09-02 13:01",
    ]
    assert once.index.equals(twice.index)


def test_existing_1130_prevents_1300_rewrite() -> None:
    frame = FakeClient().bars()
    frame.loc[len(frame)] = frame.iloc[-1]
    frame.loc[len(frame) - 1, "datetime"] = "2026-09-01 11:30:00"

    result = normalize_bars(frame)

    assert "13:00" in [item.strftime("%H:%M") for item in result.index]


def test_bars_support_every_required_interval() -> None:
    provider = MootdxProvider(FakeClient())
    identity = MarketSymbol.parse("600519.SH")

    for interval in ("1m", "5m", "15m", "30m", "1h"):
        rows = provider.bars(identity, interval, 100)
        assert rows and rows[0]["source"] == "mootdx"
        assert rows[0]["volume_unit"] == "shares"


def test_date_only_server_value_is_not_misread_as_midnight_or_compact_time() -> None:
    for value in ("20260902", "2026-09-02"):
        row = MootdxProvider(FakeClient(servertime=value)).quotes([MarketSymbol.parse("600519.SH")])[0]
        assert row["timestamp_quality"] == "invalid"
        assert row["as_of"] is None
