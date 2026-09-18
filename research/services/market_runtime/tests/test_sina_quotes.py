from __future__ import annotations

from datetime import datetime

import httpx
import pandas as pd
import pytest
from fastapi.testclient import TestClient
from zoneinfo import ZoneInfo

from market_runtime import api as api_module
from market_runtime.api import app, get_provider
from market_runtime.provider import MootdxProvider
from market_runtime.sina_quotes import (
    SINA_QUOTES_URL,
    SINA_REFERER,
    SinaQuoteError,
    SinaQuotesProvider,
    parse_sina_quote_body,
)
from market_runtime.symbols import MarketSymbol

SHANGHAI = ZoneInfo("Asia/Shanghai")


def identity(value: str) -> MarketSymbol:
    return MarketSymbol.parse(value)


def fields(
    *,
    name: str = "测试",
    source_date: str = "2026-09-02",
    source_time: str = "09:30:00",
    **overrides: str,
) -> list[str]:
    values = [""] * 33
    values[0] = name
    values[1] = "10.0"
    values[2] = "9.5"
    values[3] = "10.2"
    values[4] = "10.3"
    values[5] = "9.8"
    values[6] = "10.1"
    values[7] = "10.3"
    values[8] = "1200"
    values[9] = "12240.0"
    values[11] = "10.1"
    values[21] = "10.3"
    values[30] = source_date
    values[31] = source_time
    values[32] = "00"
    for key, value in overrides.items():
        values[int(key)] = value
    return values


def line(identity_name: str, values: list[str] | None = None) -> str:
    payload = ",".join(values if values is not None else fields())
    return f'var hq_str_{identity_name}="{payload}";'


def provider_for(
    body: str,
    requests: list[httpx.Request] | None = None,
    *,
    status_code: int = 200,
    clock: datetime = datetime(2026, 9, 2, 9, 30, 1, tzinfo=SHANGHAI),
) -> SinaQuotesProvider:
    def handler(request: httpx.Request) -> httpx.Response:
        if requests is not None:
            requests.append(request)
        return httpx.Response(
            status_code,
            content=body.encode("utf-8"),
            request=request,
        )

    return SinaQuotesProvider(
        transport=httpx.MockTransport(handler),
        clock=lambda: clock,
    )


def test_normal_sh_sz_quotes_use_full_identity_and_same_row_fields() -> None:
    requests: list[httpx.Request] = []
    symbols = [identity("600519.SH"), identity("000001.SZ"), identity("300750.SZ")]
    body = "\n".join(
        [
            line("sz300750", fields(**{"3": "300.75", "11": "300.70", "21": "300.80"})),
            line("sh600519", fields(**{"3": "1500.5", "11": "1500.4", "21": "1500.6"})),
            line("sz000001", fields(**{"3": "11.25", "11": "11.24", "21": "11.26"})),
        ]
    )

    rows = provider_for(body, requests).quotes(symbols)

    assert [row["symbol"] for row in rows] == ["600519.SH", "000001.SZ", "300750.SZ"]
    by_symbol = {row["symbol"]: row for row in rows}
    assert by_symbol["600519.SH"]["price"] == 1500.5
    assert by_symbol["600519.SH"]["bid1"] == 1500.4
    assert by_symbol["600519.SH"]["ask1"] == 1500.6
    assert by_symbol["000001.SZ"]["price"] == 11.25
    assert by_symbol["300750.SZ"]["price"] == 300.75
    assert by_symbol["600519.SH"]["volume"] == 1200.0
    assert by_symbol["600519.SH"]["amount"] == 12240.0
    assert by_symbol["600519.SH"]["volume_unit"] == "shares"
    assert by_symbol["600519.SH"]["amount_unit"] == "CNY"
    assert by_symbol["600519.SH"]["source"] == "sina"
    assert by_symbol["600519.SH"]["server_time"] == "2026-09-02T09:30:00+08:00"
    assert by_symbol["600519.SH"]["as_of"] == by_symbol["600519.SH"]["server_time"]
    assert by_symbol["600519.SH"]["timestamp_quality"] == "verified"
    assert requests[0].method == "GET"
    assert str(requests[0].url) == SINA_QUOTES_URL + "sh600519,sz000001,sz300750"
    assert requests[0].headers["Referer"] == SINA_REFERER


@pytest.mark.parametrize(
    "bad_body",
    [
        lambda: line("sh600519") + "\n" + line("sh600519"),
        lambda: line("sh600519", []) + "\n" + line("sh600519", []),
        lambda: line("sh600519", []) + "\n" + line("sh600519"),
        lambda: line("sz600519"),
        lambda: line("sh999999"),
        lambda: 'var hq_str_sh600519="not,a,valid,quote";',
        lambda: 'hq_str_sh600519="not,a,valid,quote";',
    ],
)
def test_duplicate_exchange_unexpected_and_malformed_lines_fail_closed(bad_body) -> None:
    with pytest.raises(SinaQuoteError):
        SinaQuotesProvider(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, content=bad_body().encode("utf-8"), request=request
                )
            )
        ).quotes([identity("600519.SH")])


def test_empty_body_and_empty_stock_remain_missing() -> None:
    symbols = [identity("600519.SH"), identity("000001.SZ")]
    assert provider_for("").quotes(symbols) == []
    assert provider_for(line("sh600519", [])).quotes(symbols) == []


@pytest.mark.parametrize(
    ("source_date", "source_time", "clock", "quality", "server_time", "as_of"),
    [
        (
            "",
            "09:30:00",
            datetime(2026, 9, 2, 9, 30, 1, tzinfo=SHANGHAI),
            "missing",
            None,
            None,
        ),
        (
            "bad-date",
            "09:30:00",
            datetime(2026, 9, 2, 9, 30, 1, tzinfo=SHANGHAI),
            "invalid",
            None,
            None,
        ),
        (
            "2026-09-02",
            "09:30:02",
            datetime(2026, 9, 2, 9, 30, 1, tzinfo=SHANGHAI),
            "future",
            "2026-09-02T09:30:02+08:00",
            None,
        ),
        (
            "2026-09-01",
            "14:59:59",
            datetime(2026, 9, 2, 9, 30, 1, tzinfo=SHANGHAI),
            "verified",
            "2026-09-01T14:59:59+08:00",
            "2026-09-01T14:59:59+08:00",
        ),
    ],
)
def test_source_date_time_quality_is_explicit(
    source_date: str,
    source_time: str,
    clock: datetime,
    quality: str,
    server_time: str | None,
    as_of: str | None,
) -> None:
    body = line("sh600519", fields(source_date=source_date, source_time=source_time))
    row = provider_for(body, clock=clock).quotes([identity("600519.SH")])[0]

    assert row["timestamp_quality"] == quality
    assert row["server_time"] == server_time
    assert row["as_of"] == as_of
    assert row["received_at"] == clock.isoformat()
    if quality == "verified":
        assert row["trade_date"] == source_date
    else:
        assert row["trade_date"] == (
            source_date
            if source_date in {"2026-09-02", "2026-09-01"}
            else None
        )


def test_nonfinite_and_incomplete_numbers_become_none_not_zero() -> None:
    values = fields(**{"1": "", "3": "NaN", "8": "inf", "9": "-inf", "11": "", "21": "nan"})
    row = provider_for(line("sh600519", values)).quotes([identity("600519.SH")])[0]

    assert row["open"] is None
    assert row["price"] is None
    assert row["volume"] is None
    assert row["amount"] is None
    assert row["bid1"] is None
    assert row["ask1"] is None
    assert 0 not in {row["open"], row["price"], row["volume"], row["amount"]}


@pytest.mark.parametrize("status_code", [301, 302, 307, 308, 500])
def test_redirects_and_non_success_responses_fail_closed(status_code: int) -> None:
    with pytest.raises(SinaQuoteError):
        provider_for(line("sh600519"), status_code=status_code).quotes(
            [identity("600519.SH")]
        )


def test_response_size_is_bounded() -> None:
    oversized = "x" * (1024 * 1024 + 1)
    with pytest.raises(SinaQuoteError, match="1 MiB"):
        provider_for(oversized).quotes([identity("600519.SH")])


def test_parser_retains_first_33_fields_and_maps_by_identity() -> None:
    symbols = [identity("600519.SH"), identity("000001.SZ")]
    body = line("sz000001", fields(**{"3": "11.0"})) + "\n" + line(
        "sh600519", fields(**{"3": "12.0"}) + ["optional"]
    )

    parsed = parse_sina_quote_body(body, symbols)

    assert parsed["sh600519"][3] == "12.0"
    assert len(parsed["sh600519"]) == 33
    assert parsed["sz000001"][3] == "11.0"


class FakeMootdxClient:
    def quotes(self, symbol):
        return pd.DataFrame(
            [
                {
                    "market": 1,
                    "code": symbol[0],
                    "price": 10.2,
                    "last_close": 9.5,
                    "open": 10.0,
                    "high": 10.3,
                    "low": 9.8,
                    "servertime": "09:30:00",
                    "bid1": 10.1,
                    "ask1": 10.3,
                    "volume": 100,
                    "amount": 1000,
                }
            ]
        )

    def bars(self, **_kwargs):
        return pd.DataFrame(
            {
                "datetime": ["2026-09-02 09:30:00"],
                "open": [10.0],
                "high": [10.3],
                "low": [9.8],
                "close": [10.2],
                "volume": [100.0],
                "amount": [1000.0],
            }
        )


def test_api_default_source_and_get_provider_override_remain_mootdx(monkeypatch) -> None:
    monkeypatch.delenv("MARKET_RUNTIME_QUOTE_SOURCE", raising=False)
    app.dependency_overrides[get_provider] = lambda: MootdxProvider(FakeMootdxClient())
    try:
        with TestClient(app) as client:
            response = client.post("/quotes", json={"symbols": ["600519.SH"]})
            bars = client.post(
                "/bars", json={"symbols": ["600519.SH"], "interval": "1m"}
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["quotes"][0]["source"] == "mootdx"
    assert bars.status_code == 200
    assert bars.json()["bars"]["600519.SH"][0]["source"] == "mootdx"


def test_api_explicit_sina_changes_quotes_only(monkeypatch) -> None:
    monkeypatch.setenv("MARKET_RUNTIME_QUOTE_SOURCE", "sina")

    class FakeSinaProvider:
        def quotes(self, symbols):
            return [{"symbol": symbols[0].canonical, "source": "sina"}]

    monkeypatch.setattr(api_module, "SinaQuotesProvider", FakeSinaProvider)
    app.dependency_overrides[get_provider] = lambda: MootdxProvider(FakeMootdxClient())
    try:
        with TestClient(app) as client:
            quotes = client.post("/quotes", json={"symbols": ["600519.SH"]})
            bars = client.post(
                "/bars", json={"symbols": ["600519.SH"], "interval": "1m"}
            )
    finally:
        app.dependency_overrides.clear()

    assert quotes.status_code == 200
    assert quotes.json()["quotes"][0]["source"] == "sina"
    assert bars.status_code == 200
    assert bars.json()["bars"]["600519.SH"][0]["source"] == "mootdx"


def test_quote_api_accepts_supported_index_but_bars_remain_equity_only(monkeypatch) -> None:
    monkeypatch.setenv("MARKET_RUNTIME_QUOTE_SOURCE", "sina")

    class FakeSinaProvider:
        def quotes(self, symbols):
            return [{"symbol": symbol.canonical, "source": "sina"} for symbol in symbols]

    monkeypatch.setattr(api_module, "SinaQuotesProvider", FakeSinaProvider)
    try:
        with TestClient(app) as client:
            quote_response = client.post("/quotes", json={"symbols": ["600519.SH", "000300.SH"]})
            bars_response = client.post("/bars", json={"symbols": ["000300.SH"], "interval": "1m"})
    finally:
        app.dependency_overrides.clear()

    assert quote_response.status_code == 200
    assert [row["symbol"] for row in quote_response.json()["quotes"]] == ["600519.SH", "000300.SH"]
    assert bars_response.status_code == 422


def test_api_sina_does_not_initialize_mootdx(monkeypatch) -> None:
    monkeypatch.setenv("MARKET_RUNTIME_QUOTE_SOURCE", "sina")

    class FakeSinaProvider:
        def quotes(self, symbols):
            return [{"symbol": symbols[0].canonical, "source": "sina"}]

    def fail_mootdx_initialization():
        raise AssertionError("mootdx must not initialize for Sina quotes")

    monkeypatch.setattr(api_module, "SinaQuotesProvider", FakeSinaProvider)
    monkeypatch.setattr(api_module, "get_provider", fail_mootdx_initialization)
    app.dependency_overrides.clear()
    try:
        with TestClient(app) as client:
            response = client.post("/quotes", json={"symbols": ["600519.SH"]})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["quotes"][0]["source"] == "sina"


def test_api_unknown_quote_source_fails_closed(monkeypatch) -> None:
    monkeypatch.setenv("MARKET_RUNTIME_QUOTE_SOURCE", "other")
    app.dependency_overrides[get_provider] = lambda: MootdxProvider(FakeMootdxClient())
    try:
        with TestClient(app) as client:
            response = client.post("/quotes", json={"symbols": ["600519.SH"]})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503


def test_api_readiness_rejects_unknown_quote_source(monkeypatch) -> None:
    monkeypatch.setenv("MARKET_RUNTIME_QUOTE_SOURCE", "other")

    response = TestClient(app).get("/readiness")

    assert response.status_code == 200
    assert response.json()["ready"] is False
