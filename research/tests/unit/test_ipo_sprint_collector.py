from datetime import datetime, timezone

import httpx

from competition_core.ipo_sprint import (
    collect_ipo_sprint_snapshot,
    evaluate_ipo_sprint_snapshot,
    fetch_sina_quotes,
    load_ipo_sprint_config,
)


NOW = datetime(2026, 9, 23, 5, 30, tzinfo=timezone.utc)


class FakeTushare:
    def __init__(self, *, listing="20260922", conflict=False, announcement_title=None):
        self.listing = listing
        self.conflict = conflict
        self.announcement_title = announcement_title
        self.calls = []

    def _record(self, name, params, rows):
        self.calls.append((name, params))
        return rows

    def new_share(self, **params):
        return self._record("new_share", params, [{
            "ts_code": "688001.SH", "name": "冲刺样本", "ipo_date": "20260915",
            "issue_date": self.listing, "price": 10,
        }])

    def stock_basic(self, **params):
        return self._record("stock_basic", params, [{
            "ts_code": "688001.SH", "name": "冲刺样本", "market": "科创板", "exchange": "SSE",
            "list_date": "20260921" if self.conflict else self.listing, "delist_date": None,
        }])

    def trade_cal(self, **params):
        return self._record("trade_cal", params, [
            {"exchange": "SSE", "cal_date": "20260922", "is_open": 1},
            {"exchange": "SSE", "cal_date": "20260923", "is_open": 1},
        ])

    def daily(self, **params):
        return self._record("daily", params, [{
            "ts_code": "688001.SH", "trade_date": "20260922", "pre_close": 10,
            "close": 20, "amount": 100_000,
        }])

    def daily_basic(self, **params):
        return self._record("daily_basic", params, [{
            "ts_code": "688001.SH", "trade_date": "20260922",
            "turnover_rate": 15, "volume_ratio": 2,
        }])

    def anns_d(self, **params):
        rows = [] if self.announcement_title is None else [{
            "ann_date": "20260923", "ts_code": "688001.SH", "name": "冲刺样本",
            "title": self.announcement_title,
            "url": "https://static.sse.com.cn/example.pdf", "rec_time": "20260923 12:30:00",
        }]
        return self._record("anns_d", params, rows)


def _quotes(symbols):
    assert symbols == ["688001.SH"]
    return {"688001.SH": {
        "trade_date": "2026-09-23",
        "observed_at": "2026-09-23T13:30:00+08:00",
        "last_price": 25,
        "previous_close": 20,
        "bid_price": 24.99,
        "ask_price": 25.01,
        "bid_size": 1000,
        "ask_size": 1200,
        "turnover_amount": 80_000_000,
        "suspended": False,
        "tradable": True,
        "price_locked": False,
        "source": {
            "source_name": "sina.level1",
            "source_ref": "https://hq.sinajs.cn/#fixture",
            "observed_at": "2026-09-23T13:30:00+08:00",
        },
    }}


def test_collector_is_independent_and_evaluates_a_sourced_unbounded_ipo():
    client = FakeTushare()
    snapshot = collect_ipo_sprint_snapshot(
        client, observed_at=NOW, quote_reader=_quotes,
    )
    assert set(snapshot) == {"schema_version", "snapshot_id", "stocks", "collection_summary"}
    assert len(snapshot["stocks"]) == 1
    stock = snapshot["stocks"][0]
    assert stock["symbol"] == "688001.SH"
    assert [row[0] for row in client.calls] == [
        "new_share", "stock_basic", "trade_cal", "daily", "daily_basic", "anns_d",
    ]
    params = {name: value for name, value in client.calls}
    assert params["new_share"] == {
        "start_date": "20260824", "end_date": "20260923",
        "fields": "ts_code,name,ipo_date,issue_date,price",
    }
    assert params["trade_cal"]["exchange"] == "SSE"
    assert params["anns_d"]["ts_code"] == "688001.SH"
    assert all(item["limit_regime"] == "UNBOUNDED" for item in stock["trading_sessions"])
    assert stock["liquidity"]["daily_basic_as_of_trade_date"] == "2026-09-22"
    assert stock["liquidity"]["turnover_amount_source"] == stock["realtime"]["source"]
    assert stock["risk_review"]["status"] == "CLEAR"
    assert not any(key in snapshot for key in ("S4", "S5", "S6", "terminal", "portfolio"))

    result = evaluate_ipo_sprint_snapshot(
        snapshot, evaluated_at=NOW, config=load_ipo_sprint_config(),
    )
    assert [item["symbol"] for item in result["candidates"]] == ["688001.SH"]
    assert result["route_origin"] == "IPO_SPRINT"


def test_automatic_announcement_risk_review_flags_candidate():
    snapshot = collect_ipo_sprint_snapshot(
        FakeTushare(announcement_title="关于收到监管警示函的公告"),
        observed_at=NOW, quote_reader=_quotes,
    )
    result = evaluate_ipo_sprint_snapshot(snapshot, evaluated_at=NOW)
    assert result["candidates"] == []
    assert "RISK_FLAGGED" in result["rejected"][0]["reason_codes"]
    assert snapshot["stocks"][0]["risk_review"]["categories"]["regulatory"]["has_risk"] is True


def test_identity_conflict_and_outside_window_do_not_become_candidates():
    conflict = collect_ipo_sprint_snapshot(
        FakeTushare(conflict=True), observed_at=NOW,
        quote_reader=lambda symbols: {},
    )
    assert conflict["stocks"] == []
    assert conflict["collection_summary"]["excluded"][0]["reason_codes"] == ["IDENTITY_CONFLICT"]
    old = collect_ipo_sprint_snapshot(
        FakeTushare(listing="20260901"), observed_at=NOW,
        quote_reader=lambda symbols: {},
    )
    assert old["stocks"] == []
    assert old["collection_summary"]["excluded"][0]["reason_codes"] == [
        "TRADING_CALENDAR_INCOMPLETE"
    ]


def test_sina_reader_extracts_dated_two_sided_level_one_book():
    fields = [""] * 33
    fields[0] = "冲刺样本"
    fields[1], fields[2], fields[3] = "20", "20", "25"
    fields[8], fields[9] = "100000", "80000000"
    fields[10], fields[11] = "1000", "24.99"
    fields[20], fields[21] = "1200", "25.01"
    fields[30], fields[31], fields[32] = "2026-09-23", "13:30:00", "00"
    payload = ('var hq_str_sh688001="' + ",".join(fields) + '";\n').encode("gb18030")

    def handler(request):
        assert request.headers["Referer"] == "https://finance.sina.com.cn/"
        return httpx.Response(200, content=payload)

    result = fetch_sina_quotes(["688001.SH"], transport=httpx.MockTransport(handler))
    quote = result["688001.SH"]
    assert quote["bid_size"] == 1000
    assert quote["ask_size"] == 1200
    assert quote["tradable"] is True
    assert quote["price_locked"] is False


def test_sina_unknown_status_is_not_treated_as_tradable():
    fields = ["0"] * 33
    fields[0], fields[2], fields[3] = "样本", "20", "25"
    fields[9], fields[10], fields[11] = "80000000", "1000", "24.99"
    fields[20], fields[21] = "1200", "25.01"
    fields[30], fields[31], fields[32] = "2026-09-23", "13:30:00", "UNKNOWN"
    payload = ('var hq_str_sh688001="' + ",".join(fields) + '";\n').encode("gb18030")
    result = fetch_sina_quotes(
        ["688001.SH"],
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=payload)),
    )
    assert result["688001.SH"]["tradable"] is False
    assert result["688001.SH"]["suspended"] is True
