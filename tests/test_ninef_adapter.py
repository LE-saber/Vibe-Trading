from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from vibe_trading.adapters.ninef import (
    NinefError, NinefProvider, NinefSettings, NinefTransport, _NoRedirect, _order,
)
from vibe_trading.models import OrderStatus, ProposedOrder, Side
from vibe_trading.planner import validate_market
from vibe_trading.reconciliation import account_blockers


NOW = "2026-09-04T02:00:00+00:00"


class FakeTransport:
    account_alias = "ninef-anonymous-fixture"

    def __init__(self):
        self.calls = []
        self.submission = {"code": 0, "data": {"order_id": "broker-new", "status": "待成交"}}

    def request(self, method, path, *, params=None, body=None):
        self.calls.append((method, path, dict(params or {}), dict(body or {})))
        if method == "POST":
            if path.endswith("cancel-order"):
                return {"code": 0, "data": {"accepted": True}}
            return self.submission
        if path.endswith("assets/overview"):
            return {"code": 0, "data": {"totalAssets": "1000000", "availableFunds": "699000",
                "frozenFunds": "1000", "updatedAt": NOW}}
        if path.endswith("/positions"):
            return {"code": 0, "data": [{"symbol": "600001.SH", "name": "匿名甲",
                "qty": 10000, "sellableQty": 9000, "frozenQty": 1000,
                "marketValue": "300000"}]}
        if path.endswith("order-activity"):
            if (params or {}).get("cursor") == "page-2":
                return {"code": 0, "data": {"items": [{"id": "broker-filled", "symbol": "600001.SH",
                             "status": "已成交", "createdAt": NOW,
                             "meta": {"direction": "卖出", "quantity": 100, "tradeQuantity": 100,
                             "price": "30", "tradePrice": "30.01",
                             "costBreakdown": {"commission": "5", "stampDuty": "15",
                                               "transferFee": "0"}}}],
                    "hasMore": False, "nextCursor": None}}
            return {"code": 0, "data": {"items": [{"id": "broker-open", "symbol": "600001.SH",
                "status": "待成交", "createdAt": NOW,
                "meta": {"direction": "买入", "quantity": 100, "tradeQuantity": 0,
                         "price": "29.90"}}], "hasMore": True, "nextCursor": "page-2"}}
        if path.endswith("trading-day"):
            return {"code": 0, "data": {"date": "2026-09-04", "isTradingDay": True}}
        if path.endswith("quotes/batch"):
            return {"code": 0, "data": {symbol: {"price": "30", "limit_down": "27",
                "limit_up": "33", "data_source": "official", "updated_at": NOW}
                for symbol in params["symbols"].split(",")}}
        if path.endswith("competitions"):
            return {"code": 0, "data": {"items": [{"competitionId": "competition-fixture"}]}}
        if path.endswith("competition-gate/status"):
            items = [{"userId": f"p-{i}", "displayName": f"匿名{i}", "rank": i,
                      "metricText": f"{40-i / 10}%"} for i in range(1, 51)]
            return {"code": 0, "data": {"competitionEntry": {"competitionId": "competition-fixture"},
                "leaderboard": {"updatedAt": NOW, "items": items,
                    "currentUser": {"userId": "owner", "displayName": "本人", "rank": 88,
                                    "metricText": "1.25%"}}}}
        if path.endswith("hot-topics"):
            if (params or {}).get("cursor") == "news-2":
                return {"code": 0, "data": {"items": [{"id": "n2", "title": "公告二",
                    "content": "正文二", "createdAt": NOW, "dataSource": "公告",
                    "relatedStocks": [{"symbol": "600001.SH"}]}], "hasMore": False}}
            return {"code": 0, "data": {"items": [{"id": "n1", "title": "公告一",
                "content": "正文一", "createdAt": NOW, "source": "平台"}],
                "hasMore": True, "nextCursor": "news-2"}}
        raise AssertionError((method, path, params))


def profile(tmp_path):
    path = tmp_path / "market-profile.json"
    path.write_text(json.dumps({
        "fees": {"version": "fixture-fees", "commission_rate": "0.0003",
            "minimum_commission": "5", "buy_tax_rate": "0", "sell_tax_rate": "0.0005",
            "transfer_rate": "0.00001", "valid_from": "2026-09-04", "valid_until": "2026-09-04",
            "source_ref": "anonymous verified fixture", "verified": True},
        "instrument_rules": [{"prefix": "6", "buy_lot": 100, "sell_lot": 100,
            "allow_odd_lot_liquidation": True, "tick": "0.01", "valid_from": "2026-09-04",
            "valid_until": "2026-09-04", "source_ref": "anonymous verified fixture", "verified": True}],
    }), encoding="utf-8")
    return path


def fee_order_fixture(cost_breakdown, **fields):
    return {
        "id": "broker-fee",
        "symbol": "600001.SH",
        "status": "已成交",
        "quantity": 100,
        "tradeQuantity": 100,
        "tradePrice": "30.01",
        "createdAt": NOW,
        "costBreakdown": cost_breakdown,
        **fields,
    }


def test_order_fee_paid_is_none_for_incomplete_cost_breakdown():
    order = _order(fee_order_fixture({"commission": "5", "stampDuty": "15"}),
                   datetime.fromisoformat(NOW))
    assert order.fee_paid is None


def test_order_fee_paid_sums_complete_cost_breakdown():
    order = _order(fee_order_fixture({"commission": "5", "stampDuty": "15",
                                      "transferFee": "0.10"}), datetime.fromisoformat(NOW))
    assert order.fee_paid == Decimal("20.10")


def test_order_fee_paid_accepts_authoritative_explicit_total():
    order = _order(fee_order_fixture({"commission": "5"}, totalFee="20.10"),
                   datetime.fromisoformat(NOW))
    assert order.fee_paid == Decimal("20.10")


def test_context_reads_every_order_page_and_keeps_authoritative_fields(tmp_path):
    transport = FakeTransport()
    provider = NinefProvider(transport, settings=NinefSettings(market_profile=profile(tmp_path)))
    context = provider.context(["600001.SH"])
    assert context.account.orders_complete and context.account.consistent
    assert context.account.available_cash == Decimal("699000")
    assert [order.order_id for order in context.account.orders] == ["broker-open", "broker-filled"]
    assert context.account.orders[0].status == OrderStatus.PENDING
    assert context.account.orders[1].filled_quantity == 100
    assert context.account.orders[1].average_fill_price == Decimal("30.01")
    assert context.account.orders[1].fee_paid == Decimal("20")
    assert [call[2].get("cursor") for call in transport.calls if call[1].endswith("order-activity")] == [None, "page-2"]


def test_account_preserves_provider_timestamp_separate_from_collection_time(tmp_path):
    provider = NinefProvider(FakeTransport(), settings=NinefSettings(market_profile=profile(tmp_path)))
    account = provider.context([]).account
    assert account.as_of == datetime.fromisoformat(NOW)
    assert account.collection_started_at <= account.collected_at
    assert account.as_of < account.collected_at
    issues = account_blockers(account, account.collected_at, provider_settings_policy())
    assert "STALE_ACCOUNT" in {issue.code for issue in issues}


def test_account_contract_allows_only_small_provider_clock_skew(tmp_path):
    provider = NinefProvider(FakeTransport(), settings=NinefSettings(market_profile=profile(tmp_path)))
    account = provider.context([]).account
    account.__class__.model_validate(
        account.model_dump() | {"as_of": account.collected_at + timedelta(seconds=2)}
    )
    with pytest.raises(ValueError, match="clock skew"):
        account.__class__.model_validate(
            account.model_dump() | {"as_of": account.collected_at + timedelta(seconds=2.001)}
        )


def test_transport_installs_redirect_rejection_handler():
    transport = NinefTransport("fake-local-test-token")
    handlers = transport._opener.handlers
    assert any(isinstance(handler, _NoRedirect) for handler in handlers)
    redirect = next(handler for handler in handlers if isinstance(handler, _NoRedirect))
    assert redirect.redirect_request(None, None, 302, "redirect", {}, "https://example.invalid") is None


def test_account_lock_identity_is_independent_of_token_rotation():
    first = NinefProvider(FakeTransport(), settings=NinefSettings(account_key="stable-local-key"))
    second = NinefProvider(FakeTransport(), settings=NinefSettings(account_key="stable-local-key"))
    assert first.context([]).account.account_id == second.context([]).account.account_id
    assert "stable-local-key" not in first.context([]).account.account_id


def test_unverified_market_inputs_fail_closed_without_invented_defaults():
    provider = NinefProvider(FakeTransport())
    context = provider.context(["600001.SH"])
    issues = validate_market(context, {"600001.SH"}, datetime.fromisoformat(NOW), provider_settings_policy())
    codes = {issue.code for issue in issues}
    assert {"UNVERIFIED_FEES", "UNVERIFIED_RULE", "NOT_TRADABLE"} <= codes
    assert context.fees.commission_rate is None
    assert context.rules[0].buy_lot is None


def provider_settings_policy():
    from vibe_trading.models import Policy
    return Policy(max_quote_age_seconds=3600, max_snapshot_age_seconds=3600)


def test_top_30_and_own_rank_are_independent_and_news_is_paginated():
    provider = NinefProvider(FakeTransport())
    board = provider.leaderboard()
    assert len(board.entries) == 30
    assert board.entries[-1].rank == 30
    assert board.own.rank == 88
    news = provider.news()
    assert len(news) == 2
    assert news[1].symbols == ("600001.SH",)
    assert all(item.news_id.startswith("ninef-news-") for item in news)


def test_oversized_news_is_bounded_and_marked_without_losing_the_item():
    class Oversized(FakeTransport):
        def request(self, method, path, *, params=None, body=None):
            if path.endswith("hot-topics"):
                return {"code": 0, "data": {"items": [{"id": "long", "title": "题" * 500,
                    "content": "文" * 13000, "createdAt": NOW, "source": "公告"}], "hasMore": False}}
            return super().request(method, path, params=params, body=body)
    item = NinefProvider(Oversized()).news()[0]
    assert len(item.title) == 400 and len(item.body) == 12000
    assert "truncated" in item.source_ref


@pytest.mark.parametrize("missing", ["id", "title", "body", "published_at", "source"])
def test_incomplete_news_items_are_excluded(missing):
    class Incomplete(FakeTransport):
        def request(self, method, path, *, params=None, body=None):
            if path.endswith("hot-topics"):
                item = {"id": "incomplete", "title": "标题", "content": "正文",
                        "createdAt": NOW, "source": "公告"}
                if missing == "body":
                    item.pop("content")
                elif missing == "published_at":
                    item.pop("createdAt")
                else:
                    item.pop(missing)
                return {"code": 0, "data": {"items": [item], "hasMore": False}}
            return super().request(method, path, params=params, body=body)

    assert NinefProvider(Incomplete()).news() == ()


def test_pagination_rejects_repeated_cursor():
    class Broken(FakeTransport):
        def request(self, method, path, *, params=None, body=None):
            if path.endswith("order-activity"):
                return {"code": 0, "data": {"items": [], "hasMore": True, "nextCursor": "same"}}
            return super().request(method, path, params=params, body=body)
    with pytest.raises(NinefError, match="new cursor"):
        NinefProvider(Broken()).context([])


def test_pagination_follows_next_cursor_when_has_more_is_omitted():
    class CursorOnly(FakeTransport):
        def request(self, method, path, *, params=None, body=None):
            if path.endswith("order-activity"):
                self.calls.append((method, path, dict(params or {}), dict(body or {})))
                if (params or {}).get("cursor") == "last":
                    return {"code": 0, "data": {"items": [], "hasMore": False}}
                return {"code": 0, "data": {"items": [], "nextCursor": "last"}}
            return super().request(method, path, params=params, body=body)
    transport = CursorOnly()
    NinefProvider(transport).context([])
    calls = [call for call in transport.calls if call[1].endswith("order-activity")]
    assert [call[2].get("cursor") for call in calls] == [None, "last"]


def test_pagination_rejects_missing_completion_metadata():
    class Ambiguous(FakeTransport):
        def request(self, method, path, *, params=None, body=None):
            if path.endswith("order-activity"):
                return {"code": 0, "data": {"items": []}}
            return super().request(method, path, params=params, body=body)
    with pytest.raises(NinefError, match="completion metadata"):
        NinefProvider(Ambiguous()).context([])


def test_unknown_status_is_preserved_and_blocks_account():
    class Unknown(FakeTransport):
        def request(self, method, path, *, params=None, body=None):
            if path.endswith("order-activity"):
                return {"code": 0, "data": {"items": [{"id": "mystery", "symbol": "600001.SH",
                    "status": "平台新状态", "createdAt": NOW}], "hasMore": False}}
            return super().request(method, path, params=params, body=body)
    account = NinefProvider(Unknown()).context([]).account
    assert account.orders[0].status == OrderStatus.UNKNOWN
    assert account.orders[0].is_open


def test_write_capabilities_default_false_and_one_exact_transport_attempt(tmp_path):
    transport = FakeTransport()
    provider = NinefProvider(transport, settings=NinefSettings())
    order = ProposedOrder(symbol="600001.SH", name="匿名甲", side=Side.BUY, quantity=100,
                          limit_price="30", estimated_fee="5", current_quantity=0,
                          target_quantity=100, reason="fixture")
    with pytest.raises(NinefError, match="not locally verified"):
        provider.submit(order, "intent-1")
    settings = NinefSettings(account_key="fixture-account", verification_ref="local-sanitized-proof",
                             buy_verified=True)
    provider = NinefProvider(transport, settings=settings)
    receipt = provider.submit(order, "intent-1")
    writes = [call for call in transport.calls if call[0] == "POST"]
    assert len(writes) == 1
    assert writes[0][1] == "/api/v1/stock/buy"
    assert writes[0][3] == {"code": "600001", "name": "匿名甲", "quantity": 100,
                             "source": 0, "type": 1, "price": "30"}
    assert receipt.order_id == "broker-new" and receipt.status == OrderStatus.PENDING


def test_exact_order_read_and_cancel_boundary():
    transport = FakeTransport()
    settings = NinefSettings(account_key="fixture-account", verification_ref="local-sanitized-proof",
                             cancel_verified=True)
    provider = NinefProvider(transport, settings=settings)
    assert provider.get_order("broker-filled").order_id == "broker-filled"
    provider.cancel("broker-filled")
    assert transport.calls[-1] == ("POST", "/api/v1/stock/cancel-order", {},
                                   {"stock_order_id": "broker-filled"})


def test_research_never_falls_back_to_demo():
    provider = NinefProvider(FakeTransport())
    with pytest.raises(NinefError, match="DEMO is never used"):
        provider.research(None, lambda *_: None, lambda: False)


def test_missing_quote_source_timestamp_fails_closed():
    class MissingTimestamp(FakeTransport):
        def request(self, method, path, *, params=None, body=None):
            result = super().request(method, path, params=params, body=body)
            if path.endswith("quotes/batch"):
                for quote in result["data"].values():
                    quote.pop("updated_at")
                    quote["tradable"] = True
            return result
    with pytest.raises(NinefError, match="timestamp is missing"):
        NinefProvider(MissingTimestamp()).context(["600001.SH"])


def test_submission_never_promotes_unknown_trade_id_to_full_fill():
    transport = FakeTransport()
    transport.submission = {"code": 0, "data": {"order_id": "broker-new",
        "status": "provider-new-state", "trade_id": "trade-present", "trade_price": "30"}}
    provider = NinefProvider(transport, settings=NinefSettings(
        account_key="fixture-account", verification_ref="proof", buy_verified=True))
    order = ProposedOrder(symbol="600001.SH", name="匿名甲", side=Side.BUY, quantity=100,
                          limit_price="30", estimated_fee="5", current_quantity=0,
                          target_quantity=100, reason="fixture")
    receipt = provider.submit(order, "intent-unknown")
    assert receipt.status == OrderStatus.UNKNOWN
    assert receipt.filled_quantity == 0

    transport.submission["data"]["tradeQuantity"] = 25
    partial_evidence = provider.submit(order, "intent-unknown-with-fill")
    assert partial_evidence.status == OrderStatus.UNKNOWN
    assert partial_evidence.filled_quantity == 25
