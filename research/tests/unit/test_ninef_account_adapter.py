from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from adapters.ninef.account import (
    NinefAccountAdapterError,
    fetch_ninef_account,
    resolve_ninef_token,
)
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.time_context import SHANGHAI


NOW = datetime(2026, 9, 4, 10, 30, tzinfo=SHANGHAI)


def responses(*, has_more=False, order_total=2):
    return {
        "assets": {"code": 0, "data": {"totalAssets": 1000.0, "availableFunds": 100.0,
                                             "frozenFunds": 200.0, "holdingCount": 2,
                                             "accountId": "must-not-leak"}},
        "positions": {"code": 0, "data": [
            {"symbol": "300750.SZ", "qty": 10, "sellableQty": 0, "marketValue": 300.0},
            {"symbol": "600519.SH", "qty": 20, "sellableQty": 20, "marketValue": 400.0},
        ]},
        "orders": {"code": 0, "data": {"total": order_total, "hasMore": has_more, "items": [
            {"orderId": "private-one", "symbol": "600519.SH", "status": "待成交",
             "meta": {"direction": "买入", "quantity": 100, "tradeQuantity": 0, "price": "1400"}},
            {"orderId": "private-two", "symbol": "300750.SZ", "status": "已成交",
             "meta": {"direction": "卖出", "quantity": 10, "tradeQuantity": 10, "price": "300"}},
        ]}},
    }


def fetch(payloads):
    calls = []
    times = iter((NOW, NOW + timedelta(seconds=1)))

    def requester(token, name, params=None):
        calls.append((token, name, dict(params or {})))
        return payloads[name]

    result = fetch_ninef_account("secret-fixture", requester=requester, clock=lambda: next(times))
    return result, calls


def test_maps_live_fields_to_strict_reconciled_account_without_identifiers():
    result, calls = fetch(responses())
    account = result.snapshot
    assert [name for _, name, _ in calls] == ["assets", "positions", "orders", "assets"]
    assert account.source_kind == "competition_api"
    assert account.cash_balance == Decimal("300.00")
    assert account.available_cash == Decimal("100.00")
    assert account.current_total_asset == Decimal("1000.00")
    assert account.pending_order_count == 1
    assert result.fee_schedule is None
    assert account.positions[0].sellable_quantity == 0
    serialized = str(account.to_dict()) + str(result.receipt)
    assert "must-not-leak" not in serialized and "private-one" not in serialized
    assert result.receipt["method"] == "GET_ONLY"
    assert len(result.receipt["adapter_sha256"]) == 64
    assert set(result.receipt["response_hashes"]) == {"assets_start", "positions", "order_pages", "assets_end"}
    assert result.receipt["orders"]["complete"] is True
    assert result.receipt["orders"]["unknown_count"] == 0
    assert result.receipt["orders"]["items"][0]["remaining_quantity"] == 100


def test_cancelled_provider_status_is_normalized_and_not_pending():
    payloads = responses()
    payloads["orders"]["data"]["items"][0]["status"] = "已撤销"

    result, _ = fetch(payloads)

    assert result.snapshot.pending_order_count == 0
    assert result.receipt["orders"]["items"][0]["status"] == "CANCELLED"


def test_current_day_explicit_platform_fee_rule_is_bounded_to_that_day():
    payloads = responses()
    payloads["orders"]["data"]["items"][1].update({
        "filledAt": "2026-09-04 10:15:00",
        "costBreakdown": {"feeRule": {
            "commissionRate": "0.0003", "minCommission": "5", "stampDutyRate": "0.0005",
            "market": "cn_a_share", "requestedMarket": "cn_a_share", "source": "config",
        }},
    })
    result, _ = fetch(payloads)
    fees = result.fee_schedule
    assert fees is not None
    assert fees.as_of == NOW + timedelta(seconds=1)
    assert fees.valid_through == NOW.date()
    assert fees.commission_rate == Decimal("0.0003")
    assert fees.minimum_commission == Decimal("5")
    assert fees.buy_other_rate == 0 and fees.sell_other_rate == Decimal("0.0005")


def test_fee_rule_is_not_extrapolated_without_current_day_fill_or_with_conflict():
    payloads = responses()
    rule = {"commissionRate": "0.0003", "minCommission": "5", "stampDutyRate": "0.0005",
            "market": "cn_a_share", "requestedMarket": "cn_a_share", "source": "config"}
    payloads["orders"]["data"]["items"][1].update(
        filledAt="2026-09-03 10:15:00", costBreakdown={"feeRule": rule})
    assert fetch(payloads)[0].fee_schedule is None
    payloads["orders"]["data"]["items"][1]["filledAt"] = "2026-09-04 10:15:00"
    payloads["orders"]["data"]["items"][0]["costBreakdown"] = {
        "feeRule": rule | {"commissionRate": "0.0004"}}
    assert fetch(payloads)[0].fee_schedule is None


def test_incomplete_order_page_and_holding_mismatch_fail_closed():
    with pytest.raises(NinefAccountAdapterError, match="cursor"):
        fetch(responses(has_more=True))
    payloads = responses()
    payloads["assets"]["data"]["holdingCount"] = 1
    with pytest.raises(NinefAccountAdapterError, match="holding count"):
        fetch(payloads)


def test_asset_reconciliation_is_enforced_by_core_contract():
    payloads = responses()
    payloads["assets"]["data"]["totalAssets"] = 1000.02
    with pytest.raises(PipelineError) as error:
        fetch(payloads)
    assert error.value.code == "INVALID_ACCOUNT_STATE"


def test_token_environment_wins_and_file_formats_are_bounded(tmp_path):
    missing = tmp_path / "missing.env"
    assert resolve_ninef_token(token_file=missing, environ={"NINEF_API_TOKEN": "injected"}) == "injected"
    env_file = tmp_path / "token.env"
    env_file.write_text("NINEF_API_TOKEN='file-fixture'\n", encoding="utf-8-sig")
    assert resolve_ninef_token(token_file=env_file, environ={}) == "file-fixture"
    env_file.write_text("OTHER_KEY=do-not-echo\n", encoding="utf-8")
    with pytest.raises(NinefAccountAdapterError) as error:
        resolve_ninef_token(token_file=env_file, environ={})
    assert "do-not-echo" not in str(error.value)


@pytest.mark.parametrize("orders_patch", [
    {"total": 3},
    {"total": True},
    {"items": [None, {"status": "已成交"}]},
])
def test_unreconciled_order_shapes_fail_closed(orders_patch):
    payloads = responses()
    payloads["orders"]["data"].update(orders_patch)
    with pytest.raises(NinefAccountAdapterError):
        fetch(payloads)


def test_reads_all_order_pages_and_counts_partial_cancel_pending_and_unknown_as_open():
    payloads = responses(order_total=4)
    page_two = {"code": 0, "data": {"total": 4, "hasMore": False, "items": [
        {"id": "private-three", "symbol": "600519.SH", "status": "部分成交",
         "meta": {"direction": "买入", "quantity": 100, "tradeQuantity": 40, "price": "1400"}},
        {"id": "private-four", "symbol": "300750.SZ", "status": "平台新状态",
         "meta": {"direction": "卖出", "quantity": 10, "tradeQuantity": 0, "price": "300"}},
    ]}}
    payloads["orders"]["data"].update(hasMore=True, nextCursor="page-2")
    calls = []
    times = iter((NOW, NOW + timedelta(seconds=1)))
    def requester(token, name, params=None):
        calls.append((name, dict(params or {})))
        if name == "orders" and (params or {}).get("cursor") == "page-2":
            return page_two
        return payloads[name]

    result = fetch_ninef_account("secret-fixture", requester=requester, clock=lambda: next(times))

    assert [params.get("cursor") for name, params in calls if name == "orders"] == [None, "page-2"]
    assert result.snapshot.pending_order_count == 3
    assert [row["status"] for row in result.receipt["orders"]["items"]] == [
        "PENDING", "FILLED", "PARTIALLY_FILLED", "UNKNOWN",
    ]
    assert result.receipt["orders"]["unknown_count"] == 1
    assert result.receipt["orders"]["open_count"] == result.snapshot.pending_order_count == 3


def test_rejects_duplicate_order_ids_repeated_cursor_and_mid_collection_account_change():
    payloads = responses()
    payloads["orders"]["data"]["items"][1]["orderId"] = "private-one"
    with pytest.raises(NinefAccountAdapterError, match="duplicate"):
        fetch(payloads)

    payloads = responses(order_total=3)
    payloads["orders"]["data"].update(hasMore=True, nextCursor="same")
    def repeated(token, name, params=None):
        return payloads[name]
    with pytest.raises(NinefAccountAdapterError, match="new cursor"):
        fetch_ninef_account("secret-fixture", requester=repeated, clock=iter((NOW, NOW)).__next__)

    payloads = responses()
    reads = 0
    def changing(token, name, params=None):
        nonlocal reads
        if name == "assets":
            reads += 1
            value = {**payloads[name], "data": dict(payloads[name]["data"])}
            if reads == 2:
                value["data"]["availableFunds"] = 99.0
            return value
        return payloads[name]
    with pytest.raises(NinefAccountAdapterError, match="changed during collection"):
        fetch_ninef_account("secret-fixture", requester=changing, clock=iter((NOW, NOW)).__next__)


def test_live_mark_to_market_total_assets_drift_does_not_invalidate_account_collection():
    payloads = responses()
    reads = 0

    def changing_mark(token, name, params=None):
        nonlocal reads
        if name == "assets":
            reads += 1
            value = {**payloads[name], "data": dict(payloads[name]["data"])}
            if reads == 2:
                value["data"]["totalAssets"] = 1000001.0
            return value
        return payloads[name]

    result = fetch_ninef_account(
        "secret-fixture", requester=changing_mark, clock=iter((NOW, NOW)).__next__
    )

    assert result.snapshot.current_total_asset == Decimal("1000.00")
