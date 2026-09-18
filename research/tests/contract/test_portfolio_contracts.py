from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from contracts.order import OrderIntent
from contracts.portfolio import PortfolioContractError, TargetWeights

CHINA = ZoneInfo("Asia/Shanghai")


def test_target_weights_are_long_only_and_sum_to_one() -> None:
    target = TargetWeights(
        as_of=datetime(2026, 9, 1, 10, 0, tzinfo=CHINA),
        cash_weight=0.6,
        weights=(("600519.SH", 0.2), ("300750.SZ", 0.2)),
        max_single_weight=0.2,
        snapshot_id="snapshot",
        source="test",
    )
    assert target.to_dict()["execution_mode"] == "MANUAL_CONFIRM"
    assert len(target.target_id) == 64


def test_invalid_weight_and_non_lot_order_fail_closed() -> None:
    with pytest.raises(PortfolioContractError, match="equal 1"):
        TargetWeights(
            as_of=datetime.now(CHINA),
            cash_weight=0.1,
            weights=(("600519.SH", 0.2),),
            max_single_weight=0.2,
            snapshot_id="snapshot",
            source="test",
        )
    with pytest.raises(PortfolioContractError, match="whole-lot"):
        OrderIntent(
            as_of=datetime.now(CHINA),
            symbol="600519.SH",
            side="BUY",
            quantity=50,
            target_weight=0.2,
            reference_price=100,
            max_slippage_pct=0.005,
            target_id="target",
            reason="test",
        )
