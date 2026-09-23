from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from vibe_trading.demo import DemoProvider
from vibe_trading.models import digest
from vibe_trading.ports import TerminalResearchProvider
from vibe_trading.store import Store
from vibe_trading.terminal import (
    TerminalAllocationSummary,
    TerminalAwardLine,
    TerminalCandidateSummary,
    TerminalConfidence,
    TerminalMode,
    TerminalProjection,
    TerminalSchedule,
    TerminalShadowPlan,
    TerminalSourceBindings,
)


TRADE_DATE = date(2026, 9, 23)
REMAINING_DATES = (TRADE_DATE, date(2026, 9, 24), date(2026, 9, 28),
                   date(2026, 9, 29), date(2026, 9, 30))


def _candidate(index: int) -> TerminalCandidateSummary:
    symbol = f"6000{index:02d}.SH"
    return TerminalCandidateSummary(
        symbol=symbol,
        name=f"Candidate {index}",
        score=Decimal(index),
        trigger_price="10.00",
        invalidation_price="9.50",
        limit_price="10.10",
        max_weight=Decimal("0.20"),
        hard_gate_pass=True,
        eligible=True,
        source_hash=digest({"candidate": index}),
    )


def _schedule() -> TerminalSchedule:
    return TerminalSchedule(
        candidate_rank="09:00",
        opening_observation_end="10:00",
        midday_refresh="13:15",
        primary_decision="13:30",
        final_revision="14:10",
        main_order_deadline="14:25",
        risk_freeze="14:35",
    )


def _bindings() -> TerminalSourceBindings:
    return TerminalSourceBindings(
        s4_hash="a" * 64,
        s5_hash="b" * 64,
        s5_5_hash="c" * 64,
        account_snapshot_hash="d" * 64,
        leaderboard_snapshot_hash="e" * 64,
        award_line_history_hash="f" * 64,
        config_hash="0" * 64,
        implementation_hash="1" * 64,
        realtime_observation_id="check-1",
    )


def _award_line() -> TerminalAwardLine:
    return TerminalAwardLine(
        award_rank_cutoff=30,
        current_award_line="100000",
        predicted_award_line="101000",
        favorable_award_line="100500",
        base_award_line="101000",
        adverse_award_line="102000",
        safety_buffer=Decimal("0.01"),
        target_asset="102010",
        required_return=Decimal("0.02"),
        confidence=TerminalConfidence.MEDIUM,
        method="fixture",
    )


def _allocation(*, protect: bool = False) -> TerminalAllocationSummary:
    return TerminalAllocationSummary(
        core_symbols=("600005.SH", "600006.SH", "600007.SH", "600008.SH", "600009.SH"),
        aggressive_symbols=() if protect else ("600000.SH", "600001.SH"),
        core_budget=Decimal("0.80") if protect else Decimal("0.70"),
        aggressive_budget=Decimal("0") if protect else Decimal("0.20"),
        cash_target=Decimal("0.20") if protect else Decimal("0.10"),
        total_position_count=5 if protect else 7,
        same_day_exit_unavailable=(),
    )


def _projection(*, mode: TerminalMode = TerminalMode.NORMAL, **changes) -> TerminalProjection:
    candidates = tuple(_candidate(index) for index in range(10))
    values = {
        "run_id": "run-terminal",
        "evaluation_id": "2" * 64,
        "as_of": datetime(2026, 9, 23, 1, 0, tzinfo=timezone.utc),
        "trade_date": TRADE_DATE,
        "remaining_trade_dates": REMAINING_DATES,
        "status": "COMPLETE",
        "mode": mode,
        "award_line": _award_line(),
        "watchlist": candidates,
        "focus": candidates[:5],
        "terminal": () if mode is TerminalMode.PROTECT else candidates[:2],
        "allocation": _allocation(protect=mode is TerminalMode.PROTECT),
        "schedule": _schedule(),
        "source_bindings": _bindings(),
    }
    values.update(changes)
    return TerminalProjection(**values)


def _shadow_plan(**changes) -> TerminalShadowPlan:
    values = {
        "plan_id": "shadow-plan",
        "run_id": "run-terminal",
        "evaluation_id": "2" * 64,
        "created_at": datetime(2026, 9, 23, 1, 0, tzinfo=timezone.utc),
        "status": "COMPLETE",
        "mode": TerminalMode.NORMAL,
        "allocation": _allocation(),
        "schedule": _schedule(),
        "source_bindings": _bindings(),
    }
    values.update(changes)
    return TerminalShadowPlan(**values)


def test_terminal_projection_is_bounded_hashed_and_shadow_only():
    projection = _projection()
    assert projection.source_bindings.s4_hash == "a" * 64
    assert projection.shadow_only is True
    assert projection.execution_ready is False
    assert projection.production_accepted is False
    assert projection.orders_submitted == 0
    dumped = projection.model_dump()
    forbidden = {"orders", "order", "quantity", "quantities", "intent", "approval"}
    assert not forbidden.intersection(dumped)


@pytest.mark.parametrize("field,value", [
    ("shadow_only", False),
    ("execution_ready", True),
    ("production_accepted", True),
    ("orders_submitted", 1),
])
def test_terminal_projection_flags_cannot_be_overridden(field, value):
    with pytest.raises(ValidationError):
        _projection(**{field: value})


def test_terminal_contracts_forbid_extra_fields():
    with pytest.raises(ValidationError):
        _projection(order={"symbol": "600001.SH"})
    with pytest.raises(ValidationError):
        _shadow_plan(unknown="value")


def test_terminal_candidate_tiers_are_bounded_and_nested():
    candidates = tuple(_candidate(index) for index in range(10))
    with pytest.raises(ValidationError, match="10 watch"):
        _projection(watchlist=candidates[:9])
    with pytest.raises(ValidationError):
        _projection(focus=candidates[:6])
    with pytest.raises(ValidationError, match="two to three"):
        _projection(terminal=candidates[:1])
    with pytest.raises(ValidationError):
        _projection(terminal=candidates[:4])
    with pytest.raises(ValidationError, match="nested"):
        _projection(focus=(candidates[9],))


def test_protect_mode_is_the_terminal_candidate_exception():
    projection = _projection(mode=TerminalMode.PROTECT)
    assert projection.terminal == ()
    assert projection.allocation is not None
    assert projection.allocation.aggressive_symbols == ()


@pytest.mark.parametrize("changes", [
    {"cash_target": Decimal("0.11")},
    {"max_single_weight": Decimal("0.21")},
    {"total_position_count": 0, "core_symbols": (), "aggressive_symbols": ()},
    {"total_position_count": 4},
    {"aggressive_symbols": ("60000.SH", "60000.SH")},
    {"core_symbols": ("600005.SH", "600000.SH", "600007.SH", "600008.SH", "600009.SH")},
])
def test_terminal_allocation_preserves_budget_position_and_cap_constraints(changes):
    values = _allocation().model_dump()
    values.update(changes)
    with pytest.raises(ValidationError):
        TerminalAllocationSummary(**values)


def test_terminal_shadow_plan_has_the_same_immutable_flags_and_no_orders():
    plan = _shadow_plan()
    assert plan.shadow_only is True
    assert plan.execution_ready is False
    assert plan.production_accepted is False
    assert plan.orders_submitted == 0
    assert not {"orders", "quantity", "intent", "approval"}.intersection(plan.model_dump())


def test_complete_shadow_plan_cannot_claim_unavailable_mode():
    with pytest.raises(ValidationError, match="unavailable mode"):
        _shadow_plan(mode=TerminalMode.UNAVAILABLE)


def test_terminal_research_provider_is_optional_and_runtime_checkable(tmp_path):
    class OptionalProvider:
        def terminal_projection(self, run_id):
            return _projection()

        def terminal_shadow_plan(self, run_id, evaluation_id=None):
            return _shadow_plan()

    assert isinstance(OptionalProvider(), TerminalResearchProvider)
    assert not isinstance(DemoProvider(Store(tmp_path / "demo.db")), TerminalResearchProvider)


def test_demo_leaderboard_exposes_authoritative_asset_facts(tmp_path):
    provider = DemoProvider(Store(tmp_path / "demo.db"))
    board = provider.leaderboard()
    assert board.entries[0].total_assets == Decimal("130000")
    assert board.entries[-1].total_assets == Decimal("101000")
    assert board.own.total_assets == Decimal("100000")
