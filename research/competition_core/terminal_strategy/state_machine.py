"""Deterministic Protect/Normal/Attack/Last-shot state machine."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Iterable
from zoneinfo import ZoneInfo

from .config import TerminalStrategyConfig
from .models import (
    AwardLineForecast,
    TerminalBlocker,
    TerminalMode,
    TerminalModeDecision,
    TerminalModelError,
    TerminalSnapshot,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")
_ZERO = Decimal("0")
_ONE = Decimal("1")
_MODE_RANK = {
    TerminalMode.UNAVAILABLE: -1,
    TerminalMode.PROTECT: 0,
    TerminalMode.NORMAL: 1,
    TerminalMode.ATTACK: 2,
    TerminalMode.LAST_SHOT: 3,
}


def decide_terminal_mode(
    snapshot: TerminalSnapshot,
    forecast: AwardLineForecast | None,
    config: TerminalStrategyConfig,
    *,
    previous_mode: TerminalMode | str | None = None,
    downgrade_confirmation_count: int = 0,
    full_snapshot: bool = True,
    observed_at: datetime | None = None,
) -> TerminalModeDecision:
    """Evaluate one mode using a complete, fresh, already-frozen read snapshot.

    Risk increases are immediate only when the new snapshot is complete.  Risk
    reductions require a configured number of consecutive confirmations and a
    hysteresis margin.  There is intentionally no force-mode or override
    argument.
    """

    if not isinstance(snapshot, TerminalSnapshot):
        raise TerminalModelError("snapshot must be a TerminalSnapshot")
    if not isinstance(config, TerminalStrategyConfig):
        raise TerminalModelError("config must be a TerminalStrategyConfig")
    previous = _mode(previous_mode)
    if type(downgrade_confirmation_count) is not int or downgrade_confirmation_count < 0:
        raise TerminalModelError("downgrade_confirmation_count must be non-negative")
    now = observed_at or datetime.now(tz=SHANGHAI)
    if not isinstance(now, datetime) or now.utcoffset() is None:
        raise TerminalModelError("observed_at must be timezone-aware")

    blockers = _snapshot_blockers(snapshot, config, forecast, now)
    if blockers:
        return _unavailable(snapshot, previous, downgrade_confirmation_count, blockers)
    if forecast is None or not forecast.available or forecast.required_return is None or forecast.target_asset is None:
        return _unavailable(snapshot, previous, downgrade_confirmation_count, [TerminalBlocker.AWARD_LINE_MISSING])
    if not full_snapshot:
        # The same read-only contract is used for upgrade and downgrade checks;
        # no stale partial snapshot may cause an accidental risk increase.
        return _unavailable(snapshot, previous, downgrade_confirmation_count, [TerminalBlocker.INCOMPLETE_REEVALUATION])

    target = forecast.target_asset
    required = forecast.required_return
    lead = (snapshot.own_total_assets / target) - _ONE
    final_session = snapshot.trade_date == config.competition.final_trade_date and snapshot.is_final_trade_date
    raw_mode, raw_reasons = _raw_mode(required, lead, final_session, config)
    selected = raw_mode
    confirmations = 0
    reasons = list(raw_reasons)

    if previous is not None and previous is not TerminalMode.UNAVAILABLE:
        current_rank = _MODE_RANK[previous]
        raw_rank = _MODE_RANK[raw_mode]
        if raw_rank > current_rank:
            # Upgrades are immediate after a complete snapshot.
            selected = raw_mode
            reasons.append("RISK_UPGRADE_IMMEDIATE")
        elif raw_rank < current_rank:
            if not _has_hysteresis_margin(previous, raw_mode, required, lead, config):
                selected = previous
                reasons.append("DOWNGRADE_HYSTERESIS_HOLD")
            else:
                confirmations = downgrade_confirmation_count + 1
                if confirmations >= config.mode.downgrade_confirmations:
                    selected = raw_mode
                    confirmations = 0
                    reasons.append("DOWNGRADE_CONFIRMED")
                else:
                    selected = previous
                    reasons.append("DOWNGRADE_CONFIRMATION_PENDING")
        else:
            confirmations = 0
    elif previous is TerminalMode.UNAVAILABLE:
        reasons.append("RECOVERED_FROM_UNAVAILABLE")

    return TerminalModeDecision(
        mode=selected,
        as_of=snapshot.as_of,
        trade_date=snapshot.trade_date,
        is_final_trade_date=final_session,
        target_asset=target,
        required_return=required,
        lead_return=lead,
        safety_buffer=forecast.safety_buffer,
        previous_mode=previous,
        downgrade_confirmation_count=confirmations,
        reasons=tuple(dict.fromkeys(reasons)),
        execution_ready=True,
    )


evaluate_terminal_mode = decide_terminal_mode
calculate_terminal_mode = decide_terminal_mode


@dataclass(slots=True)
class TerminalModeStateMachine:
    """Small stateful adapter for repeated checkpoints.

    State is only the previous read-only decision and confirmation counter; no
    account or order side effect is held here.
    """

    config: TerminalStrategyConfig
    previous_mode: TerminalMode | None = None
    downgrade_confirmation_count: int = 0

    def evaluate(
        self,
        snapshot: TerminalSnapshot,
        forecast: AwardLineForecast | None,
        *,
        full_snapshot: bool = True,
        observed_at: datetime | None = None,
    ) -> TerminalModeDecision:
        decision = decide_terminal_mode(
            snapshot,
            forecast,
            self.config,
            previous_mode=self.previous_mode,
            downgrade_confirmation_count=self.downgrade_confirmation_count,
            full_snapshot=full_snapshot,
            observed_at=observed_at,
        )
        self.previous_mode = decision.mode
        self.downgrade_confirmation_count = decision.downgrade_confirmation_count
        return decision


def _snapshot_blockers(
    snapshot: TerminalSnapshot,
    config: TerminalStrategyConfig,
    forecast: AwardLineForecast | None,
    observed_at: datetime,
) -> tuple[TerminalBlocker, ...]:
    blockers: list[TerminalBlocker] = []
    cutoff = config.competition.award_rank_cutoff
    if cutoff is None or snapshot.award_rank_cutoff != cutoff:
        blockers.append(TerminalBlocker.AWARD_RANK_UNCONFIRMED)
    if not (snapshot.award_rule_source or config.competition.award_rule_source):
        blockers.append(TerminalBlocker.AWARD_RULE_SOURCE_MISSING)
    if observed_at < snapshot.as_of:
        blockers.append(TerminalBlocker.FUTURE_SNAPSHOT)
    elif (observed_at - snapshot.as_of).total_seconds() > config.award_line.max_snapshot_age_seconds:
        blockers.append(TerminalBlocker.STALE_SNAPSHOT)
    if not snapshot.account_fresh or not snapshot.complete_order_history:
        blockers.append(TerminalBlocker.INCOMPLETE_ACCOUNT)
    if not snapshot.leaderboard_fresh:
        blockers.append(TerminalBlocker.INCOMPLETE_LEADERBOARD)
    if snapshot.pending_order_count:
        blockers.append(TerminalBlocker.PENDING_ORDERS)
    if snapshot.trade_date > config.competition.final_trade_date:
        blockers.append(TerminalBlocker.CALENDAR_UNAVAILABLE)
    if forecast is not None:
        blockers.extend(forecast.blockers)
        if forecast.rank_cutoff != cutoff:
            blockers.append(TerminalBlocker.AWARD_RANK_UNCONFIRMED)
        if forecast.as_of is not None and forecast.as_of != snapshot.as_of:
            blockers.append(TerminalBlocker.INCOMPLETE_REEVALUATION)
    return tuple(dict.fromkeys(blockers))


def _raw_mode(
    required: Decimal,
    lead: Decimal,
    final_session: bool,
    config: TerminalStrategyConfig,
) -> tuple[TerminalMode, list[str]]:
    mode_config = config.mode
    reasons: list[str] = [f"REQUIRED_RETURN={required}", f"LEAD_RETURN={lead}"]
    if lead >= mode_config.protect_lead_min:
        reasons.append("LEAD_EXCEEDS_PROTECT_BUFFER")
        return TerminalMode.PROTECT, reasons
    if required <= mode_config.normal_gap_max:
        reasons.append("GAP_WITHIN_NORMAL_BOUND")
        return TerminalMode.NORMAL, reasons
    if required <= mode_config.attack_gap_max:
        reasons.append("GAP_WITHIN_ATTACK_BOUND")
        return TerminalMode.ATTACK, reasons
    if final_session and required > mode_config.last_shot_min_gap:
        reasons.append("FINAL_SESSION_LAST_SHOT_GATE")
        return TerminalMode.LAST_SHOT, reasons
    # A large gap on an earlier session remains an Attack decision.  It is not
    # silently promoted to Last-shot, and the reason stays visible to the UI.
    reasons.append("LAST_SHOT_DEFERRED_NON_FINAL_SESSION")
    return TerminalMode.ATTACK, reasons


def _has_hysteresis_margin(
    previous: TerminalMode,
    candidate: TerminalMode,
    required: Decimal,
    lead: Decimal,
    config: TerminalStrategyConfig,
) -> bool:
    hysteresis = config.mode.hysteresis
    if previous is TerminalMode.PROTECT and candidate is TerminalMode.NORMAL:
        return lead < config.mode.protect_lead_min - hysteresis
    if previous is TerminalMode.NORMAL and candidate is TerminalMode.PROTECT:
        return lead >= config.mode.protect_lead_min + hysteresis
    if previous is TerminalMode.ATTACK and candidate is TerminalMode.NORMAL:
        return required <= config.mode.normal_gap_max - hysteresis
    if previous is TerminalMode.NORMAL and candidate is TerminalMode.ATTACK:
        return required > config.mode.normal_gap_max + hysteresis
    if previous is TerminalMode.LAST_SHOT and candidate is TerminalMode.ATTACK:
        return required <= config.mode.last_shot_min_gap - hysteresis
    if previous is TerminalMode.ATTACK and candidate is TerminalMode.LAST_SHOT:
        return required > config.mode.last_shot_min_gap + hysteresis
    return True


def _unavailable(
    snapshot: TerminalSnapshot,
    previous: TerminalMode | None,
    confirmation_count: int,
    blockers: Iterable[TerminalBlocker],
) -> TerminalModeDecision:
    unique = tuple(dict.fromkeys(blockers))
    return TerminalModeDecision(
        mode=TerminalMode.UNAVAILABLE,
        as_of=snapshot.as_of,
        trade_date=snapshot.trade_date,
        is_final_trade_date=False,
        target_asset=None,
        required_return=None,
        lead_return=None,
        safety_buffer=None,
        previous_mode=previous,
        downgrade_confirmation_count=confirmation_count,
        reasons=("FAIL_CLOSED",),
        blockers=unique,
        execution_ready=False,
    )


def _mode(value: TerminalMode | str | None) -> TerminalMode | None:
    if value is None:
        return None
    if isinstance(value, TerminalMode):
        return value
    try:
        return TerminalMode(value)
    except ValueError as exc:
        raise TerminalModelError("invalid previous terminal mode") from exc


__all__ = [
    "TerminalModeStateMachine", "calculate_terminal_mode", "decide_terminal_mode",
    "evaluate_terminal_mode",
]
