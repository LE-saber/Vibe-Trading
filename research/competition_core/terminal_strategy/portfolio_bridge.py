"""Strict, read-only validation for the terminal policy S6 shadow boundary.

The terminal strategy produces an allocation *intent*; this bridge is the only
place where S6 is allowed to consume that intent.  It never creates quantities
or orders.  Invalid or stale intent is represented as bounded blockers so the
existing portfolio builder can return an auditable ``NOT_READY`` result.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from contracts.symbol import CanonicalSymbol, SymbolContractError

from .allocation import AllocationBlocker, TerminalAllocationPolicy
from .models import TerminalMode


_HASH = re.compile(r"^[0-9a-f]{64}$")
SHANGHAI = ZoneInfo("Asia/Shanghai")
_POLICY_FIELDS = {
    "schema_version", "mode", "as_of", "trade_date", "core_symbols",
    "aggressive_symbols", "terminal_role", "total_position_count",
    "terminal_candidate_count", "position_bounds", "core_budget",
    "aggressive_budget", "cash_target", "max_single_weight",
    "same_day_exit_unavailable", "mode_decision_hash", "funnel_hash",
    "config_hash", "input_hash", "reasons", "blockers", "execution_ready",
    "policy_hash",
}


@dataclass(frozen=True, slots=True)
class TerminalPolicyValidation:
    """Normalized terminal policy plus bounded S6 blockers."""

    policy: TerminalAllocationPolicy | None
    blockers: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()

    @property
    def valid(self) -> bool:
        return self.policy is not None and not self.blockers


def validate_terminal_policy(
    value: TerminalAllocationPolicy | Mapping[str, Any] | None,
    *,
    indexed_symbols: Iterable[str],
    holding_symbols: Iterable[str],
    eligible_symbols: Iterable[str] | None = None,
    expected_config_hash: str | None = None,
    expected_final_trade_date: date | None = None,
    expected_trade_date: date | None = None,
    observed_at: datetime | None = None,
) -> TerminalPolicyValidation:
    """Validate a typed policy or strict v1 mapping against the current S6 universe.

    ``indexed_symbols`` is the current S4 universe and ``holding_symbols`` is
    the account inventory.  Aggressive symbols must be current S4 candidates;
    core symbols may be either eligible S4 candidates or existing holdings.
    """

    if value is None:
        return TerminalPolicyValidation(None)
    policy, parse_blockers, parse_reasons = _coerce(value)
    if policy is None:
        return TerminalPolicyValidation(None, parse_blockers, parse_reasons)

    blockers = list(parse_blockers)
    reasons = list(parse_reasons)
    indexed = set(indexed_symbols)
    holdings = set(holding_symbols)
    universe = indexed | holdings
    core = set(policy.core_symbols)
    aggressive = set(policy.aggressive_symbols)

    if not core | aggressive <= universe:
        blockers.append("TERMINAL_POLICY_SYMBOL_MISMATCH")
        reasons.append("POLICY_SYMBOL_NOT_IN_S4_OR_HOLDINGS")
    if not aggressive <= indexed:
        blockers.append("TERMINAL_POLICY_CANDIDATE_IDENTITY")
        reasons.append("AGGRESSIVE_SYMBOL_NOT_IN_CURRENT_S4")
    if eligible_symbols is not None:
        eligible = set(eligible_symbols)
        if not aggressive <= eligible:
            blockers.append("TERMINAL_POLICY_CANDIDATE_INELIGIBLE")
            reasons.append("AGGRESSIVE_SYMBOL_NOT_CURRENTLY_ELIGIBLE")
        core_candidates = (core & indexed) - holdings
        if not core_candidates <= eligible:
            blockers.append("TERMINAL_POLICY_CORE_INELIGIBLE")
            reasons.append("CORE_SYMBOL_NOT_CURRENTLY_ELIGIBLE")
    if expected_config_hash is not None and policy.config_hash != expected_config_hash:
        blockers.append("TERMINAL_POLICY_CONFIG_MISMATCH")
        reasons.append("POLICY_CONFIG_HASH_DIFFERS_FROM_CURRENT_CONFIG")
    if (
        expected_final_trade_date is not None
        and policy.mode is TerminalMode.LAST_SHOT
        and policy.trade_date != expected_final_trade_date
    ):
        blockers.append("TERMINAL_POLICY_FINAL_DAY_MISMATCH")
        reasons.append("LAST_SHOT_DATE_DIFFERS_FROM_FINAL_SESSION")
    if expected_trade_date is not None and policy.trade_date != expected_trade_date:
        blockers.append("TERMINAL_POLICY_DATE_MISMATCH")
        reasons.append("POLICY_TRADE_DATE_DIFFERS_FROM_S6_CONTEXT")
    if observed_at is not None and (
        policy.as_of > observed_at
        or (
            expected_trade_date is not None
            and policy.as_of.astimezone(SHANGHAI).date() != expected_trade_date
        )
    ):
        blockers.append("TERMINAL_POLICY_FRESHNESS")
        reasons.append("POLICY_SNAPSHOT_IS_FUTURE_OR_WRONG_DATE")

    return TerminalPolicyValidation(policy, _unique(blockers), _unique(reasons))


def _coerce(value: TerminalAllocationPolicy | Mapping[str, Any]) -> tuple[
    TerminalAllocationPolicy | None, list[str], list[str]
]:
    supplied_policy_hash = value.get("policy_hash") if isinstance(value, Mapping) else None
    if isinstance(value, TerminalAllocationPolicy):
        policy = value
    elif isinstance(value, Mapping):
        policy = _from_dict(value)
        if policy is None:
            return None, ["TERMINAL_POLICY_SCHEMA_INVALID"], ["POLICY_DICT_SCHEMA_INVALID"]
    else:
        return None, ["TERMINAL_POLICY_TYPE_INVALID"], ["POLICY_MUST_BE_TYPED_OR_V1_DICT"]

    blockers: list[str] = []
    reasons: list[str] = []
    payload = policy.to_dict()
    for field in ("mode_decision_hash", "funnel_hash", "config_hash", "input_hash", "policy_hash"):
        candidate = supplied_policy_hash if field == "policy_hash" and isinstance(value, Mapping) else payload.get(field)
        if not isinstance(candidate, str) or not _HASH.fullmatch(candidate):
            blockers.append("TERMINAL_POLICY_HASH_INVALID")
            reasons.append(f"POLICY_{field.upper()}_INVALID")
    if payload.get("execution_ready") is not False:
        blockers.append("TERMINAL_POLICY_EXECUTION_READY")
        reasons.append("POLICY_MUST_REMAIN_SHADOW_ONLY")
    if supplied_policy_hash is not None and supplied_policy_hash != policy.policy_hash:
        blockers.append("TERMINAL_POLICY_HASH_MISMATCH")
        reasons.append("POLICY_HASH_DOES_NOT_MATCH_CONTENT")
    if policy.blockers:
        blockers.append("TERMINAL_POLICY_BLOCKED")
        reasons.append("POLICY_CONTAINS_BLOCKERS")
    return policy, _unique(blockers), _unique(reasons)


def _from_dict(value: Mapping[str, Any]) -> TerminalAllocationPolicy | None:
    if set(value) != _POLICY_FIELDS or value.get("schema_version") != "competition.terminal-allocation-policy.v1":
        return None
    if value.get("terminal_role") != "aggressive_sleeve_only":
        return None
    if value.get("execution_ready") is not False:
        return None
    try:
        mode = TerminalMode(value["mode"])
        as_of = datetime.fromisoformat(_strict_text(value["as_of"]))
        trade_date = date.fromisoformat(_strict_text(value["trade_date"]))
        core = _strict_symbols(value["core_symbols"])
        aggressive = _strict_symbols(value["aggressive_symbols"])
        same_day = _strict_symbols(value["same_day_exit_unavailable"])
        bounds = value["position_bounds"]
        if not isinstance(bounds, Mapping) or set(bounds) != {"min", "max"}:
            return None
        if type(bounds["min"]) is not int or type(bounds["max"]) is not int:
            return None
        reasons = _strict_strings(value["reasons"])
        raw_blockers = _strict_strings(value["blockers"])
        blockers = tuple(AllocationBlocker(item) for item in raw_blockers)
        return TerminalAllocationPolicy(
            mode=mode,
            as_of=as_of,
            trade_date=trade_date,
            core_symbols=core,
            aggressive_symbols=aggressive,
            total_position_count=_strict_int(value["total_position_count"]),
            terminal_candidate_count=_strict_int(value["terminal_candidate_count"]),
            core_budget=_strict_decimal(value["core_budget"]),
            aggressive_budget=_strict_decimal(value["aggressive_budget"]),
            cash_target=_strict_decimal(value["cash_target"]),
            min_positions=bounds["min"],
            max_positions=bounds["max"],
            max_single_weight=_strict_decimal(value["max_single_weight"]),
            same_day_exit_unavailable=same_day,
            mode_decision_hash=_strict_text(value["mode_decision_hash"]),
            funnel_hash=_strict_text(value["funnel_hash"]),
            config_hash=_strict_text(value["config_hash"]),
            input_hash=_strict_text(value["input_hash"]),
            reasons=reasons,
            blockers=blockers,
            execution_ready=False,
        )
    except (KeyError, TypeError, ValueError, InvalidOperation):
        return None


def _strict_text(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("text required")
    return value.strip()


def _strict_int(value: Any) -> int:
    if type(value) is not int:
        raise ValueError("integer required")
    return value


def _strict_decimal(value: Any) -> Decimal:
    if not isinstance(value, str):
        raise ValueError("decimal text required")
    result = Decimal(value)
    if not result.is_finite():
        raise ValueError("finite decimal required")
    return result


def _strict_strings(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise ValueError("string list required")
    return tuple(value)


def _strict_symbols(value: Any) -> tuple[str, ...]:
    values = _strict_strings(value)
    if len(set(values)) != len(values):
        raise ValueError("duplicate symbols")
    for symbol in values:
        try:
            CanonicalSymbol.from_core(symbol)
        except (SymbolContractError, TypeError, ValueError) as exc:
            raise ValueError("canonical symbol required") from exc
    return values


def _unique(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(item for item in values if item))


__all__ = ["TerminalPolicyValidation", "validate_terminal_policy"]
