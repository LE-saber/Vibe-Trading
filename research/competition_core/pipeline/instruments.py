"""Pure mainland China A-share quantity-rule metadata.

This module describes the published 2026 quantity rules, verified on
2026-09-03, for the competition's supported interval.  It does not connect
to a broker, submit orders, or decide portfolio weights, prices, or
price-limit behavior.
Its sell metadata is a conservative baseline, not a complete catalog of
every legally permitted sell order under older or separate rules.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from competition_core.pipeline.errors import PipelineError
from contracts.symbol import CanonicalSymbol


QUANTITY_RULES_VERSION = "p10g-02.instrument-quantity.v1"
EFFECTIVE_FROM = date(2026, 7, 6)
SUPPORTED_THROUGH = date(2026, 9, 30)
RULES_VERIFIED_ON = date(2026, 9, 3)

SSE_RULES_URL = (
    "https://www.sse.com.cn/lawandrules/sselawsrules2025/stocks/exchange/"
    "c/c_20260424_10816482.shtml"
)
SSE_STAR_EDUCATION_URL = "https://edu.sse.com.cn/tib/qa/"
SZSE_RULES_URL = (
    "https://docs.static.szse.cn/www/lawrules/rule/trade/current/"
    "W020260424690713155663.pdf"
)

_BOARDS = frozenset({"MAIN", "CHINEXT", "STAR"})
_ORDER_TYPES = frozenset({"LIMIT", "MARKET"})
_TICK_SIZE = Decimal("0.01")


class QuantityRulesNotReady(PipelineError):
    """Raised when the requested date is outside the supported rule window."""

    def __init__(self, as_of: date) -> None:
        super().__init__(
            "NOT_READY",
            f"quantity rules are NOT_READY for {as_of.isoformat()}; "
            f"supported from {EFFECTIVE_FROM.isoformat()} through "
            f"{SUPPORTED_THROUGH.isoformat()}",
            as_of=as_of.isoformat(),
            effective_from=EFFECTIVE_FROM.isoformat(),
            supported_through=SUPPORTED_THROUGH.isoformat(),
            rules_verified_on=RULES_VERIFIED_ON.isoformat(),
        )


def _decimal_value(value: object, name: str, *, allow_zero: bool) -> Decimal:
    if isinstance(value, bool):
        raise TypeError(f"{name} must be a finite decimal number, not bool")
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, (int, float, str)):
        try:
            # str(float) avoids importing binary floating-point residue into
            # the Decimal calculation while still accepting ordinary callers.
            result = Decimal(str(value))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"{name} must be a finite decimal number") from exc
    else:
        raise TypeError(f"{name} must be a finite decimal number")

    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    if result < 0 or (not allow_zero and result <= 0):
        comparison = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{name} must be {comparison}")
    return result


def _positive_int(value: object, name: str) -> None:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


def _nonnegative_int(value: object) -> bool:
    return type(value) is int and value >= 0


@dataclass(frozen=True, slots=True)
class QuantityRules:
    """Immutable, serializable quantity metadata for one recognized board."""

    version: str
    effective_from: date
    supported_through: date
    rules_verified_on: date
    source_urls: tuple[str, ...]
    board: str
    min_buy: int
    min_sell: int
    buy_step: int
    sell_step: int
    limit_order_max: int
    market_order_max: int
    tick_size: Decimal
    sell_policy: str

    def __post_init__(self) -> None:
        if not isinstance(self.version, str) or not self.version.strip():
            raise ValueError("version must be a non-empty string")
        if (
            type(self.effective_from) is not date
            or type(self.supported_through) is not date
            or type(self.rules_verified_on) is not date
        ):
            raise TypeError(
                "effective_from, supported_through, and rules_verified_on must be dates"
            )
        if self.effective_from > self.supported_through:
            raise ValueError("effective_from cannot be after supported_through")
        if self.rules_verified_on < self.effective_from:
            raise ValueError("rules_verified_on cannot precede effective_from")
        if self.board not in _BOARDS:
            raise ValueError(f"unsupported board: {self.board!r}")
        if not isinstance(self.sell_policy, str) or not self.sell_policy.strip():
            raise ValueError("sell_policy must be a non-empty string")

        if isinstance(self.source_urls, (str, bytes)):
            raise TypeError("source_urls must be an iterable of URLs")
        try:
            source_urls = tuple(self.source_urls)
        except TypeError as exc:
            raise TypeError("source_urls must be an iterable of URLs") from exc
        if not source_urls or any(not isinstance(url, str) or not url.strip() for url in source_urls):
            raise ValueError("source_urls must contain non-empty strings")
        object.__setattr__(self, "source_urls", source_urls)

        for name in (
            "min_buy",
            "min_sell",
            "buy_step",
            "sell_step",
            "limit_order_max",
            "market_order_max",
        ):
            _positive_int(getattr(self, name), name)

        if isinstance(self.tick_size, bool):
            raise TypeError("tick_size must be a finite Decimal")
        if not isinstance(self.tick_size, Decimal):
            try:
                tick_size = Decimal(str(self.tick_size))
            except (InvalidOperation, ValueError) as exc:
                raise ValueError("tick_size must be a finite Decimal") from exc
            object.__setattr__(self, "tick_size", tick_size)
        if not self.tick_size.is_finite() or self.tick_size <= 0:
            raise ValueError("tick_size must be a positive finite Decimal")

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-compatible metadata; this method has no execution side effects."""

        return {
            "version": self.version,
            "effective_from": self.effective_from.isoformat(),
            "supported_through": self.supported_through.isoformat(),
            "rules_verified_on": self.rules_verified_on.isoformat(),
            "source_urls": list(self.source_urls),
            "board": self.board,
            "min_buy": self.min_buy,
            "min_sell": self.min_sell,
            "buy_step": self.buy_step,
            "sell_step": self.sell_step,
            "limit_order_max": self.limit_order_max,
            "market_order_max": self.market_order_max,
            "tick_size": format(self.tick_size, "f"),
            "sell_policy": self.sell_policy,
        }

    def is_valid_quantity(
        self,
        side: str,
        quantity: object,
        current_quantity: object,
        sellable_quantity: object,
        order_type: str = "LIMIT",
    ) -> bool:
        """Return whether one proposed order quantity passes this baseline.

        ``sellable_quantity`` is deliberately required even though it is only
        meaningful for sells.  A caller must provide the account's explicit
        T+1-available amount; this module never infers it from holdings.
        """

        return _is_valid_quantity(
            self,
            side,
            quantity,
            current_quantity,
            sellable_quantity,
            order_type,
        )


def quantity_rules(symbol: str, as_of: date) -> QuantityRules:
    """Return supported quantity metadata for a canonical A-share symbol/date."""

    identity = CanonicalSymbol.from_core(symbol)
    if type(as_of) is not date:
        raise TypeError("as_of must be a date, not datetime or another type")
    if not EFFECTIVE_FROM <= as_of <= SUPPORTED_THROUGH:
        raise QuantityRulesNotReady(as_of)

    if identity.exchange == "SSE":
        source_urls = (SSE_RULES_URL,)
        if identity.board == "STAR":
            source_urls += (SSE_STAR_EDUCATION_URL,)
    else:
        source_urls = (SZSE_RULES_URL,)

    if identity.board == "MAIN":
        values = dict(
            min_buy=100,
            min_sell=100,
            buy_step=100,
            sell_step=100,
            limit_order_max=1_000_000,
            market_order_max=1_000_000,
            sell_policy="conservative_whole_lot_or_full_cleanup_MAIN_CHINEXT",
        )
    elif identity.board == "CHINEXT":
        values = dict(
            min_buy=100,
            min_sell=100,
            buy_step=100,
            sell_step=100,
            limit_order_max=300_000,
            market_order_max=150_000,
            sell_policy="conservative_whole_lot_or_full_cleanup_MAIN_CHINEXT",
        )
    else:
        values = dict(
            min_buy=200,
            min_sell=200,
            buy_step=1,
            sell_step=1,
            limit_order_max=100_000,
            market_order_max=50_000,
            sell_policy="minimum_200_step_1_or_full_odd_lot_cleanup_STAR",
        )

    return QuantityRules(
        version=QUANTITY_RULES_VERSION,
        effective_from=EFFECTIVE_FROM,
        supported_through=SUPPORTED_THROUGH,
        rules_verified_on=RULES_VERIFIED_ON,
        source_urls=source_urls,
        board=identity.board,
        tick_size=_TICK_SIZE,
        **values,
    )


def _is_valid_quantity(
    rules: QuantityRules,
    side: object,
    quantity: object,
    current_quantity: object,
    sellable_quantity: object,
    order_type: object,
) -> bool:
    if not isinstance(rules, QuantityRules):
        return False
    if not isinstance(side, str) or side not in {"BUY", "SELL"}:
        return False
    if not isinstance(order_type, str) or order_type not in _ORDER_TYPES:
        return False
    if not all(
        _nonnegative_int(value)
        for value in (quantity, current_quantity, sellable_quantity)
    ):
        return False
    if sellable_quantity > current_quantity or quantity <= 0:
        return False

    order_max = (
        rules.limit_order_max if order_type == "LIMIT" else rules.market_order_max
    )
    if quantity > order_max:
        return False

    if side == "BUY":
        return quantity >= rules.min_buy and quantity % rules.buy_step == 0

    if quantity > sellable_quantity:
        return False

    if rules.board in {"MAIN", "CHINEXT"}:
        whole_lot = quantity >= rules.min_sell and quantity % rules.sell_step == 0
        full_position_cleanup = (
            quantity == current_quantity == sellable_quantity
        )
        return whole_lot or full_position_cleanup

    whole_star_order = quantity >= rules.min_sell and quantity % rules.sell_step == 0
    star_odd_lot_cleanup = (
        quantity < rules.min_sell and quantity == current_quantity == sellable_quantity
    )
    return whole_star_order or star_odd_lot_cleanup


def is_valid_quantity(
    rules: QuantityRules,
    side: str,
    quantity: object,
    current_quantity: object,
    sellable_quantity: object,
    order_type: str = "LIMIT",
) -> bool:
    """Module-level equivalent of :meth:`QuantityRules.is_valid_quantity`."""

    return _is_valid_quantity(
        rules,
        side,
        quantity,
        current_quantity,
        sellable_quantity,
        order_type,
    )


def floor_buy_quantity(
    budget: object,
    price: object,
    rules: QuantityRules,
) -> int:
    """Return one affordable buy order, floored to the board's buy step.

    Decimal arithmetic prevents a floating-point boundary from exceeding the
    budget.  The result is capped at ``limit_order_max`` as one order only;
    this helper never creates or implies a split order.
    """

    if not isinstance(rules, QuantityRules):
        raise TypeError("rules must be a QuantityRules instance")
    budget_decimal = _decimal_value(budget, "budget", allow_zero=True)
    price_decimal = _decimal_value(price, "price", allow_zero=False)

    affordable = int(budget_decimal // price_decimal)
    aligned = affordable - affordable % rules.buy_step
    if aligned < rules.min_buy:
        return 0
    return min(aligned, rules.limit_order_max)


__all__ = [
    "EFFECTIVE_FROM",
    "RULES_VERIFIED_ON",
    "QuantityRules",
    "QuantityRulesNotReady",
    "QUANTITY_RULES_VERSION",
    "SSE_RULES_URL",
    "SSE_STAR_EDUCATION_URL",
    "SZSE_RULES_URL",
    "SUPPORTED_THROUGH",
    "floor_buy_quantity",
    "is_valid_quantity",
    "quantity_rules",
]
