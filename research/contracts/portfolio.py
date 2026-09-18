"""Weight-centric portfolio contracts; no framework-specific positions."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from contracts.symbol import CanonicalSymbol, SymbolContractError


class PortfolioContractError(ValueError):
    pass


def _aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise PortfolioContractError(f"{name} must include a timezone")


@dataclass(frozen=True, slots=True)
class TargetWeights:
    as_of: datetime
    cash_weight: float
    weights: tuple[tuple[str, float], ...]
    max_single_weight: float
    snapshot_id: str
    source: str
    allow_short: bool = False
    execution_mode: str = "MANUAL_CONFIRM"

    def __post_init__(self) -> None:
        _aware(self.as_of, "as_of")
        if self.execution_mode != "MANUAL_CONFIRM" or self.allow_short:
            raise PortfolioContractError("competition targets must be long-only MANUAL_CONFIRM")
        if not self.snapshot_id.strip() or not self.source.strip():
            raise PortfolioContractError("snapshot_id and source are required")
        if not 0 <= self.cash_weight <= 1 or not 0 < self.max_single_weight <= 1:
            raise PortfolioContractError("cash and maximum weights must be bounded")
        seen: set[str] = set()
        total = self.cash_weight
        for symbol, weight in self.weights:
            try:
                CanonicalSymbol.from_core(symbol)
            except SymbolContractError as exc:
                raise PortfolioContractError(str(exc)) from exc
            if symbol in seen:
                raise PortfolioContractError("target symbols must be unique")
            seen.add(symbol)
            if not math.isfinite(weight) or weight <= 0 or weight > self.max_single_weight + 1e-12:
                raise PortfolioContractError("target weights must be positive and within maximum")
            total += weight
        if not math.isclose(total, 1.0, rel_tol=0, abs_tol=1e-8):
            raise PortfolioContractError("cash_weight plus symbol weights must equal 1")

    @property
    def target_id(self) -> str:
        return hashlib.sha256(
            json.dumps(self.to_dict(include_id=False), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def to_dict(self, *, include_id: bool = True) -> dict[str, Any]:
        payload = {
            "as_of": self.as_of.isoformat(),
            "cash_weight": self.cash_weight,
            "weights": dict(self.weights),
            "constraints": {
                "max_single_weight": self.max_single_weight,
                "allow_short": self.allow_short,
            },
            "snapshot_id": self.snapshot_id,
            "source": self.source,
            "execution_mode": self.execution_mode,
        }
        return {"target_id": self.target_id, **payload} if include_id else payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TargetWeights":
        constraints = payload["constraints"]
        return cls(
            as_of=datetime.fromisoformat(payload["as_of"]),
            cash_weight=float(payload["cash_weight"]),
            weights=tuple((str(key), float(value)) for key, value in payload["weights"].items()),
            max_single_weight=float(constraints["max_single_weight"]),
            allow_short=bool(constraints.get("allow_short", False)),
            snapshot_id=str(payload["snapshot_id"]),
            source=str(payload["source"]),
            execution_mode=str(payload.get("execution_mode", "MANUAL_CONFIRM")),
        )


@dataclass(frozen=True, slots=True)
class PortfolioState:
    as_of: datetime
    cash: float
    positions: tuple[tuple[str, int, float], ...]
    total_assets: float
    initial_capital: float
    source: str

    def __post_init__(self) -> None:
        _aware(self.as_of, "as_of")
        if min(self.cash, self.total_assets, self.initial_capital) < 0:
            raise PortfolioContractError("portfolio monetary values cannot be negative")
        for symbol, shares, price in self.positions:
            CanonicalSymbol.from_core(symbol)
            if shares < 0 or price < 0:
                raise PortfolioContractError("long-only shares and prices cannot be negative")

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of.isoformat(),
            "cash": self.cash,
            "positions": [
                {"symbol": symbol, "shares": shares, "price": price, "market_value": shares * price}
                for symbol, shares, price in self.positions
            ],
            "total_assets": self.total_assets,
            "initial_capital": self.initial_capital,
            "return_pct": self.total_assets / self.initial_capital - 1 if self.initial_capital else None,
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PortfolioState":
        return cls(
            as_of=datetime.fromisoformat(payload["as_of"]),
            cash=float(payload["cash"]),
            positions=tuple(
                (str(item["symbol"]), int(item["shares"]), float(item["price"]))
                for item in payload.get("positions", [])
            ),
            total_assets=float(payload["total_assets"]),
            initial_capital=float(payload["initial_capital"]),
            source=str(payload["source"]),
        )
