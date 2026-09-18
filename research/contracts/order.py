"""Manual-only order intent contract."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from contracts.symbol import CanonicalSymbol
from contracts.portfolio import PortfolioContractError, _aware


@dataclass(frozen=True, slots=True)
class OrderIntent:
    as_of: datetime
    symbol: str
    side: str
    quantity: int
    target_weight: float
    reference_price: float
    max_slippage_pct: float
    target_id: str
    reason: str
    lot_size: int = 100
    execution_mode: str = "MANUAL_CONFIRM"

    def __post_init__(self) -> None:
        _aware(self.as_of, "as_of")
        CanonicalSymbol.from_core(self.symbol)
        if self.side not in {"BUY", "SELL"} or self.execution_mode != "MANUAL_CONFIRM":
            raise PortfolioContractError("order intent must be BUY/SELL and MANUAL_CONFIRM")
        if self.quantity <= 0 or self.lot_size <= 0 or self.quantity % self.lot_size:
            raise PortfolioContractError("quantity must be a positive whole-lot amount")
        if not math.isfinite(self.reference_price) or self.reference_price <= 0:
            raise PortfolioContractError("reference price must be positive")
        if not 0 <= self.target_weight <= 1 or not 0 <= self.max_slippage_pct <= 0.1:
            raise PortfolioContractError("target weight or slippage is out of bounds")
        if not self.target_id or not self.reason.strip():
            raise PortfolioContractError("target_id and reason are required")

    @property
    def intent_id(self) -> str:
        return hashlib.sha256(
            json.dumps(self.to_dict(include_id=False), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def to_dict(self, *, include_id: bool = True) -> dict[str, Any]:
        payload = {
            "as_of": self.as_of.isoformat(),
            "symbol": self.symbol,
            "side": self.side,
            "quantity": self.quantity,
            "target_weight": self.target_weight,
            "reference_price": self.reference_price,
            "max_slippage_pct": self.max_slippage_pct,
            "target_id": self.target_id,
            "reason": self.reason,
            "lot_size": self.lot_size,
            "execution_mode": self.execution_mode,
        }
        return {"intent_id": self.intent_id, **payload} if include_id else payload
