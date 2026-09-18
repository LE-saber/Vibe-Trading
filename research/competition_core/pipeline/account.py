"""Explicit, read-only competition account snapshots; never infer an empty account."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
from typing import Any

from contracts.symbol import CanonicalSymbol, SymbolContractError
from competition_core.pipeline.artifacts import content_hash
from competition_core.pipeline.config import PipelineConfig
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.time_context import ResearchContext, SHANGHAI


def _invalid(field: str) -> PipelineError:
    # Field names only: never echo a broker export, account number or credentials.
    return PipelineError("INVALID_ACCOUNT_STATE", "account field is missing or invalid", field=field)


def _money(value: Any, field: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise _invalid(field)
    try:
        result = Decimal(str(value))
        if not result.is_finite() or result < 0 or result > Decimal("1000000000000000"):
            raise _invalid(field)
        if result != result.quantize(Decimal("0.01")):
            raise _invalid(field)
        return result.quantize(Decimal("0.01"))
    except InvalidOperation as exc:
        raise _invalid(field) from exc


def _count(value: Any, field: str) -> int:
    if type(value) is not int or not 0 <= value <= 10**12:
        raise _invalid(field)
    return value


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 160:
        raise _invalid(field)
    if any(ord(char) < 32 for char in value):
        raise _invalid(field)
    return value.strip()


@dataclass(frozen=True, slots=True)
class AccountPosition:
    symbol: str
    quantity: int
    sellable_quantity: int
    market_value: Decimal

    def __post_init__(self):
        try:
            CanonicalSymbol.from_core(self.symbol)
        except SymbolContractError as exc:
            raise _invalid("positions.symbol") from exc
        _count(self.quantity, "positions.quantity")
        _count(self.sellable_quantity, "positions.sellable_quantity")
        if self.quantity <= 0 or self.sellable_quantity > self.quantity:
            raise _invalid("positions.sellable_quantity")
        object.__setattr__(self, "market_value", _money(self.market_value, "positions.market_value"))

    def to_dict(self):
        return {"symbol": self.symbol, "quantity": self.quantity,
                "sellable_quantity": self.sellable_quantity, "market_value": str(self.market_value)}


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    as_of: datetime
    source: str
    source_kind: str
    currency: str
    cash_balance: Decimal
    available_cash: Decimal
    current_total_asset: Decimal
    positions: tuple[AccountPosition, ...]
    pending_order_count: int

    def __post_init__(self):
        if not isinstance(self.as_of, datetime) or self.as_of.utcoffset() is None:
            raise _invalid("as_of")
        object.__setattr__(self, "as_of", self.as_of.astimezone(SHANGHAI))
        object.__setattr__(self, "source", _text(self.source, "source"))
        if not isinstance(self.source_kind, str) or self.source_kind not in {"competition_export", "competition_api", "manual_user", "synthetic_test"}:
            raise _invalid("source_kind")
        if self.currency != "CNY":
            raise _invalid("currency")
        for key in ("cash_balance", "available_cash", "current_total_asset"):
            object.__setattr__(self, key, _money(getattr(self, key), key))
        if self.current_total_asset <= 0 or self.available_cash > self.cash_balance:
            raise _invalid("available_cash/current_total_asset")
        if not isinstance(self.positions, tuple) or not all(isinstance(p, AccountPosition) for p in self.positions):
            raise _invalid("positions")
        if len({p.symbol for p in self.positions}) != len(self.positions):
            raise _invalid("positions.duplicate_symbol")
        object.__setattr__(self, "positions", tuple(sorted(self.positions, key=lambda p: p.symbol)))
        _count(self.pending_order_count, "pending_order_count")
        # Exported position values may have been rounded independently to cents.
        residual = self.current_total_asset - self.cash_balance - sum(p.market_value for p in self.positions)
        if abs(residual) > Decimal("0.01"):
            raise _invalid("asset_reconciliation")

    def to_dict(self):
        return {"schema_version": "competition.account.v1", "as_of": self.as_of.isoformat(),
                "source": self.source, "source_kind": self.source_kind, "currency": self.currency,
                "cash_balance": str(self.cash_balance), "available_cash": str(self.available_cash),
                "current_total_asset": str(self.current_total_asset),
                "positions": [p.to_dict() for p in self.positions], "pending_order_count": self.pending_order_count}

    @property
    def snapshot_hash(self):
        return content_hash(self.to_dict())

    @classmethod
    def from_dict(cls, payload):
        fields = {"schema_version", "as_of", "source", "source_kind", "currency", "cash_balance",
                  "available_cash", "current_total_asset", "positions", "pending_order_count"}
        if not isinstance(payload, dict) or set(payload) != fields:
            raise _invalid("schema")
        if payload["schema_version"] != "competition.account.v1" or not isinstance(payload["positions"], list):
            raise _invalid("schema_version/positions")
        positions = []
        for row in payload["positions"]:
            if not isinstance(row, dict) or set(row) != {"symbol", "quantity", "sellable_quantity", "market_value"}:
                raise _invalid("positions.schema")
            positions.append(AccountPosition(**row))
        try:
            as_of = datetime.fromisoformat(payload["as_of"])
        except (TypeError, ValueError) as exc:
            raise _invalid("as_of") from exc
        return cls(as_of=as_of, positions=tuple(positions),
                   **{key: value for key, value in payload.items() if key not in {"schema_version", "as_of", "positions"}})


def load_account_snapshot(path: Path | None) -> tuple[AccountSnapshot, str]:
    """Read one user-specified export; no home scan, latest-file guess or persistence."""
    if path is None:
        raise PipelineError("MISSING_ACCOUNT_STATE", "a current competition account export is required")
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise _invalid("duplicate_json_key")
            result[key] = value
        return result
    try:
        target = Path(path)
        with target.open("rb") as handle:
            body = handle.read(1024 * 1024 + 1)
        if len(body) > 1024 * 1024:
            raise _invalid("export_size")
        payload = json.loads(body.decode("utf-8-sig"), object_pairs_hook=pairs,
                             parse_constant=lambda _: (_ for _ in ()).throw(_invalid("nonfinite_json")))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PipelineError("MISSING_ACCOUNT_STATE", "cannot read a valid competition account export") from exc
    return AccountSnapshot.from_dict(payload), hashlib.sha256(body).hexdigest()


def assess_account(account: AccountSnapshot | None, *, context: ResearchContext,
                   config: PipelineConfig, observed_at: datetime) -> dict:
    """Check declared export semantics, not broker authenticity or permission to trade."""
    if not isinstance(observed_at, datetime) or observed_at.utcoffset() is None or context.as_of > observed_at:
        raise PipelineError("NOT_READY", "account assessment requires a valid explicit observation clock")
    if account is None:
        return {"status": "NOT_READY", "reasons": ["MISSING_ACCOUNT_STATE"], "account_hash": None,
                "account_eligible": False, "execution_ready": False}
    reasons = []
    if account.source_kind == "synthetic_test":
        reasons.append("SYNTHETIC_ACCOUNT_NOT_PRODUCTION")
    local = observed_at.astimezone(SHANGHAI)
    age = (observed_at - account.as_of).total_seconds()
    if age < 0:
        reasons.append("FUTURE_ACCOUNT_STATE")
    if account.as_of.date() != context.research_date or local.date() != context.research_date:
        reasons.append("ACCOUNT_DATE_MISMATCH")
    if age > config.section("portfolio")["account_max_age_seconds"]:
        reasons.append("STALE_ACCOUNT_STATE")
    if account.pending_order_count:
        reasons.append("PENDING_ORDERS_REQUIRE_RECONCILIATION")
    return {"status": "READY" if not reasons else "NOT_READY", "reasons": reasons,
            "account_hash": account.snapshot_hash, "account_as_of": account.as_of.isoformat(),
            "source": account.source, "source_kind": account.source_kind, "age_seconds": age,
            "account_eligible": not reasons, "execution_ready": False,
            "authentication_scope": "user_supplied_snapshot_not_independently_broker_authenticated"}
