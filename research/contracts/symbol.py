"""Canonical mainland China A-share symbol contract.

The core representation is deliberately provider-neutral and strict:
``<six digit code>.<SH|SZ>``.  Provider-specific spellings are handled by
the explicit adapter methods below rather than by a permissive parser.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import re
from typing import Any, ClassVar, Mapping


class SymbolContractError(ValueError):
    """Raised when a symbol cannot be represented by this contract."""


_CORE_PATTERN = re.compile(r"(?P<code>[0-9]{6})\.(?P<suffix>SH|SZ)\Z")
_QLIB_PATTERN = re.compile(r"(?P<suffix>SH|SZ)(?P<code>[0-9]{6})\Z")
_VNPY_PATTERN = re.compile(
    r"(?P<code>[0-9]{6})\.(?P<exchange>SSE|SZSE)\Z"
)

_SUFFIX_TO_EXCHANGE = {"SH": "SSE", "SZ": "SZSE"}
_EXCHANGE_TO_SUFFIX = {value: key for key, value in _SUFFIX_TO_EXCHANGE.items()}
_SUPPORTED_PROVIDERS = frozenset({"tushare", "qlib", "vnpy"})


def _invalid(value: object, reason: str) -> SymbolContractError:
    return SymbolContractError(f"invalid canonical symbol {value!r}: {reason}")


def _provider_name(provider: object) -> str:
    if not isinstance(provider, str):
        raise SymbolContractError(f"unsupported symbol provider: {provider!r}")
    normalized = provider.lower()
    if normalized not in _SUPPORTED_PROVIDERS:
        raise SymbolContractError(f"unsupported symbol provider: {provider!r}")
    return normalized


def _exchange_hint(value: object) -> str:
    if not isinstance(value, str):
        raise SymbolContractError(f"unsupported exchange hint: {value!r}")
    if value in _SUFFIX_TO_EXCHANGE:
        return _SUFFIX_TO_EXCHANGE[value]
    if value in _EXCHANGE_TO_SUFFIX:
        return value
    raise SymbolContractError(f"unsupported exchange hint: {value!r}")


def _board_for_code(code: str, exchange: str) -> str:
    """Return the board for a syntactically recognized A-share code.

    The prefix allow-list intentionally excludes B shares, funds, ETFs,
    indices, bonds, and other instruments whose code shape overlaps with
    provider symbol formats.
    """

    if code == "000000":
        raise SymbolContractError(f"invalid canonical symbol code {code!r}")

    if exchange == "SSE":
        if code.startswith(("600", "601", "603", "605")):
            return "MAIN"
        if code.startswith(("688", "689")):
            return "STAR"
    elif exchange == "SZSE":
        if code.startswith(("000", "001", "002", "003")):
            return "MAIN"
        if code.startswith(("300", "301", "302")):
            return "CHINEXT"
    raise SymbolContractError(
        f"invalid canonical symbol code {code!r} for exchange {exchange!r}"
    )


def _parse_core(value: object) -> tuple[str, str, str, str]:
    if not isinstance(value, str):
        raise _invalid(value, "must be a string")
    match = _CORE_PATTERN.fullmatch(value)
    if match is None:
        raise _invalid(value, "expected six digits followed by .SH or .SZ")

    code = match.group("code")
    suffix = match.group("suffix")
    exchange = _SUFFIX_TO_EXCHANGE[suffix]
    board = _board_for_code(code, exchange)
    return code, suffix, exchange, board


def _parse_provider(value: object, provider: str) -> tuple[str, str | None]:
    """Parse a provider spelling into a core symbol and embedded exchange."""

    if provider == "tushare":
        # Tushare's stock spelling is the core spelling verbatim.
        _parse_core(value)
        return value, None  # type: ignore[return-value]

    if not isinstance(value, str):
        raise _invalid(value, f"invalid {provider} symbol: must be a string")

    if provider == "qlib":
        match = _QLIB_PATTERN.fullmatch(value)
        if match is None:
            raise _invalid(value, "expected Qlib form SH600000 or SZ000977")
        suffix = match.group("suffix")
        code = match.group("code")
        return f"{code}.{suffix}", _SUFFIX_TO_EXCHANGE[suffix]

    match = _VNPY_PATTERN.fullmatch(value)
    if match is None:
        raise _invalid(value, "expected vn.py form 600000.SSE or 000977.SZSE")
    exchange = match.group("exchange")
    suffix = _EXCHANGE_TO_SUFFIX[exchange]
    code = match.group("code")
    return f"{code}.{suffix}", exchange


@dataclass(frozen=True, slots=True)
class CanonicalSymbol:
    """Immutable, JSON-friendly identity for a mainland China A-share.

    ``symbol`` is the sole core spelling.  Use :meth:`from_provider` for
    provider-specific spellings and :meth:`to_provider` when crossing an
    adapter boundary.
    """

    symbol: str
    code: str
    exchange: str
    market: str
    board: str
    currency: str
    lot_size: int

    MARKET: ClassVar[str] = "CN_A"
    CURRENCY: ClassVar[str] = "CNY"
    LOT_SIZE: ClassVar[int] = 100

    def __post_init__(self) -> None:
        code, _suffix, exchange, board = _parse_core(self.symbol)

        if self.code != code:
            raise _invalid(self.symbol, "code does not match symbol")
        if self.exchange != exchange:
            raise _invalid(self.symbol, "exchange conflicts with symbol suffix")
        if self.market != self.MARKET:
            raise _invalid(self.symbol, f"market must be {self.MARKET}")
        if self.board != board:
            raise _invalid(self.symbol, "board does not match the recognized code")
        if self.currency != self.CURRENCY:
            raise _invalid(self.symbol, f"currency must be {self.CURRENCY}")
        if type(self.lot_size) is not int or self.lot_size != self.LOT_SIZE:
            raise _invalid(self.symbol, f"lot_size must be {self.LOT_SIZE}")

    @classmethod
    def _from_core(cls, value: object, *, exchange: object = None) -> "CanonicalSymbol":
        code, _suffix, expected_exchange, board = _parse_core(value)
        if exchange is not None and _exchange_hint(exchange) != expected_exchange:
            raise _invalid(value, "exchange conflicts with symbol suffix")
        return cls(
            symbol=value,  # type: ignore[arg-type]
            code=code,
            exchange=expected_exchange,
            market=cls.MARKET,
            board=board,
            currency=cls.CURRENCY,
            lot_size=cls.LOT_SIZE,
        )

    @classmethod
    def from_core(
        cls, value: object, *, exchange: object = None
    ) -> "CanonicalSymbol":
        """Parse the strict core spelling, optionally checking an exchange hint."""

        return cls._from_core(value, exchange=exchange)

    @classmethod
    def parse(cls, value: object, *, exchange: object = None) -> "CanonicalSymbol":
        """Alias for :meth:`from_core` kept as the concise parsing entrypoint."""

        return cls.from_core(value, exchange=exchange)

    @classmethod
    def from_symbol(
        cls, value: object, *, exchange: object = None
    ) -> "CanonicalSymbol":
        """Alias for :meth:`from_core`."""

        return cls.from_core(value, exchange=exchange)

    @classmethod
    def from_tushare(
        cls, value: object, *, exchange: object = None
    ) -> "CanonicalSymbol":
        """Parse Tushare's stock spelling, which is the core spelling verbatim."""

        return cls._from_core(value, exchange=exchange)

    @classmethod
    def from_qlib(
        cls, value: object, *, exchange: object = None
    ) -> "CanonicalSymbol":
        """Parse Qlib's exchange-prefix spelling, such as ``SZ000977``."""

        core, embedded_exchange = _parse_provider(value, "qlib")
        if exchange is not None and _exchange_hint(exchange) != embedded_exchange:
            raise _invalid(value, "exchange conflicts with provider prefix")
        return cls._from_core(core)

    @classmethod
    def from_vnpy(
        cls, value: object, *, exchange: object = None
    ) -> "CanonicalSymbol":
        """Parse vn.py's exchange-suffix spelling, such as ``000977.SZSE``."""

        core, embedded_exchange = _parse_provider(value, "vnpy")
        if exchange is not None and _exchange_hint(exchange) != embedded_exchange:
            raise _invalid(value, "exchange conflicts with provider suffix")
        return cls._from_core(core)

    @classmethod
    def from_provider(
        cls,
        value: object,
        provider: object,
        *,
        exchange: object = None,
    ) -> "CanonicalSymbol":
        """Parse one of the explicitly supported provider representations."""

        normalized_provider = _provider_name(provider)
        if normalized_provider == "tushare":
            return cls.from_tushare(value, exchange=exchange)
        if normalized_provider == "qlib":
            return cls.from_qlib(value, exchange=exchange)
        return cls.from_vnpy(value, exchange=exchange)

    @property
    def suffix(self) -> str:
        """Return the two-letter core suffix (``SH`` or ``SZ``)."""

        return _EXCHANGE_TO_SUFFIX[self.exchange]

    def to_tushare(self) -> str:
        """Return Tushare's stock spelling (the core spelling verbatim)."""

        return self.symbol

    def to_qlib(self) -> str:
        """Return Qlib's exchange-prefix spelling."""

        return f"{self.suffix}{self.code}"

    def to_vnpy(self) -> str:
        """Return vn.py's exchange-suffix spelling."""

        return f"{self.code}.{self.exchange}"

    def to_provider(self, provider: object) -> str:
        """Format this symbol for a supported provider."""

        normalized_provider = _provider_name(provider)
        if normalized_provider == "tushare":
            return self.to_tushare()
        if normalized_provider == "qlib":
            return self.to_qlib()
        return self.to_vnpy()

    def to_dict(self) -> dict[str, Any]:
        """Return the seven primitive contract fields for JSON encoding."""

        return asdict(self)

    def as_dict(self) -> dict[str, Any]:
        """Alias for :meth:`to_dict`."""

        return self.to_dict()

    def to_json(self) -> str:
        """Return the contract as a deterministic JSON object."""

        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "CanonicalSymbol":
        """Rebuild a symbol from its JSON-compatible contract mapping."""

        if not isinstance(value, Mapping):
            raise SymbolContractError("canonical symbol payload must be a mapping")
        fields = {
            "symbol",
            "code",
            "exchange",
            "market",
            "board",
            "currency",
            "lot_size",
        }
        if set(value) != fields:
            raise SymbolContractError(
                "canonical symbol payload must contain exactly the contract fields"
            )
        return cls(**{field: value[field] for field in fields})

    def __str__(self) -> str:
        return self.symbol


def parse_symbol(value: object, *, exchange: object = None) -> CanonicalSymbol:
    """Parse a core canonical symbol."""

    return CanonicalSymbol.from_core(value, exchange=exchange)


def from_provider(
    value: object,
    provider: object,
    *,
    exchange: object = None,
) -> CanonicalSymbol:
    """Module-level provider parsing helper."""

    return CanonicalSymbol.from_provider(value, provider, exchange=exchange)


def to_provider(symbol: CanonicalSymbol, provider: object) -> str:
    """Module-level provider formatting helper."""

    if not isinstance(symbol, CanonicalSymbol):
        raise SymbolContractError("to_provider requires a CanonicalSymbol instance")
    return symbol.to_provider(provider)
