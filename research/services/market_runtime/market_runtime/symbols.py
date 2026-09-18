"""Strict Core-canonical to mootdx symbol boundary."""

from __future__ import annotations

import re
from dataclasses import dataclass

_PATTERN = re.compile(r"(?P<code>\d{6})\.(?P<suffix>SH|SZ)\Z")
_INDEX_PATTERN = re.compile(r"(?:000\d{3}\.SH|399\d{3}\.SZ)\Z")


class MarketSymbolError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class MarketSymbol:
    canonical: str
    code: str
    exchange: str
    mootdx_market: int

    @classmethod
    def parse(cls, value: str) -> "MarketSymbol":
        if not isinstance(value, str) or (match := _PATTERN.fullmatch(value)) is None:
            raise MarketSymbolError(f"invalid Core canonical symbol: {value!r}")
        code = match.group("code")
        suffix = match.group("suffix")
        valid = (
            suffix == "SH" and code.startswith(("600", "601", "603", "605", "688", "689"))
        ) or (
            suffix == "SZ" and code.startswith(("000", "001", "002", "003", "300", "301", "302"))
        )
        if not valid:
            raise MarketSymbolError(f"symbol code conflicts with exchange suffix: {value}")
        return cls(
            canonical=value,
            code=code,
            exchange="SSE" if suffix == "SH" else "SZSE",
            mootdx_market=1 if suffix == "SH" else 0,
        )

    @classmethod
    def parse_quote(cls, value: str) -> "MarketSymbol":
        """Accept the configured benchmark index only at the quote boundary."""
        if isinstance(value, str) and _INDEX_PATTERN.fullmatch(value):
            code, suffix = value.split(".")
            return cls(canonical=value, code=code, exchange="SSE" if suffix == "SH" else "SZSE",
                       mootdx_market=1 if suffix == "SH" else 0)
        return cls.parse(value)
