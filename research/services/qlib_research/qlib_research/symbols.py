"""The only Qlib symbol conversion used by the research sidecar."""

from __future__ import annotations

import re

_CANONICAL = re.compile(r"^(\d{6})\.(SH|SZ)$")
_QLIB = re.compile(r"^(SH|SZ)(\d{6})$")


def canonical_to_qlib(symbol: str) -> str:
    match = _CANONICAL.fullmatch(symbol)
    if not match:
        raise ValueError(f"invalid canonical A-share symbol: {symbol!r}")
    code, suffix = match.groups()
    return f"{suffix}{code}"


def qlib_to_canonical(symbol: str) -> str:
    match = _QLIB.fullmatch(symbol.upper())
    if not match:
        raise ValueError(f"invalid Qlib A-share symbol: {symbol!r}")
    suffix, code = match.groups()
    return f"{code}.{suffix}"
