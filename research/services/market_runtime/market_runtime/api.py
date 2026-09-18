"""FastAPI boundary for the isolated mootdx runtime."""

from __future__ import annotations

from datetime import datetime
import os
from zoneinfo import ZoneInfo

import uvicorn
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field

from market_runtime import __version__
from market_runtime.provider import FREQUENCIES, MootdxProvider
from market_runtime.sina_quotes import SinaQuoteError, SinaQuotesProvider
from market_runtime.symbols import MarketSymbol, MarketSymbolError

app = FastAPI(title="Competition Market Runtime", version=__version__)


class QuotesRequest(BaseModel):
    symbols: list[str] = Field(min_length=1, max_length=50)


class BarsRequest(BaseModel):
    symbols: list[str] = Field(min_length=1, max_length=20)
    interval: str
    offset: int = Field(default=800, ge=1, le=800)


def get_provider() -> MootdxProvider:
    return MootdxProvider()


def _configured_quote_source() -> str:
    source = os.environ.get("MARKET_RUNTIME_QUOTE_SOURCE", "mootdx")
    if source not in {"mootdx", "sina"}:
        raise ValueError("MARKET_RUNTIME_QUOTE_SOURCE must be 'mootdx' or 'sina'")
    return source


def get_quote_provider() -> MootdxProvider | SinaQuotesProvider:
    """Select the quote source without constructing mootdx for Sina."""

    try:
        source = _configured_quote_source()
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if source == "sina":
        return SinaQuotesProvider()

    # Keep existing tests and callers that override get_provider working even
    # though the quote dependency is now source-aware.
    provider_factory = app.dependency_overrides.get(get_provider, get_provider)
    try:
        return provider_factory()
    except Exception as exc:  # noqa: BLE001 - provider initialization envelope
        raise HTTPException(status_code=503, detail=f"mootdx unavailable: {exc}") from exc


@app.get("/health")
def health() -> dict[str, str]:
    return {"service": "competition-market-runtime", "status": "ok", "version": __version__}


@app.get("/version")
def version() -> dict[str, str]:
    return health()


@app.get("/readiness")
def readiness() -> dict[str, object]:
    try:
        source = _configured_quote_source()
    except ValueError as exc:
        return {**health(), "ready": False, "reason": str(exc)}
    if source == "mootdx":
        try:
            get_provider()
        except Exception as exc:  # noqa: BLE001
            return {**health(), "ready": False, "reason": f"mootdx initialization failed: {type(exc).__name__}"}
    return {**health(), "ready": True, "reason": None}


@app.post("/quotes")
def quotes(
    request: QuotesRequest,
    provider: MootdxProvider | SinaQuotesProvider = Depends(get_quote_provider),
) -> dict:
    try:
        identities = [MarketSymbol.parse_quote(symbol) for symbol in request.symbols]
        rows = provider.quotes(identities)
    except MarketSymbolError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except SinaQuoteError as exc:
        raise HTTPException(status_code=503, detail=f"sina unavailable: {exc}") from exc
    except Exception as exc:  # noqa: BLE001 - isolated provider failure envelope
        source = os.environ.get("MARKET_RUNTIME_QUOTE_SOURCE", "mootdx")
        label = "sina" if source == "sina" else "mootdx"
        raise HTTPException(status_code=503, detail=f"{label} unavailable: {exc}") from exc
    return {
        "as_of": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(),
        "quotes": rows,
        "missing": sorted(set(request.symbols) - {row["symbol"] for row in rows}),
    }


@app.post("/bars")
def bars(
    request: BarsRequest, provider: MootdxProvider = Depends(get_provider)
) -> dict:
    if request.interval not in FREQUENCIES:
        raise HTTPException(status_code=422, detail=f"interval must be one of {sorted(FREQUENCIES)}")
    try:
        identities = [MarketSymbol.parse(symbol) for symbol in request.symbols]
    except MarketSymbolError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    rows: dict[str, list[dict]] = {}
    errors: dict[str, str] = {}
    for identity in identities:
        try:
            rows[identity.canonical] = provider.bars(identity, request.interval, request.offset)
        except Exception as exc:  # noqa: BLE001 - one symbol must not drop the batch
            errors[identity.canonical] = f"{type(exc).__name__}: {exc}"
    return {"interval": request.interval, "bars": rows, "errors": errors}


def main() -> None:
    uvicorn.run(app, host="127.0.0.1", port=8920)


if __name__ == "__main__":
    main()
