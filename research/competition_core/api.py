"""Minimal HTTP surface for the Competition Core skeleton."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
import json
import os
from typing import Final
from urllib.request import ProxyHandler, build_opener

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from adapters.tushare.client import create_tushare_client
from competition_core import __version__
from competition_core.config import load_config, parse_runtime_endpoint
from competition_core.storage import EvidenceRepository, RepositoryError, SnapshotCache, WorkflowRepository
from competition_core.universe import CompetitionUniverse
from competition_core.universe.ranking import SORT_KEYS, screen_competition_universe
from contracts.evidence import EvidenceBundle, EvidenceContractError
from contracts.symbol import CanonicalSymbol, SymbolContractError


SERVICE_NAME: Final = "competition-core"
STATUS: Final = "ok"
MAX_CANDIDATE_LIMIT: Final = 20

app = FastAPI(title="Competition Core", version=__version__)

from competition_core.research_api import router as research_router
app.include_router(research_router)


def _structured_error(
    status_code: int,
    code: str,
    message: str,
    details: dict | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "code": code,
            "message": message,
            "details": {} if details is None else details,
        },
    )


@app.exception_handler(RequestValidationError)
async def _request_validation_error(
    request, exc: RequestValidationError
) -> JSONResponse:
    """Keep candidate limit validation structured without echoing raw input."""
    if request.url.path.startswith("/pipeline/"):
        return _structured_error(422, "INVALID_ARGUMENTS", "invalid research request")
    if request.url.path == "/candidates":
        for error in exc.errors():
            if tuple(error.get("loc", ())) != ("query", "limit"):
                continue
            if error.get("type") == "less_than_equal":
                raw_requested = error.get("input")
                try:
                    requested = int(raw_requested)
                except (TypeError, ValueError):
                    requested = None
                if requested is not None and not isinstance(raw_requested, bool):
                    return _structured_error(
                        422,
                        "LIMIT_EXCEEDED",
                        f"limit must be <= {MAX_CANDIDATE_LIMIT}",
                        {"requested": requested, "maximum": MAX_CANDIDATE_LIMIT},
                    )
            return _structured_error(
                422,
                "INVALID_ARGUMENTS",
                "invalid candidates query parameters",
            )
    return JSONResponse(
        status_code=422,
        content={"detail": jsonable_encoder(exc.errors())},
    )


def _status_payload() -> dict[str, str]:
    return {"service": SERVICE_NAME, "status": STATUS, "version": __version__}


@app.get("/health")
def health() -> dict[str, str]:
    return _status_payload()


@app.get("/version")
def version() -> dict[str, str]:
    return _status_payload()


@app.get("/readiness")
def readiness() -> dict:
    reasons: list[str] = []
    try:
        load_config()
    except Exception as exc:  # noqa: BLE001
        reasons.append(f"config:{type(exc).__name__}")
    if not os.getenv("TUSHARE_TOKEN", "").strip():
        reasons.append("TUSHARE_TOKEN is not configured")
    if len(os.getenv("COMPETITION_CORE_SERVICE_TOKEN", "")) < 32:
        reasons.append("Competition Core service identity is not configured")
    try:
        EvidenceRepository()
        WorkflowRepository()
    except Exception as exc:  # noqa: BLE001
        reasons.append(f"storage:{type(exc).__name__}")
    return {**_status_payload(), "ready": not reasons, "reasons": reasons}


_SYSTEM_ENDPOINTS = {
    "market_runtime": "http://127.0.0.1:8920",
    "ai_decision": "http://127.0.0.1:8930",
    "qlib_research": "http://127.0.0.1:8940",
    "vibe": "http://127.0.0.1:8899",
}


def _probe_component(item: tuple[str, str]) -> tuple[str, dict]:
    name, endpoint = item
    try:
        opener = build_opener(ProxyHandler({}))
        with opener.open(f"{endpoint}/health", timeout=1.5) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return name, {"status": "ok", "endpoint": endpoint, "detail": payload}
    except Exception as exc:  # noqa: BLE001 - aggregate must survive component failure
        return name, {"status": "unavailable", "endpoint": endpoint, "error_type": type(exc).__name__}


@app.get("/system/health")
def system_health() -> dict:
    with ThreadPoolExecutor(max_workers=len(_SYSTEM_ENDPOINTS)) as executor:
        components = dict(executor.map(_probe_component, _SYSTEM_ENDPOINTS.items()))
    core_ready = readiness()
    components["competition_core"] = {
        "status": "ok" if core_ready["ready"] else "degraded",
        "endpoint": "http://127.0.0.1:8910",
        "detail": core_ready,
    }
    unavailable = [name for name, item in components.items() if item["status"] == "unavailable"]
    return {
        "service": SERVICE_NAME,
        "status": "ok" if not unavailable and core_ready["ready"] else "degraded",
        "components": components,
        "unavailable": unavailable,
        "checked_at": datetime.now().astimezone().isoformat(),
    }


def get_tushare_client():
    """FastAPI dependency kept explicit for contract and parity tests."""
    try:
        return create_tushare_client()
    except Exception as exc:  # noqa: BLE001 - endpoint may use an explicitly stale cache
        failure = exc
        failure_type = type(exc).__name__

        class UnavailableTushare:
            def __getattr__(self, _name):
                def unavailable(**_kwargs):
                    raise RuntimeError(f"Tushare client unavailable: {failure_type}") from failure

                return unavailable

        return UnavailableTushare()


def get_snapshot_cache() -> SnapshotCache:
    return SnapshotCache()


@app.get("/universe/snapshot")
def universe_snapshot(
    snapshot_date: str = Query(alias="date"),
    phase: str = Query(default="postclose", pattern="^(preopen|postclose)$"),
    client=Depends(get_tushare_client),
    cache: SnapshotCache = Depends(get_snapshot_cache),
) -> dict:
    try:
        config = load_config()
        payload = CompetitionUniverse(client=client, config=config).build(
            snapshot_date, phase
        ).to_dict()
        payload.update({"stale": False, "fallback_used": False})
        cache.put("universe", date.fromisoformat(snapshot_date), phase, payload)
        return payload
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - provider errors degrade to explicit stale cache
        try:
            payload = cache.latest("universe", date.fromisoformat(snapshot_date), phase)
        except (KeyError, ValueError):
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        payload.update(
            {
                "stale": True,
                "fallback_used": True,
                "requested_date": snapshot_date,
                "stale_reason": f"Tushare unavailable: {type(exc).__name__}",
            }
        )
        return payload


@app.get("/candidates")
def candidates(
    snapshot_date: str = Query(alias="date"),
    phase: str = Query(default="postclose", pattern="^(preopen|postclose)$"),
    sort_by: str = Query(default="amount"),
    limit: int = Query(default=MAX_CANDIDATE_LIMIT, ge=1, le=MAX_CANDIDATE_LIMIT),
    client=Depends(get_tushare_client),
    cache: SnapshotCache = Depends(get_snapshot_cache),
) -> dict:
    if limit > MAX_CANDIDATE_LIMIT:
        return _structured_error(
            422,
            "LIMIT_EXCEEDED",
            f"limit must be <= {MAX_CANDIDATE_LIMIT}",
            {"requested": limit, "maximum": MAX_CANDIDATE_LIMIT},
        )
    if sort_by not in SORT_KEYS:
        return _structured_error(
            422,
            "INVALID_ARGUMENTS",
            f"sort_by must be one of {sorted(SORT_KEYS)}",
        )
    try:
        payload = screen_competition_universe(
            client=client,
            config=load_config(),
            as_of=snapshot_date,
            phase=phase,
            sort_by=sort_by,
            limit=limit,
        )
        payload.update({"stale": False, "fallback_used": False})
        cache.put(
            "candidates",
            date.fromisoformat(snapshot_date),
            phase,
            payload,
            variant=sort_by,
        )
        return payload
    except ValueError as exc:
        return _structured_error(400, "INVALID_ARGUMENTS", str(exc))
    except Exception as exc:  # noqa: BLE001 - provider errors degrade to explicit stale cache
        try:
            payload = cache.latest(
                "candidates", date.fromisoformat(snapshot_date), phase, variant=sort_by
            )
        except (KeyError, ValueError):
            return _structured_error(
                503,
                "SOURCE_UNAVAILABLE",
                "candidate data source is unavailable",
                {"exception_type": type(exc).__name__},
            )
        payload["rows"] = payload.get("rows", [])[:limit]
        payload["returned"] = len(payload["rows"])
        payload.update(
            {
                "limit": limit,
                "stale": True,
                "fallback_used": True,
                "requested_date": snapshot_date,
                "stale_reason": f"Tushare unavailable: {type(exc).__name__}",
            }
        )
        return payload


@app.get("/symbols/{symbol}")
def symbol_identity(symbol: str) -> dict:
    try:
        return CanonicalSymbol.from_core(symbol).to_dict()
    except SymbolContractError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


def get_evidence_repository() -> EvidenceRepository:
    return EvidenceRepository()


@app.post("/evidence", status_code=201)
def create_evidence(
    payload: dict, repository: EvidenceRepository = Depends(get_evidence_repository)
) -> dict[str, str]:
    try:
        bundle = EvidenceBundle.from_dict(payload)
        return {"evidence_id": repository.put(bundle)}
    except (EvidenceContractError, SymbolContractError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except RepositoryError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/evidence/latest")
def latest_evidence_many(
    symbols: str = Query(min_length=1),
    repository: EvidenceRepository = Depends(get_evidence_repository),
) -> dict[str, list[dict]]:
    requested = [item.strip() for item in symbols.split(",") if item.strip()]
    if not requested:
        raise HTTPException(status_code=422, detail="symbols must not be empty")
    try:
        bundles = repository.latest_many(requested)
    except SymbolContractError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"evidence": [bundles[symbol].to_dict() for symbol in requested if symbol in bundles]}


@app.get("/evidence/latest/{symbol}")
def latest_evidence(
    symbol: str, repository: EvidenceRepository = Depends(get_evidence_repository)
) -> dict:
    try:
        return repository.latest(symbol).to_dict()
    except SymbolContractError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"no evidence for {symbol}") from exc


@app.get("/evidence/{evidence_id}")
def evidence_by_id(
    evidence_id: str, repository: EvidenceRepository = Depends(get_evidence_repository)
) -> dict:
    try:
        return repository.get(evidence_id).to_dict()
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="evidence not found") from exc
    except RepositoryError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def get_workflow_repository() -> WorkflowRepository:
    return WorkflowRepository()


@app.get("/portfolio/state")
def portfolio_state(repository: WorkflowRepository = Depends(get_workflow_repository)) -> dict:
    try:
        return repository.latest_portfolio().to_dict()
    except KeyError:
        return _structured_error(
            404,
            "MISSING_ACCOUNT_STATE",
            "portfolio state is not available",
            {"resource": "portfolio_state"},
        )


@app.get("/portfolio/target/latest")
def portfolio_target(repository: WorkflowRepository = Depends(get_workflow_repository)) -> dict:
    try:
        return repository.latest_target().to_dict()
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/order-intents/latest")
def order_intents(repository: WorkflowRepository = Depends(get_workflow_repository)) -> dict:
    return {"order_intents": repository.latest_intents(), "execution_mode": "MANUAL_CONFIRM"}


def main() -> None:
    config = load_config()
    host, port = parse_runtime_endpoint(config.runtime_endpoint)
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
