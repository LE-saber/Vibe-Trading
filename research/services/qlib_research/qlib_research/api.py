"""Operational surface; research runs remain explicit batch jobs."""

from __future__ import annotations

import importlib.metadata
import os

import uvicorn
from fastapi import FastAPI

from qlib_research import __version__

app = FastAPI(title="Competition Qlib Research", version=__version__)


@app.get("/health")
def health() -> dict[str, str]:
    return {"service": "competition-qlib-research", "status": "ok", "version": __version__}


@app.get("/version")
def version() -> dict[str, str]:
    return {**health(), "framework": "Microsoft Qlib", "framework_version": importlib.metadata.version("pyqlib")}


@app.get("/readiness")
def readiness() -> dict[str, object]:
    canonical = os.getenv("QLIB_CANONICAL_PARQUET", "").strip()
    universe = os.getenv("QLIB_UNIVERSE_PARQUET", "").strip()
    ready = bool(canonical and universe)
    return {
        **health(),
        "ready": ready,
        "reason": None if ready else "canonical and PIT universe parquet paths are not configured",
    }


def main() -> None:
    uvicorn.run(app, host="127.0.0.1", port=8940)


if __name__ == "__main__":
    main()
