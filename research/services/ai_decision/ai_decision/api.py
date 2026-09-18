"""FastAPI surface for bounded TradingAgents analysis."""

from __future__ import annotations

import uvicorn
from fastapi import Depends, FastAPI, HTTPException

from ai_decision import __version__
from ai_decision.models import AnalyzeRequest, BatchAnalyzeRequest, DecisionOutput
from ai_decision.runner import DecisionConfigurationError, DecisionRunner

app = FastAPI(title="Competition AI Decision", version=__version__)
_RUNNER = DecisionRunner()


def get_runner() -> DecisionRunner:
    return _RUNNER


@app.get("/health")
def health() -> dict[str, str]:
    return {"service": "competition-ai-decision", "status": "ok", "version": __version__}


@app.get("/version")
def version() -> dict[str, str]:
    return health()


@app.get("/readiness")
def readiness(runner: DecisionRunner = Depends(get_runner)) -> dict[str, object]:
    ready = runner.provider_ready()
    return {
        **health(),
        "ready": ready,
        "reason": None if ready else "TradingAgents LLM provider is not configured",
    }


@app.post("/decisions/analyze", response_model=DecisionOutput)
def analyze(
    request: AnalyzeRequest, runner: DecisionRunner = Depends(get_runner)
) -> DecisionOutput:
    try:
        return runner.analyze(request)
    except DecisionConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - sidecar failure isolation
        raise HTTPException(status_code=502, detail=f"TradingAgents analysis failed: {type(exc).__name__}") from exc


@app.post("/decisions/batch", response_model=list[DecisionOutput])
def analyze_batch(
    request: BatchAnalyzeRequest, runner: DecisionRunner = Depends(get_runner)
) -> list[DecisionOutput]:
    outputs: list[DecisionOutput] = []
    try:
        for item in request.items:
            outputs.append(runner.analyze(item))
    except DecisionConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - no partial batch masquerading as complete
        raise HTTPException(status_code=502, detail=f"TradingAgents batch failed: {type(exc).__name__}") from exc
    return outputs


def main() -> None:
    uvicorn.run(app, host="127.0.0.1", port=8930)


if __name__ == "__main__":
    main()
