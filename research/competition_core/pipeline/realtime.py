"""Append-only quote observations of a frozen S5 shortlist, prior to portfolio sealing."""

from datetime import date, datetime
from pathlib import Path
import re
import time
import uuid

from adapters.market_runtime.quotes import fetch_quotes, runtime_origin
from competition_core.pipeline.artifacts import atomic_json, content_hash, file_hash, read_json
from competition_core.pipeline.config import PipelineConfig
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.persistence import PipelineRepository
from competition_core.pipeline.time_context import ResearchContext, SHANGHAI

ROOT = Path(__file__).resolve().parents[2]


def _inputs(repository):
    inputs = read_json(repository.directory / "inputs.json")
    body = {key: value for key, value in inputs.items() if key != "specification_hash"}
    if content_hash(body) != inputs.get("specification_hash") or repository.state()["specification_hash"] != inputs.get("specification_hash"):
        raise PipelineError("STALE_DATA", "frozen research specification changed")
    config = PipelineConfig.from_dict(inputs["config"])
    if config.config_hash != inputs["config_hash"]:
        raise PipelineError("STALE_DATA", "research configuration hash mismatch")
    data = inputs["context"]
    context = ResearchContext(date.fromisoformat(data["research_date"]), data["phase"],
                              date.fromisoformat(data["market_data_reference_date"]), datetime.fromisoformat(data["as_of"]))
    stages = {name: repository.load_stage(name) for name in ("S0", "S1", "S2", "S3", "S4", "S5")}
    if any(stage is None for stage in stages.values()) or stages["S4"]["status"] != "SUCCESS":
        raise PipelineError("NOT_READY", "realtime check requires committed S0 through S5")
    for stage in stages.values():
        if (stage.get("config_hash") != inputs["config_hash"] or stage.get("snapshot_id") != inputs["snapshot_id"]
                or any(stage.get(key) != value for key, value in data.items())):
            raise PipelineError("STALE_DATA", "stage context differs from frozen research inputs")
    if stages["S4"]["output_symbols"] != stages["S5"]["input_symbols"] or not 10 <= len(stages["S5"]["input_symbols"]) <= 15:
        raise PipelineError("IDENTITY_CONFLICT", "realtime identities must be the original S4 finalists")
    if file_hash(inputs["manifest_path"]) != inputs["manifest_hash"]:
        raise PipelineError("STALE_DATA", "research dataset manifest changed")
    return inputs, config, context, stages


def _fingerprint():
    files = ["competition_core/pipeline/realtime.py", "competition_core/pipeline/realtime_gate.py",
             "competition_core/pipeline/time_context.py", "competition_core/pipeline/config.py",
             "competition_core/pipeline/artifacts.py", "competition_core/pipeline/persistence.py",
             "competition_core/pipeline/account.py",
             "adapters/market_runtime/quotes.py", "contracts/symbol.py"]
    return {name: file_hash(ROOT / name) for name in files}


def run_realtime_check(*, repository: PipelineRepository, check_id: str | None = None,
                       endpoint: str | None = None, quote_source=None, clock=None, account=None) -> dict:
    """A new observation can retry unavailable quotes without rerunning S0–S5.

    This does not commit S5_5/S6: the portfolio coordinator must recheck freshness
    and account constraints, then seal the chosen observation with the final draft.
    A saved observation is immutable evidence, never an automatically fresh cache.
    """
    from competition_core.pipeline.realtime_gate import evaluate_realtime_quotes

    check_id = check_id or f"quote-{uuid.uuid4().hex[:20]}"
    if not isinstance(check_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,95}", check_id):
        raise PipelineError("INVALID_ARGUMENTS", "invalid realtime check identifier")
    clock = clock or (lambda: datetime.now(SHANGHAI))  # Boundary observation only.
    origin = runtime_origin(endpoint)
    quote_source = quote_source or (lambda symbols: fetch_quotes(symbols, endpoint=origin))
    with repository.lock():
        inputs, config, context, stages = _inputs(repository)
        if repository.load_stage("S6") is not None:
            raise PipelineError("NOT_READY", "a sealed portfolio run cannot acquire a new quote observation")
        directory = repository.directory / "realtime" / check_id
        if directory.exists():
            raise PipelineError("NOT_READY", "quote observation identifier already used; retain it and choose a new identifier")
        started_at = clock()
        if not isinstance(started_at, datetime) or started_at.utcoffset() is None or context.as_of > started_at:
            raise PipelineError("NOT_READY", "quote observation requires a valid explicit clock")
        symbols = list(stages["S5"]["input_symbols"])
        if account is not None:
            from competition_core.pipeline.account import AccountSnapshot
            if not isinstance(account, AccountSnapshot):
                raise PipelineError("INVALID_ACCOUNT_STATE", "validated account snapshot required")
            # Extra held identities are for valuation/exit checks, never new Alpha candidates.
            symbols += sorted({p.symbol for p in account.positions} - set(symbols))
        if len(symbols) > 50:
            raise PipelineError("LIMIT_EXCEEDED", "shortlist plus held instruments exceeds quote batch limit")
        benchmark_symbol = config.section("screening")["benchmark_symbol"]
        requested_quote_symbols = symbols + ([benchmark_symbol] if benchmark_symbol not in symbols else [])
        if len(requested_quote_symbols) > 50:
            raise PipelineError("LIMIT_EXCEEDED", "quote batch including benchmark exceeds limit")
        fingerprint = _fingerprint()
        request = {"schema_version": "p10.realtime.request.v2", "check_id": check_id,
                   "context": context.to_dict(), "snapshot_id": inputs["snapshot_id"],
                   "config_hash": inputs["config_hash"], "stage_hashes": {key: stage["artifact_hash"] for key, stage in stages.items()},
                   "code_hashes": fingerprint, "symbols": symbols, "endpoint": origin,
                   "benchmark_symbol": benchmark_symbol,
                   "account_hash": account.snapshot_hash if account is not None else None,
                   "started_at": started_at.isoformat()}
        atomic_json(directory / "request.json", request, immutable=True)
        repository.update(status="RUNNING", current_stage="S5_5", execution_ready=False, production_accepted=False)
        started = time.monotonic()
        transport_result, failure = None, None
        try:
            # A saved historical run cannot be promoted to today's execution by
            # replacing only its quotes. Do not waste requests in that case.
            if started_at.astimezone(SHANGHAI).date() != context.research_date:
                failure = {"code": "RESEARCH_DATE_NOT_CURRENT"}
            else:
                try:
                    transport_result = quote_source(requested_quote_symbols)
                except Exception as exc:
                    code = exc.code if isinstance(exc, PipelineError) and exc.code in {
                        "SOURCE_UNAVAILABLE", "NOT_CONFIGURED", "IDENTITY_CONFLICT", "LIMIT_EXCEEDED"
                    } else "SOURCE_UNAVAILABLE"
                    failure = {"code": code, "exception_type": type(exc).__name__}
                    if isinstance(exc, PipelineError) and type(exc.details.get("http_status")) is int:
                        failure["http_status"] = exc.details["http_status"]
            observed_at = clock()
            if not isinstance(observed_at, datetime) or observed_at.utcoffset() is None or observed_at < started_at:
                raise PipelineError("NOT_READY", "quote observation clock moved backwards")
            if transport_result is not None and (not isinstance(transport_result, dict)
                    or not isinstance(transport_result.get("response"), dict)
                    or not isinstance(transport_result["response"].get("quotes"), list)):
                raise PipelineError("SOURCE_UNAVAILABLE", "quote adapter response shape is invalid")
            all_quotes = [] if transport_result is None else transport_result["response"]["quotes"]
            benchmark_rows = [row for row in all_quotes if row.get("symbol") == benchmark_symbol]
            if len(benchmark_rows) > 1:
                raise PipelineError("IDENTITY_CONFLICT", "quote response contains duplicate benchmark rows")
            benchmark_quote = benchmark_rows[0] if benchmark_rows else None
            quotes = [row for row in all_quotes if row.get("symbol") != benchmark_symbol]
            atomic_json(directory / "response.json", {"transport": transport_result, "error": failure,
                        "received_at": observed_at.isoformat()}, immutable=True)
            if failure and failure["code"] == "RESEARCH_DATE_NOT_CURRENT":
                raise PipelineError("RESEARCH_DATE_NOT_CURRENT", "historical research cannot use current-session quotes")
            result = evaluate_realtime_quotes(symbols=symbols, quotes=quotes, context=context,
                        config=config, observed_at=observed_at, snapshot_id=inputs["snapshot_id"],
                        benchmark_quote=benchmark_quote)
            if fingerprint != _fingerprint():
                raise PipelineError("STALE_DATA", "quote gate code changed during observation")
            # Checking quote quality never grants AI approval or trading authority.
            ai_eligible = {row["symbol"] for row in stages["S5"]["rows"]
                           if row.get("ai_status") == "SUCCESS" and row.get("ai_eligible") is True
                           and row.get("ai_verdict") in {"KEEP", "WATCH"}}
            quote_eligible = {row["symbol"] for row in result["rows"] if row.get("execution_eligible") is True}
            eligible = [symbol for symbol in symbols if symbol in ai_eligible & quote_eligible]
            report = {"schema_version": "p10.realtime.observation.v1", "run_id": repository.run_id,
                      "check_id": check_id, "request_hash": file_hash(directory / "request.json"),
                      "response_hash": file_hash(directory / "response.json"), "result": result,
                      "source_error": failure, "ai_and_quote_eligible": eligible,
                      "enough_candidates_for_portfolio": len(eligible) >= config.section("risk")["min_positions"],
                      "execution_ready": False, "production_accepted": False,
                      "elapsed_seconds": round(time.monotonic() - started, 6)}
            atomic_json(directory / "observation.json", report | {"integrity_hash": content_hash(report)}, immutable=True)
            repository.update(status="PARTIAL", completed_through="S5", pending_stage="S5_5",
                              last_realtime_check=check_id, execution_ready=False, error=None)
            return report
        except Exception as exc:
            error = {"code": exc.code if isinstance(exc, PipelineError) else "REALTIME_CHECK_FAILED",
                     "exception_type": type(exc).__name__}
            atomic_json(directory / "failure.json", error, immutable=True)
            repository.update(status="PARTIAL", completed_through="S5", pending_stage="S5_5",
                              last_realtime_check=check_id, execution_ready=False, error=error)
            raise
