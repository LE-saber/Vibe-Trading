"""Read-only S6 to fixed execution-plan bridge.

This module consumes only a completed, sealed Core S6 attempt.  It does not
fetch data, call a broker, change run state, or grant approval.  The resulting
plan is an immutable research artifact for a later approval/execution package.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re
from typing import Any

from contracts.symbol import CanonicalSymbol

from competition_core.pipeline.account import AccountSnapshot
from competition_core.pipeline.artifacts import (
    atomic_json,
    confined_path,
    content_hash,
    file_hash,
    read_json,
)
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.instruments import QuantityRules, quantity_rules
from competition_core.pipeline.portfolio import FeeSchedule
from competition_core.pipeline.realtime import _inputs


EXECUTION_PLAN_SCHEMA_VERSION = "p10.execution_plan.v1"
BRIDGE_VERSION = "s6_to_execution_plan.v1"
PLAN_PATH = "execution-plan.json"

_HEX64 = re.compile(r"[0-9a-f]{64}")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,95}")
_ATTEMPT_FILES = frozenset(
    {
        "request.json",
        "account.json",
        "fees.json",
        "trading-metadata.json",
        "holding-review-consumption.json",
        "quote-response.json",
        "gate.json",
        "result.json",
    }
)
_DEMO_MARKERS = ("synthetic", "demo", "fixture", "mock", "fake")
_TERMINAL_SHADOW_BLOCKER = "TERMINAL_POLICY_SHADOW_ONLY"
_TERMINAL_SHADOW_ALLOCATION_MARKER = "terminal_shadow"


def _fail(code: str, message: str, **details: Any) -> None:
    raise PipelineError(code, message, **details)


def _require(condition: bool, code: str, message: str, **details: Any) -> None:
    if not condition:
        _fail(code, message, **details)


def _identifier(value: Any, field: str) -> str:
    _require(isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
             "INVALID_ARGUMENTS", f"{field} is invalid")
    return value


def _hash(value: Any, field: str) -> str:
    _require(isinstance(value, str) and _HEX64.fullmatch(value) is not None,
             "STALE_DATA", f"{field} is missing or invalid")
    return value


def _provider_content_hash(value: Any) -> str:
    """Match the Ninef receipt hash, which predates Core's newline hash."""
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _decimal(value: Any, field: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, int, float, str)):
        _fail("STALE_DATA", f"{field} is not numeric")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise PipelineError("STALE_DATA", f"{field} is not numeric") from exc
    if not result.is_finite() or result < 0 or (positive and result == 0):
        _fail("STALE_DATA", f"{field} is outside supported bounds")
    return result


def _money(value: Any, field: str) -> str:
    result = _decimal(value, field)
    aligned = result.quantize(Decimal("0.01"))
    _require(result == aligned, "STALE_DATA",
             f"{field} is not cent aligned")
    return format(aligned, "f")


def _read(path: Path, *, expected_hash: str | None = None, label: str,
          missing_code: str = "NOT_READY") -> Any:
    try:
        return read_json(path, expected_hash=expected_hash)
    except PipelineError:
        raise
    except (OSError, UnicodeError, ValueError) as exc:
        raise PipelineError(missing_code, f"{label} is unavailable") from exc


def _read_sealed(path: Path, *, label: str) -> tuple[dict[str, Any], str]:
    value = _read(path, label=label)
    _require(isinstance(value, dict), "STALE_DATA", f"{label} is not an object")
    stored = value.pop("integrity_hash", None)
    _require(stored == content_hash(value), "STALE_DATA", f"{label} integrity hash mismatch")
    return value, file_hash(path)


def _relative(root: Path, path: Path) -> str:
    return str(path.relative_to(root)).replace("\\", "/")


def _reject_demo(*values: Any) -> None:
    text = " ".join(str(value).lower() for value in values if value is not None)
    if any(marker in text for marker in _DEMO_MARKERS):
        _fail("NOT_READY", "demo or synthetic input cannot create an execution plan")


def _has_terminal_shadow(value: Any) -> bool:
    """Return whether a portfolio/request carries the terminal shadow boundary."""

    if not isinstance(value, dict):
        return False
    if any(key in value for key in ("terminal_evaluation_id", "terminal_manifest_hash")):
        return True
    if isinstance(value.get("allocation_version"), str):
        if _TERMINAL_SHADOW_ALLOCATION_MARKER in value["allocation_version"]:
            return True
    policy = value.get("policy")
    if isinstance(policy, dict) and any(
        key in policy for key in ("terminal_shadow", "terminal_evaluation_binding")
    ):
        return True
    summary = value.get("summary")
    if isinstance(summary, dict) and (
        summary.get("terminal_shadow_only") is True
        or "terminal_evaluation_id" in summary
    ):
        return True
    risk_checks = value.get("risk_checks")
    if isinstance(risk_checks, dict) and (
        risk_checks.get("terminal_policy_shadow_only") is True
        or _TERMINAL_SHADOW_BLOCKER in risk_checks.get("blocking_reasons", [])
    ):
        return True
    return False


def _validate_attempt_files(root: Path, seal: dict[str, Any], directory: Path) -> None:
    files = seal.get("files")
    _require(isinstance(files, dict) and set(files) == _ATTEMPT_FILES,
             "STALE_DATA", "portfolio seal file manifest is incomplete")
    for name, digest in files.items():
        _hash(digest, f"portfolio seal {name} hash")
        path = confined_path(directory, name)
        _require(path.parent == directory and path.is_file() and not path.is_symlink(),
                 "STALE_DATA", f"portfolio seal artifact is unavailable: {name}")
        _require(file_hash(path) == digest, "STALE_DATA",
                 f"portfolio seal artifact changed: {name}")


def _validate_news(root: Path, stages: dict[str, dict[str, Any]], context: dict[str, Any]) -> dict[str, Any]:
    references = stages["S5"].get("source_provenance", [])
    news_refs = [item for item in references if isinstance(item, dict) and item.get("path") == "news-context.json"]
    review_refs = [item for item in references if isinstance(item, dict) and item.get("path") == "review-inputs.json"]
    _require(len(news_refs) == 1 and len(review_refs) == 1, "NOT_READY",
             "S5 news and review bindings are required before plan creation")
    news_hash = _hash(news_refs[0].get("sha256"), "news context hash")
    review_hash = _hash(review_refs[0].get("sha256"), "review input hash")
    news_path = confined_path(root, "news-context.json")
    review_path = confined_path(root, "review-inputs.json")
    _require(file_hash(news_path) == news_hash and file_hash(review_path) == review_hash,
             "STALE_DATA", "S5 news/review source changed")
    news = _read(news_path, expected_hash=news_hash, label="news context")
    review_inputs = _read(review_path, expected_hash=review_hash, label="review inputs")
    _require(isinstance(news, dict) and isinstance(review_inputs, dict), "STALE_DATA",
             "S5 news/review binding is invalid")
    snapshot_hash = _hash(news.get("snapshot_hash"), "news snapshot hash")
    unsigned_news = {key: value for key, value in news.items() if key != "snapshot_hash"}
    _require(snapshot_hash == content_hash(unsigned_news), "STALE_DATA", "news snapshot hash mismatch")
    _require(news.get("status") in {"AVAILABLE", "EMPTY_VERIFIED"}, "NOT_READY",
             "S5 news snapshot is not verified")
    _require(news.get("cutoff") == context.get("as_of"), "STALE_DATA",
             "S5 news cutoff differs from the frozen context")
    _require(isinstance(news.get("items"), list), "STALE_DATA", "S5 news items are invalid")
    _require(review_inputs.get("news_context_hash") == snapshot_hash, "STALE_DATA",
             "review input is not bound to the frozen news snapshot")
    _require(review_inputs.get("context") == context, "STALE_DATA",
             "review input context differs from the frozen context")
    return {
        "snapshot_hash": snapshot_hash,
        "file_hash": news_hash,
        "review_inputs_hash": review_hash,
        "status": news["status"],
    }


def _validate_account_import(root: Path, request: dict[str, Any], account: AccountSnapshot,
                             fees: FeeSchedule) -> tuple[str, str | None]:
    source_hash = _hash(request.get("account_source_hash"), "account source hash")
    pointer_path = confined_path(root, "execution-inputs.json")
    pointer = _read(pointer_path, label="execution input pointer")
    _require(isinstance(pointer, dict) and set(pointer) == {"id"}, "STALE_DATA",
             "execution input pointer is invalid")
    _require(pointer["id"] == source_hash, "STALE_DATA",
             "sealed account source does not match the current import pointer")
    source_path = confined_path(root, f"execution-imports/{source_hash}.json")
    source = _read(source_path, expected_hash=source_hash, label="execution account import")
    _require(isinstance(source, dict) and set(source) <= {"account", "fees", "source_receipt"}
             and {"account", "fees"} <= set(source), "STALE_DATA",
             "execution account import schema is invalid")
    _require(source["account"] == account.to_dict() and source["fees"] == fees.to_dict(),
             "STALE_DATA", "execution account import differs from sealed inputs")
    receipt = source.get("source_receipt")
    if receipt is not None:
        _require(isinstance(receipt, dict), "STALE_DATA", "account source receipt is invalid")
        if receipt.get("orders") is not None:
            _require(receipt.get("orders", {}).get("complete") is True
                     and receipt.get("collection_consistent") is True,
                     "NOT_READY", "account order collection is not complete")
        if receipt.get("account_snapshot_hash") is not None:
            _require(receipt["account_snapshot_hash"] == account.snapshot_hash,
                     "STALE_DATA", "account source receipt snapshot differs")
        if receipt.get("fee_schedule_hash") is not None:
            _require(receipt["fee_schedule_hash"] in {content_hash(fees.to_dict()),
                                                        _provider_content_hash(fees.to_dict())},
                     "STALE_DATA", "account source receipt fee differs")
    _reject_demo(account.source, account.source_kind, fees.source)
    return source_hash, content_hash(receipt) if receipt is not None else None


def _validate_status(root: Path, request: dict[str, Any], account: AccountSnapshot,
                     metadata: dict[str, Any], stages: dict[str, dict[str, Any]],
                     context: dict[str, Any], stage_hashes: dict[str, str],
                     symbols: list[str]) -> dict[str, Any]:
    status_id = request.get("status_id")
    _require(status_id is not None, "NOT_READY", "a sealed execution-status observation is required")
    status_id = _identifier(status_id, "status_id")
    proofs = request.get("execution_status_provenance")
    _require(isinstance(proofs, list) and proofs, "NOT_READY",
             "execution-status source proofs are required")
    seen: set[str] = set()
    proof_hashes: dict[str, str] = {}
    status_prefix = f"execution-status/{status_id}/"
    for proof in proofs:
        _require(isinstance(proof, dict) and set(proof) == {"path", "sha256"},
                 "STALE_DATA", "execution-status proof is invalid")
        path_text = proof["path"]
        digest = _hash(proof["sha256"], "execution-status proof hash")
        _require(isinstance(path_text, str) and path_text.startswith(status_prefix),
                 "STALE_DATA", "execution-status proof escapes its observation")
        _require(path_text not in seen, "STALE_DATA", "duplicate execution-status proof")
        seen.add(path_text)
        path = confined_path(root, path_text)
        _require(path.is_file() and not path.is_symlink() and file_hash(path) == digest,
                 "STALE_DATA", "execution-status proof changed")
        proof_hashes[path_text] = digest
    required = {f"{status_prefix}{name}" for name in ("request.json", "observation.json", "binding.json")}
    _require(required <= set(proof_hashes), "NOT_READY", "execution-status binding proofs are incomplete")
    status_dir = confined_path(root, f"execution-status/{status_id}")
    status_request = _read(status_dir / "request.json", expected_hash=proof_hashes[f"{status_prefix}request.json"],
                           label="execution-status request")
    observation, _ = _read_sealed(status_dir / "observation.json", label="execution-status observation")
    binding, _ = _read_sealed(status_dir / "binding.json", label="execution-status binding")
    _require(status_request.get("context") == context
             and set(status_request.get("symbols", [])) == set(symbols),
             "STALE_DATA", "execution-status request differs from the sealed portfolio")
    _require(observation.get("context") == context and observation.get("status") == "READY"
             and observation.get("execution_ready") is False
             and observation.get("production_accepted") is False,
             "NOT_READY", "execution-status observation is not complete")
    _require(binding.get("run_id") == root.name and binding.get("status_id") == status_id
             and binding.get("context") == context and binding.get("snapshot_id") == request["snapshot_id"]
             and binding.get("config_hash") == request["config_hash"]
             and binding.get("stage_hashes") == stage_hashes
             and binding.get("account_hash") == account.snapshot_hash
             and binding.get("observation_hash") == proof_hashes[f"{status_prefix}observation.json"],
             "STALE_DATA", "execution-status binding differs from the frozen run")
    _require(observation.get("metadata") == metadata, "STALE_DATA",
             "sealed trading metadata differs from execution-status observation")
    _require(isinstance(observation.get("observed_at"), str), "STALE_DATA",
             "execution-status observation time is missing")
    try:
        status_observed_at = datetime.fromisoformat(observation["observed_at"])
    except ValueError as exc:
        raise PipelineError("STALE_DATA", "execution-status observation time is invalid") from exc
    _require(status_observed_at.utcoffset() is not None, "STALE_DATA",
             "execution-status observation time is naive")
    for symbol, row in metadata.items():
        _require(isinstance(row, dict) and row.get("symbol", symbol) == symbol
                 and row.get("trade_date") == context["research_date"]
                 and row.get("source") and row.get("tradable") is True
                 and row.get("suspended") is False
                 and row.get("new_listing_special_period") is False,
                 "NOT_READY", f"execution status is not orderable for {symbol}")
        _require(_decimal(row.get("down_limit"), "down_limit", positive=True)
                 < _decimal(row.get("up_limit"), "up_limit", positive=True),
                 "STALE_DATA", f"execution status limits are invalid for {symbol}")
    _reject_demo(*(row.get("source") for row in metadata.values() if isinstance(row, dict)))
    return {
        "status_id": status_id,
        "observation_hash": proof_hashes[f"{status_prefix}observation.json"],
        "binding_hash": proof_hashes[f"{status_prefix}binding.json"],
        "proofs": [{"path": path, "sha256": digest} for path, digest in sorted(proof_hashes.items())],
        "metadata_hash": content_hash(metadata),
        "observed_at": observation.get("observed_at"),
    }


def _validate_quotes(root: Path, request: dict[str, Any], account: AccountSnapshot,
                     gate: dict[str, Any], quote_response: dict[str, Any],
                     context: dict[str, Any], symbols: list[str]) -> dict[str, Any]:
    check_id = _identifier(request.get("check_id"), "check_id")
    quote_dir = confined_path(root, f"realtime/{check_id}")
    quote_request_hash = _hash(request.get("quote_request_hash"), "quote request hash")
    quote_response_hash = _hash(request.get("quote_response_hash"), "quote response hash")
    saved_request = _read(quote_dir / "request.json", expected_hash=quote_request_hash,
                          label="quote request")
    observation, _ = _read_sealed(quote_dir / "observation.json", label="quote observation")
    response = _read(quote_dir / "response.json", expected_hash=quote_response_hash, label="quote response")
    _require(saved_request.get("run_id", root.name) == root.name and saved_request.get("check_id") == check_id
             and saved_request.get("context") == context
             and saved_request.get("snapshot_id") == request["snapshot_id"]
             and saved_request.get("config_hash") == request["config_hash"]
             and saved_request.get("stage_hashes") == request["stage_hashes"]
             and set(saved_request.get("symbols", [])) == set(symbols)
             and saved_request.get("account_hash") == account.snapshot_hash,
             "STALE_DATA", "quote request differs from the sealed portfolio")
    _require(observation.get("run_id") == root.name and observation.get("check_id") == check_id
             and observation.get("request_hash") == quote_request_hash
             and observation.get("response_hash") == quote_response_hash
             and observation.get("execution_ready") is False,
             "STALE_DATA", "quote observation differs from its source receipt")
    _require(response == quote_response, "STALE_DATA", "sealed quote response differs from the quote receipt")
    _require(isinstance(response, dict) and isinstance(response.get("transport"), dict),
             "NOT_READY", "a transport-backed quote observation is required")
    transport = response["transport"]
    raw = transport.get("response")
    _require(isinstance(raw, dict) and isinstance(raw.get("quotes"), list), "STALE_DATA",
             "quote response shape is invalid")
    quote_rows = [row for row in raw["quotes"] if isinstance(row, dict) and row.get("symbol") in symbols]
    _require(len(quote_rows) == len(symbols) and len({row.get("symbol") for row in quote_rows}) == len(symbols),
             "NOT_READY", "quote observation does not cover the sealed symbols")
    _reject_demo(*(row.get("source") for row in quote_rows),
                 *(row.get("timestamp_source") for row in quote_rows))
    _require(isinstance(gate, dict) and set(gate.get("input_symbols", [])) == set(symbols),
             "STALE_DATA", "sealed quote gate identity differs")
    gate_rows = gate.get("rows")
    _require(isinstance(gate_rows, list) and len(gate_rows) == len(symbols)
             and all(isinstance(row, dict) for row in gate_rows)
             and {row.get("symbol") for row in gate_rows if isinstance(row, dict)} == set(symbols),
             "STALE_DATA", "sealed quote gate rows differ")
    for row in gate_rows:
        _require(row.get("execution_eligible") is True, "NOT_READY",
                 "a sealed order symbol is not quote eligible")
    _require(set(gate.get("output_symbols", [])) >= set(symbols), "NOT_READY",
             "sealed quote gate did not pass every required symbol")
    _require(gate.get("meta", {}).get("snapshot_id") == request["snapshot_id"],
             "STALE_DATA", "quote gate snapshot differs")
    return {
        "check_id": check_id,
        "request_hash": quote_request_hash,
        "response_hash": quote_response_hash,
        "observation_hash": file_hash(quote_dir / "observation.json"),
        "gate_hash": file_hash(confined_path(root, f"portfolio-attempts/{request['draft_id']}/gate.json")),
        "observed_at": observation.get("result", {}).get("meta", {}).get("observed_at"),
        "benchmark_symbol": saved_request.get("benchmark_symbol"),
    }


def _row_index(result: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = result.get("rows")
    _require(isinstance(rows, list), "STALE_DATA", "S6 rows are missing")
    indexed: dict[str, dict[str, Any]] = {}
    for row in rows:
        _require(isinstance(row, dict), "STALE_DATA", "S6 row is invalid")
        symbol = row.get("symbol")
        try:
            CanonicalSymbol.from_core(symbol)
        except (TypeError, ValueError) as exc:
            raise PipelineError("STALE_DATA", "S6 row identity is invalid") from exc
        _require(symbol not in indexed, "STALE_DATA", "S6 contains duplicate symbols")
        indexed[symbol] = row
    _require(result.get("input_symbols") == list(indexed), "STALE_DATA",
             "S6 row order differs from its input order")
    return indexed


def _validate_seal(repository: Any) -> dict[str, Any]:
    root = Path(repository.directory).resolve()
    inputs, config, context_obj, stages = _inputs(repository)
    s55 = repository.load_stage("S5_5")
    s6 = repository.load_stage("S6")
    _require(s55 is not None and s6 is not None and s6.get("status") == "SUCCESS",
             "NOT_READY", "a committed S5.5/S6 seal is required")
    seal, seal_file_hash = _read_sealed(root / "portfolio-seal.json", label="portfolio seal")
    expected_seal_fields = {"directory", "account_hash", "check_id", "status_id", "fee_hash",
                            "trading_metadata_hash", "files"}
    _require(set(seal) == expected_seal_fields, "STALE_DATA", "portfolio seal schema is invalid")
    directory_text = seal.get("directory")
    _require(isinstance(directory_text, str), "STALE_DATA", "portfolio seal directory is invalid")
    directory_text = directory_text.replace("\\", "/")
    parts = directory_text.split("/")
    _require(len(parts) == 2 and parts[0] == "portfolio-attempts", "STALE_DATA",
             "portfolio seal directory is outside portfolio attempts")
    attempt_id = _identifier(parts[1], "portfolio attempt ID")
    directory = confined_path(root, directory_text)
    _require(directory.is_dir() and not directory.is_symlink(), "STALE_DATA",
             "portfolio attempt directory is unavailable")
    _validate_attempt_files(root, seal, directory)
    request = _read(directory / "request.json", expected_hash=seal["files"]["request.json"], label="portfolio request")
    account_payload = _read(directory / "account.json", expected_hash=seal["files"]["account.json"], label="sealed account")
    fees_payload = _read(directory / "fees.json", expected_hash=seal["files"]["fees.json"], label="sealed fee schedule")
    metadata = _read(directory / "trading-metadata.json", expected_hash=seal["files"]["trading-metadata.json"], label="trading metadata")
    holding_consumption = _read(directory / "holding-review-consumption.json",
                                expected_hash=seal["files"]["holding-review-consumption.json"],
                                label="holding review consumption")
    quote_response = _read(directory / "quote-response.json", expected_hash=seal["files"]["quote-response.json"],
                           label="sealed quote response")
    gate = _read(directory / "gate.json", expected_hash=seal["files"]["gate.json"], label="sealed quote gate")
    attempt_result = _read(directory / "result.json", expected_hash=seal["files"]["result.json"], label="sealed S6 result")
    _require(isinstance(request, dict) and isinstance(metadata, dict) and isinstance(holding_consumption, dict),
             "STALE_DATA", "portfolio attempt inputs are invalid")
    try:
        account = AccountSnapshot.from_dict(account_payload)
        fees = FeeSchedule.from_dict(fees_payload)
    except (TypeError, ValueError) as exc:
        raise PipelineError("STALE_DATA", "sealed account or fee schedule is invalid") from exc
    _require(seal["account_hash"] == account.snapshot_hash, "STALE_DATA", "portfolio seal account hash mismatch")
    _require(seal["fee_hash"] == content_hash(fees.to_dict()), "STALE_DATA", "portfolio seal fee hash mismatch")
    _require(seal["trading_metadata_hash"] == content_hash(metadata), "STALE_DATA",
             "portfolio seal trading metadata hash mismatch")
    _require(request.get("run_id", repository.run_id) == repository.run_id, "STALE_DATA",
             "portfolio request belongs to another run")
    _require(request.get("draft_id") == attempt_id and request.get("check_id") == seal["check_id"]
             and request.get("status_id") == seal["status_id"], "STALE_DATA",
             "portfolio seal/request identity differs")
    stage_hashes = {key: value["artifact_hash"] for key, value in stages.items()}
    _require(request.get("context") == context_obj.to_dict() and request.get("snapshot_id") == inputs["snapshot_id"]
             and request.get("config_hash") == config.config_hash
             and request.get("stage_hashes") == stage_hashes,
             "STALE_DATA", "portfolio request differs from frozen research")
    _require(request.get("code_hashes") == __import__("competition_core.pipeline.portfolio_runner", fromlist=["_code"])._code(),
             "STALE_DATA", "portfolio implementation hash changed")
    _require(request.get("holding_review_consumption_hash") == content_hash(holding_consumption)
             and holding_consumption.get("account_hash") == account.snapshot_hash
             and holding_consumption.get("account_source_hash") == request.get("account_source_hash")
             and holding_consumption.get("portfolio_authority") is False
             and holding_consumption.get("blocking_reasons") == [],
             "STALE_DATA", "holding review consumption is not sealed for this account")
    portfolio = _read(root / "portfolio.json", label="published portfolio")
    _require(isinstance(portfolio, dict), "STALE_DATA", "published portfolio is invalid")
    _require(not _has_terminal_shadow(portfolio) and not _has_terminal_shadow(request), "NOT_READY",
             "terminal shadow portfolio cannot create an execution plan")
    _require(portfolio.get("stage") == "S6" and portfolio.get("portfolio_status") == "DRAFT_READY"
             and portfolio.get("execution_ready") is True and portfolio.get("production_accepted") is False
             and portfolio.get("execution_mode") == "MANUAL_CONFIRM", "NOT_READY",
             "portfolio is not a sealed manual-confirm draft")
    semantic = {key: s6[key] for key in s6.get("semantic_keys", []) if key in s6}
    _require(content_hash(portfolio) == s6.get("semantic_hash") and portfolio == semantic,
             "STALE_DATA", "published portfolio differs from sealed S6")
    _require({key: value for key, value in portfolio.items() if key != "source_provenance"}
             == attempt_result, "STALE_DATA", "S6 attempt result differs from published portfolio")
    _require(portfolio.get("summary", {}).get("account_hash") == account.snapshot_hash
             and portfolio.get("summary", {}).get("fee_schedule_hash") == content_hash(fees.to_dict()),
             "STALE_DATA", "S6 summary is not bound to account and fees")
    _require(portfolio.get("risk_checks", {}).get("blocking_reasons") == []
             and portfolio.get("risk_checks", {}).get("constraints_relaxed") is False
             and portfolio.get("policy", {}).get("sell_proceeds_fund_buys") is False,
             "NOT_READY", "S6 has unresolved risk blockers")
    account_source_hash, source_receipt_hash = _validate_account_import(root, request, account, fees)
    context = context_obj.to_dict()
    news = _validate_news(root, stages, context)
    rows = _row_index(portfolio)
    symbols = list(portfolio.get("input_symbols", []))
    _require(set(symbols) == set(rows), "STALE_DATA", "portfolio symbols differ from quote scope")
    status = _validate_status(root, request, account, metadata, stages, context, stage_hashes, symbols)
    quote = _validate_quotes(root, request, account, gate, quote_response, context, symbols)
    _require(set(metadata) == set(rows), "STALE_DATA", "trading metadata symbols differ from portfolio")
    return {
        "root": root,
        "inputs": inputs,
        "config": config,
        "context": context_obj,
        "stages": stages,
        "stage_hashes": stage_hashes,
        "s6": s6,
        "seal": seal,
        "seal_file_hash": seal_file_hash,
        "attempt_id": attempt_id,
        "directory": directory,
        "request": request,
        "portfolio": portfolio,
        "account": account,
        "fees": fees,
        "metadata": metadata,
        "gate": gate,
        "news": news,
        "status": status,
        "quote": quote,
        "account_source_hash": account_source_hash,
        "source_receipt_hash": source_receipt_hash,
        "rows": rows,
    }


def _validate_price(value: Any, *, field: str, rules: QuantityRules,
                    metadata: dict[str, Any]) -> Decimal:
    price = _decimal(value, field, positive=True)
    _require(price % rules.tick_size == 0, "STALE_DATA", f"{field} is off tick")
    lower = _decimal(metadata.get("down_limit"), "down_limit", positive=True)
    upper = _decimal(metadata.get("up_limit"), "up_limit", positive=True)
    _require(lower <= price <= upper, "NOT_READY", f"{field} is outside the verified price limits")
    return price


def _build_orders(bound: dict[str, Any], plan_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    portfolio = bound["portfolio"]
    account: AccountSnapshot = bound["account"]
    fees: FeeSchedule = bound["fees"]
    context = bound["context"]
    metadata = bound["metadata"]
    rows = bound["rows"]
    positions = {position.symbol: position for position in account.positions}
    ordered_symbols = list(portfolio["output_symbols"]) + sorted(set(rows) - set(portfolio["output_symbols"]))
    orders: list[dict[str, Any]] = []
    holds: list[dict[str, Any]] = []
    quantity_hashes: dict[str, str] = {}
    total_fees = Decimal(0)
    total_buy = Decimal(0)
    total_sell_net = Decimal(0)
    cash = account.available_cash
    cash_steps: list[dict[str, Any]] = []
    risk_checks = portfolio["risk_checks"]
    gate_rows = {row["symbol"]: row for row in bound["gate"]["rows"]}
    for symbol in ordered_symbols:
        row = rows[symbol]
        current = row.get("current_quantity")
        sellable = row.get("sellable_quantity")
        delta = row.get("delta_quantity")
        target = row.get("target_quantity")
        if target is None and delta is None:
            _require(row.get("selected") is False and current == 0 and sellable == 0
                     and symbol not in portfolio["output_symbols"], "STALE_DATA",
                     f"inactive S6 row is inconsistent for {symbol}")
            continue
        _require(type(current) is int and type(sellable) is int and type(delta) is int and type(target) is int,
                 "STALE_DATA", f"S6 quantities are invalid for {symbol}")
        position = positions.get(symbol)
        expected_current = position.quantity if position else 0
        expected_sellable = position.sellable_quantity if position else 0
        _require((current, sellable) == (expected_current, expected_sellable), "STALE_DATA",
                 f"S6 account quantities differ for {symbol}")
        _require(target == current + delta and target >= 0, "STALE_DATA",
                 f"S6 target/delta relation differs for {symbol}")
        rules_payload = row.get("quantity_rules")
        _require(isinstance(rules_payload, dict), "NOT_READY", f"quantity rules are missing for {symbol}")
        try:
            rules = quantity_rules(symbol, context.research_date)
        except (TypeError, ValueError, PipelineError) as exc:
            raise PipelineError("NOT_READY", f"quantity rules are unavailable for {symbol}") from exc
        _require(rules_payload == rules.to_dict(), "STALE_DATA", f"quantity rules changed for {symbol}")
        rules_hash = content_hash(rules_payload)
        quantity_hashes[symbol] = rules_hash
        side = row.get("side")
        expected_side = "BUY" if delta > 0 else "SELL" if delta < 0 else "HOLD"
        _require(side == expected_side, "STALE_DATA", f"S6 side does not match delta for {symbol}")
        if delta == 0:
            _require(row.get("order_price") is None and row.get("gross_buy_cash") == "0.00"
                     and row.get("gross_sell_proceeds") == "0.00" and row.get("estimated_fees") == "0.00",
                     "STALE_DATA", f"zero-delta row is not a HOLD for {symbol}")
            holds.append({"symbol": symbol, "quantity": current, "target_quantity": target,
                          "reason": row.get("holding_disposition") or "NO_REBALANCE"})
            continue
        quantity = abs(delta)
        _require(rules.is_valid_quantity(side, quantity, current, sellable, "LIMIT"), "NOT_READY",
                 f"quantity rules reject the sealed {side} for {symbol}")
        gate_row = gate_rows.get(symbol)
        _require(isinstance(gate_row, dict) and gate_row.get("execution_eligible") is True
                 and gate_row.get("realtime_gate") == "PASS"
                 and row.get("realtime_gate") == gate_row.get("realtime_gate"),
                 "STALE_DATA", f"S6 realtime gate differs for {symbol}")
        _require(isinstance(gate_row.get("quote"), dict)
                 and _decimal(gate_row["quote"].get("price"), "quote price", positive=True)
                 == _decimal(row.get("reference_price"), "reference_price", positive=True),
                 "STALE_DATA", f"S6 reference price differs from the quote for {symbol}")
        reference = _validate_price(row.get("reference_price"), field="reference_price",
                                    rules=rules, metadata=metadata[symbol])
        limit = _validate_price(row.get("order_price"), field="order_price",
                                rules=rules, metadata=metadata[symbol])
        gross = (limit * quantity).quantize(Decimal("0.01"))
        fee = fees.charge(side, gross)
        _require(row.get("gross_buy_cash") == (format(gross, "f") if side == "BUY" else "0.00")
                 and row.get("gross_sell_proceeds") == (format(gross, "f") if side == "SELL" else "0.00"),
                 "STALE_DATA", f"S6 gross value differs for {symbol}")
        _require(row.get("estimated_fees") == format(fee, "f"), "STALE_DATA",
                 f"S6 fee differs for {symbol}")
        local = {"symbol": symbol, "side": side, "quantity": quantity,
                 "current_quantity": current, "sellable_quantity": sellable, "target_quantity": target,
                 "delta_quantity": delta, "order_type": "LIMIT", "limit_price": format(limit, "f"),
                 "reference_price": format(reference, "f"), "gross_value": format(gross, "f"),
                 "estimated_fee": format(fee, "f"), "quantity_rules": rules_payload,
                 "quantity_rules_hash": rules_hash,
                 "price_limits": {"down_limit": format(_decimal(metadata[symbol]["down_limit"], "down_limit"), "f"),
                                  "up_limit": format(_decimal(metadata[symbol]["up_limit"], "up_limit"), "f")}}
        local["order_ref"] = f"{plan_id}-order-{len(orders) + 1:03d}"
        orders.append(local)
        total_fees += fee
        if side == "BUY":
            worst = gross + fee
            cash -= worst
            total_buy += worst
            _require(cash >= 0, "NOT_READY", "sealed buy sequence is not cash feasible")
        else:
            net = gross - fee
            total_sell_net += net
            worst = Decimal(0)
        cash_steps.append({"order_ref": local["order_ref"], "side": side,
                           "cash_reserved": format(worst, "f"), "available_cash_after": format(cash, "f"),
                           "sell_proceeds_available_before_fill": "0.00"})
    _require(_money(risk_checks.get("required_buy_cash"), "required_buy_cash") == _money(total_buy, "total_buy"),
             "STALE_DATA", "S6 required buy cash differs from order plan")
    _require(_money(risk_checks.get("estimated_fees"), "estimated_fees") == _money(total_fees, "total_fees"),
             "STALE_DATA", "S6 estimated fees differ from order plan")
    _require(_money(risk_checks.get("projected_net_sell_proceeds"), "projected_net_sell_proceeds")
             == _money(total_sell_net, "total_sell_net"), "STALE_DATA", "S6 sell proceeds differ from order plan")
    _require(risk_checks.get("unfilled_sale_proceeds_spent") == "0.00", "NOT_READY",
             "S6 uses unfilled sale proceeds")
    reserve = _decimal(risk_checks.get("cash_reserve"), "cash_reserve")
    _require(account.cash_balance - total_buy >= reserve, "NOT_READY", "order plan violates the cash buffer")
    _require(account.available_cash - total_buy >= 0, "NOT_READY", "order plan exceeds available cash")
    economics = {
        "starting_cash_balance": format(account.cash_balance, "f"),
        "starting_available_cash": format(account.available_cash, "f"),
        "cash_buffer": format(reserve, "f"),
        "buy_worst_case_cash": _money(total_buy, "total_buy"),
        "sell_net_proceeds_projection": _money(total_sell_net, "total_sell_net"),
        "estimated_fees": _money(total_fees, "total_fees"),
        "available_cash_after_buy_orders": _money(account.available_cash - total_buy, "available_cash_after"),
        "cash_balance_after_all_planned_fills": _money(account.cash_balance - total_buy + total_sell_net,
                                                        "cash_balance_after"),
        "unfilled_sale_proceeds_used": False,
        "cash_steps": cash_steps,
    }
    return orders, holds, {"by_symbol": quantity_hashes, "economics": economics}


def _plan_record(bound: dict[str, Any], plan_id: str) -> dict[str, Any]:
    portfolio = bound["portfolio"]
    request = bound["request"]
    account: AccountSnapshot = bound["account"]
    fees: FeeSchedule = bound["fees"]
    _require(account.pending_order_count == 0, "NOT_READY",
             "pending orders require reconciliation before plan creation")
    orders, holds, quantity = _build_orders(bound, plan_id)
    body: dict[str, Any] = {
        "schema_version": EXECUTION_PLAN_SCHEMA_VERSION,
        "bridge_version": BRIDGE_VERSION,
        "origin": "competition_s6",
        "run_id": bound["root"].name,
        "plan_id": plan_id,
        "generated_at": request["observed_at"],
        "research_only": True,
        "external_write_allowed": False,
        "production_accepted": False,
        "orders_submitted": 0,
        "source": {
            "portfolio_seal_hash": bound["seal_file_hash"],
            "portfolio_attempt_id": bound["attempt_id"],
            "portfolio_attempt_directory": _relative(bound["root"], bound["directory"]),
            "s6_artifact_hash": bound["s6"]["artifact_hash"],
            "s6_semantic_hash": bound["s6"]["semantic_hash"],
            "portfolio_file_hash": file_hash(bound["root"] / "portfolio.json"),
        },
        "research": {
            "context": request["context"],
            "snapshot_id": request["snapshot_id"],
            "config_hash": request["config_hash"],
            "stage_hashes": bound["stage_hashes"],
            "review_set": bound["stages"]["S5"]["input_symbols"],
            "review_set_hash": content_hash(bound["stages"]["S5"]["input_symbols"]),
            "news_context_hash": bound["news"]["snapshot_hash"],
            "news_context_file_hash": bound["news"]["file_hash"],
            "review_inputs_hash": bound["news"]["review_inputs_hash"],
        },
        "account": {
            "account_key": f"account-{bound['account_source_hash'][:32]}",
            "snapshot_hash": account.snapshot_hash,
            "source_hash": bound["account_source_hash"],
            "source_receipt_hash": bound["source_receipt_hash"],
            "source_kind": account.source_kind,
            "as_of": account.as_of.isoformat(),
            "currency": account.currency,
            "cash_balance": format(account.cash_balance, "f"),
            "available_cash": format(account.available_cash, "f"),
            "current_total_asset": format(account.current_total_asset, "f"),
            "positions": [position.to_dict() for position in account.positions],
            "pending_order_count": account.pending_order_count,
            "activity_fingerprint": content_hash({"snapshot_hash": account.snapshot_hash,
                                                    "pending_order_count": account.pending_order_count}),
        },
        "market": {
            "quote_observation": bound["quote"],
            "execution_status": bound["status"],
            "trading_metadata_hash": content_hash(bound["metadata"]),
            "fee_schedule_hash": content_hash(fees.to_dict()),
        },
        "fee_schedule": fees.to_dict(),
        "quantity_rules": quantity["by_symbol"],
        "orders": orders,
        "holds": holds,
        "economics": quantity["economics"],
        "approval": {
            "state": "NOT_APPROVED",
            "required": True,
            "plan_id": plan_id,
            "plan_hash": None,
            "approved_at": None,
            "expires_at": None,
        },
    }
    unsigned_hash = content_hash(body)
    body["approval"]["plan_hash"] = unsigned_hash
    unsigned = body | {"plan_hash": unsigned_hash}
    return unsigned | {"integrity_hash": content_hash(unsigned)}


def _validate_record(repository: Any, record: dict[str, Any], *, plan_id: str | None = None) -> dict[str, Any]:
    _require(isinstance(record, dict), "STALE_DATA", "execution plan is not an object")
    stored_integrity = record.get("integrity_hash")
    unsigned = {key: value for key, value in record.items() if key != "integrity_hash"}
    _require(stored_integrity == content_hash(unsigned), "STALE_DATA", "execution plan integrity hash mismatch")
    stored_plan_hash = unsigned.get("plan_hash")
    body = {key: value for key, value in unsigned.items() if key != "plan_hash"}
    approval = body.get("approval")
    _require(isinstance(approval, dict) and approval.get("plan_hash") == stored_plan_hash,
             "STALE_DATA", "execution plan approval reference is invalid")
    approval["plan_hash"] = None
    _require(stored_plan_hash == content_hash(body), "STALE_DATA", "execution plan hash mismatch")
    approval["plan_hash"] = stored_plan_hash
    _require(record.get("schema_version") == EXECUTION_PLAN_SCHEMA_VERSION
             and record.get("origin") == "competition_s6"
             and record.get("research_only") is True
             and record.get("external_write_allowed") is False
             and record.get("production_accepted") is False
             and record.get("orders_submitted") == 0,
             "STALE_DATA", "execution plan safety boundary is invalid")
    if plan_id is not None:
        _require(record.get("plan_id") == plan_id, "STALE_DATA", "execution plan ID differs")
    expected = _plan_record(_validate_seal(repository), record["plan_id"])
    _require(record == expected, "STALE_DATA", "execution plan differs from sealed S6 inputs")
    return record


def create_execution_plan(repository: Any, *, plan_id: str | None = None) -> dict[str, Any]:
    """Create or return the one immutable research plan for a sealed run.

    Repeating this call with the same sealed inputs returns the same bytes.  A
    plan is never approval, current-readiness, or a broker action.
    """

    with repository.lock():
        bound = _validate_seal(repository)
        default_id = f"plan-{bound['seal_file_hash']}"
        chosen_id = _identifier(plan_id, "plan_id") if plan_id is not None else default_id
        path = bound["root"] / PLAN_PATH
        record = _plan_record(bound, chosen_id)
        if path.exists():
            existing = _read(path, label="execution plan")
            _require(existing.get("plan_id") == chosen_id, "STALE_DATA",
                     "this run already has a different execution plan")
            return _validate_record(repository, existing, plan_id=chosen_id)
        atomic_json(path, record, immutable=True)
        return _validate_record(repository, _read(path, label="execution plan"), plan_id=chosen_id)


def read_execution_plan(repository: Any, *, plan_id: str | None = None) -> dict[str, Any]:
    """Read and revalidate the immutable plan without changing the run."""

    path = Path(repository.directory).resolve() / PLAN_PATH
    record = _read(path, label="execution plan")
    return _validate_record(repository, record, plan_id=plan_id)


# The build name is useful to callers that treat plan creation as a pure
# bridge operation; it intentionally has the same idempotent semantics.
build_execution_plan = create_execution_plan


__all__ = [
    "BRIDGE_VERSION",
    "EXECUTION_PLAN_SCHEMA_VERSION",
    "PLAN_PATH",
    "build_execution_plan",
    "create_execution_plan",
    "read_execution_plan",
]
