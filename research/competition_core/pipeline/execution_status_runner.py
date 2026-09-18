"""Observe execution references for exactly the frozen finalists/current holdings."""

from datetime import datetime
import re
import uuid

from competition_core.pipeline.artifacts import atomic_json, content_hash, file_hash, read_json
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.execution_status import collect_execution_status, load_execution_status
from competition_core.pipeline.realtime import _inputs
from competition_core.pipeline.time_context import SHANGHAI


def _status_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,95}", value):
        raise PipelineError("INVALID_ARGUMENTS", "invalid execution status identifier")
    return value


def _scope(stages, account):
    from competition_core.pipeline.account import AccountSnapshot
    if account is not None and not isinstance(account, AccountSnapshot):
        raise PipelineError("INVALID_ACCOUNT_STATE", "validated account snapshot required")
    symbols = list(stages["S4"]["output_symbols"])
    if account is not None:
        symbols += sorted({p.symbol for p in account.positions} - set(symbols))
    return symbols


def run_execution_status_check(*, repository, client, source_name, account=None,
                               status_id=None, clock=None):
    status_id = _status_id(status_id or f"status-{uuid.uuid4().hex[:20]}")
    clock = clock or (lambda: datetime.now(SHANGHAI))
    with repository.lock():
        inputs, _, context, stages = _inputs(repository)
        if repository.load_stage("S6") is not None or (repository.directory / "portfolio-seal.json").exists():
            raise PipelineError("NOT_READY", "sealed portfolio cannot replace its execution references")
        symbols = _scope(stages, account)
        identities = {r["symbol"]: r.get("identity", {}) for r in stages["S0"]["rows"]}
        from contracts.symbol import CanonicalSymbol
        # Board special-period metadata is frozen in the original S0 identity,
        # not read from mutable global config during a resumed research run.
        rules = {}
        for symbol in symbols:
            identity = identities.get(symbol, {})
            value = identity.get("new_listing_special_sessions")
            if type(value) is not int or not 0 <= value <= 30:
                raise PipelineError("NOT_READY", "held/candidate identity lacks frozen special-period metadata", symbol=symbol)
            board = CanonicalSymbol.from_core(symbol).board
            if board in rules and rules[board] != value:
                raise PipelineError("STALE_DATA", "inconsistent frozen board metadata")
            rules[board] = value
        # Unrequested boards are unused by the collector; preserve only their
        # existing S0 metadata rather than inventing rules to satisfy a schema.
        for symbol, identity in identities.items():
            board = CanonicalSymbol.from_core(symbol).board
            value = identity.get("new_listing_special_sessions")
            if board not in rules and type(value) is int and 0 <= value <= 30:
                rules[board] = value
        directory = repository.directory / "execution-status" / status_id
        result = collect_execution_status(client=client, symbols=symbols, context=context,
            special_sessions_by_board=rules, directory=directory, source_name=source_name, clock=clock)
        binding = {"run_id": repository.run_id, "status_id": status_id,
            "context": context.to_dict(), "snapshot_id": inputs["snapshot_id"], "config_hash": inputs["config_hash"],
            "stage_hashes": {k: v["artifact_hash"] for k, v in stages.items()},
            "account_hash": account.snapshot_hash if account else None,
            "observation_hash": file_hash(directory / "observation.json")}
        atomic_json(directory / "binding.json", binding | {"integrity_hash": content_hash(binding)}, immutable=True)
        repository.update(last_execution_status=status_id, execution_ready=False, production_accepted=False)
        return result | {"run_id": repository.run_id, "status_id": status_id}


def read_run_execution_status(*, repository, status_id, account, observed_at):
    status_id = _status_id(status_id)
    inputs, config, context, stages = _inputs(repository)
    directory = repository.directory / "execution-status" / status_id
    binding = read_json(directory / "binding.json")
    if binding.pop("integrity_hash", None) != content_hash(binding):
        raise PipelineError("STALE_DATA", "execution status binding hash mismatch")
    if (binding["run_id"] != repository.run_id or binding["status_id"] != status_id
            or binding["context"] != context.to_dict() or binding["snapshot_id"] != inputs["snapshot_id"]
            or binding["config_hash"] != config.config_hash
            or binding["stage_hashes"] != {k: v["artifact_hash"] for k, v in stages.items()}
            or binding["account_hash"] != (account.snapshot_hash if account else None)
            or file_hash(directory / "observation.json") != binding["observation_hash"]):
        raise PipelineError("STALE_DATA", "execution status does not match frozen research/account")
    result = load_execution_status(directory, context=context, symbols=_scope(stages, account),
                                  observed_at=observed_at, max_age_seconds=config.section("realtime")["max_quote_age_seconds"])
    names = ["request.json", "observation.json", "binding.json"] + [item["path"] for item in result["source_provenance"]]
    proofs = [{"path": f"execution-status/{status_id}/{name}", "sha256": file_hash(directory / name)} for name in names]
    return result, proofs
