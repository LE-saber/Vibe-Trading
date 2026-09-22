"""Immutable local persistence for read-only terminal evaluations.

The persistence boundary is intentionally boring: canonical JSON is written
with the existing immutable artifact primitive, the manifest is written last,
and reads require an exact file set plus a hash for every artifact.  There is
no database, network client, approval path, or broker capability here.
"""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any, Mapping

from competition_core.pipeline.artifacts import (
    atomic_json,
    content_hash,
    file_hash,
    json_bytes,
    read_json,
)
from competition_core.pipeline.errors import PipelineError

from .service import TerminalEvaluationResult
from .models import TerminalModelError


_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}\Z")
_HASH = re.compile(r"(?:sha256:)?[0-9a-fA-F]{64}\Z")
_ARTIFACT_NAMES = (
    "terminal-snapshot.json",
    "award-line-history.json",
    "award-line-forecast.json",
    "mode-decision.json",
    "candidates-watch.json",
    "candidates-focus.json",
    "candidates-terminal.json",
    "allocation-policy.json",
    "source-hashes.json",
)
_ARTIFACT_SET = frozenset(_ARTIFACT_NAMES)


class TerminalPersistenceError(TerminalModelError):
    """Raised when an evaluation artifact is unsafe, incomplete, or changed."""


class TerminalEvaluationPersistence:
    """Store and verify ``runs/<run_id>/terminal/<evaluation_id>/`` artifacts.

    ``root`` may be either a runs directory itself or a parent directory.  A
    path whose final component is ``runs`` is treated as the runs directory;
    otherwise a ``runs`` child is used.  This keeps the public API convenient
    for both an application runs root and a temporary test root.
    """

    def __init__(self, root: str | Path) -> None:
        if not isinstance(root, (str, Path)):
            raise TerminalPersistenceError("persistence root must be a path")
        supplied = Path(root)
        _reject_symlink_ancestors(supplied)
        if supplied.name == "runs":
            runs_root = supplied
        else:
            runs_root = supplied / "runs"
        self.runs_root = runs_root.resolve()

    def path_for(self, run_id: str, evaluation_id: str, *, create: bool = False) -> Path:
        _safe_component(run_id, "run_id")
        _safe_component(evaluation_id, "evaluation_id")
        if create:
            directory = self._create_directory(run_id, evaluation_id)
        else:
            directory = self._existing_directory(run_id, evaluation_id)
        return directory

    def persist(self, result: TerminalEvaluationResult) -> Path:
        """Persist one result idempotently, writing the manifest last."""

        if not isinstance(result, TerminalEvaluationResult):
            raise TerminalPersistenceError("result must be TerminalEvaluationResult")
        directory = self.path_for(result.run_id, result.evaluation_id, create=True)
        manifest_path = directory / "decision-manifest.json"
        if manifest_path.exists():
            # A complete existing directory is the idempotent fast path.  A
            # partial/tampered directory is never silently repaired.
            try:
                loaded = self.load(result.run_id, result.evaluation_id)
            except TerminalPersistenceError:
                raise
            if loaded["manifest"].get("result_hash") != result.result_hash:
                raise TerminalPersistenceError("immutable evaluation identity has conflicting content")
            return directory

        payloads = _artifact_payloads(result)
        try:
            for name in _ARTIFACT_NAMES:
                _write_immutable(directory / name, payloads[name])
            artifact_hashes = {
                name: file_hash(directory / name)
                for name in _ARTIFACT_NAMES
            }
            manifest = _manifest_payload(result, artifact_hashes)
            manifest["manifest_hash"] = content_hash(manifest)
            _write_immutable(manifest_path, manifest)
        except TerminalPersistenceError:
            raise
        except (OSError, PipelineError, TypeError, ValueError) as exc:
            raise TerminalPersistenceError("terminal evaluation persistence failed") from exc
        # Verify the just-created directory before reporting success.  In
        # particular this catches a missing/partial manifest immediately.
        self.load(result.run_id, result.evaluation_id)
        return directory

    save = persist

    def load(self, run_id: str, evaluation_id: str) -> dict[str, Any]:
        """Load and verify one complete immutable evaluation directory."""

        directory = self.path_for(run_id, evaluation_id, create=False)
        try:
            entries = {item.name for item in directory.iterdir()}
        except OSError as exc:
            raise TerminalPersistenceError("terminal evaluation directory is unavailable") from exc
        expected = _ARTIFACT_SET | {"decision-manifest.json"}
        if entries != expected:
            raise TerminalPersistenceError("terminal evaluation file set is incomplete or has extras")
        for name in expected:
            path = directory / name
            if path.is_symlink() or not path.is_file():
                raise TerminalPersistenceError(f"terminal evaluation artifact is not a regular file: {name}")

        manifest_path = directory / "decision-manifest.json"
        manifest = _read_canonical(manifest_path, "decision manifest")
        if not isinstance(manifest, dict):
            raise TerminalPersistenceError("decision manifest must be an object")
        stored_manifest_hash = manifest.pop("manifest_hash", None)
        if not _valid_hash(stored_manifest_hash):
            raise TerminalPersistenceError("decision manifest hash is missing or invalid")
        if content_hash(manifest) != stored_manifest_hash:
            raise TerminalPersistenceError("decision manifest hash mismatch")
        _validate_manifest_header(manifest, run_id, evaluation_id)

        artifact_hashes = manifest.get("artifact_hashes")
        if not isinstance(artifact_hashes, dict) or set(artifact_hashes) != _ARTIFACT_SET:
            raise TerminalPersistenceError("decision manifest artifact set is invalid")
        artifacts: dict[str, Any] = {}
        for name in _ARTIFACT_NAMES:
            digest = artifact_hashes.get(name)
            if not _valid_hash(digest):
                raise TerminalPersistenceError(f"artifact hash is missing or invalid: {name}")
            path = directory / name
            if file_hash(path) != digest:
                raise TerminalPersistenceError(f"artifact hash mismatch: {name}")
            artifacts[name] = _read_canonical(path, name)

        source_payload = artifacts["source-hashes.json"]
        _validate_source_hashes(source_payload, manifest)
        _validate_cross_artifacts(artifacts, manifest)
        return {
            "path": directory,
            "manifest": manifest | {"manifest_hash": stored_manifest_hash},
            "artifacts": artifacts,
            "source_hashes": source_payload,
        }

    read = load
    verify = load
    load_evaluation = load
    read_evaluation = load

    def _create_directory(self, run_id: str, evaluation_id: str) -> Path:
        _ensure_directory(self.runs_root, create=True)
        run_directory = self.runs_root / run_id
        _ensure_directory(run_directory, create=True)
        terminal_directory = run_directory / "terminal"
        _ensure_directory(terminal_directory, create=True)
        evaluation_directory = terminal_directory / evaluation_id
        _ensure_directory(evaluation_directory, create=True)
        return evaluation_directory

    def _existing_directory(self, run_id: str, evaluation_id: str) -> Path:
        _ensure_directory(self.runs_root, create=False)
        run_directory = self.runs_root / run_id
        _ensure_directory(run_directory, create=False)
        terminal_directory = run_directory / "terminal"
        _ensure_directory(terminal_directory, create=False)
        evaluation_directory = terminal_directory / evaluation_id
        _ensure_directory(evaluation_directory, create=False)
        return evaluation_directory


TerminalEvaluationStore = TerminalEvaluationPersistence
TerminalPersistence = TerminalEvaluationPersistence
TerminalStrategyPersistence = TerminalEvaluationPersistence


def persist_terminal_evaluation(result: TerminalEvaluationResult, root: str | Path) -> Path:
    return TerminalEvaluationPersistence(root).persist(result)


def load_terminal_evaluation(root: str | Path, run_id: str, evaluation_id: str) -> dict[str, Any]:
    return TerminalEvaluationPersistence(root).load(run_id, evaluation_id)


def _artifact_payloads(result: TerminalEvaluationResult) -> dict[str, dict[str, Any]]:
    funnel = result.funnel
    source = result.source_hashes.to_dict()
    source["run_id"] = result.run_id
    source["evaluation_id"] = result.evaluation_id
    source["externally_hash_bound"] = [
        "account_snapshot_hash",
        "leaderboard_snapshot_hash",
    ]
    allocation = (
        result.allocation_policy.to_dict()
        if result.allocation_policy is not None
        else {
            "schema_version": "competition.terminal-allocation-policy-result.v1",
            "available": False,
            "policy": None,
            "blockers": list(result.blockers),
            "reasons": list(result.reasons),
            "execution_ready": False,
        }
    )
    return {
        "terminal-snapshot.json": result.snapshot.to_dict(),
        "award-line-history.json": {
            "schema_version": "competition.award-line-history.v1",
            "points": [item.to_dict() for item in result.award_line_history],
            "execution_ready": False,
        },
        "award-line-forecast.json": result.forecast.to_dict(),
        "mode-decision.json": result.mode_decision.to_dict(),
        "candidates-watch.json": _tier_payload("watch", result),
        "candidates-focus.json": _tier_payload("focus", result),
        "candidates-terminal.json": _tier_payload("terminal", result),
        "allocation-policy.json": allocation,
        "source-hashes.json": source,
    }


def _tier_payload(tier: str, result: TerminalEvaluationResult) -> dict[str, Any]:
    tier_attribute = "watchlist" if tier == "watch" else tier
    items = getattr(result.funnel, tier_attribute)
    return {
        "schema_version": f"competition.terminal-candidates-{tier}.v1",
        "run_id": result.run_id,
        "evaluation_id": result.evaluation_id,
        "mode": result.funnel.mode.value,
        "as_of": result.funnel.as_of.isoformat(),
        "trade_date": result.funnel.trade_date.isoformat(),
        "tier": tier,
        "candidates": [item.to_dict() for item in items],
        "funnel_hash": result.funnel.funnel_hash,
        "input_hash": result.funnel.input_hash,
        "execution_ready": False,
    }


def _manifest_payload(
    result: TerminalEvaluationResult,
    artifact_hashes: Mapping[str, str],
) -> dict[str, Any]:
    return {
        "schema_version": "competition.terminal-decision-manifest.v1",
        "run_id": result.run_id,
        "evaluation_id": result.evaluation_id,
        "request_hash": result.request_hash,
        "result_hash": result.result_hash,
        "status": result.status,
        "complete": True,
        "execution_ready": False,
        "artifact_names": list(_ARTIFACT_NAMES),
        "artifact_hashes": dict(artifact_hashes),
        "source_hashes_hash": result.source_hashes.source_hashes_hash,
        "s4_hash": result.source_hashes.s4_hash,
        "s5_hash": result.source_hashes.s5_hash,
        "s5_5_hash": result.source_hashes.s5_5_hash,
        "account_snapshot_hash": result.source_hashes.account_snapshot_hash,
        "leaderboard_snapshot_hash": result.source_hashes.leaderboard_snapshot_hash,
        "implementation_hash": result.source_hashes.implementation_hash,
        "config_hash": result.config_hash,
        "award_line_forecast_hash": result.forecast.forecast_hash,
        "mode_decision_hash": result.mode_decision.decision_hash,
        "candidate_funnel_hash": result.funnel.funnel_hash,
        "allocation_policy_hash": result.allocation_policy.policy_hash if result.allocation_policy else None,
        "externally_hash_bound": ["account_snapshot_hash", "leaderboard_snapshot_hash"],
        "missing_external_payloads": ["account.json", "leaderboard.json"],
        "blockers": list(result.blockers),
        "reasons": list(result.reasons),
    }


def _validate_manifest_header(manifest: Mapping[str, Any], run_id: str, evaluation_id: str) -> None:
    if manifest.get("schema_version") != "competition.terminal-decision-manifest.v1":
        raise TerminalPersistenceError("unsupported terminal decision manifest")
    if manifest.get("run_id") != run_id or manifest.get("evaluation_id") != evaluation_id:
        raise TerminalPersistenceError("terminal decision manifest identity mismatch")
    for field in (
        "request_hash", "result_hash", "source_hashes_hash", "s4_hash", "s5_hash",
        "s5_5_hash", "account_snapshot_hash", "leaderboard_snapshot_hash",
        "implementation_hash", "config_hash", "award_line_forecast_hash",
        "mode_decision_hash", "candidate_funnel_hash",
    ):
        if not _valid_hash(manifest.get(field)):
            raise TerminalPersistenceError(f"manifest {field} is missing or invalid")
    if manifest.get("status") not in {"COMPLETE", "BLOCKED", "UNAVAILABLE"}:
        raise TerminalPersistenceError("manifest status is invalid")
    if manifest.get("complete") is not True or manifest.get("execution_ready") is not False:
        raise TerminalPersistenceError("terminal decision manifest is not a complete read-only seal")
    if manifest.get("artifact_names") != list(_ARTIFACT_NAMES):
        raise TerminalPersistenceError("manifest artifact names are not canonical")
    if manifest.get("externally_hash_bound") != ["account_snapshot_hash", "leaderboard_snapshot_hash"]:
        raise TerminalPersistenceError("manifest external bindings are invalid")
    if manifest.get("missing_external_payloads") != ["account.json", "leaderboard.json"]:
        raise TerminalPersistenceError("manifest external payload declaration is invalid")


def _validate_source_hashes(payload: Any, manifest: Mapping[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise TerminalPersistenceError("source-hashes artifact must be an object")
    expected = {
        "schema_version", "s4_hash", "s5_hash", "s5_5_hash", "account_snapshot_hash",
        "leaderboard_snapshot_hash", "config_hash", "implementation_hash", "source_hashes_hash",
        "run_id", "evaluation_id", "externally_hash_bound",
    }
    if set(payload) != expected:
        raise TerminalPersistenceError("source-hashes artifact fields are invalid")
    if payload.get("schema_version") != "competition.terminal-source-hashes.v1":
        raise TerminalPersistenceError("source-hashes schema is invalid")
    for field in (
        "s4_hash", "s5_hash", "s5_5_hash", "account_snapshot_hash",
        "leaderboard_snapshot_hash", "config_hash", "implementation_hash",
    ):
        if not _valid_hash(payload.get(field)):
            raise TerminalPersistenceError(f"source hash is missing or invalid: {field}")
    body = {key: payload[key] for key in (
        "schema_version", "s4_hash", "s5_hash", "s5_5_hash", "account_snapshot_hash",
        "leaderboard_snapshot_hash", "config_hash", "implementation_hash",
    )}
    if payload.get("source_hashes_hash") != content_hash(body):
        raise TerminalPersistenceError("source-hashes content hash mismatch")
    if payload.get("config_hash") != manifest.get("config_hash"):
        raise TerminalPersistenceError("source-hashes config binding mismatch")
    for field in (
        "s4_hash", "s5_hash", "s5_5_hash", "account_snapshot_hash",
        "leaderboard_snapshot_hash", "implementation_hash",
    ):
        if payload.get(field) != manifest.get(field):
            raise TerminalPersistenceError(f"source-hashes manifest binding mismatch: {field}")
    if payload.get("externally_hash_bound") != ["account_snapshot_hash", "leaderboard_snapshot_hash"]:
        raise TerminalPersistenceError("source-hashes external bindings are invalid")


def _validate_cross_artifacts(artifacts: Mapping[str, Any], manifest: Mapping[str, Any]) -> None:
    forecast = artifacts["award-line-forecast.json"]
    decision = artifacts["mode-decision.json"]
    for payload, schema, digest_field in (
        (forecast, "competition.award-line-forecast.v1", "award_line_forecast_hash"),
        (decision, "competition.terminal-mode-decision.v1", "mode_decision_hash"),
    ):
        if not isinstance(payload, dict) or payload.get("schema_version") != schema:
            raise TerminalPersistenceError("terminal cross-artifact schema mismatch")
        if content_hash(payload) != manifest.get(digest_field):
            raise TerminalPersistenceError(f"terminal cross-artifact hash mismatch: {digest_field}")
    for name in ("candidates-watch.json", "candidates-focus.json", "candidates-terminal.json"):
        payload = artifacts[name]
        if not isinstance(payload, dict) or payload.get("funnel_hash") != manifest.get("candidate_funnel_hash"):
            raise TerminalPersistenceError(f"candidate artifact funnel binding mismatch: {name}")
        if payload.get("execution_ready") is not False:
            raise TerminalPersistenceError(f"candidate artifact is execution-ready: {name}")
    allocation = artifacts["allocation-policy.json"]
    if not isinstance(allocation, dict) or allocation.get("execution_ready") is not False:
        raise TerminalPersistenceError("allocation artifact is invalid")
    expected_allocation = manifest.get("allocation_policy_hash")
    if expected_allocation is not None:
        if allocation.get("policy_hash") != expected_allocation:
            raise TerminalPersistenceError("allocation policy hash binding mismatch")
    elif allocation.get("available") is not False:
        raise TerminalPersistenceError("unavailable allocation envelope is invalid")


def _write_immutable(path: Path, payload: Mapping[str, Any]) -> None:
    if path.exists() or path.is_symlink():
        if path.is_symlink() or not path.is_file():
            raise TerminalPersistenceError(f"artifact target is not a regular file: {path.name}")
    try:
        atomic_json(path, dict(payload), immutable=True)
    except (OSError, PipelineError, TypeError, ValueError) as exc:
        raise TerminalPersistenceError(f"cannot immutably write artifact: {path.name}") from exc


def _read_canonical(path: Path, label: str) -> Any:
    try:
        raw = path.read_bytes()
        value = read_json(path)
    except (OSError, UnicodeError, ValueError) as exc:
        raise TerminalPersistenceError(f"{label} is not valid JSON") from exc
    if raw != json_bytes(value):
        raise TerminalPersistenceError(f"{label} is not canonical JSON")
    return value


def _ensure_directory(path: Path, *, create: bool) -> None:
    if path.exists() or path.is_symlink():
        if path.is_symlink() or not path.is_dir():
            raise TerminalPersistenceError(f"persistence path is not a regular directory: {path}")
        return
    if not create:
        raise TerminalPersistenceError(f"persistence path is missing: {path}")
    try:
        if not path.parent.exists():
            _ensure_directory(path.parent, create=True)
        path.mkdir()
    except FileExistsError:
        if path.is_symlink() or not path.is_dir():
            raise TerminalPersistenceError(f"persistence path is unsafe: {path}")
    except OSError as exc:
            raise TerminalPersistenceError(f"cannot create persistence path: {path}") from exc


def _reject_symlink_ancestors(path: Path) -> None:
    """Reject an existing symlink anywhere in the user-selected root path."""

    current = path
    while True:
        if current.is_symlink():
            raise TerminalPersistenceError("persistence root contains a symlink")
        parent = current.parent
        if parent == current:
            return
        current = parent


def _safe_component(value: Any, field: str) -> None:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None or ".." in value:
        raise TerminalPersistenceError(f"{field} is invalid")


def _valid_hash(value: Any) -> bool:
    return isinstance(value, str) and _HASH.fullmatch(value) is not None


__all__ = [
    "TerminalEvaluationPersistence",
    "TerminalEvaluationStore",
    "TerminalPersistence",
    "TerminalPersistenceError",
    "TerminalStrategyPersistence",
    "load_terminal_evaluation",
    "persist_terminal_evaluation",
]
