from dataclasses import replace
import importlib
import json

import pytest

from competition_core.terminal_strategy import (
    TerminalEvaluationPersistence,
    TerminalPersistenceError,
    TerminalStrategyConfig,
    TerminalStrategyService,
)

from test_terminal_strategy_service import _config, _request


persistence_module = importlib.import_module("competition_core.terminal_strategy.persistence")


def _result(*, candidates=3):
    return TerminalStrategyService().evaluate(_request(candidates=candidates))


def test_persist_writes_documented_immutable_artifacts_and_manifest(tmp_path):
    result = _result()
    store = TerminalEvaluationPersistence(tmp_path)
    directory = store.persist(result)
    assert directory == tmp_path / "runs" / result.run_id / "terminal" / result.evaluation_id
    assert {path.name for path in directory.iterdir()} == {
        "terminal-snapshot.json",
        "award-line-history.json",
        "award-line-forecast.json",
        "mode-decision.json",
        "candidates-watch.json",
        "candidates-focus.json",
        "candidates-terminal.json",
        "allocation-policy.json",
        "source-hashes.json",
        "decision-manifest.json",
    }
    loaded = store.load(result.run_id, result.evaluation_id)
    assert loaded["manifest"]["complete"] is True
    assert loaded["manifest"]["execution_ready"] is False
    assert loaded["manifest"]["result_hash"] == result.result_hash
    assert loaded["manifest"]["externally_hash_bound"] == [
        "account_snapshot_hash", "leaderboard_snapshot_hash"
    ]
    assert not (directory / "account.json").exists()
    assert not (directory / "leaderboard.json").exists()


def test_unavailable_result_is_still_auditable_without_fake_allocation(tmp_path):
    config = _config(cutoff=None)
    result = TerminalStrategyService().evaluate(_request(config=config, core_count=5))
    store = TerminalEvaluationPersistence(tmp_path)
    directory = store.persist(result)
    loaded = store.load(result.run_id, result.evaluation_id)
    allocation = loaded["artifacts"]["allocation-policy.json"]
    assert directory.is_dir()
    assert result.status == "UNAVAILABLE"
    assert allocation["available"] is False
    assert allocation["policy"] is None
    assert loaded["manifest"]["status"] == "UNAVAILABLE"


def test_same_result_is_idempotent_and_conflicting_content_is_rejected(tmp_path):
    result = _result()
    store = TerminalEvaluationPersistence(tmp_path)
    first = store.persist(result)
    second = store.persist(result)
    assert first == second
    conflicting = replace(result, reasons=("CONFLICTING_RESULT",))
    with pytest.raises(TerminalPersistenceError, match="conflicting content"):
        store.persist(conflicting)


def test_changed_config_gets_a_new_evaluation_directory(tmp_path):
    first = _result()
    payload = _config().to_dict()
    payload["mode"]["hysteresis"] = "0.004"
    changed_config = TerminalStrategyConfig.from_dict(payload)
    changed = TerminalStrategyService().evaluate(_request(config=changed_config))
    assert changed.evaluation_id != first.evaluation_id
    store = TerminalEvaluationPersistence(tmp_path)
    assert store.persist(first) != store.persist(changed)


def test_path_traversal_and_symlink_escape_are_rejected(tmp_path):
    store = TerminalEvaluationPersistence(tmp_path)
    with pytest.raises(TerminalPersistenceError):
        store.path_for("../escape", "a" * 64)
    with pytest.raises(TerminalPersistenceError):
        store.path_for("run", "../escape")
    outside = tmp_path / "outside"
    outside.mkdir()
    runs = tmp_path / "runs"
    runs.mkdir()
    try:
        (runs / "symlink-run").symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("directory symlinks unavailable on this test host")
    with pytest.raises(TerminalPersistenceError):
        store.path_for("symlink-run", "a" * 64, create=True)


def test_tampering_or_extra_files_fails_closed(tmp_path):
    result = _result()
    store = TerminalEvaluationPersistence(tmp_path)
    directory = store.persist(result)
    (directory / "candidates-watch.json").write_text('{"tampered":true}\n', encoding="utf-8")
    with pytest.raises(TerminalPersistenceError, match="hash mismatch"):
        store.load(result.run_id, result.evaluation_id)

    # A separate clean evaluation checks strict file-set validation.
    result = _result(candidates=2)
    directory = store.persist(result)
    (directory / "unexpected.json").write_text("{}\n", encoding="utf-8")
    with pytest.raises(TerminalPersistenceError, match="file set"):
        store.load(result.run_id, result.evaluation_id)


def test_partial_write_has_no_complete_manifest(tmp_path, monkeypatch):
    result = _result()
    store = TerminalEvaluationPersistence(tmp_path)
    original = persistence_module.atomic_json
    calls = {"count": 0}

    def fail_after_two(path, payload, *, immutable=False):
        calls["count"] += 1
        if calls["count"] == 3:
            raise OSError("synthetic write interruption")
        return original(path, payload, immutable=immutable)

    monkeypatch.setattr(persistence_module, "atomic_json", fail_after_two)
    with pytest.raises(TerminalPersistenceError):
        store.persist(result)
    directory = store.path_for(result.run_id, result.evaluation_id, create=False)
    assert not (directory / "decision-manifest.json").exists()
    with pytest.raises(TerminalPersistenceError):
        store.load(result.run_id, result.evaluation_id)


def test_manifest_and_source_hash_tampering_are_detected(tmp_path):
    result = _result()
    store = TerminalEvaluationPersistence(tmp_path)
    directory = store.persist(result)
    manifest_path = directory / "decision-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["status"] = "BLOCKED"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    with pytest.raises(TerminalPersistenceError, match="manifest"):
        store.load(result.run_id, result.evaluation_id)

    # Recreate a clean result and tamper with the source binding instead.
    result = _result(candidates=2)
    directory = store.persist(result)
    source_path = directory / "source-hashes.json"
    source = json.loads(source_path.read_text(encoding="utf-8"))
    source["s4_hash"] = "0" * 64
    source_path.write_text(json.dumps(source, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    with pytest.raises(TerminalPersistenceError, match="hash mismatch"):
        store.load(result.run_id, result.evaluation_id)
