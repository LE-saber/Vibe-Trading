"""Account-input reuse tests for the two read-only observation scripts."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from decimal import Decimal

import pytest

from competition_core.pipeline.account import AccountPosition, AccountSnapshot
from competition_core.pipeline.artifacts import atomic_json, content_hash
from competition_core.pipeline.errors import PipelineError


ROOT = Path(__file__).resolve().parents[2]


def _load(name):
    path = ROOT / "scripts" / name
    spec = importlib.util.spec_from_file_location(name.replace("-", "_"), path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _repository(tmp_path, *, with_import=True):
    directory = tmp_path / "runs" / "run-1"
    directory.mkdir(parents=True)
    repository = type("Repository", (), {"directory": directory})()
    if with_import:
        from tests.unit.test_pipeline_portfolio import inputs

        values = inputs()
        account = AccountSnapshot.from_dict(values["account"].to_dict() | {
            "cash_balance": "999000.00",
            "available_cash": "999000.00",
            "positions": [AccountPosition(
                "600001.SH", 100, 100, Decimal("1000.00")
            ).to_dict()],
        })
        fees = values["fees"]
        body = {"account": account.to_dict(), "fees": fees.to_dict(),
                "source_receipt": {"schema_version": "ninef.account.fetch.v2"}}
        input_id = content_hash(body)
        atomic_json(directory / f"execution-imports/{input_id}.json", body, immutable=True)
        atomic_json(directory / "execution-inputs.json", {"id": input_id}, immutable=True)
        return repository, account
    return repository, None


@pytest.mark.parametrize("script_name", ["check-realtime.py", "check-execution-status.py"])
def test_missing_pointer_preserves_legacy_accountless_checks(tmp_path, script_name):
    module = _load(script_name)
    repository, _ = _repository(tmp_path, with_import=False)
    assert module._imported_account(repository) is None


@pytest.mark.parametrize("script_name", ["check-realtime.py", "check-execution-status.py"])
def test_existing_imported_pointer_reuses_account_with_positions(tmp_path, script_name):
    module = _load(script_name)
    repository, expected = _repository(tmp_path)
    account = module._imported_account(repository)
    assert isinstance(account, AccountSnapshot)
    assert account.snapshot_hash == expected.snapshot_hash
    assert len(account.positions) == len(expected.positions)


@pytest.mark.parametrize("script_name", ["check-realtime.py", "check-execution-status.py"])
@pytest.mark.parametrize("tamper", ["pointer", "import"])
def test_present_pointer_or_import_tamper_fails_closed(tmp_path, script_name, tamper):
    module = _load(script_name)
    repository, _ = _repository(tmp_path)
    pointer = repository.directory / "execution-inputs.json"
    if tamper == "pointer":
        atomic_json(pointer, {"id": "f" * 64})
    else:
        input_id = pointer.read_text(encoding="utf-8").split('"')[3]
        atomic_json(repository.directory / f"execution-imports/{input_id}.json", {"tampered": True})
    with pytest.raises(PipelineError):
        module._imported_account(repository)


@pytest.mark.parametrize("script_name", ["check-realtime.py", "check-execution-status.py"])
def test_pointer_symlink_fails_closed(tmp_path, script_name):
    module = _load(script_name)
    repository, _ = _repository(tmp_path)
    pointer = repository.directory / "execution-inputs.json"
    pointer.unlink()
    pointer.symlink_to(repository.directory / "missing.json")
    with pytest.raises(PipelineError):
        module._imported_account(repository)


@pytest.mark.parametrize("script_name", ["check-realtime.py", "check-execution-status.py"])
def test_script_error_projection_does_not_expose_account_or_token(script_name):
    module = _load(script_name)
    projected = module._safe_error(
        PipelineError("SOURCE_UNAVAILABLE", "token=private-value", account={"cash": "secret"})
    )
    assert projected["details"] == {}
    assert "private-value" not in str(projected)
    assert "secret" not in str(projected)


@pytest.mark.parametrize("script_name", ["check-realtime.py", "check-execution-status.py"])
def test_script_main_passes_imported_holdings_when_account_file_is_omitted(
    tmp_path, monkeypatch, capsys, script_name
):
    module = _load(script_name)
    repository, expected = _repository(tmp_path)
    (repository.directory / "run.json").write_text("{}", encoding="utf-8")
    calls = {"roots": []}

    def repository_factory(root, run_id):
        calls["roots"].append((root, run_id))
        return repository

    monkeypatch.setattr(module, "PipelineRepository", repository_factory)
    if script_name == "check-realtime.py":
        def realtime(**kwargs):
            calls["account"] = kwargs["account"]
            return {
                "run_id": "run-1", "check_id": "check-1", "source_error": None,
                "ai_and_quote_eligible": [], "enough_candidates_for_portfolio": False,
                "execution_ready": False,
            }

        monkeypatch.setattr(module, "run_realtime_check", realtime)
    else:
        monkeypatch.setattr(module, "configured_tushare_client", lambda **kwargs: object())

        def status(**kwargs):
            calls["account"] = kwargs["account"]
            return {
                "run_id": "run-1", "status_id": "status-1", "status": "PARTIAL",
                "summary": {}, "execution_ready": False,
            }

        monkeypatch.setattr(module, "run_execution_status_check", status)
    monkeypatch.setattr(
        sys,
        "argv",
        [script_name, "--runs-root", str(tmp_path / "runs"), "--run-id", "run-1"],
    )
    module.main()
    capsys.readouterr()
    assert calls["roots"] == [(tmp_path / "runs", "run-1")]
    assert calls["account"].snapshot_hash == expected.snapshot_hash
    assert len(calls["account"].positions) == 1
