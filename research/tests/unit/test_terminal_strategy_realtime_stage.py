"""Terminal projection of saved realtime observations; no network or orders."""

from pathlib import Path
import os

import pytest

from competition_core.pipeline.account import AccountPosition, AccountSnapshot
from competition_core.pipeline.artifacts import atomic_json, content_hash, file_hash, read_json
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.realtime import run_realtime_check
from competition_core.terminal_strategy.realtime_stage import load_terminal_realtime_stage
from tests.integration.test_realtime_pipeline import prepared, quotes
from tests.pipeline_fixtures import context


def _observed(tmp_path: Path, *, account=None):
    repository = prepared(tmp_path)
    run_realtime_check(
        repository=repository,
        check_id="terminal-observation",
        quote_source=quotes,
        clock=lambda: context().as_of,
        account=account,
    )
    return repository


def test_projection_is_deterministic_and_filters_extra_holdings(tmp_path):
    account = AccountSnapshot(
        as_of=context().as_of,
        source="explicit.synthetic.fixture",
        source_kind="synthetic_test",
        currency="CNY",
        cash_balance="1000.00",
        available_cash="1000.00",
        current_total_asset="1100.00",
        positions=(AccountPosition("688001.SH", 100, 100, "100.00"),),
        pending_order_count=0,
    )
    repository = _observed(tmp_path, account=account)
    first = load_terminal_realtime_stage(repository, "terminal-observation")
    second = load_terminal_realtime_stage(repository, "terminal-observation")

    assert first == second
    assert first["artifact_hash"] == content_hash({k: v for k, v in first.items() if k != "artifact_hash"})
    assert first["input_symbols"] == repository.load_stage("S4")["output_symbols"]
    assert "688001.SH" not in first["input_symbols"]
    assert {row["symbol"] for row in first["rows"]} == set(first["input_symbols"])
    assert first["terminal_observation_binding"]["check_id"] == "terminal-observation"
    assert repository.load_stage("S5_5") is None


@pytest.mark.parametrize("name", ["request.json", "response.json", "observation.json"])
def test_projection_rejects_tampered_observation_files(tmp_path, name):
    repository = _observed(tmp_path)
    path = repository.directory / "realtime" / "terminal-observation" / name
    atomic_json(path, {"tampered": True})
    with pytest.raises(PipelineError):
        load_terminal_realtime_stage(repository, "terminal-observation")


def test_projection_rejects_symlinked_files(tmp_path):
    repository = _observed(tmp_path)
    directory = repository.directory / "realtime" / "terminal-observation"
    target = directory / "response-real.json"
    path = directory / "response.json"
    path.replace(target)
    try:
        os.symlink(target, path)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks are unavailable on this host")
    with pytest.raises(PipelineError, match="symlink"):
        load_terminal_realtime_stage(repository, "terminal-observation")


def test_projection_rejects_invalid_identifier_before_read(tmp_path):
    repository = prepared(tmp_path)
    with pytest.raises(PipelineError, match="identifier"):
        load_terminal_realtime_stage(repository, "../escape")


def test_projection_rejects_internally_rehashed_wrong_context(tmp_path):
    repository = _observed(tmp_path)
    directory = repository.directory / "realtime" / "terminal-observation"
    request_path = directory / "request.json"
    request = read_json(request_path)
    request["context"] = dict(request["context"], phase="postclose")
    atomic_json(request_path, request)

    observation_path = directory / "observation.json"
    observation = read_json(observation_path)
    observation.pop("integrity_hash")
    observation["request_hash"] = file_hash(request_path)
    atomic_json(observation_path, observation | {"integrity_hash": content_hash(observation)})

    with pytest.raises(PipelineError, match="frozen research"):
        load_terminal_realtime_stage(repository, "terminal-observation")
