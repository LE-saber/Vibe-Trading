"""Pure command-line coverage for the read-only terminal strategy surface."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys

import pytest

from competition_core.pipeline.artifacts import atomic_json
from competition_core.pipeline.errors import PipelineError
from competition_core.terminal_strategy import TerminalEvaluationPersistence, TerminalStrategyService
from competition_core.research_tasks import ResearchTasks


SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "terminal-strategy.py"


def _module():
    spec = importlib.util.spec_from_file_location("terminal_strategy_cli_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _persisted(root: Path):
    # The service fixture has a confirmed award cutoff and a complete funnel;
    # no network reader is involved in the read-only view commands.
    unit_dir = Path(__file__).parent
    if str(unit_dir) not in sys.path:
        sys.path.insert(0, str(unit_dir))
    from test_terminal_strategy_service import _request

    run_id = "terminal-fixture-run"
    directory = root / run_id
    directory.mkdir(parents=True)
    atomic_json(directory / "task-request.json", {"run_id": run_id}, immutable=True)
    result = TerminalStrategyService().evaluate(_request())
    TerminalEvaluationPersistence(root).persist(result)
    return run_id, result


def test_parser_exposes_only_read_only_terminal_commands_and_accepts_global_root(tmp_path):
    module = _module()
    args = module.build_parser().parse_args(
        ["--runs-root", str(tmp_path / "runs"), "candidates", "run-1", "--tier", "watch"]
    )
    assert args.command == "candidates"
    assert args.runs_root == tmp_path / "runs"
    assert not any(action.dest in {"mode", "price", "quantity", "order"}
                   for action in module.build_parser()._actions)
    with pytest.raises(module._CLIUsageError):
        module.build_parser().parse_args(["approve", "run-1"])


def test_read_views_use_hash_verified_persistence(tmp_path):
    module = _module()
    root = tmp_path / "runs"
    run_id, result = _persisted(root)

    status = module.dispatch(module.build_parser().parse_args(["--runs-root", str(root), "status", run_id]))
    decision = module.dispatch(module.build_parser().parse_args(
        ["--runs-root", str(root), "decision", run_id, "--evaluation-id", result.evaluation_id]
    ))
    candidates = module.dispatch(module.build_parser().parse_args(
        ["--runs-root", str(root), "candidates", run_id, "--tier", "terminal"]
    ))
    history = module.dispatch(module.build_parser().parse_args(
        ["--runs-root", str(root), "history", run_id, "--limit", "1"]
    ))

    assert status["runs"][0]["run_id"] == run_id
    assert decision["evaluation_id"] == result.evaluation_id
    assert candidates["tier"] == "terminal"
    assert len(history["history"]) == 1
    assert decision["execution_ready"] is False


def test_evaluate_uses_shared_runtime_and_readers(monkeypatch, tmp_path):
    module = _module()
    from test_terminal_strategy_pipeline_adapter import _inputs as candidate_inputs
    from test_terminal_strategy_snapshot_adapter import _leaderboard_view
    from test_terminal_strategy_runtime import _account

    run_id = "terminal-cli-evaluate"
    directory = tmp_path / "runs" / run_id
    directory.mkdir(parents=True)
    atomic_json(directory / "task-request.json", {"run_id": run_id}, immutable=True)
    values = candidate_inputs()
    monkeypatch.setattr(
        "competition_core.research_api._terminal_stage_bundle",
        lambda tasks, supplied_run: (directory, {
            "S4": values["s4_stage"], "S5": values["s5_stage"], "S5_5": values["s5_5_stage"]
        }),
    )
    calls = {"account": 0, "leaderboard": 0}

    def account_reader():
        calls["account"] += 1
        return _account()

    def leaderboard_reader():
        calls["leaderboard"] += 1
        return _leaderboard_view()

    monkeypatch.setattr("competition_core.research_api.get_ninef_account_view", account_reader)
    monkeypatch.setattr("competition_core.research_api.get_ninef_leaderboard_view", leaderboard_reader)
    # ``_evaluate`` imports the readers from research_api and delegates to the
    # same orchestration function used by the FastAPI endpoint.
    body = module._evaluate(tmp_path / "runs", run_id)
    assert body["status"] == "UNAVAILABLE"
    assert body["execution_ready"] is False
    assert body["orders_submitted"] == 0
    assert calls == {"account": 1, "leaderboard": 1}


def test_plan_delegates_only_to_shadow_helper(monkeypatch, tmp_path):
    module = _module()
    calls = {}

    def shadow(**kwargs):
        calls.update(kwargs)
        return {
            "schema_version": "competition.terminal-shadow-plan.v1",
            "run_id": kwargs["run_id"],
            "status": "SHADOW",
            "shadow_only": True,
            "execution_ready": False,
            "production_accepted": False,
            "orders_submitted": 0,
        }

    monkeypatch.setattr("competition_core.research_portfolio.create_terminal_shadow_plan", shadow)
    output = module._plan(tmp_path / "runs", "run-1", None)
    assert output["shadow_only"] is True
    assert output["execution_ready"] is False
    assert calls["run_id"] == "run-1"
    assert "execution_service" not in module.__dict__


def test_errors_are_stable_and_redacted(monkeypatch, capsys):
    module = _module()

    def failure(_args):
        raise PipelineError("SOURCE_UNAVAILABLE", "provider token=secret-value")

    monkeypatch.setattr(module, "dispatch", failure)
    assert module.main(["status"]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload == {
        "error": {
            "code": "SOURCE_UNAVAILABLE",
            "details": {},
            "message": "terminal strategy source is unavailable",
        },
        "ok": False,
    }
    assert "secret-value" not in captured.err


def test_invalid_run_id_is_nonzero_json_stderr(capsys):
    module = _module()
    assert module.main(["status", "../escape"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    body = json.loads(captured.err)
    assert body["error"]["code"] == "INVALID_ARGUMENTS"
    assert "escape" not in captured.err
