"""Frozen-only audits cannot lazily initialize a network SDK client."""

import importlib.util
from pathlib import Path

import pytest

from competition_core.pipeline.errors import PipelineError


def test_frozen_only_blocks_network_before_sdk_initialization(monkeypatch):
    path = Path(__file__).resolve().parents[2] / "scripts/run-research.py"
    spec = importlib.util.spec_from_file_location("research_cli_offline_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "create_tushare_client", lambda: pytest.fail("must not initialize the SDK"))
    client = module._LazySource(frozen_only=True)
    with pytest.raises(PipelineError) as failure:
        client.moneyflow(trade_date="20260901")
    assert failure.value.code == "NOT_READY"
    assert client.client is None
