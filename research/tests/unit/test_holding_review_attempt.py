from contextlib import contextmanager
from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from competition_core.pipeline.artifacts import atomic_json, content_hash, read_json
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.account import AccountPosition, AccountSnapshot
from competition_core.pipeline.reviews import run_holding_review_attempt, run_holding_reviews
from competition_core.pipeline.sources import ResearchDataClient
from tests.unit.test_holding_evidence_builder import batches, panel, SESSIONS, _candidate as _holding_candidate
from tests.unit.test_pipeline_research_packet import _bundle, _candidate, _config, _context
from tests.unit.test_pipeline_evidence_bundle import _member


RUNTIME = {
    "provider": "deepseek",
    "model": "synthetic-model",
    "generation_parameters": {
        "extra_body": {"max_tokens": 4096, "thinking": {"type": "disabled"}},
        "response_format": {"type": "json_object"},
    },
}


class RepositoryFixture:
    def __init__(self, root: Path):
        self.run_id = "holding-review-fixture"
        self.directory = root / self.run_id
        self.directory.mkdir(parents=True)
        self._snapshot_id = _bundle().snapshot_id
        self._specification_hash = None
        finalists = [f"600{index:03d}.SH" for index in range(1, 11)]
        stage_binding = _context().to_dict() | {"snapshot_id": self._snapshot_id,
                                                "config_hash": _config().config_hash}
        self.stages = {
            "S1": stage_binding | {"artifact_hash": "1" * 64,
                                    "output_symbols": finalists + [_bundle().symbol.symbol, "600519.SH"],
                                    "rows": [_candidate(), _holding_candidate()]},
            "S4": stage_binding | {"artifact_hash": "4" * 64, "output_symbols": finalists},
            "S5": stage_binding | {"artifact_hash": "5" * 64, "output_symbols": finalists},
        }
        config, context = _config(), _context()
        vibe_root = Path(__file__).resolve().parents[3] / "Vibe-Trading"
        specification = {
            "context": context.to_dict(), "snapshot_id": self._snapshot_id,
            "config": config.to_dict(), "config_hash": config.config_hash,
            "vibe_root": str(vibe_root),
        }
        self._specification_hash = content_hash(specification)
        atomic_json(self.directory / "inputs.json",
                    specification | {"specification_hash": self._specification_hash}, immutable=True)

    @contextmanager
    def lock(self):
        yield

    def state(self):
        return {"snapshot_id": self._snapshot_id, "specification_hash": self._specification_hash}

    def load_stage(self, stage):
        value = self.stages.get(stage)
        return deepcopy(value) if value is not None else None


def news_context():
    value = {"schema_version": "ninef.news.v1", "status": "EMPTY_VERIFIED",
             "cutoff": _context().as_of.isoformat(), "fetched_at": _context().as_of.isoformat(), "items": []}
    return value | {"snapshot_hash": content_hash(value)}


def answer(packet):
    review = {
        "symbol": packet["symbol"], "verdict": "KEEP",
        "strengths": ["量化证据支持继续持仓复核[technical.quant.quant_score]"],
        "risks": [], "evidence_conflicts": [], "missing_critical_info": [],
        "summary": "仅保留为既有持仓研究结论[technical.quant.quant_score]",
    }
    return {"ok": True, "runtime": RUNTIME, "packet_hash": packet["packet_hash"],
            "tool_calls": [], "invalid_tool_calls": [], "finish_reason": "stop",
            "response_model": "synthetic-model", "llm_calls": 1,
            "content": json.dumps(review, ensure_ascii=False)}


def invoke(repo, **changes):
    bundle = _bundle()
    values = {
        "repository": repo, "attempt_id": "holding-001", "candidate": _candidate(bundle),
        "bundle": bundle, "context": _context(), "config": _config(),
        "holding_source_hash": content_hash({"symbol": bundle.symbol.symbol, "quantity": 100}),
        "news_context": news_context(), "probe": lambda **_: RUNTIME,
        "invoker": lambda **kwargs: answer(kwargs["packet"]),
    }
    values.update(changes)
    return run_holding_review_attempt(**values)


def test_attempt_is_isolated_role_bound_and_idempotent(tmp_path):
    repo = RepositoryFixture(tmp_path)
    before = deepcopy(repo.stages)
    first = invoke(repo)
    second = invoke(repo, invoker=lambda **_: pytest.fail("completed attempt must not call model again"))

    assert first == second
    assert first["status"] == "SUCCESS" and first["portfolio_authority"] is False
    assert repo.stages == before
    root = repo.directory / "holding-review-attempts" / "holding-001"
    packet = read_json(root / "packet.json")
    assert packet["schema_version"] == "p10.holding_research_packet.v2"
    assert packet["research_role"] == "existing_holding"
    assert "不是S4新买入候选" in packet["user_prompt"]
    assert first["holding_source_hash"] not in packet["user_prompt"]
    assert read_json(root / "evidence.json") == _bundle().to_dict()
    assert not (repo.directory / "S4.json").exists()
    assert not (repo.directory / "S5.json").exists()


def test_attempt_identity_change_fails_without_second_model_call(tmp_path):
    repo = RepositoryFixture(tmp_path)
    invoke(repo)
    with pytest.raises(PipelineError, match="immutable artifact content conflict"):
        invoke(repo, holding_source_hash="a" * 64,
               invoker=lambda **_: pytest.fail("identity conflict must precede model call"))


def test_s4_finalist_cannot_use_supplemental_holding_path(tmp_path):
    repo = RepositoryFixture(tmp_path)
    repo.stages["S4"]["output_symbols"].append(_bundle().symbol.symbol)
    with pytest.raises(PipelineError, match="only for S1 holdings outside S4"):
        invoke(repo, invoker=lambda **_: pytest.fail("invalid role must precede model call"))


def test_unselected_s1_row_can_use_supplemental_holding_path(tmp_path):
    repo = RepositoryFixture(tmp_path)
    repo.stages["S1"]["output_symbols"].remove(_bundle().symbol.symbol)

    result = invoke(repo)

    assert result["status"] == "SUCCESS"
    assert result["research_role"] == "existing_holding"


def account(quantity=100, symbol="600519.SH"):
    market_value = Decimal(quantity * 10).quantize(Decimal("0.01"))
    cash = Decimal("1000000.00") - market_value
    return AccountSnapshot(as_of=_context().as_of, source="explicit.synthetic.fixture",
        source_kind="synthetic_test", currency="CNY", cash_balance=cash, available_cash=cash,
        current_total_asset=Decimal("1000000.00"),
        positions=(AccountPosition(symbol, quantity, quantity, market_value),),
        pending_order_count=0)


def test_coordinator_uses_frozen_batches_and_account_change_gets_new_attempt(tmp_path, monkeypatch):
    repo = RepositoryFixture(tmp_path)
    monkeypatch.setattr("competition_core.pipeline.evidence_builder.fetch_supplements",
                        lambda cache, sessions, cache_only: batches())
    source = ResearchDataClient(SimpleNamespace(), _context())
    calls = []
    before = deepcopy(repo.stages)

    def run(snapshot, source_hash):
        return run_holding_reviews(repository=repo, account=snapshot,
            account_source_hash=source_hash, snapshot={"snapshot_id": _bundle().snapshot_id,
                "members": [_member()]}, panel=panel(), sessions=SESSIONS, context=_context(),
            config=_config(), source_client=source, source_name="tushare", news_context=news_context(),
            probe=lambda **_: RUNTIME,
            invoker=lambda **kwargs: calls.append(kwargs["packet"]["packet_hash"]) or answer(kwargs["packet"]))

    first = run(account(100), "b" * 64)
    second = run(account(200), "c" * 64)

    assert first["reviewed_symbols"] == second["reviewed_symbols"] == ["600519.SH"]
    assert first["records"][0]["attempt_id"] != second["records"][0]["attempt_id"]
    assert len(calls) == 2 and repo.stages == before
    current = read_json(repo.directory / "holding-review-current.json")
    assert current["index_hash"] == content_hash(second)


def test_coordinator_filters_unselected_rows_and_continues_reviewable_holdings(tmp_path, monkeypatch):
    repo = RepositoryFixture(tmp_path)
    repo.stages["S1"]["output_symbols"].remove("600519.SH")
    monkeypatch.setattr("competition_core.pipeline.evidence_builder.fetch_supplements",
                        lambda cache, sessions, cache_only: batches())
    source = ResearchDataClient(SimpleNamespace(), _context())
    amount = Decimal("1000.00")
    positions = tuple(
        AccountPosition(symbol, 100, 100, amount)
        for symbol in ("600519.SH", "600011.SH")
    )
    snapshot = AccountSnapshot(as_of=_context().as_of, source="explicit.synthetic.fixture",
        source_kind="synthetic_test", currency="CNY", cash_balance=Decimal("1000000.00") - amount * 2,
        available_cash=Decimal("1000000.00") - amount * 2,
        current_total_asset=Decimal("1000000.00"), positions=positions, pending_order_count=0)
    called_symbols = []

    result = run_holding_reviews(repository=repo, account=snapshot, account_source_hash="d" * 64,
        snapshot={"snapshot_id": _bundle().snapshot_id, "members": [_member()]},
        panel=panel(), sessions=SESSIONS, context=_context(), config=_config(), source_client=source,
        source_name="tushare", news_context=news_context(), probe=lambda **_: RUNTIME,
        invoker=lambda **kwargs: called_symbols.append(kwargs["packet"]["symbol"]) or answer(kwargs["packet"]))

    assert called_symbols == ["600519.SH"]
    assert result["reviewed_symbols"] == ["600519.SH"]
    assert result["blockers"] == ["MISSING_HOLDING_RESEARCH_COVERAGE:600011.SH"]
    assert result["input_symbols"] == ["600011.SH", "600519.SH"]
