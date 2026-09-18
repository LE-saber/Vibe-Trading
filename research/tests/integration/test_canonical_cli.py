"""Actual CLI/dataset integration using explicitly synthetic endpoint fixtures."""

from datetime import datetime
import importlib.util
import json
from pathlib import Path

from tests.unit.test_pipeline_dataset import Source, SYMBOLS


def test_cli_uses_real_canonical_cache_and_replays_it(tmp_path, monkeypatch):
    script = Path(__file__).resolve().parents[2] / "scripts/prepare-canonical.py"
    spec = importlib.util.spec_from_file_location("canonical_cli_integration", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setenv("TUSHARE_BASE_URL", "https://fixture.invalid")
    payload = {
        "schema_version": 1, "competition_id": "fixture-only", "as_of": "2026-09-02",
        "snapshot_phase": "preopen", "market_data_reference_date": "2026-09-01",
        "members": [{"ts_code": symbol, "is_eligible": True} for symbol in SYMBOLS],
        "sources": [], "warnings": ["SYNTHETIC TEST DATA"],
    }
    payload["snapshot_id"] = module._universe_snapshot_id(payload)
    payload.update(stale=False, fallback_used=False)
    snapshot = tmp_path / "synthetic-universe.json"
    snapshot.write_text(json.dumps(payload), encoding="utf-8")
    args = module._parser().parse_args([
        "--date", "2026-09-02", "--phase", "intraday", "--as-of", "2026-09-02T10:00:00+08:00",
        "--snapshot", str(snapshot), "--output-root", str(tmp_path / "canonical"), "--sessions", "120",
    ])
    source = Source()
    clock = lambda: datetime.fromisoformat("2026-09-02T11:00:00+08:00")
    first = module.run(args, client_factory=lambda: source, now_clock=clock)
    assert first["ok"]
    assert not first["production_pass"]
    assert first["quality"]["full_qfq_history_ratio"] == 1
    assert first["cross_exdate"]["status"] == "passed"
    assert first["dataset"]["batch_ref_count"] == 361
    source.calls.clear()
    second = module.run(args, client_factory=lambda: source, now_clock=clock)
    assert second["dataset"] == first["dataset"]
    assert second["report_paths"] == first["report_paths"]
    assert all(endpoint == "trade_cal" for endpoint, _ in source.calls)
