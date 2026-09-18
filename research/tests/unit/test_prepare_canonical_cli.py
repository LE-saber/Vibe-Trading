from __future__ import annotations

from datetime import date, datetime, timedelta
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

import pandas as pd
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = PROJECT_ROOT / "scripts" / "prepare-canonical.py"
SPEC = importlib.util.spec_from_file_location("prepare_canonical_cli", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
prepare = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = prepare
SPEC.loader.exec_module(prepare)

OBSERVED = datetime.fromisoformat("2026-08-31T10:00:00+08:00")
REFERENCE = date(2026, 8, 28)


def _basic(ts_code: str, name: str, market: str, exchange: str) -> dict[str, object]:
    return {
        "ts_code": ts_code,
        "symbol": ts_code[:6],
        "name": name,
        "market": market,
        "exchange": exchange,
        "list_status": "L",
        "list_date": "20200101",
        "delist_date": None,
    }


class FakeClient:
    def trade_cal(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"exchange": "SSE", "cal_date": "20260828", "is_open": 1, "pretrade_date": "20260827"},
                {"exchange": "SSE", "cal_date": "20260831", "is_open": 1, "pretrade_date": "20260828"},
            ]
        )

    def stock_basic(self, **_kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            [
                _basic("300750.SZ", "宁德时代", "创业板", "SZSE"),
                _basic("600519.SH", "贵州茅台", "主板", "SSE"),
                _basic("000001.SZ", "平安银行", "主板", "SZSE"),
            ]
        )

    def daily(self, **kwargs) -> pd.DataFrame:
        codes = ["300750.SZ", "600519.SH", "000001.SZ"]
        trade_date = kwargs["trade_date"]
        fields = kwargs.get("fields", "")
        if "open" in fields:
            return pd.DataFrame(
                [
                    {
                        "ts_code": code,
                        "trade_date": trade_date,
                        "open": 10.0,
                        "high": 11.0,
                        "low": 9.0,
                        "close": 10.5,
                        "vol": 100.0,
                        "amount": 200.0,
                    }
                    for code in codes
                ]
            )
        return pd.DataFrame(
            [
                {"ts_code": code, "trade_date": trade_date, "amount": 200.0, "vol": 100.0}
                for code in codes
            ]
        )

    def adj_factor(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"ts_code": code, "trade_date": kwargs["trade_date"], "adj_factor": 1.0}
                for code in ("300750.SZ", "600519.SH", "000001.SZ")
            ]
        )

    def daily_basic(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "ts_code": code,
                    "trade_date": kwargs["trade_date"],
                    "turnover_rate": 1.0,
                    "volume_ratio": 1.0,
                    "total_mv": 1000.0,
                    "circ_mv": 900.0,
                }
                for code in ("300750.SZ", "600519.SH", "000001.SZ")
            ]
        )

    def suspend_d(self, **_kwargs) -> pd.DataFrame:
        return pd.DataFrame(columns=["ts_code", "trade_date", "suspend_type", "suspend_timing"])


class FakeDataset:
    seen_symbols: list[str] = []

    def __init__(self, root: Path, *, source_name: str) -> None:
        self.root = Path(root)
        self.source_name = source_name

    def prepare(self, _client, context, symbols, *, snapshot_id: str, sessions: int) -> dict[str, object]:
        type(self).seen_symbols = list(symbols)
        rows = []
        for symbol in symbols:
            for offset in range(sessions):
                current = REFERENCE - timedelta(days=sessions - offset - 1)
                rows.append(
                    {
                        "symbol": symbol,
                        "trade_date": current.isoformat(),
                        "open_raw": 10.0,
                        "high_raw": 11.0,
                        "low_raw": 9.0,
                        "close_raw": 10.5,
                        "open_qfq": 10.0,
                        "high_qfq": 11.0,
                        "low_qfq": 9.0,
                        "close_qfq": 10.5,
                        "volume_raw": 100.0,
                        "volume_shares": 10000.0,
                        "amount_raw": 200.0,
                        "amount_cny": 200000.0,
                        "adj_factor": 1.0,
                        "turnover_rate": 1.0,
                        "volume_ratio": 1.0,
                        "total_mv": 1000.0,
                        "circ_mv": 900.0,
                    }
                )
        frame = pd.DataFrame(rows)
        self.root.mkdir(parents=True, exist_ok=True)
        panel_path = self.root / "panel.csv"
        frame.to_csv(panel_path, index=False)
        manifest_path = self.root / "manifest.json"
        quality_path = self.root / "quality.json"
        quality_path.write_text("{}", encoding="utf-8")
        manifest = {
            "snapshot_id": snapshot_id,
            "manifest_path": str(manifest_path),
            "panel_path": str(panel_path),
            "quality_path": str(quality_path),
            "dataset_id": "fake-dataset-1",
            "market_data_reference_date": context.market_data_reference_date.isoformat(),
            "sessions": sessions,
            "batch_refs": ["fake-batch-1"],
        }
        manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
        return manifest

    def load(self, manifest_path: Path) -> pd.DataFrame:
        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
        return pd.read_csv(manifest["panel_path"])


def _args(tmp_path: Path, *, phase: str = "preopen", date_value: str = "2026-08-31"):
    return prepare._parser().parse_args(
        [
            "--date",
            date_value,
            "--phase",
            phase,
            "--as-of",
            f"{date_value}T09:00:00+08:00",
            "--output-root",
            str(tmp_path),
            "--sessions",
            "120",
        ]
    )


def _run(tmp_path: Path, *, phase: str = "preopen", snapshot: Path | None = None):
    args = _args(tmp_path, phase=phase)
    if snapshot is not None:
        args.snapshot = snapshot
    return prepare.run(
        args,
        client_factory=FakeClient,
        dataset_class=FakeDataset,
        now_clock=lambda: OBSERVED,
    )


def test_preopen_builds_full_universe_and_writes_immutable_reports(tmp_path: Path):
    report = _run(tmp_path)

    assert report["ok"] is True
    assert report["engineering_result"] == "NOT_PRODUCTION_PASS"
    assert report["production_pass"] is False
    assert report["calendar"]["market_data_reference_date"] == "2026-08-28"
    assert report["snapshot"]["snapshot_count"] == 1
    assert report["snapshot"]["eligible_count"] == 3
    assert report["snapshot"]["symbols_passed_count"] == 3
    assert FakeDataset.seen_symbols == ["000001.SZ", "300750.SZ", "600519.SH"]
    assert report["quality"]["coverage_ge_sessions_ratio"] == 1.0
    assert report["quality"]["missing_factor_count"] == 0
    assert report["quality"]["volume_negative_count"] == 0
    assert report["quality"]["invalid_ohlc_count"] == 0
    assert report["quality"]["duplicate_count"] == 0
    assert report["quality"]["date_leakage_count"] == 0
    assert report["determinism"]["same_manifest_load_exact"] is True
    assert report["cross_exdate"]["status"] == "not_run"
    assert Path(report["report_paths"]["json"]).is_file()
    assert Path(report["report_paths"]["markdown"]).is_file()
    snapshot_payload = json.loads(Path(report["snapshot"]["path"]).read_text(encoding="utf-8"))
    assert snapshot_payload["snapshot_id"] == prepare._universe_snapshot_id(snapshot_payload)
    assert Path(report["snapshot"]["meta_path"]).is_file()


def test_validate_source_uses_fresh_endpoint_comparisons(tmp_path: Path):
    args = _args(tmp_path)
    args.validate_source = True
    report = prepare.run(
        args,
        client_factory=FakeClient,
        dataset_class=FakeDataset,
        now_clock=lambda: OBSERVED,
    )

    assert report["source_validation"]["status"] == "passed"
    assert report["source_validation"]["cache_used"] is False
    assert report["source_validation"]["request_mode"] == "fresh_direct_endpoint"
    assert report["source_validation"]["selected_symbols"] == [
        "300750.SZ",
        "600519.SH",
        "000001.SZ",
    ]
    assert report["source_validation"]["items"]
    assert all(item["pass"] for item in report["source_validation"]["items"])


def test_intraday_maps_to_preopen_and_saved_snapshot_hash_is_verified(tmp_path: Path):
    first = _run(tmp_path / "first", phase="intraday")
    snapshot_path = Path(first["snapshot"]["path"])

    second = _run(tmp_path / "second", phase="intraday", snapshot=snapshot_path)
    assert second["calendar"]["universe_phase"] == "preopen"
    assert second["snapshot"]["path"] == str(snapshot_path.resolve())
    assert second["snapshot"]["meta_path"] is None
    assert FakeDataset.seen_symbols == ["000001.SZ", "300750.SZ", "600519.SH"]

    api_shape = tmp_path / "api-shape.json"
    payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    payload["stale"] = False
    payload["fallback_used"] = False
    api_shape.write_text(json.dumps(payload), encoding="utf-8")
    api_report = _run(tmp_path / "api-shape-run", phase="intraday", snapshot=api_shape)
    assert api_report["snapshot"]["snapshot_id"] == first["snapshot"]["snapshot_id"]

    tampered = tmp_path / "tampered.json"
    payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    payload["members"][0]["name"] = "tampered"
    tampered.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(prepare.PipelineError, match="content hash"):
        _run(tmp_path / "third", phase="intraday", snapshot=tampered)

    unknown = tmp_path / "unknown.json"
    payload["unexpected"] = "reject"
    unknown.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(prepare.PipelineError, match="unsupported top-level"):
        _run(tmp_path / "fourth", phase="intraday", snapshot=unknown)


def test_historical_without_snapshot_and_preclose_postclose_fail_closed(tmp_path: Path):
    historical = _args(tmp_path / "historical", date_value="2026-08-31")
    with pytest.raises(prepare.PipelineError, match="actual today"):
        prepare.run(
            historical,
            client_factory=FakeClient,
            dataset_class=FakeDataset,
            now_clock=lambda: datetime.fromisoformat("2026-09-02T10:00:00+08:00"),
        )

    with pytest.raises(prepare.PipelineError, match="postclose"):
        _run(tmp_path / "postclose", phase="postclose")


def test_allowlist_quotes_and_recursive_secret_redaction(tmp_path: Path):
    env_file = tmp_path / "tushare.env"
    env_file.write_text(
        'TUSHARE_TOKEN="sk-sensitive"\n'
        "TUSHARE_BASE_URL='https://vendor.example/pro'\n"
        "OTHER_SECRET='must-not-be-read'\n",
        encoding="utf-8",
    )
    assert prepare._read_allowlisted_env(env_file) == {
        "TUSHARE_TOKEN": "sk-sensitive",
        "TUSHARE_BASE_URL": "https://vendor.example/pro",
    }
    nested = prepare._safe_details(
        {
            "outer": {
                "api_key": "sk-sensitive",
                "authorization": {"token": "another-secret"},
                "safe": "kept",
            }
        },
        ["sk-sensitive", "another-secret"],
    )
    assert nested["outer"]["api_key"] == "[REDACTED]"
    assert nested["outer"]["authorization"] == "[REDACTED]"
    assert nested["outer"]["safe"] == "kept"
    text = prepare._safe_text(
        '{"api_key":"sk-sensitive","authorization": "Bearer another-secret"}',
        ["sk-sensitive", "another-secret"],
    )
    assert "sk-sensitive" not in text
    assert "another-secret" not in text
    assert "[REDACTED]" in text


def test_missing_token_error_is_structured_and_does_not_echo_env(tmp_path: Path, monkeypatch, capsys):
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    env_file = tmp_path / "empty.env"
    env_file.write_text("TUSHARE_TOKEN=''\n", encoding="utf-8")
    exit_code = prepare.main(
        [
            "--date",
            "2026-08-31",
            "--as-of",
            "2026-08-31T09:00:00+08:00",
            "--env-file",
            str(env_file),
            "--output-root",
            str(tmp_path / "out"),
        ]
    )
    output = capsys.readouterr().out
    assert exit_code == 2
    assert '"code": "NOT_CONFIGURED"' in output
    assert "新进程没有可用凭据" in output
    assert "TUSHARE_TOKEN" not in output
    assert "sk-sensitive" not in output


def test_empty_dotenv_token_does_not_erase_injected_secure_credential(monkeypatch):
    import os

    monkeypatch.setenv("TUSHARE_TOKEN", "fixture-injected-secret")
    with prepare._temporary_env({"TUSHARE_TOKEN": ""}):
        assert os.environ["TUSHARE_TOKEN"] == "fixture-injected-secret"
    with prepare._temporary_env({"TUSHARE_TOKEN": "fixture-explicit-replacement"}):
        assert os.environ["TUSHARE_TOKEN"] == "fixture-explicit-replacement"
    assert os.environ["TUSHARE_TOKEN"] == "fixture-injected-secret"
