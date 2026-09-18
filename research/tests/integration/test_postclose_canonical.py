"""Postclose CLI integration using only explicitly synthetic Tushare data.

These tests intentionally call the real ``prepare-canonical.py`` entrypoint
and let it resolve the real ``CanonicalDataset`` implementation.  The fake
client never performs network I/O; its endpoint responses are marked as
synthetic by the source URL and fixture metadata.
"""

from __future__ import annotations

from datetime import datetime
import importlib.util
import json
from pathlib import Path
import sys
from typing import Any

import pandas as pd
import pytest

from competition_core.pipeline.dataset import CanonicalDataset
from competition_core.pipeline.artifacts import content_hash
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.sources import ResearchDataClient
from competition_core.pipeline.time_context import ResearchContext


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "prepare-canonical.py"
SPEC = importlib.util.spec_from_file_location("prepare_canonical_postclose_cli", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
prepare = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = prepare
SPEC.loader.exec_module(prepare)


RESEARCH_DATE = "2026-09-02"
AS_OF = datetime.fromisoformat("2026-09-02T18:00:00+08:00")
OBSERVED = datetime.fromisoformat("2026-09-02T18:05:00+08:00")
CURRENT_TRADE_DATE = "20260902"
SESSION_DATES = ("20260831", "20260901", "20260902")
CALENDAR_DATES = ("20260828", *SESSION_DATES)
SYMBOLS = ("000001.SZ", "300750.SZ", "600519.SH")
SYNTHETIC_BASE_URL = "https://synthetic-tushare.invalid"


def _stock_basic_row(ts_code: str) -> dict[str, Any]:
    names = {
        "000001.SZ": ("平安银行", "主板", "SZSE"),
        "300750.SZ": ("宁德时代", "创业板", "SZSE"),
        "600519.SH": ("贵州茅台", "主板", "SSE"),
    }
    name, market, exchange = names[ts_code]
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


class FakeTushare:
    """A deterministic, network-free Tushare double with capped pages."""

    synthetic = True
    page_cap = 2

    def __init__(self, *, missing_current_daily: str | None = None) -> None:
        self.missing_current_daily = missing_current_daily
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.page_results: list[dict[str, Any]] = []

    def stock_basic(self, **kwargs: Any) -> pd.DataFrame:
        self.calls.append(("stock_basic", dict(kwargs)))
        rows = [_stock_basic_row(symbol) for symbol in SYMBOLS]
        return self._select_and_page(rows, kwargs, default_columns=list(rows[0]))

    def trade_cal(self, **kwargs: Any) -> pd.DataFrame:
        self.calls.append(("trade_cal", dict(kwargs)))
        rows = [
            {
                "exchange": "SSE",
                "cal_date": trade_date,
                "is_open": 1,
                "pretrade_date": CALENDAR_DATES[index - 1] if index else None,
            }
            for index, trade_date in enumerate(CALENDAR_DATES)
        ]
        start_date = str(kwargs.get("start_date", "00000000")).replace("-", "")
        end_date = str(kwargs.get("end_date", "99999999")).replace("-", "")
        rows = [row for row in rows if start_date <= row["cal_date"] <= end_date]
        fields = self._requested_fields(kwargs, list(rows[0]))
        return pd.DataFrame(rows, columns=fields)

    def suspend_d(self, **kwargs: Any) -> pd.DataFrame:
        self.calls.append(("suspend_d", dict(kwargs)))
        fields = self._requested_fields(
            kwargs, ["ts_code", "trade_date", "suspend_type", "suspend_timing"]
        )
        return pd.DataFrame(columns=fields)

    def daily(self, **kwargs: Any) -> pd.DataFrame:
        return self._market_endpoint("daily", kwargs)

    def adj_factor(self, **kwargs: Any) -> pd.DataFrame:
        return self._market_endpoint("adj_factor", kwargs)

    def daily_basic(self, **kwargs: Any) -> pd.DataFrame:
        return self._market_endpoint("daily_basic", kwargs)

    def _market_endpoint(self, endpoint: str, kwargs: dict[str, Any]) -> pd.DataFrame:
        self.calls.append((endpoint, dict(kwargs)))
        trade_date = str(kwargs.get("trade_date", "")).replace("-", "")
        rows = [self._market_row(symbol, trade_date, endpoint) for symbol in SYMBOLS]
        if endpoint == "daily" and trade_date == CURRENT_TRADE_DATE:
            rows = [row for row in rows if row["ts_code"] != self.missing_current_daily]
        default_columns = {
            "daily": [
                "ts_code",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "vol",
                "amount",
            ],
            "adj_factor": ["ts_code", "trade_date", "adj_factor"],
            "daily_basic": [
                "ts_code",
                "trade_date",
                "turnover_rate",
                "volume_ratio",
                "total_mv",
                "circ_mv",
            ],
        }[endpoint]
        return self._select_and_page(rows, kwargs, default_columns=default_columns)

    @staticmethod
    def _market_row(ts_code: str, trade_date: str, endpoint: str) -> dict[str, Any]:
        row: dict[str, Any] = {
            "ts_code": ts_code,
            "trade_date": trade_date,
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.5,
            "vol": 100.0,
            "amount": 200.0,
            "adj_factor": 1.0,
            "turnover_rate": 1.0,
            "volume_ratio": 1.0,
            "total_mv": 1000.0,
            "circ_mv": 900.0,
        }
        return row

    def _select_and_page(
        self,
        rows: list[dict[str, Any]],
        kwargs: dict[str, Any],
        *,
        default_columns: list[str],
    ) -> pd.DataFrame:
        columns = self._requested_fields(kwargs, default_columns)
        if "limit" not in kwargs:
            return pd.DataFrame(rows, columns=columns)

        offset = int(kwargs.get("offset", 0))
        page = rows[offset : offset + self.page_cap]
        self.page_results.append(
            {
                "endpoint": self.calls[-1][0],
                "trade_date": kwargs.get("trade_date"),
                "limit": kwargs.get("limit"),
                "offset": offset,
                "rowcount": len(page),
            }
        )
        return pd.DataFrame(page, columns=columns)

    @staticmethod
    def _requested_fields(kwargs: dict[str, Any], default: list[str]) -> list[str]:
        raw_fields = kwargs.get("fields")
        if not raw_fields:
            return default
        return [field.strip() for field in str(raw_fields).split(",") if field.strip()]


def _cli_args(
    output_root: Path,
    *,
    as_of: str = "2026-09-02T18:00:00+08:00",
    snapshot: Path | None = None,
) -> list[str]:
    args = [
        "--date",
        RESEARCH_DATE,
        "--phase",
        "postclose",
        "--as-of",
        as_of,
        "--output-root",
        str(output_root),
        "--sessions",
        "3",
    ]
    if snapshot is not None:
        args.extend(["--snapshot", str(snapshot)])
    return args


def _run_cli(
    output_root: Path,
    source: FakeTushare,
    capsys: pytest.CaptureFixture[str],
    *,
    as_of: str = "2026-09-02T18:00:00+08:00",
    observed: datetime = OBSERVED,
    snapshot: Path | None = None,
) -> tuple[int, dict[str, Any]]:
    exit_code = prepare.main(
        _cli_args(output_root, as_of=as_of, snapshot=snapshot),
        client_factory=lambda: source,
        now_clock=lambda: observed,
    )
    output = capsys.readouterr().out
    assert output.strip(), "the real CLI must emit a JSON report"
    return exit_code, json.loads(output)


def _final_artifacts(output_root: Path) -> list[Path]:
    snapshots = list((output_root / "snapshots").glob("universe-*.json"))
    manifests = list((output_root / "datasets").rglob("manifest.json"))
    return sorted(path for path in [*snapshots, *manifests] if path.is_file())


def _read_manifest(report: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    path = Path(report["dataset"]["manifest_path"])
    return path, json.loads(path.read_text(encoding="utf-8"))


def _read_snapshot(report: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    path = Path(report["snapshot"]["path"])
    return path, json.loads(path.read_text(encoding="utf-8"))


def _assert_complete_postclose(report: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    assert report["ok"] is True
    assert report["calendar"]["research_date"] == RESEARCH_DATE
    assert report["calendar"]["phase"] == "postclose"
    assert report["calendar"]["market_data_reference_date"] == RESEARCH_DATE
    assert report["dataset"]["sessions"] == 3

    manifest_path, manifest = _read_manifest(report)
    assert manifest_path.is_file()
    assert manifest["phase"] == "postclose"
    assert manifest["research_date"] == RESEARCH_DATE
    assert manifest["market_data_reference_date"] == RESEARCH_DATE
    proof = manifest.get("publication_proof")
    assert isinstance(proof, dict) and proof
    assert proof["completed_daily_date"] == RESEARCH_DATE
    assert proof["expected_count"] == 3
    assert proof["covered_counts"] == {
        "daily": 3,
        "adj_factor": 3,
        "daily_basic": 3,
    }
    return manifest_path, manifest


@pytest.fixture(autouse=True)
def _mark_all_source_data_synthetic(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TUSHARE_BASE_URL", SYNTHETIC_BASE_URL)


def test_postclose_publishes_current_day_snapshot_and_canonical_manifest(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source = FakeTushare()
    output_root = tmp_path / "canonical"

    exit_code, report = _run_cli(output_root, source, capsys)

    assert exit_code == 0
    snapshot_path, snapshot = _read_snapshot(report)
    assert snapshot_path.is_file()
    assert snapshot["as_of"] == RESEARCH_DATE
    assert snapshot["snapshot_phase"] == "postclose"
    assert snapshot["market_data_reference_date"] == RESEARCH_DATE
    assert report["snapshot"]["eligible_count"] == 3
    assert "synthetic" in report["source"]
    for member in snapshot["members"]:
        provenance = member["field_sources"]
        assert provenance["identity"] == "tushare.stock_basic"
        assert provenance["listing_status"] == "tushare.stock_basic"
        assert provenance["suspension"] == "tushare.suspend_d"

    manifest_path, manifest = _assert_complete_postclose(report)
    panel = CanonicalDataset(output_root, source_name=report["source"]).load(manifest_path)
    assert len(panel) == 3 * 3
    assert manifest["dataset_id"] == report["dataset"]["dataset_id"]

    unpaged_daily = [
        kwargs
        for endpoint, kwargs in source.calls
        if endpoint == "daily"
        and "limit" not in kwargs
        and kwargs.get("trade_date") == CURRENT_TRADE_DATE
    ]
    assert unpaged_daily
    assert {"ts_code", "trade_date", "amount", "vol"} <= set(
        unpaged_daily[0]["fields"].split(",")
    )

    for endpoint in ("daily", "adj_factor", "daily_basic"):
        for trade_date in SESSION_DATES:
            observed_pages = {
                (item["offset"], item["rowcount"])
                for item in source.page_results
                if item["endpoint"] == endpoint and item["trade_date"] == trade_date
            }
            assert {"6000"} == {
                str(item["limit"])
                for item in source.page_results
                if item["endpoint"] == endpoint and item["trade_date"] == trade_date
            }
            assert {(0, 2), (2, 1), (3, 0)} <= observed_pages


def test_postclose_missing_current_daily_does_not_commit_then_retries_same_root(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source = FakeTushare(missing_current_daily="600519.SH")
    output_root = tmp_path / "retryable"

    failed_code, failed_report = _run_cli(output_root, source, capsys)

    assert failed_code == 2
    assert failed_report["ok"] is False
    assert failed_report["error"]["code"] == "DATA_COVERAGE_INSUFFICIENT"
    assert _final_artifacts(output_root) == []
    historical_cache_before = {
        str(path.relative_to(output_root)): path.read_bytes()
        for path in output_root.rglob("*")
        if path.is_file()
        and "batches" in path.parts
        and any(trade_date in path.name for trade_date in SESSION_DATES[:-1])
    }

    source.missing_current_daily = None
    source.calls.clear()
    source.page_results.clear()
    success_code, success_report = _run_cli(output_root, source, capsys)

    assert success_code == 0
    _assert_complete_postclose(success_report)
    assert _final_artifacts(output_root)
    for relative_path, contents in historical_cache_before.items():
        assert (output_root / relative_path).read_bytes() == contents


@pytest.mark.parametrize(
    "as_of",
    ["2026-09-02T14:59:00+08:00", "2026-09-02T18:06:00+08:00"],
)
def test_postclose_rejects_before_any_fake_source_request(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    as_of: str,
) -> None:
    source = FakeTushare()

    exit_code, report = _run_cli(tmp_path / "rejected", source, capsys, as_of=as_of)

    assert exit_code == 2
    assert report["ok"] is False
    assert report["error"]["code"] == "NOT_READY"
    assert source.calls == []
    assert _final_artifacts(tmp_path / "rejected") == []


def test_postclose_saved_snapshot_reuses_manifest_without_market_data_requests(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source = FakeTushare()
    output_root = tmp_path / "replay"

    first_code, first_report = _run_cli(output_root, source, capsys)
    assert first_code == 0
    snapshot_path, _ = _read_snapshot(first_report)
    first_manifest_path, first_manifest = _assert_complete_postclose(first_report)
    first_dataset_id = first_manifest["dataset_id"]
    first_snapshot_id = first_manifest["snapshot_id"]

    source.calls.clear()
    source.page_results.clear()
    second_code, second_report = _run_cli(
        output_root, source, capsys, snapshot=snapshot_path
    )

    assert second_code == 0
    second_manifest_path, second_manifest = _assert_complete_postclose(second_report)
    assert second_report["dataset"]["dataset_id"] == first_dataset_id
    assert second_manifest["dataset_id"] == first_dataset_id
    assert second_manifest["snapshot_id"] == first_snapshot_id
    assert second_manifest_path == first_manifest_path
    assert all(
        endpoint not in {"daily", "adj_factor", "daily_basic"}
        for endpoint, _kwargs in source.calls
    )


@pytest.mark.parametrize("endpoint", ["adj_factor", "daily_basic"])
def test_partial_postclose_endpoint_is_not_cached_and_retry_refetches(tmp_path, capsys, endpoint):
    source = FakeTushare()
    original = getattr(source, endpoint)

    def partial(**kwargs):
        frame = original(**kwargs)
        if kwargs.get("trade_date") == CURRENT_TRADE_DATE:
            return frame.loc[frame.ts_code != "600519.SH"]
        return frame

    setattr(source, endpoint, partial)
    code, report = _run_cli(tmp_path, source, capsys)
    assert code == 2 and report["error"]["code"] == "DATA_COVERAGE_INSUFFICIENT"
    assert _final_artifacts(tmp_path) == []
    assert not [path for path in tmp_path.rglob("*20260902*.parquet") if endpoint in path.parts]
    setattr(source, endpoint, original)
    code, report = _run_cli(tmp_path, source, capsys)
    assert code == 0
    _assert_complete_postclose(report)


def test_postclose_rejects_daily_derived_suspension_provenance(tmp_path, capsys):
    source = FakeTushare()

    def unavailable(**kwargs):
        raise RuntimeError("synthetic unavailable suspension endpoint")

    source.suspend_d = unavailable
    code, report = _run_cli(tmp_path, source, capsys)
    assert code == 2
    assert report["error"]["code"] in {"STALE_DATA", "SOURCE_UNAVAILABLE"}
    assert _final_artifacts(tmp_path) == []


def test_postclose_snapshot_liquidity_race_rejected_before_manifest(tmp_path, capsys):
    source = FakeTushare()
    original = source.daily

    def revised(**kwargs):
        frame = original(**kwargs)
        if "limit" not in kwargs:
            frame["amount"] = frame.amount * 2
        return frame

    source.daily = revised
    code, report = _run_cli(tmp_path, source, capsys)
    assert code == 2 and report["error"]["code"] == "STALE_DATA"
    assert _final_artifacts(tmp_path) == []


def test_postclose_proof_cannot_narrow_independent_snapshot_denominator(tmp_path, capsys):
    code, report = _run_cli(tmp_path, FakeTushare(), capsys)
    assert code == 0
    path, manifest = _read_manifest(report)
    proof = manifest["publication_proof"]
    proof["expected_symbols"] = ["600519.SH"]
    proof["expected_count"] = 1
    proof["expected_symbols_hash"] = content_hash(proof["expected_symbols"])
    proof["covered_counts"] = dict(daily=1, adj_factor=1, daily_basic=1)
    proof["proof_id"] = content_hash({key: value for key, value in proof.items() if key != "proof_id"})
    manifest["manifest_hash"] = content_hash({key: value for key, value in manifest.items() if key != "manifest_hash"})
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(PipelineError, match="independent snapshot membership"):
        CanonicalDataset(tmp_path, source_name=report["source"]).load(path)


def test_postclose_direct_dataset_cannot_skip_publication_snapshot(tmp_path):
    source = FakeTushare()
    context = ResearchContext(AS_OF.date(), "postclose", AS_OF.date(), AS_OF)
    client = ResearchDataClient(source, context, observed_clock=lambda: OBSERVED)
    with pytest.raises(PipelineError, match="publication snapshot"):
        CanonicalDataset(tmp_path).prepare(client, context, list(SYMBOLS), snapshot_id="synthetic", sessions=3)
    assert source.calls == []
