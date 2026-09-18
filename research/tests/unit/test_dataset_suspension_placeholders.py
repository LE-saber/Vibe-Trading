"""Synthetic P10B regressions for proof-backed daily suspension placeholders."""

from datetime import date, datetime

import pandas as pd
import pytest

from competition_core.pipeline.artifacts import file_hash, read_json
from competition_core.pipeline.dataset import CanonicalDataset
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.sources import ResearchDataClient
from competition_core.pipeline.time_context import ResearchContext


SYMBOLS = ["600717.SH", "688121.SH", "688189.SH"]
PLACEHOLDER_SYMBOL = "600717.SH"
SUSPENSION_DATE = date(2026, 8, 31)
SUSPENSION_DATE_KEY = SUSPENSION_DATE.strftime("%Y%m%d")
SUSPENSION_DATE_ISO = SUSPENSION_DATE.isoformat()
OBSERVED_AT = datetime.fromisoformat("2026-09-02T11:00:00+08:00")
SUSPENSION_COLUMNS = ("ts_code", "trade_date", "suspend_type", "suspend_timing")


def context() -> ResearchContext:
    return ResearchContext(
        date(2026, 9, 2),
        "intraday",
        date(2026, 9, 1),
        datetime.fromisoformat("2026-09-02T10:00:00+08:00"),
    )


class SuspensionSource:
    """Fully synthetic source; every endpoint response is local fixture data."""

    def __init__(
        self,
        *,
        placeholder: bool = False,
        positive_volume_zero_ohl: bool = False,
        suspension_evidence: str = "confirmed",
    ) -> None:
        self.placeholder = placeholder
        self.positive_volume_zero_ohl = positive_volume_zero_ohl
        self.suspension_evidence = suspension_evidence
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.reject_calls = False

    def _record(self, endpoint: str, params: dict[str, object]) -> None:
        self.calls.append((endpoint, dict(params)))
        if self.reject_calls:
            raise AssertionError(f"unexpected source refetch: {endpoint}")

    @staticmethod
    def _page(rows: list[dict[str, object]], params: dict[str, object]) -> pd.DataFrame:
        offset = int(params.get("offset", 0))
        page = rows[offset:offset + 2]
        columns = list(rows[0]) if rows else []
        return pd.DataFrame(page, columns=columns)

    def trade_cal(self, **params: object) -> pd.DataFrame:
        self._record("trade_cal", params)
        rows = [
            {
                "exchange": "SSE",
                "cal_date": day.strftime("%Y%m%d"),
                "is_open": 1,
                "pretrade_date": "",
            }
            for day in pd.bdate_range(str(params["start_date"]), str(params["end_date"]))
        ]
        return pd.DataFrame(rows)

    def daily(self, **params: object) -> pd.DataFrame:
        self._record("daily", params)
        trade_date = str(params["trade_date"])
        factor = 2.0 if trade_date >= "20260901" else 1.0
        price = 10.0 / factor
        rows: list[dict[str, object]] = []
        for symbol in SYMBOLS:
            open_price, high_price, low_price = price, price * 1.1, price * 0.9
            volume, amount = 100.0, 200.0
            if (
                symbol == PLACEHOLDER_SYMBOL
                and trade_date == SUSPENSION_DATE_KEY
                and (self.placeholder or self.positive_volume_zero_ohl)
            ):
                open_price = high_price = low_price = 0.0
                if self.placeholder:
                    volume = amount = 0.0
                elif self.positive_volume_zero_ohl:
                    volume, amount = 100.0, 200.0
            rows.append(
                {
                    "ts_code": symbol,
                    "trade_date": trade_date,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": price,
                    "vol": volume,
                    "amount": amount,
                }
            )
        return self._page(rows, params)

    def daily_basic(self, **params: object) -> pd.DataFrame:
        self._record("daily_basic", params)
        trade_date = str(params["trade_date"])
        rows = [
            {
                "ts_code": symbol,
                "trade_date": trade_date,
                "turnover_rate": 1.0,
                "volume_ratio": 1.1,
                "total_mv": 10000.0,
                "circ_mv": 9000.0,
            }
            for symbol in SYMBOLS
        ]
        return self._page(rows, params)

    def adj_factor(self, **params: object) -> pd.DataFrame:
        self._record("adj_factor", params)
        trade_date = str(params["trade_date"])
        factor = 2.0 if trade_date >= "20260901" else 1.0
        rows = [
            {"ts_code": symbol, "trade_date": trade_date, "adj_factor": factor}
            for symbol in SYMBOLS
        ]
        return self._page(rows, params)

    def suspend_d(self, **params: object) -> pd.DataFrame:
        self._record("suspend_d", params)
        mode = self.suspension_evidence
        if mode == "failed":
            raise RuntimeError("synthetic suspension source failure")
        if mode == "missing":
            rows = [
                {
                    "ts_code": SYMBOLS[1],
                    "trade_date": SUSPENSION_DATE_KEY,
                    "suspend_type": "S",
                    "suspend_timing": None,
                }
            ]
        else:
            trade_date = "20260830" if mode == "wrong_date" else SUSPENSION_DATE_KEY
            timing = "09:30-10:00" if mode == "intraday" else None
            rows = [
                {
                    "ts_code": PLACEHOLDER_SYMBOL,
                    "trade_date": trade_date,
                    "suspend_type": "S",
                    "suspend_timing": timing,
                }
            ]
        return pd.DataFrame(rows, columns=SUSPENSION_COLUMNS)


def prepare(tmp_path, source: SuspensionSource):
    dataset = CanonicalDataset(tmp_path, "fixture")
    client = ResearchDataClient(
        source,
        context(),
        observed_clock=lambda: OBSERVED_AT,
    )
    manifest = dataset.prepare(
        client,
        context(),
        SYMBOLS,
        snapshot_id="fixture-snapshot",
        sessions=3,
    )
    return dataset, manifest


def _batch_path(tmp_path, manifest, endpoint: str, trade_date: date):
    reference = next(
        ref
        for ref in manifest["batch_refs"]
        if ref["endpoint"] == endpoint and ref["trade_date"] == trade_date.isoformat()
    )
    return tmp_path / reference["path"]


def test_confirmed_placeholder_is_excluded_without_fabricating_or_mutating_data(tmp_path):
    clean_dataset, clean_manifest = prepare(tmp_path / "clean", SuspensionSource())
    clean_panel = clean_dataset.load(clean_manifest["manifest_path"])

    source = SuspensionSource(placeholder=True)
    dataset, manifest = prepare(tmp_path / "placeholder", source)
    panel = dataset.load(manifest["manifest_path"])

    omitted = (clean_panel["symbol"] == PLACEHOLDER_SYMBOL) & clean_panel["trade_date"].eq(SUSPENSION_DATE_ISO)
    expected_panel = clean_panel.loc[~omitted].reset_index(drop=True)
    pd.testing.assert_frame_equal(panel.reset_index(drop=True), expected_panel)
    assert panel["volume_raw"].eq(100.0).all()
    assert panel["amount_raw"].eq(200.0).all()
    assert not ((panel["symbol"] == PLACEHOLDER_SYMBOL) & panel["trade_date"].eq(SUSPENSION_DATE_ISO)).any()

    raw_daily = pd.read_parquet(_batch_path(tmp_path / "placeholder", manifest, "daily", SUSPENSION_DATE))
    raw_row = raw_daily.loc[
        (raw_daily["ts_code"] == PLACEHOLDER_SYMBOL)
        & raw_daily["trade_date"].eq(SUSPENSION_DATE_KEY)
    ].iloc[0]
    assert raw_row[["open", "high", "low", "close", "vol", "amount"]].tolist() == [0.0, 0.0, 0.0, 10.0, 0.0, 0.0]

    quality = read_json(tmp_path / "placeholder" / manifest["quality_path"])
    entry = quality["symbols"][PLACEHOLDER_SYMBOL]
    assert entry["missing_dates"] == [SUSPENSION_DATE_ISO]
    assert entry["confirmed_suspension_dates"] == [SUSPENSION_DATE_ISO]
    assert {"missing_daily_bar", "confirmed_suspension"} <= set(entry["gap_reasons"])
    assert quality["confirmed_suspension_events"] == [
        {"symbol": PLACEHOLDER_SYMBOL, "trade_date": SUSPENSION_DATE_ISO}
    ]

    refs = manifest["suspension_refs"]
    assert len(refs) == 1
    proof_path = tmp_path / "placeholder" / refs[0]["path"]
    assert refs[0]["trade_date"] == SUSPENSION_DATE_ISO
    assert refs[0]["hash"] == file_hash(proof_path)
    assert [endpoint for endpoint, _ in source.calls if endpoint == "suspend_d"] == ["suspend_d"]


@pytest.mark.parametrize("evidence", ["missing", "failed", "wrong_date", "intraday"])
def test_placeholder_requires_same_day_full_day_suspension_evidence(tmp_path, evidence):
    source = SuspensionSource(placeholder=True, suspension_evidence=evidence)
    with pytest.raises(PipelineError):
        prepare(tmp_path, source)
    assert [endpoint for endpoint, _ in source.calls if endpoint == "suspend_d"] == ["suspend_d"]


def test_zero_ohl_with_positive_volume_is_rejected_without_suspension_request(tmp_path):
    source = SuspensionSource(positive_volume_zero_ohl=True)
    with pytest.raises(PipelineError):
        prepare(tmp_path, source)
    assert not any(endpoint == "suspend_d" for endpoint, _ in source.calls)


def test_corrupt_suspension_proof_rejects_load(tmp_path):
    source = SuspensionSource(placeholder=True)
    dataset, manifest = prepare(tmp_path, source)
    proof_path = tmp_path / manifest["suspension_refs"][0]["path"]
    calls_before_load = list(source.calls)
    proof_path.write_text("{}\n", encoding="utf-8")

    with pytest.raises(PipelineError, match="suspension"):
        dataset.load(manifest["manifest_path"])
    assert source.calls == calls_before_load


def test_same_cache_replay_validates_proof_without_refetch(tmp_path):
    source = SuspensionSource(placeholder=True)
    dataset, manifest = prepare(tmp_path, source)
    source.calls.clear()
    source.reject_calls = True

    repeated = dataset.prepare(
        ResearchDataClient(source, context(), observed_clock=lambda: OBSERVED_AT),
        context(),
        SYMBOLS[::-1],
        snapshot_id="fixture-snapshot",
        sessions=3,
    )

    assert repeated == manifest
    assert source.calls == []
