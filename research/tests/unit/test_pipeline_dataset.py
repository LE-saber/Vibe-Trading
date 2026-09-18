from datetime import date, datetime

import pandas as pd
import pytest

from competition_core.pipeline.artifacts import read_json
from competition_core.pipeline.dataset import CanonicalDataset
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.sources import ResearchDataClient
from competition_core.pipeline.time_context import ResearchContext

SYMBOLS = ["000001.SZ", "300750.SZ", "600519.SH"]


def context(reference=date(2026, 9, 1)):
    return ResearchContext(date(2026, 9, 2), "intraday", reference,
                           datetime.fromisoformat("2026-09-02T10:00:00+08:00"))


class Source:
    """Explicit engineering fixture; no source request in these tests is live."""

    def __init__(self):
        self.calls = []
        self.fail = None
        self.missing_factor = False
        self.ignore_offset = False
        self.wrong_date = False

    def __getattr__(self, endpoint):
        def fetch(**params):
            self.calls.append((endpoint, dict(params)))
            if self.fail == (endpoint, params.get("trade_date")):
                raise RuntimeError("fixture source failure")
            if endpoint == "trade_cal":
                rows = [dict(exchange="SSE", cal_date=day.strftime("%Y%m%d"), is_open=1, pretrade_date="")
                        for day in pd.bdate_range(params["start_date"], params["end_date"])]
                cap = 6000
            else:
                trade_date = params["trade_date"]
                factor = 2.0 if trade_date >= "20260901" else 1.0
                price = 10.0 / factor
                rows = []
                for symbol in SYMBOLS:
                    row = dict(ts_code=symbol, trade_date="20260902" if self.wrong_date else trade_date)
                    if endpoint == "daily":
                        row.update(open=price, high=price * 1.1, low=price * .9, close=price, vol=100., amount=200.)
                    elif endpoint == "adj_factor":
                        if self.missing_factor and symbol == "300750.SZ" and trade_date == "20260831":
                            continue
                        row.update(adj_factor=factor)
                    else:
                        row.update(turnover_rate=1., volume_ratio=1.1, total_mv=10000., circ_mv=9000.)
                    rows.append(row)
                cap = 2  # Compatible vendor imposes a smaller cap than requested.
            offset = 0 if self.ignore_offset else params.get("offset", 0)
            return pd.DataFrame(rows[offset:offset + cap])
        return fetch


def wrapped(source, ctx=None):
    return ResearchDataClient(source, ctx or context(),
                              observed_clock=lambda: datetime.fromisoformat("2026-09-02T11:00:00+08:00"))


def prepare(tmp_path, source=None, sessions=3):
    source = source or Source()
    dataset = CanonicalDataset(tmp_path, "fixture")
    manifest = dataset.prepare(wrapped(source), context(), SYMBOLS, snapshot_id="fixture-snapshot", sessions=sessions)
    return dataset, manifest, source


def test_qfq_around_corporate_action_leaves_raw_volume_and_amount_unchanged(tmp_path):
    dataset, manifest, _ = prepare(tmp_path)
    panel = dataset.load(manifest["manifest_path"])
    catl = panel.loc[panel.symbol == "300750.SZ"]
    assert catl.close_raw.tolist() == [10., 10., 5.]
    assert catl.adj_factor.tolist() == [1., 1., 2.]
    for field, expected in (("open", 5.), ("close", 5.), ("high", 5.5), ("low", 4.5)):
        assert catl[f"{field}_qfq"].tolist() == pytest.approx([expected] * 3)
    assert catl.volume_raw.tolist() == [100.] * 3
    assert catl.volume_shares.tolist() == [10000.] * 3
    assert catl.amount_raw.tolist() == [200.] * 3
    assert catl.amount_cny.tolist() == [200000.] * 3
    assert catl.close_qfq.pct_change().dropna().tolist() == [0., 0.]
    assert len(manifest["batch_refs"]) == 10


def test_missing_date_factor_stays_missing_without_raw_or_backfill(tmp_path):
    source = Source()
    source.missing_factor = True
    dataset, manifest, _ = prepare(tmp_path, source)
    panel = dataset.load(manifest["manifest_path"])
    row = panel.loc[(panel.symbol == "300750.SZ") & (panel.trade_date == "2026-08-31")].iloc[0]
    assert pd.isna(row.close_qfq)
    assert row.close_raw == 10
    assert row.volume_raw == 100
    quality = read_json(tmp_path / manifest["quality_path"])
    catl = next(row for row in quality["per_symbol"] if row["symbol"] == "300750.SZ")
    assert catl["missing_factor_count"] == 1
    assert catl["qfq_bars"] == 2


def test_pagination_and_same_manifest_replay_do_not_refetch(tmp_path):
    dataset, manifest, source = prepare(tmp_path)
    assert [params["offset"] for endpoint, params in source.calls
            if endpoint == "daily" and params["trade_date"] == "20260901"] == [0, 2, 3]
    before = len(source.calls)
    repeated = dataset.prepare(wrapped(source), context(), SYMBOLS[::-1], snapshot_id="fixture-snapshot", sessions=3)
    assert repeated == manifest
    assert len(source.calls) == before
    pd.testing.assert_frame_equal(dataset.load(manifest["manifest_path"]), dataset.load(repeated["manifest_path"]))


@pytest.mark.parametrize("fault,pattern", [("ignore_offset", "pagination"), ("wrong_date", "outside requested")])
def test_invalid_pagination_and_future_rows_fail_closed(tmp_path, fault, pattern):
    source = Source()
    setattr(source, fault, True)
    with pytest.raises(PipelineError, match=pattern):
        prepare(tmp_path, source)


@pytest.mark.parametrize("endpoint", ["daily", "adj_factor", "daily_basic"])
def test_whole_market_empty_batch_is_not_success(tmp_path, endpoint):
    source = Source()
    setattr(source, endpoint, lambda **_: pd.DataFrame())
    with pytest.raises(PipelineError, match="whole-market batch is empty"):
        prepare(tmp_path, source)


def test_partial_failure_preserves_completed_day_batches(tmp_path):
    source = Source()
    source.fail = ("adj_factor", "20260831")
    with pytest.raises(PipelineError, match="source request failed"):
        prepare(tmp_path, source)
    source.fail = None
    source.calls.clear()
    dataset, manifest, _ = prepare(tmp_path, source)
    assert not any(params.get("trade_date") == "20260828" for _, params in source.calls)
    assert not any(endpoint == "daily" and params.get("trade_date") == "20260831" for endpoint, params in source.calls)
    assert len(dataset.load(manifest["manifest_path"])) == 9


@pytest.mark.parametrize("artifact", ["panel_path", "quality_path", "batch"])
def test_corrupt_artifacts_are_rejected_on_replay(tmp_path, artifact):
    dataset, manifest, _ = prepare(tmp_path)
    path = tmp_path / (manifest["batch_refs"][1]["path"] if artifact == "batch" else manifest[artifact])
    path.write_bytes(b"corrupt fixture")
    with pytest.raises(PipelineError, match="hash mismatch"):
        dataset.load(manifest["manifest_path"])


def test_manifest_tampering_is_rejected(tmp_path):
    dataset, manifest, _ = prepare(tmp_path)
    path = tmp_path / manifest["manifest_path"]
    path.write_text(path.read_text(encoding="utf-8").replace("fixture-snapshot", "other-snapshot"), encoding="utf-8")
    with pytest.raises(PipelineError, match="manifest content hash"):
        dataset.load(path)


def test_full_120_session_quality_and_no_future_factor_anchor(tmp_path):
    dataset, manifest, source = prepare(tmp_path, sessions=120)
    panel = dataset.load(manifest["manifest_path"])
    quality = read_json(tmp_path / manifest["quality_path"])
    assert quality["full_qfq_history_ratio"] == 1
    assert panel.groupby("symbol").size().tolist() == [120] * 3
    assert all(params.get("trade_date", "") <= "20260901" for _, params in source.calls)
    assert panel.trade_date.max() == "2026-09-01"


def test_additional_session_reuses_overlapping_day_batches(tmp_path):
    _, first, source = prepare(tmp_path, sessions=2)
    source.calls.clear()
    dataset, second, _ = prepare(tmp_path, source, sessions=3)
    assert first["dataset_id"] != second["dataset_id"]
    assert {params["trade_date"] for endpoint, params in source.calls if endpoint != "trade_cal"} == {"20260828"}
    assert len(dataset.load(second["manifest_path"])) == 9


def test_interruption_before_final_manifest_is_recoverable(tmp_path, monkeypatch):
    import competition_core.pipeline.dataset as module

    source = Source()
    original_write = module.atomic_json

    def interrupted(path, value, **kwargs):
        if str(path).endswith("manifest.json"):
            raise RuntimeError("fixture process stopped before commit")
        return original_write(path, value, **kwargs)

    monkeypatch.setattr(module, "atomic_json", interrupted)
    with pytest.raises(RuntimeError, match="process stopped"):
        prepare(tmp_path, source)
    assert list(tmp_path.glob("datasets/*/panel.parquet"))
    assert not list(tmp_path.glob("datasets/*/manifest.json"))
    monkeypatch.setattr(module, "atomic_json", original_write)
    source.calls.clear()
    dataset, resumed, _ = prepare(tmp_path, source)
    assert source.calls == []
    assert len(dataset.load(resumed["manifest_path"])) == 9


def test_cached_batches_do_not_bypass_observation_clock(tmp_path):
    dataset, _, source = prepare(tmp_path)
    client = ResearchDataClient(source, context(), observed_clock=lambda: datetime.fromisoformat("2026-09-02T09:00:00+08:00"))
    with pytest.raises(PipelineError, match="observation clock"):
        dataset.prepare(client, context(), SYMBOLS, snapshot_id="fixture-snapshot", sessions=3)


def test_mid_page_failure_reuses_verified_pages_at_later_observation(tmp_path):
    source = Source()
    original_daily = source.daily
    interrupted = True

    def daily(**params):
        if interrupted and params["trade_date"] == "20260828" and params["offset"] == 2:
            raise RuntimeError("fixture interrupted after first page")
        return original_daily(**params)

    source.daily = daily
    with pytest.raises(PipelineError):
        prepare(tmp_path, source)
    interrupted = False
    source.calls.clear()
    client = ResearchDataClient(source, context(),
                               observed_clock=lambda: datetime.fromisoformat("2026-09-02T11:20:00+08:00"))
    dataset = CanonicalDataset(tmp_path, "fixture")
    manifest = dataset.prepare(client, context(), SYMBOLS, snapshot_id="fixture-snapshot", sessions=3)
    assert not any(endpoint == "daily" and params["trade_date"] == "20260828" and params["offset"] == 0
                   for endpoint, params in source.calls)
    assert len(dataset.load(manifest["manifest_path"])) == 9


@pytest.mark.parametrize("invalid", [0., -1., float("inf"), float("nan")])
def test_invalid_anchor_factor_never_becomes_raw_price_fallback(tmp_path, invalid):
    source = Source()
    fetch = source.adj_factor

    def altered(**params):
        frame = fetch(**params)
        if not frame.empty and params["trade_date"] == "20260901":
            frame.loc[frame.ts_code == "300750.SZ", "adj_factor"] = invalid
        return frame

    source.adj_factor = altered
    dataset, manifest, _ = prepare(tmp_path, source)
    panel = dataset.load(manifest["manifest_path"])
    assert panel.loc[panel.symbol == "300750.SZ", "close_qfq"].isna().all()
    assert panel.loc[panel.symbol == "300750.SZ", "volume_raw"].eq(100).all()
