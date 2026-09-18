"""Reproduce actual cross-ex-date momentum from a frozen canonical manifest."""

import argparse
import json
import math
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from competition_core.pipeline.artifacts import atomic_json, content_hash, file_hash
from competition_core.universe.ranking import _qfq_price


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True, type=Path)
    args = parser.parse_args()
    manifest_path = args.manifest.resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["manifest_hash"] == content_hash({k: v for k, v in manifest.items() if k != "manifest_hash"})
    root = manifest_path.parents[2]
    panel_path = root / manifest["panel_path"]
    assert file_hash(panel_path) == manifest["panel_hash"]
    symbols = ["300750.SZ", "600519.SH", "000001.SZ"]
    panel = pd.read_parquet(panel_path, filters=[("symbol", "in", symbols)])
    calendar = manifest["session_dates"]
    examples = []
    for symbol, group in panel.groupby("symbol", sort=True):
        group = group.set_index("trade_date").reindex(calendar)
        changes = group.adj_factor.ne(group.adj_factor.shift()) & group.adj_factor.shift().notna() & group.adj_factor.notna()
        anchor = group.loc[manifest["market_data_reference_date"], "adj_factor"]
        for day in group.index[changes]:
            location = calendar.index(day)
            for horizon in (1, 5, 20):
                if location < horizon:
                    continue
                previous = group.iloc[location - horizon]
                current = group.loc[day]
                if previous.isna().any() or current.isna().any():
                    continue
                adjusted_now = _qfq_price(float(current.close_raw), float(current.adj_factor), float(anchor))
                adjusted_before = _qfq_price(float(previous.close_raw), float(previous.adj_factor), float(anchor))
                expected = adjusted_now / adjusted_before - 1
                actual = current.close_qfq / previous.close_qfq - 1
                assert math.isclose(actual, expected, rel_tol=1e-11, abs_tol=1e-12)
                assert current.volume_shares == current.volume_raw * 100
                examples.append({"symbol": symbol, "date": day, "horizon_sessions": horizon,
                                 "raw_return": float(current.close_raw / previous.close_raw - 1),
                                 "qfq_return": float(actual), "core_qfq_return": float(expected),
                                 "raw_volume_lots": float(current.volume_raw),
                                 "volume_shares": float(current.volume_shares)})
    assert {row["symbol"] for row in examples} == set(symbols)
    result = {"status": "PASS", "production_pipeline_pass": False,
              "snapshot_id": manifest["snapshot_id"], "dataset_id": manifest["dataset_id"],
              "panel_hash": manifest["panel_hash"], "examples": examples}
    target = ROOT / "storage/artifacts/pipeline-engineering" / f"momentum-{manifest['dataset_id']}.json"
    atomic_json(target, result, immutable=True)
    print(json.dumps({"path": str(target), "checks": len(examples), "examples": examples[:3]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
