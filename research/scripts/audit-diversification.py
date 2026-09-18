"""Independent frozen-panel rank correlation and finalist-constraint audit."""

import argparse
from collections import Counter
import math
from pathlib import Path
import statistics
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from competition_core.pipeline.artifacts import atomic_json, content_hash, read_json
from competition_core.pipeline.dataset import CanonicalDataset
from competition_core.pipeline.persistence import PipelineRepository


def ranks(values):
    # Independent average-rank implementation, not the production pandas helper.
    positions = {}
    for index, value in enumerate(sorted(values), 1):
        positions.setdefault(value, []).append(index)
    return [statistics.mean(positions[value]) for value in values]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_id")
    args = parser.parse_args()
    repo = PipelineRepository(ROOT / "runs", args.run_id)
    inputs = read_json(repo.directory / "inputs.json")
    manifest = read_json(inputs["manifest_path"])
    panel = CanonicalDataset(Path(inputs["dataset_root"]), manifest["source"]).load(Path(inputs["manifest_path"]))
    s3, s4 = repo.load_stage("S3"), repo.load_stage("S4")
    risk = inputs["config"]["risk"]
    symbols = sorted(s3["output_symbols"])
    dates = manifest["session_dates"][-risk["correlation_sessions"] - 1:]
    values = {}
    for symbol in symbols:
        bars = panel.loc[panel.symbol == symbol].set_index("trade_date")
        returns = {}
        for previous, day in zip(dates, dates[1:]):
            if previous not in bars.index or day not in bars.index:
                continue
            a, b = float(bars.loc[previous, "close_qfq"]), float(bars.loc[day, "close_qfq"])
            if math.isfinite(a) and math.isfinite(b) and a > 0 and b > 0:
                returns[day] = b / a - 1
        values[symbol] = returns
    failures, comparisons, edges = [], 0, []
    for a in symbols:
        for b in symbols:
            dates_shared = sorted(set(values[a]) & set(values[b]))
            x, y = [values[a][day] for day in dates_shared], [values[b][day] for day in dates_shared]
            expected = (statistics.correlation(ranks(x), ranks(y)) if len(x) >= risk["min_pair_observations"]
                        and len(set(x)) > 1 and len(set(y)) > 1 else None)
            actual = s4["correlation"]["correlation_matrix"][a][b]
            count = s4["correlation"]["observation_counts"][a][b]
            ok = count == len(x) and (actual is None if expected is None else actual is not None and abs(expected - actual) < 1e-12)
            comparisons += 1
            if not ok:
                failures.append({"left": a, "right": b, "expected": expected, "actual": actual, "expected_count": len(x), "actual_count": count})
            if a < b and expected is not None and abs(expected) >= risk["corr_threshold"]:
                edges.append((a, b))
    # Reconstruct graph independently, then check selection capacity.
    graph = {symbol: set() for symbol in symbols}
    for a, b in edges:
        graph[a].add(b)
        graph[b].add(a)
    pending, components = set(symbols), []
    while pending:
        stack, component = [min(pending)], set()
        while stack:
            symbol = stack.pop()
            if symbol in component:
                continue
            component.add(symbol)
            stack.extend(graph[symbol] - component)
        pending -= component
        components.append(sorted(component))
    selected = set(s4["output_symbols"])
    actual_components = sorted(sorted(group["symbols"]) for group in s4["correlation"]["clusters"])
    checks = {"cluster_membership": sorted(components) == actual_components,
              "cluster_caps": all(len(selected & set(group)) <= risk["max_candidates_per_cluster"] for group in components),
              "count_10_to_target": 10 <= len(selected) <= inputs["config"]["stages"]["s4_target_n"],
              "no_relaxation": s4["summary"]["constraints_relaxed"] is False,
              "no_llm_or_tool": s4["summary"]["llm_calls"] == s4["summary"]["agent_tool_calls"] == 0}
    originals = {row["symbol"]: row for row in s3["rows"]}
    checks["scores_preserved"] = all(row["quant_score"] == originals[row["symbol"]]["quant_score"]
                                     and row["selection_score"] == originals[row["symbol"]]["selection_score"] for row in s4["rows"])
    checks["all_decisions_explained"] = all(row["keep_reasons"] if row["selected"] else row["reject_reasons"] for row in s4["rows"])
    kept = [row for row in s4["rows"] if row["selected"]]
    checks["overheat_cap"] = sum(row["overheat_score"] >= risk["overheat_threshold"] for row in kept) <= risk["max_overheated_candidates"]
    sectors = Counter((row["sector_metadata"]["source"], row["sector_metadata"]["classification"], row["sector_metadata"]["value"])
                      for row in kept if row["sector_metadata"]["status"] == "available")
    checks["available_sector_cap"] = all(count <= risk["max_candidates_per_sector"] for count in sectors.values())
    report = {"schema_version": "p10.s4.independent_audit.v1", "run_id": args.run_id,
              "status": "PASS" if all(checks.values()) and not failures else "NOT_ACCEPTED",
              "production_pipeline_status": "NOT_ACCEPTED", "checks": checks,
              "pair_comparisons": comparisons, "mismatches": failures, "high_correlation_pairs": edges,
              "components": components, "selected_symbols": s4["output_symbols"],
              "sector_unavailable_count": s4["summary"]["sector_unavailable_count"],
              "reference_date": inputs["context"]["market_data_reference_date"], "config_hash": inputs["config_hash"],
              "s4_semantic_hash": s4["semantic_hash"], "manifest_hash": inputs["manifest_hash"],
              "method": "independent_average_ranks_statistics_correlation_on_verified_frozen_panel"}
    report["report_id"] = content_hash(report)
    atomic_json(repo.directory / "diversification-audit.json", report, immutable=True)
    print({key: report[key] for key in ("status", "pair_comparisons", "mismatches", "checks")}, flush=True)
    raise SystemExit(0 if report["status"] == "PASS" else 1)


if __name__ == "__main__":
    main()
