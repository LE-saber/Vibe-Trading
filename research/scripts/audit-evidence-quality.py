"""Independently reconcile S3 Evidence with frozen raw source rows and canonical data."""

from collections import Counter
from contextlib import redirect_stdout
from io import StringIO
import argparse
import json
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from competition_core.pipeline.artifacts import atomic_bytes, atomic_json, file_hash, read_json
from competition_core.pipeline.persistence import PipelineRepository
from competition_core.storage.evidence import EvidenceRepository


def number(value):
    if value is None or isinstance(value, bool):
        return None
    value = float(value)
    return value if math.isfinite(value) else None


def nonnegative(value):
    value = number(value)
    return value if value is not None and value >= 0 else None


def same(actual, expected):
    if expected is None or isinstance(expected, bool):
        return actual is expected
    if isinstance(expected, (int, float)):
        return actual is not None and math.isclose(actual, expected, rel_tol=1e-10, abs_tol=1e-6)
    return actual == expected


def audit_run(directory: Path) -> dict:
    import pandas as pd

    directory = directory.resolve()
    if not (directory / "S3.json").is_file():
        raise ValueError("a completed S3 checkpoint is required")
    repository = PipelineRepository(directory.parent, directory.name)
    s2, s3 = repository.load_stage("S2"), repository.load_stage("S3")
    inputs = read_json(directory / "inputs.json")
    manifest = read_json(inputs["manifest_path"], expected_hash=inputs["manifest_hash"])
    panel_path = Path(inputs["dataset_root"]) / manifest["panel_path"]
    assert file_hash(panel_path) == manifest["panel_hash"]
    dates = manifest["session_dates"][-5:]
    symbols = s3["input_symbols"]
    panel = pd.read_parquet(panel_path, filters=[("symbol", "in", symbols), ("trade_date", "in", dates)]).set_index(["symbol", "trade_date"])
    store = EvidenceRepository()
    candidates = {row["symbol"]: row for row in s2["rows"]}
    batches, source_profile = {}, []
    for reference in s3["source_provenance"]:
        if "request_id" not in reference:
            continue
        batch = read_json(directory / "sources" / f"{reference['request_id']}.json")
        day = batch["request"]["trade_date"]
        batches[(batch["endpoint"], day)] = batch
        keys = [(row["ts_code"], row["trade_date"], row.get("reason") if batch["endpoint"] == "top_list" else None) for row in batch["data"]]
        assert len(keys) == len(set(keys)) and all(key[1] == day for key in keys)
        source_profile.append({"endpoint": batch["endpoint"], "trade_date": day, "status": batch["status"],
                               "source_rows": len(keys), "request_id": batch["request_id"], "observed_at": batch["observed_at"]})
    indexed = {key: {row["ts_code"]: row for row in batch["data"]} for key, batch in batches.items() if key[0] != "top_list"}
    checks, preview = [], []
    for candidate in s3["rows"]:
        symbol = candidate["symbol"]
        bundle = store.get(candidate["evidence_id"])
        def check(field, actual, expected):
            checks.append({"symbol": symbol, "field": field, "actual": actual, "expected": expected, "pass": same(actual, expected)})
        check("identity", bundle.symbol.symbol, symbol)
        check("snapshot", bundle.snapshot_id, inputs["snapshot_id"])
        check("as_of", bundle.as_of.isoformat(), inputs["context"]["as_of"])
        check("quant_score", candidate["quant_score"], candidates[symbol]["quant_score"])
        check("bundle.quant_score", bundle.sections["technical"]["quant"]["quant_score"], candidates[symbol]["quant_score"])
        latest = panel.loc[(symbol, dates[-1])]
        for field in ("volume_raw", "volume_shares", "amount_raw", "amount_cny"):
            check(f"market.{field}", bundle.sections["market"]["reference"][field], number(latest[field]))
        for field in ("open_qfq", "high_qfq", "low_qfq", "close_qfq", "close_raw", "adj_factor"):
            check(f"market.{field}", bundle.sections["market"]["prices"][field], number(latest[field]))
        check("volume_lots_to_shares", number(latest["volume_shares"]), number(latest["volume_raw"]) * 100)
        check("amount_thousands_to_cny", number(latest["amount_cny"]), number(latest["amount_raw"]) * 1000)
        flows, margins, amounts = [], [], []
        for day in dates:
            compact = day.replace("-", "")
            raw = indexed[("moneyflow", compact)].get(symbol, {})
            legs = [nonnegative(raw.get(field)) for field in ("buy_lg_amount", "sell_lg_amount", "buy_elg_amount", "sell_elg_amount")]
            value = (legs[0] - legs[1] + legs[2] - legs[3]) * 10000 if all(leg is not None for leg in legs) else None
            flows.append(value)
            row = next(row for row in bundle.sections["flow"]["rows"] if row["trade_date"] == day)
            check(f"flow.{day}.main", row["main"], value)
            net = number(raw.get("net_mf_amount"))
            check(f"flow.{day}.vendor_net", row["net"], net * 10000 if net is not None else None)
            raw_margin = indexed[("margin_detail", compact)].get(symbol, {})
            buy, repay = [nonnegative(raw_margin.get(field)) for field in ("rzmre", "rzche")]
            margins.append(buy - repay if buy is not None and repay is not None else None)
            amounts.append(number(panel.loc[(symbol, day), "amount_cny"]) if (symbol, day) in panel.index else None)
        flow_sum = sum(flows) if all(value is not None for value in flows) else None
        amount_sum = sum(amounts) if all(value is not None for value in amounts) else None
        flow_ratio = flow_sum / amount_sum if flow_sum is not None and amount_sum is not None and amount_sum > 0 else None
        check("flow.five_sessions", bundle.sections["flow"]["main_flow_5d_cny"], flow_sum)
        check("flow.latest", bundle.sections["flow"]["latest_main_flow_cny"], flows[-1])
        check("flow.ratio", bundle.sections["flow"]["main_flow_ratio_5d"], flow_ratio)
        check("flow.coverage", bundle.sections["flow"]["coverage"], sum(value is not None for value in flows) / 5)
        check("margin.net_five_sessions", bundle.sections["margin"]["financing_net_buy_5d_cny"], sum(margins) if all(value is not None for value in margins) else None)
        check("margin.latest_balance", bundle.sections["margin"]["latest_financing_balance_cny"], nonnegative(raw_margin.get("rzye")))
        dragon = batches[("top_list", dates[-1].replace("-", ""))]
        entries = [row for row in dragon["data"] if row["ts_code"] == symbol]
        check("dragon.listed", bundle.sections["dragon_tiger"]["listed"], bool(entries) if dragon["status"] == "SUCCESS" else None)
        check("dragon.nonaggregated_net", bundle.sections["dragon_tiger"]["latest_net_buy_cny"], entries[0]["net_amount"] if len(entries) == 1 else None)
        if candidate["selected"]:
            preview.append({"symbol": symbol, "rank": candidate["rank"], "quant_score": candidate["quant_score"],
                            "selection_score": candidate["selection_score"], "flow_5d_cny": flow_sum,
                            "evidence_coverage": candidate["evidence_coverage"], "risk_flags": candidate["risk_flags"]})
    failures = [check for check in checks if not check["pass"]]
    return {"run_id": directory.name, "status": "PASS" if not failures else "NOT_ACCEPTED", "production_pipeline_status": "NOT_ACCEPTED",
            "snapshot_id": inputs["snapshot_id"], "context": inputs["context"], "config_hash": inputs["config_hash"],
            "s3_file_hash": file_hash(directory / "S3.json"), "source_profile": source_profile,
            "check_count": len(checks), "failed_check_count": len(failures), "checks": checks,
            "coverage_incomplete_counts": s3["summary"]["coverage_incomplete_counts"],
            "selected": sorted(preview, key=lambda row: row["rank"]),
            "caveats": ["off_hours_historical_evidence_only", "not_realtime_or_account_acceptance", "source_observation_time_is_not_market_data_cutoff"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_id")
    args = parser.parse_args()
    directory = PipelineRepository(ROOT / "runs", args.run_id).directory
    cells, scope = [], {}
    def markdown(text):
        cells.append({"cell_type": "markdown", "metadata": {}, "source": text.splitlines(keepends=True)})
    def code(text):
        output = StringIO()
        with redirect_stdout(output):
            exec(compile(text, "evidence-quality-audit", "exec"), scope)
        count = 1 + sum(cell["cell_type"] == "code" for cell in cells)
        cells.append({"cell_type": "code", "metadata": {}, "source": text.splitlines(keepends=True), "execution_count": count,
                      "outputs": [{"output_type": "stream", "name": "stdout", "text": output.getvalue().splitlines(keepends=True)}]})
    markdown("# S3 source reconciliation\n## tl;dr\nSee executed Results below. This is historical Evidence validation, not a portfolio recommendation.")
    markdown("## Context & Methods\nGrain: candidate symbol × completed exchange session.\n### Key Assumptions\n"
             "Use the frozen S3 source responses, Core Evidence Store and hash-verified canonical panel; no source fetches, missing imputation or current quote substitution. "
             "Independent arithmetic is in scripts/audit-evidence-quality.py. Capture times are separate from data cutoffs.")
    code("import runpy\nfrom pathlib import Path\n"
         f"module = runpy.run_path({str(Path(__file__).resolve())!r}, run_name='evidence_audit_companion')\n"
         f"directory = Path({str(directory)!r})\n")
    markdown("## Data\nFull S2 candidate population; five dated market-wide flow/margin batches and one dated dragon-tiger query.")
    code("report = module['audit_run'](directory)\nprint(report['context'])\nprint(report['source_profile'])\n")
    markdown("## Results")
    code("print({key: report[key] for key in ('status','check_count','failed_check_count','coverage_incomplete_counts')})\n"
         "assert report['failed_check_count'] == 0\nprint(report['selected'][:5])\n")
    report = scope["report"]
    markdown(f"## Takeaways\n{report['check_count']} field comparisons; {report['failed_check_count']} mismatches. "
             "Coverage gaps remain explicit and must not be interpreted as zero activity. "
             "Code cells executed sequentially under Core Python; no Jupyter kernel is installed or claimed. "
             f"Rerun: `.venv/Scripts/python.exe scripts/audit-evidence-quality.py {directory.name}`.")
    target = directory / "evidence-quality-audit"
    atomic_json(target.with_suffix(".json"), report, immutable=True)
    notebook = {"cells": cells, "metadata": {"language_info": {"name": "python", "version": sys.version.split()[0]}}, "nbformat": 4, "nbformat_minor": 4}
    assert all(isinstance(cell["source"], list) for cell in cells)
    atomic_json(target.with_suffix(".ipynb"), notebook, immutable=True)
    print({key: report[key] for key in ("status", "check_count", "failed_check_count", "coverage_incomplete_counts")}, flush=True)
    print(target.with_suffix(".ipynb"), flush=True)


if __name__ == "__main__":
    main()
