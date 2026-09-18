"""Constraint behavior is testable without network, scoring, or an LLM."""

import copy
import json

import pytest

from competition_core.pipeline.config import PipelineConfig, load_pipeline_config
from competition_core.pipeline.diversification import select_finalists
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.artifacts import content_hash
from contracts.symbol import CanonicalSymbol
from tests.pipeline_fixtures import context


def inputs(count=20):
    symbols = [f"600{i:03d}.SH" for i in range(1, count + 1)]
    candidates = [{"symbol": symbol, "rank": i + 1, "selection_score": 100 - i,
                   "quant_score": 80 - i, "overheat_score": 30., "features": {"rsi_14": 50.},
                   "selected": True, "evidence_id": f"evidence-{i}"} for i, symbol in enumerate(symbols)]
    members = {symbol: {"ts_code": symbol, "exchange": CanonicalSymbol.from_core(symbol).exchange,
                        "is_eligible": True, "board": "main_board"} for symbol in symbols}
    correlation = {"symbols": symbols, "reference_date": "2026-09-01",
                   "correlation_matrix": {a: {b: 1. if a == b else .1 for b in symbols} for a in symbols},
                   "clusters": [{"cluster_id": symbol, "symbols": [symbol]} for symbol in symbols]}
    return candidates, members, correlation


def select(data, config=None):
    candidates, members, correlation = data
    return select_finalists(candidates=candidates, members=members, correlation=correlation,
                             context=context(), config=config or load_pipeline_config())


def test_cluster_selection_not_simple_top12_and_preserves_scores_inputs():
    data = inputs()
    symbols = data[2]["symbols"]
    data[2]["clusters"] = [{"cluster_id": "correlated", "symbols": symbols[:4]}, *data[2]["clusters"][4:]]
    before = copy.deepcopy(data)
    result = select(data)
    assert result["status"] == "SUCCESS" and len(result["output_symbols"]) == 12
    assert result["output_symbols"][:3] == [symbols[0], symbols[1], symbols[4]]
    assert result["summary"]["differs_from_naive_top"]
    rejected = {row["symbol"]: row for row in result["rows"]}
    assert "correlation_cluster_cap" in rejected[symbols[2]]["reject_reasons"]
    assert data == before
    assert all(row["quant_score"] == before[0][i]["quant_score"] and row["selection_score"] == before[0][i]["selection_score"]
               for i, row in enumerate(result["rows"]))


def test_verified_sector_cap_but_unknown_sector_not_one_invented_industry():
    data = inputs()
    assert len(select(data)["output_symbols"]) == 12  # No industry data for every stock.
    for member in list(data[1].values())[:5]:
        member["sector_metadata"] = {"value": "sector-A", "classification": "test-v1", "source": "fixture.PIT",
                                     "as_of": "2026-09-01T15:00:00+08:00"}
    result = select(data)
    assert result["summary"]["sector_counts"][0]["count"] == 3
    assert "verified_sector_cap" in result["rows"][3]["reject_reasons"]
    assert result["summary"]["sector_unavailable_count"] == 15


def test_style_cap_and_missing_heat_cannot_be_claimed_cool():
    data = inputs()
    for row in data[0][:10]:
        row["overheat_score"] = 90.
    data[0][11]["overheat_score"] = None
    result = select(data)
    assert len(result["output_symbols"]) == 12 and result["summary"]["selected_overheated_count"] == 6
    assert "overheated_style_cap" in result["rows"][6]["reject_reasons"]
    assert "missing_or_invalid_overheat_score" in result["rows"][11]["reject_reasons"]


def test_hard_constraints_never_relaxed_to_pad_shortfall():
    data = inputs()
    data[2]["clusters"] = [{"cluster_id": "all", "symbols": data[2]["symbols"]}]
    result = select(data)
    assert result["status"] == "PARTIAL" and len(result["output_symbols"]) == 2
    assert result["summary"]["shortfall"] == 10 and not result["summary"]["constraints_relaxed"]


def test_unknown_pair_not_treated_as_independence():
    data = inputs()
    a, b = data[2]["symbols"][:2]
    data[2]["correlation_matrix"][a][b] = data[2]["correlation_matrix"][b][a] = None
    result = select(data)
    assert b not in result["output_symbols"]
    assert "unknown_correlation_to_selected" in result["rows"][1]["reject_reasons"]


def test_return_window_requires_one_more_price_session():
    payload = load_pipeline_config().to_dict()
    payload["risk"]["correlation_sessions"] = payload["dataset"]["warmup_sessions"]
    with pytest.raises(PipelineError):
        PipelineConfig.from_dict(payload)


def test_numeric_cluster_ids_survive_json_hash_roundtrip():
    data = inputs()
    for number, cluster in enumerate(data[2]["clusters"], 1):
        cluster["cluster_id"] = number
    result = select(data)
    assert content_hash(result) == content_hash(json.loads(json.dumps(result)))


def test_stable_ties_and_permuted_input_order():
    data = inputs()
    for row in data[0]:
        row["selection_score"] = 50.
    expected = select(data)
    data[0].reverse()
    data[2]["clusters"].reverse()
    # Correlation output normally sorts clusters; only selector records unchanged object.
    actual = select(data)
    assert expected["output_symbols"] == actual["output_symbols"] == sorted(data[2]["symbols"])[:12]
    assert expected["rows"] == actual["rows"]


@pytest.mark.parametrize("fault", ["future_sector", "naive_sector", "ineligible", "duplicate", "future_reference"])
def test_invalid_identity_and_time_fail_closed(fault):
    data = inputs()
    first = data[2]["symbols"][0]
    if fault.endswith("sector"):
        data[1][first]["sector_metadata"] = {"value": "A", "classification": "fixture", "source": "PIT",
            "as_of": "2026-09-03T15:00:00+08:00" if fault == "future_sector" else "2026-09-01T15:00:00"}
    elif fault == "ineligible":
        data[1][first]["is_eligible"] = False
    elif fault == "duplicate":
        data[0].append(copy.deepcopy(data[0][0]))
    else:
        data[2]["reference_date"] = "2026-09-02"
    with pytest.raises(PipelineError):
        select(data)
