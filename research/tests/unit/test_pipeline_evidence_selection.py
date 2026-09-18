"""Synthetic S3 selection: evidence never overwrites the baseline score."""

import copy

import pytest

from adapters.vibe.evidence import evidence_from_vibe_result
from competition_core.pipeline.config import PipelineConfig
from competition_core.pipeline.evidence_builder import evidence_policy, select_evidence
from competition_core.pipeline.errors import PipelineError
from tests.pipeline_fixtures import context, small_config


def inputs(count=12):
    rows, bundles = [], {}
    for i in range(count):
        symbol = f"600{i + 1:03d}.SH"
        rows.append(dict(symbol=symbol, rank=i + 1, quant_score=80 - i, data_confidence=.9,
                         features={"latest_volume_raw": 100 + i}, selected=True))
        values = {"flow": {"main_flow_ratio_5d": (i - 5) / 100, "coverage": 1.},
                  "margin": {"coverage": 1.}, "dragon_tiger": {"coverage": 1., "listed": False},
                  "risk": {"flags": []}}
        bundles[symbol] = evidence_from_vibe_result(dict(symbol=symbol, as_of=context().as_of.isoformat(), snapshot_id="fixture",
            sections={name: dict(values=body, source="synthetic-fixture", as_of=context().as_of.isoformat()) for name, body in values.items()}, missing=[]))
    return rows, bundles


def test_selection_is_deterministic_keeps_all_rows_scores_and_raw_volume():
    rows, bundles = inputs()
    original = copy.deepcopy(rows)
    result = select_evidence(rows, bundles, small_config())
    assert result == select_evidence(rows[::-1], bundles, small_config())
    assert rows == original
    assert len(result["output_symbols"]) == 10 and len(result["rows"]) == 12
    for prior, after in zip(original, result["rows"]):
        assert prior["quant_score"] == after["quant_score"]
        assert prior["features"] == after["features"]
        assert after["evidence_id"] and after["keep_reasons"] + after["reject_reasons"]


def test_missing_flow_removes_weight_without_zero_imputation():
    rows, bundles = inputs(1)
    payload = bundles[rows[0]["symbol"]].to_dict()
    payload.pop("evidence_id")
    payload["flow"].update(main_flow_ratio_5d=None, coverage=0.)
    from contracts.evidence import EvidenceBundle
    bundles[rows[0]["symbol"]] = EvidenceBundle.from_dict(payload)
    row = select_evidence(rows, bundles, small_config())["rows"][0]
    assert row["main_flow_ratio_5d"] is None and row["s3_components"]["flow"] is None
    assert row["s3_effective_weights"]["flow"] is None
    assert row["s3_effective_weights"]["quant"] == pytest.approx(.85 / .9)
    assert row["evidence_confidence"] == pytest.approx(2 / 3)


def test_low_confidence_excluded_even_with_best_quant_score_not_padded():
    rows, bundles = inputs(10)
    rows[0].update(quant_score=100, data_confidence=.37)
    result = select_evidence(rows, bundles, small_config())
    assert result["status"] == "PARTIAL" and len(result["output_symbols"]) == 9
    assert result["rows"][0]["reject_reasons"] == ["data_confidence_below_s3_minimum"]


def test_old_config_cannot_silently_gain_evidence_weights():
    payload = small_config().to_dict()
    payload.pop("evidence")
    with pytest.raises(PipelineError, match="explicit versioned"):
        evidence_policy(PipelineConfig.from_dict(payload))


def test_flow_unit_inconsistency_retained_but_not_rewarded():
    rows, bundles = inputs(1)
    symbol = rows[0]["symbol"]
    payload = bundles[symbol].to_dict()
    payload.pop("evidence_id")
    payload["flow"]["main_flow_ratio_5d"] = 3.0
    from contracts.evidence import EvidenceBundle
    bundles[symbol] = EvidenceBundle.from_dict(payload)
    row = select_evidence(rows, bundles, small_config())["rows"][0]
    assert row["main_flow_ratio_5d"] == 3.0
    assert row["s3_components"]["flow"] is None
    assert "flow_ratio_outside_actual_amount_bound" in row["risk_flags"]
