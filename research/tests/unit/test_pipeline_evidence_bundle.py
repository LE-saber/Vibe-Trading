from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from datetime import date, datetime
import math

import pytest

from competition_core.pipeline.evidence_bundle import build_evidence
from competition_core.pipeline.time_context import ResearchContext
from contracts.evidence import EvidenceContractError


SYMBOL = "600519.SH"
SNAPSHOT_ID = "synthetic-snapshot"
REFERENCE = "2026-09-01"


def _context() -> ResearchContext:
    return ResearchContext(
        research_date=date(2026, 9, 2),
        phase="intraday",
        market_data_reference_date=date.fromisoformat(REFERENCE),
        as_of=datetime.fromisoformat("2026-09-02T14:50:00+08:00"),
    )


def _candidate(*, reference_daily: dict | None = None) -> dict:
    row = {
        "symbol": SYMBOL,
        "rank": 3,
        "quant_score": 61.25,
        "trend_score": 70.0,
        "momentum_score": 0.0,
        "relative_strength_score": -2.5,
        "liquidity_score": 55.0,
        "volatility_quality_score": 48.0,
        "data_quality_score": 99.0,
        "component_scores": {
            "trend": 70.0,
            "momentum": 0.0,
            "relative_strength": -2.5,
            "liquidity": 55.0,
            "volatility_quality": 48.0,
            "data_quality": 99.0,
        },
        "feature_percentiles": {"macd_histogram": 0.0, "momentum_5d": 25.0},
        "effective_weights": {"trend": 0.25, "momentum": 0.25},
        "overheat_score": 0.0,
        "data_confidence": 0.875,
        "features": {
            "latest_close_qfq": 101.2,
            "latest_close_raw": 100.0,
            "latest_volume_raw": 999.0,
            "latest_volume_shares": 99900.0,
            "latest_amount_raw": 888.0,
            "latest_amount_cny": 888000.0,
            "rsi_14": 0.0,
            "macd_histogram": -0.25,
            "momentum_5d": -0.01,
            "turnover_rate": 0.0,
            "volume_ratio": 0.0,
            "technical_missing_fields": [],
        },
    }
    if reference_daily is not None:
        row["reference_daily"] = reference_daily
    return row


def _member() -> dict:
    return {
        "ts_code": SYMBOL,
        "name": "synthetic stock",
        "exchange": "SSE",
        "market": "主板",
        "list_date": "2001-08-27",
        "trade_rule_metadata": {"price_limit": 0.1, "lot_size": 100},
        "exclusion_reasons": [],
    }


def _reference_daily() -> dict:
    return {
        "symbol": SYMBOL,
        "trade_date": REFERENCE,
        "open_raw": 99.0,
        "high_raw": 103.0,
        "low_raw": 98.0,
        "close_raw": 100.0,
        "open_qfq": 100.2,
        "high_qfq": 104.2,
        "low_qfq": 99.2,
        "close_qfq": 101.2,
        "volume_raw": 123.0,
        "amount_raw": 456.0,
        "volume_shares": 12300.0,
        "amount_cny": 456000.0,
        "adj_factor": 1.01,
        "total_mv": 123456.0,
        "circ_mv": 100000.0,
        "turnover_rate": 1.5,
        "volume_ratio": 0.9,
    }


def _supplements() -> dict:
    return {
        "flow": {
            "as_of": REFERENCE,
            "source": "tushare.moneyflow",
            "coverage": 1.0,
            "main_flow_5d_cny": -12.0,
            "risk_flags": ["main_flow_5d_negative"],
        },
        "dragon_tiger": {
            "as_of": REFERENCE,
            "source": "tushare.top_list",
            "coverage": 1.0,
            "listed": False,
            "entries": [],
            "risk_flags": [],
        },
        "margin": {
            "as_of": REFERENCE,
            "source": "tushare.margin_detail",
            "coverage": 0.0,
            "trade_date": REFERENCE,
            "financing_net_buy_5d_cny": None,
            "risk_flags": ["financing_unknown"],
        },
    }


def _build(candidate: dict | None = None, member: dict | None = None, supplements: dict | None = None):
    return build_evidence(
        candidate=candidate or _candidate(reference_daily=_reference_daily()),
        member=member or _member(),
        context=_context(),
        snapshot_id=SNAPSHOT_ID,
        source="tushare",
        supplements=supplements or _supplements(),
    )


def _leaves(value, prefix: str):
    if isinstance(value, Mapping):
        for key, item in value.items():
            yield from _leaves(item, f"{prefix}.{key}")
    else:
        yield prefix, value


def test_build_is_deterministic_and_maps_reference_daily_without_mutation() -> None:
    candidate = _candidate(reference_daily=_reference_daily())
    original_features = deepcopy(candidate["features"])
    first = _build(candidate=candidate)
    second = _build(candidate=candidate)

    assert first.evidence_id == second.evidence_id
    assert set(first.sections) == {
        "market",
        "technical",
        "flow",
        "margin",
        "dragon_tiger",
        "fundamental",
        "events",
        "risk",
    }
    assert candidate["features"] == original_features
    assert first.sections["market"]["prices"]["close_qfq"] == 101.2
    assert first.sections["market"]["reference"]["volume_raw"] == 123.0
    assert first.sections["market"]["reference"]["volume_shares"] == 12300.0
    assert first.sections["market"]["reference"]["amount_raw"] == 456.0
    assert first.sections["market"]["reference"]["amount_cny"] == 456000.0
    assert first.sections["fundamental"]["total_mv"] == 123456.0
    assert first.sections["fundamental"]["circ_mv"] == 100000.0
    assert first.sections["fundamental"]["market_cap_unit"] == "万元"


def test_none_is_missing_zero_and_signed_values_are_preserved() -> None:
    bundle = _build()

    assert bundle.sections["technical"]["features"]["macd_histogram"] == -0.25
    assert bundle.sections["technical"]["features"]["rsi_14"] == 0.0
    assert bundle.sections["flow"]["main_flow_5d_cny"] == -12.0
    assert bundle.sections["dragon_tiger"]["entries"] == []
    assert bundle.sections["margin"]["financing_net_buy_5d_cny"] is None
    assert "margin.financing_net_buy_5d_cny" in bundle.missing
    assert "events.news" in bundle.missing
    assert bundle.sections["events"]["coverage"] == "not_requested"
    assert all(value is not None for value in bundle.sections["market"]["reference"].values())


def test_technical_quant_preserves_s2_values_and_each_nonnull_leaf_is_provenanced() -> None:
    candidate = _candidate(reference_daily=_reference_daily())
    bundle = _build(candidate=candidate)
    quant = bundle.sections["technical"]["quant"]

    assert quant["quant_score"] == candidate["quant_score"]
    assert quant["component_scores"] == candidate["component_scores"]
    assert quant["feature_percentiles"] == candidate["feature_percentiles"]
    assert quant["effective_weights"] == candidate["effective_weights"]
    assert quant["s2rank"] == candidate["rank"]
    assert {item.field for item in bundle.provenance} == {
        path for section, value in bundle.sections.items() for path, leaf in _leaves(value, section)
        if leaf is not None
    }
    sources = {item.field: item.source for item in bundle.provenance}
    assert sources["market.identity.name"] == f"competition_universe({SNAPSHOT_ID})"
    assert sources["technical.quant.quant_score"] == "competition_pipeline.s2"
    assert sources["market.prices.close_qfq"] == "tushare.daily+adj_factor"
    assert sources["technical.features.rsi_14"] == "pure_vibe_helpers"
    assert sources["fundamental.total_mv"] == "tushare.daily_basic"
    assert sources["flow.main_flow_5d_cny"] == "tushare.moneyflow"
    assert sources["margin.coverage"] == "tushare.margin_detail"


@pytest.mark.parametrize(
    "change",
    [
        lambda candidate, member, supplements: member.update(ts_code="600000.SH"),
        lambda candidate, member, supplements: member.update(exchange="SZSE"),
        lambda candidate, member, supplements: supplements["flow"].update(as_of="2026-09-03"),
        lambda candidate, member, supplements: candidate["reference_daily"].update(trade_date="2026-09-03"),
    ],
)
def test_identity_and_future_mismatches_fail_closed(change) -> None:
    candidate = _candidate(reference_daily=_reference_daily())
    member = _member()
    supplements = _supplements()
    change(candidate, member, supplements)

    with pytest.raises((EvidenceContractError, ValueError)):
        _build(candidate=candidate, member=member, supplements=supplements)


def test_nested_nonfinite_candidate_value_is_rejected() -> None:
    candidate = _candidate(reference_daily=_reference_daily())
    candidate["component_scores"]["trend"] = math.nan

    with pytest.raises(EvidenceContractError, match="finite"):
        _build(candidate=candidate)


def test_features_fallback_is_kept_when_reference_daily_is_absent() -> None:
    bundle = _build(candidate=_candidate())

    assert bundle.sections["market"]["reference"]["price"] == 101.2
    assert bundle.sections["market"]["reference"]["volume_raw"] == 999.0
    assert bundle.sections["fundamental"]["total_mv"] is None
    assert "fundamental.total_mv" in bundle.missing


def test_benchmark_limits_and_derived_quality_keep_their_actual_source_lineage():
    candidate = _candidate(reference_daily=_reference_daily())
    candidate["features"].update(benchmark_relative_20d=.1, benchmark_return_60d=.2,
                                 latest_limit_up=0., recent_limit_down_count=1., source_confidence=1.)
    bundle = _build(candidate=candidate)
    sources = {item.field: item.source for item in bundle.provenance}
    assert "index_daily" in sources["technical.features.benchmark_relative_20d"]
    assert "index_daily" in sources["technical.features.benchmark_return_60d"]
    assert "stk_limit" in sources["technical.features.latest_limit_up"]
    assert "stk_limit" in sources["technical.features.recent_limit_down_count"]
    assert sources["technical.features.source_confidence"] == "competition_pipeline.canonical_quality"
