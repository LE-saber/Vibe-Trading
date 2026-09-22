from datetime import date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from competition_core.pipeline.artifacts import content_hash
from competition_core.terminal_strategy import CandidateAdapterBlocker, adapt_pipeline_candidates


SHANGHAI = ZoneInfo("Asia/Shanghai")
OBSERVED = datetime(2026, 9, 22, 13, 15, tzinfo=SHANGHAI)
STAGE_AS_OF = "2026-09-22T13:00:00+08:00"
SYMBOLS = ["600001.SH", "000002.SZ"]


def _row(symbol: str, *, include_relative=True, quote_as_of=OBSERVED):
    features = {
        "amount_cny": "100000000",
        "turnover_rate": "5",
        "volume_ratio": "1.5",
        "liquidity_score": "85",
        "opening_gap": "0.01",
        "close_strength": "0.85",
        "intraday_max_drawdown": "0.01",
        "trend_continuity": "0.85",
        "consecutive_strong_sessions": 3,
        "single_day_pulse": False,
        "overheat_score": "30",
        "correlation_cluster": "cluster-a",
        "correlation_to_holdings": {"600000.SH": "0.30"},
        "momentum_5d": "0.99",  # must never be used as relative strength
        "board": "MAIN",
        "listed_sessions": 1000,
        "is_new_listing": False,
        "price_limit_pct": "0.10",
        "price_limit_rule_known": True,
        "new_listing_rule_known": True,
    }
    if include_relative:
        features.update({
            "relative_strength_1d": "0.010",
            "relative_strength_3d": "0.025",
            "relative_strength_5d": "0.040",
            "benchmark_relative_1d": "0.005",
            "benchmark_relative_3d": "0.010",
            "benchmark_relative_5d": "0.015",
        })
    s4 = {
        "symbol": symbol,
        "selected": True,
        "features": features,
        "sector_metadata": {"value": "fixture-industry", "status": "available"},
    }
    s5 = {
        "symbol": symbol,
        "ai_status": "SUCCESS",
        "ai_verdict": "KEEP",
        "evidence_id": f"evidence-{symbol}",
        "evidence": {"complete": True, "quality": "0.90", "missing": [], "sources": ["fixture.evidence"]},
        "risk_facts": {
            "complete": True, "announcement_risk": False, "regulatory_risk": False,
            "reduction_risk": False, "earnings_risk": False,
        },
        "trigger_price": "10.00", "invalidation_price": "9.50", "limit_price": "10.10", "max_weight": "0.20",
    }
    s55 = {
        "symbol": symbol,
        "realtime_gate": "PASS",
        "execution_eligible": True,
        "quote": {
            "server_time": quote_as_of.isoformat(), "trade_date": "2026-09-22",
            "price": "10.00", "prev_close": "9.80", "open": "9.90", "high": "10.10", "low": "9.80",
            "bid1": "9.99", "ask1": "10.01", "tradeable": True, "is_suspended": False, "price_locked": False,
        },
        "metrics": {"amount_cny": "100000000", "quote_age_seconds": "0"},
    }
    return s4, s5, s55


def _stage(name, rows, inputs, outputs, *, as_of=STAGE_AS_OF):
    body = {
        "stage": name, "status": "SUCCESS", "input_symbols": inputs, "output_symbols": outputs,
        "rows": rows, "snapshot_id": "fixture-snapshot", "config_hash": "a" * 64,
        "research_date": "2026-09-22", "market_data_reference_date": "2026-09-22",
        "as_of": as_of, "phase": "intraday",
    }
    return body | {"artifact_hash": content_hash(body)}


def _inputs(*, symbols=SYMBOLS, quote_as_of=OBSERVED, include_relative=True, same_day=()):
    rows = [_row(symbol, include_relative=include_relative, quote_as_of=quote_as_of) for symbol in symbols]
    s4_rows, s5_rows, s55_rows = zip(*rows)
    facts = {
        symbol: {"holding_quantity": 100, "sellable_quantity": 50, "orders_reconciled": True}
        for symbol in symbols
    }
    return {
        "s4_stage": _stage("S4", list(s4_rows), list(symbols), list(symbols)),
        "s5_stage": _stage("S5", list(s5_rows), list(symbols), list(symbols)),
        "s5_5_stage": _stage("S5_5", list(s55_rows), list(symbols), list(symbols)),
        "account_position_facts": facts,
        "observed_at": OBSERVED,
        "same_day_exit_unavailable_symbols": same_day,
    }


def _adapt(**changes):
    values = _inputs()
    values.update(changes)
    return adapt_pipeline_candidates(**values)


def test_complete_mapping_is_sorted_and_read_only():
    result = _adapt()
    assert result.diagnostics.ready
    assert [item.symbol for item in result.candidates] == sorted(SYMBOLS)
    candidate = result.candidates[0]
    assert candidate.relative_strength_3d == Decimal("0.025")
    assert candidate.current_price == Decimal("10.00")
    assert candidate.previous_close == Decimal("9.80")
    assert candidate.industry == "fixture-industry"
    assert candidate.evidence_sources == (f"evidence-{candidate.symbol}", "fixture.evidence")
    assert candidate.holding_quantity == 100 and candidate.sellable_quantity == 50
    assert candidate.orders_reconciled is True
    assert result.execution_ready is False


def test_momentum_is_not_relative_strength_and_missing_facts_are_explicit():
    values = _inputs(include_relative=False)
    values["account_position_facts"] = {}
    result = adapt_pipeline_candidates(**values)
    candidate = result.candidates[0]
    assert candidate.relative_strength_5d is None
    assert candidate.holding_quantity is None
    assert candidate.sellable_quantity is None
    assert candidate.orders_reconciled is None
    assert any(field.endswith("relative_strength_5d") for field in result.diagnostics.missing_fields)
    assert any(field.endswith("holding_quantity") for field in result.diagnostics.missing_fields)


def test_stage_artifact_hash_mismatch_is_fail_closed():
    values = _inputs()
    values["s5_stage"]["artifact_hash"] = "f" * 64
    result = adapt_pipeline_candidates(**values)
    assert result.candidates == ()
    assert CandidateAdapterBlocker.STAGE_HASH_MISMATCH in result.diagnostics.blockers


def test_stage_identity_relation_mismatch_is_fail_closed():
    values = _inputs()
    values["s5_stage"] = _stage("S5", list(values["s5_stage"]["rows"]), SYMBOLS, [SYMBOLS[0]])
    result = adapt_pipeline_candidates(**values)
    assert result.candidates == ()
    assert CandidateAdapterBlocker.STAGE_RELATION_INVALID in result.diagnostics.blockers


def test_partial_status_is_only_allowed_for_realtime_stage():
    values = _inputs()
    partial = dict(values["s5_5_stage"])
    body = {key: value for key, value in partial.items() if key != "artifact_hash"}
    body["status"] = "PARTIAL"
    values["s5_5_stage"] = body | {"artifact_hash": content_hash(body)}
    assert adapt_pipeline_candidates(**values).candidates

    for name in ("s4_stage", "s5_stage"):
        changed = _inputs()
        body = {key: value for key, value in changed[name].items() if key != "artifact_hash"}
        body["status"] = "PARTIAL"
        changed[name] = body | {"artifact_hash": content_hash(body)}
        result = adapt_pipeline_candidates(**changed)
        assert result.candidates == ()
        assert CandidateAdapterBlocker.STAGE_STATUS_NOT_SUCCESS in result.diagnostics.blockers


def test_duplicate_row_symbol_is_rejected():
    values = _inputs(symbols=[SYMBOLS[0]])
    row = values["s4_stage"]["rows"][0]
    values["s4_stage"] = _stage("S4", [row, dict(row)], [SYMBOLS[0]], [SYMBOLS[0]])
    result = adapt_pipeline_candidates(**values)
    assert result.candidates == ()
    assert CandidateAdapterBlocker.DUPLICATE_SYMBOL in result.diagnostics.blockers


def test_stale_quote_is_blocked_but_fact_is_preserved_for_funnel():
    stale = OBSERVED - timedelta(seconds=61)
    values = _inputs(quote_as_of=stale)
    result = adapt_pipeline_candidates(**values)
    assert CandidateAdapterBlocker.QUOTE_STALE in result.diagnostics.blockers
    assert result.candidates[0].quote_as_of == stale


def test_future_quote_is_blocked():
    values = _inputs(quote_as_of=OBSERVED + timedelta(seconds=1))
    result = adapt_pipeline_candidates(**values)
    assert CandidateAdapterBlocker.QUOTE_FUTURE in result.diagnostics.blockers


def test_same_day_flag_only_comes_from_explicit_set():
    known = _adapt(same_day_exit_unavailable_symbols=(SYMBOLS[0],))
    known_by_symbol = {item.symbol: item for item in known.candidates}
    assert known_by_symbol[SYMBOLS[0]].same_day_exit_unavailable is True
    unknown = _adapt(same_day_exit_unavailable_symbols=None)
    assert all(item.same_day_exit_unavailable is None for item in unknown.candidates)
    assert CandidateAdapterBlocker.SAME_DAY_INPUT_MISSING in unknown.diagnostics.blockers


def test_naive_observed_at_is_rejected():
    values = _inputs()
    values["observed_at"] = datetime(2026, 9, 22, 13, 15)
    result = adapt_pipeline_candidates(**values)
    assert result.candidates == ()
    assert CandidateAdapterBlocker.OBSERVED_AT_INVALID in result.diagnostics.blockers


def test_output_order_and_hash_are_stable():
    first = _adapt()
    values = _inputs()
    for key in ("s4_stage", "s5_stage", "s5_5_stage"):
        values[key]["rows"] = list(reversed(values[key]["rows"]))
        body = {field: value for field, value in values[key].items() if field != "artifact_hash"}
        values[key]["artifact_hash"] = content_hash(body)
    second = adapt_pipeline_candidates(**values)
    assert [item.symbol for item in first.candidates] == [item.symbol for item in second.candidates]
    assert first.diagnostics.output_hash == second.diagnostics.output_hash


def test_stage_context_future_is_rejected():
    values = _inputs()
    values["s4_stage"] = _stage("S4", values["s4_stage"]["rows"], SYMBOLS, SYMBOLS, as_of="2026-09-22T13:16:00+08:00")
    result = adapt_pipeline_candidates(**values)
    assert CandidateAdapterBlocker.STAGE_FUTURE in result.diagnostics.blockers
