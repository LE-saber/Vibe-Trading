from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import sys

import pytest

from competition_core.ipo_sprint import (
    IpoSprintError,
    evaluate_ipo_sprint_snapshot,
    load_ipo_sprint_config,
)


NOW = datetime(2026, 9, 23, 5, 30, tzinfo=timezone.utc)


def _source(name="fixture"):
    return {
        "source_name": name,
        "source_ref": f"https://example.test/{name}",
        "observed_at": "2026-09-23T13:29:00+08:00",
    }


def _risk_source(symbol, category):
    return {
        "source_name": "tushare.anns_d.classifier",
        "source_ref": (
            f"tushare://anns_d/{symbol}/{category}"
            f"?start=20260824&end=20260923&sha256={'a' * 64}"
        ),
        "observed_at": "2026-09-23T13:29:00+08:00",
    }


def _stock(symbol, *, daily_return=30.0, amount=80_000_000):
    if "." not in symbol:
        symbol = f"{symbol}.SZ"
    return {
        "symbol": symbol,
        "name": f"新股{symbol}",
        "listing_date": "2026-09-22",
        "trading_sessions": [
            {"trade_date": "2026-09-22", "limit_regime": "UNBOUNDED"},
            {"trade_date": "2026-09-23", "limit_regime": "UNBOUNDED"},
        ],
        "rule_evidence": {"unbounded_session_count": 5, "source": _source("rule")},
        "identity_evidence": _source("identity"),
        "returns_pct": {
            "windows": {"1d": daily_return, "3d": None, "5d": None},
            "source": _source("returns"),
        },
        "liquidity": {
            "turnover_amount": amount,
            "turnover_amount_source": _source("realtime"),
            "daily_basic_as_of_trade_date": "2026-09-22",
            "turnover_rate_pct": 5.0,
            "volume_ratio": 2.0,
            "daily_basic_source": _source("liquidity"),
        },
        "risk_review": {
            "complete": True,
            "status": "CLEAR",
            "categories": {
                category: {
                    "status": "CLEAR",
                    "has_risk": False,
                    "source": _risk_source(symbol, category),
                }
                for category in ("announcement", "regulatory", "reduction", "earnings")
            },
            "facts": [],
        },
        "realtime": {
            "trade_date": "2026-09-23",
            "observed_at": "2026-09-23T13:29:30+08:00",
            "last_price": 25.0,
            "bid_price": 24.99,
            "ask_price": 25.01,
            "bid_size": 1000,
            "ask_size": 1200,
            "suspended": False,
            "tradable": True,
            "price_locked": False,
            "source": _source("realtime"),
        },
    }


def _snapshot(*stocks):
    return {
        "schema_version": "competition.ipo-sprint-snapshot.v1",
        "snapshot_id": "fixture-snapshot",
        "stocks": list(stocks),
        "collection_summary": {
            "scanned_count": len(stocks),
            "included_count": len(stocks),
            "excluded_count": 0,
            "excluded": [],
        },
    }


def _evaluate(snapshot):
    return evaluate_ipo_sprint_snapshot(
        snapshot, evaluated_at=NOW, config=load_ipo_sprint_config()
    )


def test_returns_separate_candidates_and_rejected_with_explicit_route_origin():
    rejected = _stock("000002", daily_return=4.0)
    result = _evaluate(_snapshot(_stock("000001"), rejected))

    assert [item["symbol"] for item in result["candidates"]] == ["000001.SZ"]
    assert result["candidates"][0]["route_origin"] == "IPO_SPRINT"
    assert result["rejected"] == [{
        "symbol": "000002.SZ",
        "name": "新股000002.SZ",
        "route_origin": "IPO_SPRINT",
        "reason_codes": ["MOMENTUM_BELOW_MINIMUM"],
    }]
    assert result["route_origin"] == "IPO_SPRINT"
    assert result["orders_submitted"] == 0
    assert result["read_only"] is True
    assert result["execution_ready"] is False
    assert result["production_accepted"] is False


def test_s4_s5_s6_or_terminal_candidate_payload_is_rejected_as_foreign_input():
    snapshot = _snapshot(_stock("000001"))
    snapshot["s4_candidates"] = [{"symbol": "600000.SH"}]
    with pytest.raises(IpoSprintError):
        _evaluate(snapshot)


def test_collection_summary_is_mandatory_for_auditable_universe_coverage():
    snapshot = _snapshot(_stock("000001"))
    del snapshot["collection_summary"]
    with pytest.raises(IpoSprintError):
        _evaluate(snapshot)


def test_unbounded_is_an_explicit_regime_and_must_be_inside_rule_window():
    stock = _stock("000001")
    stock["trading_sessions"][1]["limit_regime"] = "LIMITED"
    result = _evaluate(_snapshot(stock))
    assert result["candidates"] == []
    assert "LIMIT_REGIME_NOT_UNBOUNDED" in result["rejected"][0]["reason_codes"]

    stock = _stock("000003")
    stock["rule_evidence"]["unbounded_session_count"] = 1
    result = _evaluate(_snapshot(stock))
    assert "OUTSIDE_UNBOUNDED_LISTING_WINDOW" in result["rejected"][0]["reason_codes"]


def test_identity_and_rule_evidence_are_required_and_cannot_be_future_dated():
    missing_identity = _stock("000001")
    missing_identity["identity_evidence"] = None
    future_rule = _stock("000002")
    future_rule["rule_evidence"]["source"]["observed_at"] = "2026-09-24T08:00:00+08:00"

    result = _evaluate(_snapshot(missing_identity, future_rule))
    by_symbol = {item["symbol"]: item["reason_codes"] for item in result["rejected"]}
    assert "IDENTITY_EVIDENCE_INVALID" in by_symbol["000001.SZ"]
    assert "LISTING_RULE_EVIDENCE_INVALID" in by_symbol["000002.SZ"]


def test_stale_or_future_realtime_quotes_are_rejected():
    stale = _stock("000001")
    stale["realtime"]["observed_at"] = (NOW - timedelta(seconds=61)).isoformat()
    future = _stock("000002")
    future["realtime"]["observed_at"] = (NOW + timedelta(seconds=1)).isoformat()

    result = _evaluate(_snapshot(stale, future))
    by_symbol = {item["symbol"]: item["reason_codes"] for item in result["rejected"]}
    assert "QUOTE_STALE" in by_symbol["000001.SZ"]
    assert "QUOTE_FROM_FUTURE" in by_symbol["000002.SZ"]


def test_quote_must_be_from_continuous_auction_not_lunch_or_close_auction():
    lunch = _stock("000001")
    lunch["realtime"]["observed_at"] = "2026-09-23T11:30:20+08:00"
    lunch["realtime"]["source"]["observed_at"] = lunch["realtime"]["observed_at"]
    close_auction = _stock("000002")
    close_auction["realtime"]["observed_at"] = "2026-09-23T14:57:10+08:00"
    close_auction["realtime"]["source"]["observed_at"] = close_auction["realtime"]["observed_at"]
    evaluated = datetime(2026, 9, 23, 6, 57, 20, tzinfo=timezone.utc)

    result = evaluate_ipo_sprint_snapshot(
        _snapshot(lunch, close_auction), evaluated_at=evaluated,
        config=load_ipo_sprint_config(),
    )
    by_symbol = {item["symbol"]: item["reason_codes"] for item in result["rejected"]}
    assert "OUTSIDE_CONTINUOUS_AUCTION" in by_symbol["000001.SZ"]
    assert "OUTSIDE_CONTINUOUS_AUCTION" in by_symbol["000002.SZ"]


def test_incomplete_or_flagged_risk_review_and_missing_fact_source_are_rejected():
    incomplete = _stock("000001")
    incomplete["risk_review"]["complete"] = False
    flagged = _stock("000002")
    flagged["risk_review"]["categories"]["announcement"].update(
        status="FLAGGED", has_risk=True
    )
    missing_fact_source = _stock("000003")
    missing_categories = _stock("000004")
    del missing_categories["risk_review"]["categories"]["earnings"]
    missing_fact_source["risk_review"]["facts"] = [{
        "category": "announcement", "severity": "LOW", "description": "示例事项", "source": None,
    }]

    result = _evaluate(_snapshot(incomplete, flagged, missing_fact_source, missing_categories))
    by_symbol = {item["symbol"]: item["reason_codes"] for item in result["rejected"]}
    assert "RISK_FACTS_INCOMPLETE" in by_symbol["000001.SZ"]
    assert "RISK_FLAGGED" in by_symbol["000002.SZ"]
    assert "RISK_FACTS_INCOMPLETE" in by_symbol["000003.SZ"]
    assert "RISK_FACTS_INCOMPLETE" in by_symbol["000004.SZ"]


def test_stale_risk_review_is_rejected():
    stock = _stock("000001")
    stale = "2026-09-22T13:29:00+08:00"
    for review in stock["risk_review"]["categories"].values():
        review["source"]["observed_at"] = stale
    result = _evaluate(_snapshot(stock))
    assert "RISK_FACTS_INCOMPLETE" in result["rejected"][0]["reason_codes"]


def test_operator_claim_cannot_replace_collector_announcement_evidence():
    stock = _stock("000001")
    stock["risk_review"]["categories"]["announcement"]["source"] = _source("operator")
    result = _evaluate(_snapshot(stock))
    assert "RISK_FACTS_INCOMPLETE" in result["rejected"][0]["reason_codes"]


def test_rank_is_repeatable_and_uses_momentum_then_return_liquidity_and_symbol():
    slower_window = _stock("000002", daily_return=20.0, amount=90_000_000)
    slower_window["returns_pct"]["windows"]["available_window"] = {"sessions": 2, "return_pct": 60.0}
    faster = _stock("000001", daily_return=40.0, amount=60_000_000)
    faster["returns_pct"]["windows"]["available_window"] = {"sessions": 2, "return_pct": 60.0}
    tied = _stock("000003", daily_return=40.0, amount=60_000_000)
    snapshot = _snapshot(slower_window, faster, tied)

    forward = _evaluate(snapshot)
    reverse = _evaluate(_snapshot(*reversed(snapshot["stocks"])))
    assert forward == reverse
    assert [item["symbol"] for item in forward["candidates"]] == [
        "000001.SZ", "000003.SZ", "000002.SZ"
    ]


def test_liquidity_and_executable_two_sided_book_are_permanent_hard_gates():
    low_liquidity = _stock("000001", amount=49_999_999)
    locked_book = _stock("000002")
    locked_book["realtime"]["price_locked"] = True
    no_ask = _stock("000003")
    no_ask["realtime"]["ask_size"] = 0
    no_return_source = _stock("000004")
    del no_return_source["returns_pct"]["source"]
    no_liquidity_source = _stock("000005")
    del no_liquidity_source["liquidity"]["daily_basic_source"]
    no_realtime_source = _stock("000006")
    del no_realtime_source["realtime"]["source"]

    result = _evaluate(_snapshot(
        low_liquidity, locked_book, no_ask,
        no_return_source, no_liquidity_source, no_realtime_source,
    ))
    by_symbol = {item["symbol"]: item["reason_codes"] for item in result["rejected"]}
    assert "LIQUIDITY_BELOW_MINIMUM" in by_symbol["000001.SZ"]
    assert "PRICE_LOCKED" in by_symbol["000002.SZ"]
    assert "ORDER_BOOK_INVALID" in by_symbol["000003.SZ"]
    assert "RETURN_EVIDENCE_INVALID" in by_symbol["000004.SZ"]
    assert "LIQUIDITY_EVIDENCE_INVALID" in by_symbol["000005.SZ"]
    assert "REALTIME_EVIDENCE_INVALID" in by_symbol["000006.SZ"]


def test_daily_liquidity_as_of_must_be_previous_completed_session():
    stock = _stock("000001")
    stock["liquidity"]["daily_basic_as_of_trade_date"] = "2026-09-23"
    result = _evaluate(_snapshot(stock))
    assert "LIQUIDITY_AS_OF_DATE_INVALID" in result["rejected"][0]["reason_codes"]


def test_collection_exclusions_are_preserved_separately_from_evaluated_rejections():
    snapshot = _snapshot(_stock("000001"))
    snapshot["collection_summary"] = {
        "scanned_count": 2,
        "included_count": 1,
        "excluded_count": 1,
        "excluded": [{
            "symbol": "688099.SH", "name": "缺证据新股", "route_origin": "IPO_SPRINT",
            "reason_codes": ["STOCK_BASIC_MISSING"],
        }],
    }
    result = _evaluate(snapshot)
    assert result["collection_excluded_count"] == 1
    assert result["collection_excluded"][0]["reason_codes"] == ["STOCK_BASIC_MISSING"]


def test_input_contract_rejects_price_limit_percentage_shortcut():
    stock = _stock("000001")
    stock["price_limit_pct"] = 0
    with pytest.raises(IpoSprintError):
        _evaluate(_snapshot(stock))


def test_cli_requires_explicit_output_and_operates_without_web_dependencies(tmp_path):
    research_root = Path(__file__).resolve().parents[2]
    script = research_root / "scripts" / "ipo-sprint.py"
    input_path = tmp_path / "snapshot.json"
    output_path = tmp_path / "result.json"
    cli_stock = _stock("000001")
    input_path.write_text(json.dumps(_snapshot(cli_stock)), encoding="utf-8")

    missing_output = subprocess.run(
        [sys.executable, str(script), "--input", str(input_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert missing_output.returncode == 2
    assert not output_path.exists()

    completed = subprocess.run(
        [sys.executable, str(script), "--input", str(input_path), "--output", str(output_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    result = json.loads(output_path.read_text(encoding="utf-8"))
    assert result["route_origin"] == "IPO_SPRINT"
    assert result["candidate_count"] + result["rejected_count"] == 1


def test_collect_refuses_to_overwrite_token_input(tmp_path):
    research_root = Path(__file__).resolve().parents[2]
    script = research_root / "scripts" / "ipo-sprint.py"
    token_path = tmp_path / "token.txt"
    token_path.write_text("secret-token", encoding="utf-8")
    completed = subprocess.run(
        [
            sys.executable, str(script), "collect", "--token-file", str(token_path),
            "--snapshot-output", str(token_path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 2
    assert token_path.read_text(encoding="utf-8") == "secret-token"


def test_atomic_temp_name_cannot_overwrite_input_and_config_is_protected(tmp_path):
    research_root = Path(__file__).resolve().parents[2]
    script = research_root / "scripts" / "ipo-sprint.py"
    output_path = tmp_path / "result.json"
    input_path = tmp_path / "result.json.tmp"
    input_payload = json.dumps(_snapshot(_stock("000001")))
    input_path.write_text(input_payload, encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable, str(script), "evaluate", "--input", str(input_path),
            "--output", str(output_path),
        ],
        capture_output=True, text=True, check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert input_path.read_text(encoding="utf-8") == input_payload

    config_path = tmp_path / "policy.yaml"
    config_text = (research_root / "config" / "ipo-sprint.yaml").read_text(encoding="utf-8")
    config_path.write_text(config_text, encoding="utf-8")
    protected = subprocess.run(
        [
            sys.executable, str(script), "evaluate", "--input", str(input_path),
            "--config", str(config_path), "--output", str(config_path),
        ],
        capture_output=True, text=True, check=False,
    )
    assert protected.returncode == 2
    assert config_path.read_text(encoding="utf-8") == config_text


def test_canonical_symbol_validation_uses_core_contract_and_accepts_bj_core_form():
    result = _evaluate(_snapshot(_stock("000001.SH"), _stock("920001.BJ")))
    by_symbol = {item["symbol"]: item for item in result["rejected"]}
    assert "SYMBOL_INVALID" in by_symbol["000001.SH"]["reason_codes"]
    assert [item["symbol"] for item in result["candidates"]] == ["920001.BJ"]
