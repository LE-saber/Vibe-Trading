"""Conservative postclose publication proof, separate from Universe selection."""

from datetime import date, datetime, time
import hashlib
import json
import math
from typing import Any

import pandas as pd

from competition_core.pipeline.artifacts import content_hash
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.sources import _date
from competition_core.pipeline.time_context import ResearchContext, SHANGHAI
from contracts.symbol import CanonicalSymbol, SymbolContractError


def expected_publication_symbols(
    *, reference_date: date, stock_basic: pd.DataFrame, suspensions: pd.DataFrame,
) -> list[str]:
    """Independent publication coverage denominator, never derived from daily.

    Callers must obtain dated identity data via ResearchDataClient or a frozen
    point-in-time source artifact. ST/risk exclusions from the research Universe
    do not narrow this source-publication check. An unknown suspension response
    is not evidence that there are no suspensions.
    """
    required = {"ts_code", "exchange", "list_status", "list_date"}
    if not isinstance(stock_basic, pd.DataFrame) or stock_basic.empty or not required <= set(stock_basic):
        raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "publication needs independent listed-stock identity")
    if stock_basic.ts_code.isna().any() or stock_basic.ts_code.duplicated().any():
        raise PipelineError("STALE_DATA", "publication identity has missing or duplicate symbols")
    if not isinstance(suspensions, pd.DataFrame):
        raise PipelineError("SOURCE_UNAVAILABLE", "publication needs a successful suspension response")
    suspended: set[str] = set()
    if not suspensions.empty:
        if not {"ts_code", "trade_date", "suspend_type"} <= set(suspensions):
            raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "publication suspension fields missing")
        if (not suspensions.trade_date.map(_date).eq(reference_date).all()
                or not suspensions.suspend_type.eq("S").all()
                or suspensions.ts_code.isna().any()):
            raise PipelineError("STALE_DATA", "publication suspension identity/date mismatch")
        suspended = set(suspensions.ts_code)
    expected = []
    for row in stock_basic.to_dict("records"):
        try:
            identity = CanonicalSymbol.from_core(row["ts_code"])
        except SymbolContractError:
            continue  # B shares, funds, indices and non-SSE/SZSE identities.
        if identity.exchange != row["exchange"]:
            raise PipelineError("STALE_DATA", "publication stock exchange conflicts with symbol")
        if row["list_status"] not in {"L", "D", "P"}:
            raise PipelineError("STALE_DATA", "publication stock listing status is unknown")
        if row["list_status"] == "L":
            listing_day = _date(row["list_date"])
            if listing_day == date(1970, 1, 1):
                raise PipelineError("DATA_COVERAGE_INSUFFICIENT",
                                    "publication stock_basic has a placeholder listing date",
                                    symbol=identity.symbol, source="stock_basic", field="list_date",
                                    value=str(row["list_date"]))
            if listing_day <= reference_date and identity.symbol not in suspended:
                expected.append(identity.symbol)
    if not expected:
        raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "publication has no independent expected stocks")
    return sorted(expected)


def verify_completed_daily(
    *, reference_date: date, as_of: datetime, expected_symbols: list[str],
    daily: pd.DataFrame, adj_factor: pd.DataFrame, daily_basic: pd.DataFrame,
    source: str, observed_at: datetime, snapshot_id: str | None = None,
) -> dict[str, Any]:
    """Require expected non-suspended listed stocks across all three endpoints.

    This proves snapshot coverage, not a vendor's promise against later revisions.
    Freeze source batch hashes in the subsequent canonical manifest. The caller
    derives expected_symbols from today's dated identity/suspension information,
    not from whichever symbols happened to appear in daily.
    """
    if observed_at.utcoffset() is None or as_of.utcoffset() is None or as_of > observed_at:
        raise PipelineError("NOT_READY", "publication cutoff is naive or later than observation")
    if not isinstance(source, str) or not source.strip():
        raise PipelineError("NOT_READY", "publication source provenance is required")
    if as_of.astimezone(SHANGHAI).date() != reference_date or as_of.astimezone(SHANGHAI).time() < time(15):
        raise PipelineError("NOT_READY", "postclose publication proof requires current-session after-close as_of")
    expected = set(expected_symbols)
    if not expected or len(expected) != len(expected_symbols):
        raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "publication proof needs unique independent expected symbols")
    for symbol in expected:
        CanonicalSymbol.from_core(symbol)
    counts = {endpoint: verify_publication_frame(endpoint=endpoint, frame=frame,
              reference_date=reference_date, expected_symbols=sorted(expected))
              for endpoint, frame in (("daily", daily), ("adj_factor", adj_factor), ("daily_basic", daily_basic))}
    proof = {"completed_daily_date": reference_date.isoformat(), "as_of": as_of.isoformat(),
             "source": source, "observed_at": observed_at.isoformat(),
             "expected_symbols_hash": content_hash(sorted(expected)),
             "expected_count": len(expected), "covered_counts": counts,
             "expected_symbols": sorted(expected), "snapshot_id": snapshot_id,
             "method": "independent_listed_minus_suspended_three_endpoint_coverage"}
    return {**proof, "proof_id": content_hash(proof)}


def verify_publication_frame(*, endpoint: str, frame: pd.DataFrame, reference_date: date,
                             expected_symbols: list[str]) -> int:
    """Validate an endpoint before freezing its completed postclose batch."""
    required = {"daily": {"open", "high", "low", "close", "vol", "amount"},
                "adj_factor": {"adj_factor"}, "daily_basic": {"turnover_rate"}}[endpoint]
    expected = set(expected_symbols)
    if not expected or not isinstance(frame, pd.DataFrame) or not {"ts_code", "trade_date", *required} <= set(frame):
        raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "incomplete publication schema", endpoint=endpoint)
    if frame.duplicated(["ts_code", "trade_date"]).any() or not frame.trade_date.map(_date).eq(reference_date).all():
        raise PipelineError("STALE_DATA", "publication has duplicate or wrong-date rows", endpoint=endpoint)
    absent = sorted(expected - set(frame.ts_code))
    if absent:
        raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "publication missing expected listed stocks",
                            endpoint=endpoint, missing_count=len(absent), examples=absent[:20])
    numeric = frame.loc[frame.ts_code.isin(expected), sorted(required)].apply(pd.to_numeric, errors="coerce")
    if numeric.isna().any().any() or numeric.isin([float("inf"), -float("inf")]).any().any():
        raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "publication has invalid values", endpoint=endpoint)
    if endpoint == "adj_factor" and (numeric.adj_factor <= 0).any():
        raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "publication has invalid factors")
    if endpoint == "daily":
        if (numeric[["open", "high", "low", "close"]] <= 0).any().any() or (numeric[["vol", "amount"]] < 0).any().any():
            raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "publication has invalid price/volume")
        if ((numeric.high < numeric[["open", "close", "low"]].max(axis=1)).any()
                or (numeric.low > numeric[["open", "close", "high"]].min(axis=1)).any()):
            raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "publication has inconsistent OHLC")
    if endpoint == "daily_basic" and (numeric.turnover_rate < 0).any():
        raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "publication has invalid turnover_rate")
    return len(numeric)


def snapshot_publication_symbols(snapshot: dict[str, Any], context: ResearchContext) -> list[str]:
    """Trust only exact-hash PIT identities with independent suspension provenance."""
    if not isinstance(snapshot, dict) or not isinstance(snapshot.get("members"), list):
        raise PipelineError("STALE_DATA", "publication snapshot members must be a list")
    context.validate_snapshot(snapshot)
    fields = {"schema_version", "competition_id", "as_of", "snapshot_phase", "market_data_reference_date",
              "members", "sources", "warnings"}
    if not fields <= set(snapshot) or set(snapshot) - fields - {"snapshot_id", "stale", "fallback_used"}:
        raise PipelineError("STALE_DATA", "publication snapshot schema mismatch")
    payload = json.dumps({key: snapshot[key] for key in fields}, ensure_ascii=False, sort_keys=True,
                         separators=(",", ":"), allow_nan=False).encode("utf-8")
    if hashlib.sha256(payload).hexdigest() != snapshot["snapshot_id"]:
        raise PipelineError("STALE_DATA", "publication snapshot hash mismatch")
    basic, suspended = [], []
    for member in snapshot["members"]:
        if not isinstance(member, dict) or not isinstance(member.get("field_sources"), dict):
            raise PipelineError("STALE_DATA", "publication member provenance is invalid")
        sources = member.get("field_sources", {})
        if (sources.get("identity") != "tushare.stock_basic" or sources.get("listing_status") != "tushare.stock_basic"
                or sources.get("suspension") != "tushare.suspend_d" or type(member.get("is_suspended")) is not bool):
            raise PipelineError("STALE_DATA", "publication requires independent dated identity and suspension provenance")
        basic.append({field: member.get(field) for field in ("ts_code", "exchange", "list_status", "list_date")})
        if member["is_suspended"]:
            suspended.append({"ts_code": member["ts_code"], "trade_date": context.research_date.strftime("%Y%m%d"), "suspend_type": "S"})
    return expected_publication_symbols(reference_date=context.research_date, stock_basic=pd.DataFrame(basic),
                                         suspensions=pd.DataFrame(suspended))


def verify_snapshot_liquidity(snapshot: dict[str, Any], daily: pd.DataFrame, expected_symbols: list[str]) -> None:
    """A publication arriving during snapshot construction must not change its inputs."""
    rows = daily.set_index("ts_code")
    expected = set(expected_symbols)
    for member in snapshot["members"]:
        if member["ts_code"] not in expected:
            continue
        if member.get("liquidity_date") != snapshot["market_data_reference_date"]:
            raise PipelineError("STALE_DATA", "snapshot was built before complete daily publication; rebuild it")
        row = rows.loc[member["ts_code"]]
        for key, value in (("recent_volume_lots", row.vol), ("recent_amount_cny", float(row.amount) * 1000)):
            try:
                same = math.isclose(float(member[key]), float(value), rel_tol=1e-10, abs_tol=1e-8)
            except (TypeError, ValueError, KeyError):
                same = False
            if not same:
                raise PipelineError("STALE_DATA", "snapshot liquidity differs from verified publication; rebuild it")
