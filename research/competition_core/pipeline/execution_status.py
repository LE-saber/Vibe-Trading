"""Current-day execution reference data, isolated from historical research APIs.

Daily listing/suspension/limit references are not a live halt feed or quote-date
proof. The independent Market Runtime quote gate remains mandatory.
"""

from datetime import date, datetime, timedelta, time
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import re

import pandas as pd

from contracts.symbol import CanonicalSymbol
from competition_core.pipeline.artifacts import atomic_json, content_hash, file_hash, read_json, confined_path
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.time_context import SHANGHAI

ROOT = Path(__file__).resolve().parents[2]
PAGE_SIZE = 5000  # Below the documented stk_limit single-call maximum5800.
MAX_PAGES = 8
FIELDS = {
    "trade_cal": "exchange,cal_date,is_open,pretrade_date",
    "stock_basic": "ts_code,symbol,name,market,exchange,list_status,list_date,delist_date",
    "suspend_d": "ts_code,trade_date,suspend_type,suspend_timing",
    "stk_limit": "ts_code,trade_date,pre_close,up_limit,down_limit",
}
KEYS = {"trade_cal": ("exchange", "cal_date"), "stock_basic": ("ts_code",),
        "suspend_d": ("ts_code", "trade_date", "suspend_type", "suspend_timing"),
        "stk_limit": ("ts_code", "trade_date")}
MARKETS = {"主板": "MAIN", "创业板": "CHINEXT", "科创板": "STAR"}


def _day(value):
    text = str(value).replace("-", "")
    if not re.fullmatch(r"[0-9]{8}", text):
        raise ValueError("invalid reference date")
    return datetime.strptime(text, "%Y%m%d").date()


def _price(value):
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        number = Decimal(str(value))
        if not number.is_finite() or not 0 < number < Decimal("1e12") or number % Decimal("0.01"):
            return None
        return format(number.quantize(Decimal("0.01")), "f")
    except InvalidOperation:
        return None


def _clock(clock, context, started=None):
    value = clock()
    if (not isinstance(value, datetime) or value.utcoffset() is None or value < context.as_of
            or value.astimezone(SHANGHAI).date() != context.research_date or (started is not None and value < started)):
        raise PipelineError("NOT_READY", "execution references require the actual current research date and clock")
    return value.astimezone(SHANGHAI)


def status_code_hashes():
    return {name: file_hash(ROOT / name) for name in (
        "competition_core/pipeline/execution_status.py", "competition_core/pipeline/time_context.py",
        "competition_core/pipeline/artifacts.py", "contracts/symbol.py", "adapters/tushare/client.py")}


class _Requests:
    def __init__(self, client, directory, context, source, clock, started):
        self.client, self.directory, self.context = client, directory, context
        self.source, self.clock, self.started = source, clock, started
        self.receipts = []
        self.calls = 0

    def fetch(self, endpoint, parameters, *, label, paged=True):
        """Finish only on an empty page; ignored offsets/repeated keys fail closed."""
        rows, seen, offset, observations = [], set(), 0, []
        for page in range(MAX_PAGES if paged else 1):
            now = _clock(self.clock, self.context, self.started)
            if (now - self.started).total_seconds() > 60:
                raise PipelineError("SOURCE_UNAVAILABLE", "execution reference collection exceeded its time budget")
            params = dict(parameters, fields=FIELDS[endpoint])
            if paged:
                params.update(limit=PAGE_SIZE, offset=offset)
            record = {"endpoint": endpoint, "parameters": params, "source": self.source,
                      "started_at": now.isoformat(), "response_kind": "parsed_sdk_dataframe"}
            path = self.directory / "sources" / f"{label}-{page:02d}.json"
            try:
                self.calls += 1
                frame = getattr(self.client, endpoint)(**params)
                received = _clock(self.clock, self.context, now)
                if not isinstance(frame, pd.DataFrame):
                    raise ValueError("expected DataFrame")
                # Only advertised fields are retained: do not log an unexpected
                # credential/debug column returned by a compatible service.
                required = FIELDS[endpoint].split(",")
                if not frame.empty and not set(required) <= set(frame.columns):
                    raise ValueError("missing source columns")
                data = json.loads(frame.reindex(columns=required).to_json(orient="records", force_ascii=False))
                record.update(received_at=received.isoformat(), status="SUCCESS", rows=data,
                              unexpected_column_count_omitted=len(set(frame.columns) - set(required)))
                atomic_json(path, record, immutable=True)
                self.receipts.append({"path": str(path.relative_to(self.directory)).replace("\\", "/"),
                                      "sha256": file_hash(path), "endpoint": endpoint, "received_at": received.isoformat()})
                observations.append(received)
                if not data:
                    return rows, min(observations)
                if paged and len(data) > PAGE_SIZE:
                    raise ValueError("oversized source page")
                keys = [tuple(row[key] for key in KEYS[endpoint]) for row in data]
                if len(set(keys)) != len(keys) or seen.intersection(keys):
                    raise ValueError("duplicate source keys or ignored pagination")
                for row in data:
                    if endpoint in {"suspend_d", "stk_limit"} and _day(row["trade_date"]) != self.context.research_date:
                        raise ValueError("source returned a different execution date")
                    if endpoint == "stock_basic" and row["list_status"] != parameters["list_status"]:
                        raise ValueError("source ignored listing status filter")
                seen.update(keys)
                rows.extend(data)
                offset += len(data)
                if not paged:
                    return rows, min(observations)
            except Exception as exc:
                # Do not echo SDK exception bodies: they can include credentials.
                failure = {"status": "MISSING", "endpoint": endpoint, "parameters": params,
                           "error": {"code": "SOURCE_UNAVAILABLE", "exception_type": type(exc).__name__}}
                failed_path = self.directory / "sources" / f"{label}-{page:02d}-failure.json"
                atomic_json(failed_path, failure, immutable=True)
                self.receipts.append({"path": str(failed_path.relative_to(self.directory)).replace("\\", "/"),
                                      "sha256": file_hash(failed_path), "endpoint": endpoint})
                return None, None
        return None, None


def _calendar(rows, exchange, start, end):
    if rows is None:
        return None
    try:
        by_date = {_day(row["cal_date"]): row for row in rows}
        expected = {start + timedelta(days=n) for n in range((end - start).days + 1)}
        if len(by_date) != len(rows) or set(by_date) != expected:
            return None
        if any(row["exchange"] != exchange or type(row["is_open"]) not in {int, str}
               or str(row["is_open"]) not in {"0", "1"} for row in rows):
            return None
        opens = sorted(day for day, row in by_date.items() if str(row["is_open"]) == "1")
        if not opens or opens[-1] != end:
            return None
        return opens
    except (KeyError, TypeError, ValueError):
        return None


def _suspended(records, at):
    """Interpret documented daily/full-day or explicit intraday intervals only."""
    if records is None:
        return None
    if any(row["suspend_type"] not in {"S", "R"} for row in records):
        return None
    stops = [row for row in records if row["suspend_type"] == "S"]
    resumes = [row for row in records if row["suspend_type"] == "R"]
    if stops and resumes:
        return None  # No ordering/source-event timestamp: do not guess a state.
    for row in stops:
        value = row.get("suspend_timing")
        if value is None or value == "":
            return True
        if not isinstance(value, str) or not re.fullmatch(r"\d\d:\d\d(?:[:]\d\d)?-\d\d:\d\d(?:[:]\d\d)?", value):
            return None
        try:
            start, end = (time.fromisoformat(item) for item in value.split("-"))
            if start >= end:
                return None
            if start <= at.timetz().replace(tzinfo=None) < end:
                return True
        except ValueError:
            return None
    return False


def _next_suspension_boundary(records, at):
    """Expire before any announced interval transition, even inside the normal TTL."""
    boundaries = []
    for row in records or []:
        timing = row.get("suspend_timing")
        if row.get("suspend_type") != "S" or not isinstance(timing, str):
            continue
        if not re.fullmatch(r"\d\d:\d\d(?:[:]\d\d)?-\d\d:\d\d(?:[:]\d\d)?", timing):
            continue
        try:
            start, end = (time.fromisoformat(item) for item in timing.split("-"))
            if start >= end:
                continue
            boundaries += [value for edge in (start, end)
                           if (value := datetime.combine(at.date(), edge, SHANGHAI)) > at]
        except ValueError:
            continue
    return min(boundaries).isoformat() if boundaries else None


def collect_execution_status(*, client, symbols, context, special_sessions_by_board,
                             directory, source_name, clock=None):
    """Collect a fresh, noncached reference observation; never return order authority."""
    if (not isinstance(symbols, list) or not 1 <= len(symbols) <= 50 or len(set(symbols)) != len(symbols)):
        raise PipelineError("IDENTITY_CONFLICT", "execution status requires 1–50 unique canonical symbols")
    identities = {symbol: CanonicalSymbol.from_core(symbol) for symbol in symbols}
    if (not isinstance(special_sessions_by_board, dict) or set(special_sessions_by_board) != {"MAIN", "CHINEXT", "STAR"}
            or any(type(value) is not int or not 0 <= value <= 30 for value in special_sessions_by_board.values())):
        raise PipelineError("NOT_CONFIGURED", "explicit board special-session metadata is required")
    if not isinstance(source_name, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", source_name):
        raise PipelineError("INVALID_ARGUMENTS", "non-secret source label required")
    clock = clock or (lambda: datetime.now(SHANGHAI))
    started = _clock(clock, context)
    directory = Path(directory)
    if directory.exists():
        raise PipelineError("NOT_READY", "execution status observation ID already exists")
    fingerprint = status_code_hashes()
    request = {"schema_version": "p10.execution_status.request.v1", "symbols": symbols,
               "context": context.to_dict(), "source": source_name, "started_at": started.isoformat(),
               "special_sessions_by_board": special_sessions_by_board, "code_hashes": fingerprint}
    atomic_json(directory / "request.json", request, immutable=True)
    source = _Requests(client, directory, context, source_name, clock, started)
    today = context.research_date
    calendar_start = today - timedelta(days=60)
    calendars, times = {}, []
    for exchange in sorted({i.exchange for i in identities.values()}):
        rows, stamp = source.fetch("trade_cal", {"exchange": exchange, "start_date": calendar_start.strftime("%Y%m%d"),
            "end_date": today.strftime("%Y%m%d")}, label=f"calendar-{exchange}", paged=False)
        calendars[exchange] = _calendar(rows, exchange, calendar_start, today)
        opens = calendars[exchange]
        if context.phase != "postclose" and (opens is None or len(opens) < 2 or opens[-2] != context.market_data_reference_date):
            calendars[exchange] = None
        if stamp:
            times.append(stamp)
    basic, basic_conflicts = {}, set()
    for state in ("L", "D", "P"):
        if state != "L" and set(symbols) <= set(basic):
            break  # Listed identities already proven; no unnecessary D/P queries.
        rows, stamp = source.fetch("stock_basic", {"exchange": "", "list_status": state}, label=f"listing-{state}")
        if rows is None:
            continue
        if stamp:
            times.append(stamp)
        for row in rows:
            if row["ts_code"] in basic:
                basic_conflicts.add(row["ts_code"])
            basic[row["ts_code"]] = row
    suspensions, stamp = source.fetch("suspend_d", {"trade_date": today.strftime("%Y%m%d")}, label="suspensions")
    if stamp:
        times.append(stamp)
    limits, stamp = source.fetch("stk_limit", {"trade_date": today.strftime("%Y%m%d")}, label="limits")
    if stamp:
        times.append(stamp)
    observed = _clock(clock, context, started)
    oldest = min(times) if times else started
    limit_by = {r["ts_code"]: r for r in limits} if limits is not None else {}
    metadata = {}
    for symbol, identity in identities.items():
        row, reasons = basic.get(symbol), []
        special, listing_ok = None, False
        listing_day = None
        opens = calendars[identity.exchange]
        if symbol in basic_conflicts or row is None:
            reasons.append("missing_listing_reference")
        else:
            try:
                actual = CanonicalSymbol.from_tushare(row["ts_code"], exchange=row["exchange"])
                candidate_listing_day = _day(row["list_date"])
                if candidate_listing_day == date(1970, 1, 1):
                    raise ValueError("placeholder listing date")
                listing_day = candidate_listing_day
                if actual != identity or MARKETS.get(row["market"]) != identity.board or listing_day > today:
                    raise ValueError("listing identity/board/date differs")
                name = row["name"]
                if not isinstance(name, str) or not name.strip():
                    raise ValueError("missing name")
                # Same default risk metadata used by current Universe; no changes
                # to that Universe or to the membership of frozen research runs.
                risky = "ST" in name.upper() or "退市" in name or name.endswith("退") or bool(row.get("delist_date"))
                listing_ok = row["list_status"] == "L" and not risky
                if not listing_ok:
                    reasons.append("current_listing_or_competition_risk")
            except (KeyError, TypeError, ValueError):
                reasons.append("invalid_listing_identity")
        if opens is None:
            reasons.append("calendar_not_complete_or_not_open")
        elif listing_day is not None:
            count = sum(day >= listing_day for day in opens)
            threshold = special_sessions_by_board[identity.board]
            if listing_day < calendar_start and count <= threshold:
                reasons.append("insufficient_listing_calendar")
            else:
                special = count <= threshold
        stops = [r for r in suspensions if r["ts_code"] == symbol] if suspensions is not None else None
        suspended = _suspended(stops, observed)
        if suspended is None:
            reasons.append("unknown_suspension_reference")
        limit = limit_by.get(symbol, {})
        upper, lower = _price(limit.get("up_limit")), _price(limit.get("down_limit"))
        if upper is None or lower is None or Decimal(lower) >= Decimal(upper):
            upper = lower = None
            reasons.append("missing_or_invalid_exact_price_limits")
        metadata[symbol] = {"symbol": symbol, "source": source_name + ".execution_reference",
            "as_of": oldest.isoformat(), "trade_date": today.isoformat(), "tradable": listing_ok and suspended is False,
            "suspended": suspended, "new_listing_special_period": special, "up_limit": upper, "down_limit": lower,
            "issues": reasons, "missing_fields": [reason for reason in reasons if reason != "current_listing_or_competition_risk"],
            "status_scope": "daily_reference_not_live_halt_feed",
            "valid_until": _next_suspension_boundary(stops, observed),
            "list_date": listing_day.isoformat() if listing_day else None,
            "field_sources": {"listing": source_name + ".stock_basic", "suspended": source_name + ".suspend_d",
                "new_listing_special_period": source_name + ".trade_cal+competition_board_metadata",
                "price_limits": source_name + ".stk_limit"}}
    if status_code_hashes() != fingerprint:
        raise PipelineError("STALE_DATA", "execution status code changed during observation")
    usable = [s for s, row in metadata.items() if not row["missing_fields"] and row["tradable"]
              and row["new_listing_special_period"] is False]
    complete = sum(not row["missing_fields"] for row in metadata.values())
    result = {"schema_version": "p10.execution_status.observation.v1", "context": context.to_dict(),
        "source": source_name, "observed_at": observed.isoformat(), "oldest_source_observation": oldest.isoformat(),
        "request_hash": file_hash(directory / "request.json"), "source_provenance": source.receipts,
        "metadata": metadata, "status": "READY" if complete == len(symbols) else "PARTIAL",
        "usable_symbols": usable, "execution_ready": False, "production_accepted": False,
        "summary": {"requested_count": len(symbols), "complete_reference_count": complete,
                    "usable_count": len(usable), "source_calls": source.calls}}
    atomic_json(directory / "observation.json", result | {"integrity_hash": content_hash(result)}, immutable=True)
    return result


def load_execution_status(directory, *, context, symbols, observed_at, max_age_seconds):
    directory = Path(directory)
    result = read_json(directory / "observation.json")
    if result.pop("integrity_hash", None) != content_hash(result):
        raise PipelineError("STALE_DATA", "execution reference observation hash mismatch")
    request = read_json(directory / "request.json", expected_hash=result["request_hash"])
    if (result["context"] != context.to_dict() or request["context"] != context.to_dict()
            or set(request["symbols"]) != set(symbols) or set(result["metadata"]) != set(symbols)
            or request["code_hashes"] != status_code_hashes()):
        raise PipelineError("STALE_DATA", "execution reference context/identities/implementation differ")
    for source in result["source_provenance"]:
        if file_hash(confined_path(directory, source["path"])) != source["sha256"]:
            raise PipelineError("STALE_DATA", "execution reference source receipt changed")
    current = _clock(lambda: observed_at, context)
    oldest = datetime.fromisoformat(result["oldest_source_observation"])
    if not 0 <= (current - oldest).total_seconds() <= max_age_seconds:
        raise PipelineError("STALE_DATA", "execution reference observation expired")
    if any(row.get("valid_until") is not None and current >= datetime.fromisoformat(row["valid_until"])
           for row in result["metadata"].values()):
        raise PipelineError("STALE_DATA", "execution reference suspension interval changed; refresh required")
    return result
