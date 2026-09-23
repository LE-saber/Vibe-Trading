"""Independent, read-only data collection for the IPO sprint route."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
import re
from typing import Any

import httpx


SHANGHAI = timezone(timedelta(hours=8))
SNAPSHOT_SCHEMA = "competition.ipo-sprint-snapshot.v1"
SINA_URL = "https://hq.sinajs.cn/list="
SINA_REFERER = "https://finance.sina.com.cn/"
MAX_RESPONSE_BYTES = 1024 * 1024
RULES = {
    "SH": {
        "exchange": "SSE",
        "sessions": 5,
        "source": "https://www.sse.com.cn/lawandrules/sselawsrules2025/stocks/exchange/c/c_20260424_10816482.shtml",
    },
    "SZ": {
        "exchange": "SZSE",
        "sessions": 5,
        "source": "https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf",
    },
}
_ASSIGNMENT = re.compile(r'^var\s+hq_str_(?P<identity>(?:sh|sz)\d{6})\s*=\s*"(?P<body>[^"]*)"\s*;\s*$')
_RISK_KEYWORDS = {
    "announcement": (
        "风险提示", "停牌", "终止", "重大事项", "诉讼", "仲裁", "质押", "冻结", "退市",
    ),
    "regulatory": (
        "监管", "问询", "警示", "立案", "处罚", "纪律处分", "异常波动", "重点监控",
    ),
    "reduction": ("减持", "持股变动", "股份变动"),
    "earnings": (
        "业绩", "盈利预测", "亏损", "财务报告", "季度报告", "半年度报告", "年度报告",
        "营业收入", "净利润",
    ),
}


class IpoSprintCollectionError(RuntimeError):
    """Raised when an independent source cannot be collected safely."""


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip()) or (
        isinstance(value, float) and math.isnan(value)
    )


def _day(value: Any) -> date | None:
    text = str(value or "").strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        return None
    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError:
        return None


def _rows(value: Any, endpoint: str) -> list[dict[str, Any]]:
    if value is None:
        raise IpoSprintCollectionError(f"{endpoint} returned no response")
    if hasattr(value, "to_dict"):
        try:
            value = value.to_dict("records")
        except (TypeError, ValueError) as exc:
            raise IpoSprintCollectionError(f"{endpoint} response is invalid") from exc
    if not isinstance(value, list) or any(not isinstance(row, Mapping) for row in value):
        raise IpoSprintCollectionError(f"{endpoint} response is invalid")
    return [dict(row) for row in value]


def _call(client: Any, endpoint: str, **params: Any) -> list[dict[str, Any]]:
    method = getattr(client, endpoint, None)
    if not callable(method):
        raise IpoSprintCollectionError(f"{endpoint} is unavailable")
    try:
        return _rows(method(**params), endpoint)
    except IpoSprintCollectionError:
        raise
    except Exception as exc:
        raise IpoSprintCollectionError(f"{endpoint} request failed") from exc


def _hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def _source(name: str, reference: str, observed_at: datetime) -> dict[str, str]:
    return {
        "source_name": name,
        "source_ref": reference,
        "observed_at": observed_at.isoformat(),
    }


def _announcement_risk_review(
    symbol: str,
    rows: list[dict[str, Any]],
    *,
    observed_at: datetime,
    start_date: date,
    end_date: date,
) -> dict[str, Any]:
    """Build a conservative title-based review from the complete anns_d response."""

    if len(rows) >= 2_000:
        raise IpoSprintCollectionError("anns_d response may be truncated")
    normalized: list[dict[str, str]] = []
    for row in rows:
        announced = _day(row.get("ann_date"))
        title = str(row.get("title") or "").strip()
        url = str(row.get("url") or "").strip()
        if (
            row.get("ts_code") != symbol
            or announced is None
            or not start_date <= announced <= end_date
            or not title
            or not re.fullmatch(r"https?://[^\s]+", url)
        ):
            raise IpoSprintCollectionError("anns_d evidence is invalid")
        normalized.append({
            "ann_date": announced.isoformat(),
            "title": title,
            "url": url,
        })
    evidence_hash = _hash(normalized)
    categories: dict[str, Any] = {}
    facts: list[dict[str, Any]] = []
    for category, keywords in _RISK_KEYWORDS.items():
        matches = [
            row for row in normalized
            if any(keyword in row["title"] for keyword in keywords)
        ]
        categories[category] = {
            "status": "FLAGGED" if matches else "CLEAR",
            "has_risk": bool(matches),
            "source": _source(
                "tushare.anns_d.classifier",
                (
                    f"tushare://anns_d/{symbol}/{category}"
                    f"?start={start_date:%Y%m%d}&end={end_date:%Y%m%d}"
                    f"&sha256={evidence_hash}"
                ),
                observed_at,
            ),
        }
        severity = "HIGH" if category in {"announcement", "regulatory"} else "MEDIUM"
        facts.extend({
            "category": category,
            "severity": severity,
            "description": match["title"],
            "source": _source("tushare.anns_d", match["url"], observed_at),
        } for match in matches)
    return {
        "complete": True,
        "status": "FLAGGED" if facts else "CLEAR",
        "categories": categories,
        "facts": facts,
    }


def fetch_sina_quotes(
    symbols: list[str],
    *,
    transport: httpx.BaseTransport | None = None,
) -> dict[str, dict[str, Any]]:
    """Fetch a bounded, dated two-sided Sina level-1 snapshot."""

    if not isinstance(symbols, list) or not 1 <= len(symbols) <= 50 or len(symbols) != len(set(symbols)):
        raise IpoSprintCollectionError("Sina quote request must contain 1-50 unique symbols")
    identities: dict[str, str] = {}
    for symbol in symbols:
        match = re.fullmatch(r"(\d{6})\.(SH|SZ)", symbol)
        if match is None:
            raise IpoSprintCollectionError("Sina quote identity is unsupported")
        identity = ("sh" if match.group(2) == "SH" else "sz") + match.group(1)
        identities[identity] = symbol
    url = SINA_URL + ",".join(identities)
    try:
        with httpx.Client(timeout=8.0, trust_env=False, verify=True, follow_redirects=False, transport=transport) as client:
            with client.stream("GET", url, headers={"Referer": SINA_REFERER}) as response:
                if response.status_code != 200:
                    raise IpoSprintCollectionError("Sina quote request failed")
                chunks: list[bytes] = []
                size = 0
                for chunk in response.iter_bytes():
                    size += len(chunk)
                    if size > MAX_RESPONSE_BYTES:
                        raise IpoSprintCollectionError("Sina quote response exceeds size budget")
                    chunks.append(chunk)
    except IpoSprintCollectionError:
        raise
    except httpx.HTTPError as exc:
        raise IpoSprintCollectionError("Sina quote request failed") from exc
    body = b"".join(chunks)
    try:
        text = body.decode("gb18030")
    except UnicodeDecodeError as exc:
        raise IpoSprintCollectionError("Sina quote response is not decodable") from exc
    response_hash = hashlib.sha256(body).hexdigest()
    result: dict[str, dict[str, Any]] = {}
    for line in text.lstrip("\ufeff").splitlines():
        if not line.strip():
            continue
        match = _ASSIGNMENT.fullmatch(line.strip())
        if match is None or match.group("identity") not in identities:
            raise IpoSprintCollectionError("Sina quote response identity is invalid")
        symbol = identities[match.group("identity")]
        if symbol in result:
            raise IpoSprintCollectionError("Sina quote response contains duplicates")
        fields = match.group("body").split(",")
        if len(fields) < 33:
            raise IpoSprintCollectionError("Sina quote response is incomplete")
        quote_date = _day(fields[30])
        try:
            quote_time = datetime.strptime(fields[31].strip(), "%H:%M:%S").time()
        except ValueError:
            quote_time = None
        observed = datetime.combine(quote_date, quote_time, SHANGHAI) if quote_date and quote_time else None
        last, previous = _finite(fields[3]), _finite(fields[2])
        bid_size, bid, ask_size, ask = (_finite(fields[10]), _finite(fields[11]), _finite(fields[20]), _finite(fields[21]))
        amount = _finite(fields[9])
        status_ok = fields[32].strip() == "00"
        book_ok = all(value is not None and value > 0 for value in (last, bid_size, bid, ask_size, ask))
        result[symbol] = {
            "trade_date": quote_date.isoformat() if quote_date else None,
            "observed_at": observed.isoformat() if observed else None,
            "last_price": last,
            "previous_close": previous,
            "bid_price": bid,
            "ask_price": ask,
            "bid_size": bid_size,
            "ask_size": ask_size,
            "turnover_amount": amount,
            "suspended": not status_ok,
            "tradable": bool(status_ok and book_ok),
            "price_locked": not bool(book_ok and bid < ask),
            "source": _source("sina.level1", f"{SINA_URL}#sha256={response_hash}", observed or datetime.now(timezone.utc)),
        }
    if set(result) != set(symbols):
        raise IpoSprintCollectionError("Sina quote response is incomplete")
    return result


def _indexed_unique(rows: list[dict[str, Any]], field: str, endpoint: str) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = row.get(field)
        if not isinstance(key, str) or not key or key in result:
            raise IpoSprintCollectionError(f"{endpoint} identity is invalid")
        result[key] = row
    return result


def _return_windows(
    *,
    session_number: int,
    issue_price: float,
    last_price: float,
    previous_close: float,
    completed_closes: Mapping[date, float],
    sessions: list[date],
) -> dict[str, Any]:
    windows: dict[str, Any] = {"1d": None, "3d": None, "5d": None}
    baseline_1d = issue_price if session_number == 1 else previous_close
    if baseline_1d > 0:
        windows["1d"] = (last_price / baseline_1d - 1) * 100
    for horizon, label in ((3, "3d"), (5, "5d")):
        if session_number < horizon:
            continue
        baseline_index = session_number - horizon
        if baseline_index == 0:
            baseline = issue_price
        else:
            baseline = completed_closes.get(sessions[baseline_index - 1])
        if baseline is not None and baseline > 0:
            windows[label] = (last_price / baseline - 1) * 100
    windows["available_window"] = {
        "sessions": session_number,
        "return_pct": (last_price / issue_price - 1) * 100,
    }
    return windows


def collect_ipo_sprint_snapshot(
    client: Any,
    *,
    observed_at: datetime,
    start_date: date | None = None,
    quote_reader: Callable[[list[str]], Mapping[str, Mapping[str, Any]]] = fetch_sina_quotes,
) -> dict[str, Any]:
    """Collect a self-contained IPO snapshot without touching research runs."""

    if not isinstance(observed_at, datetime) or observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise IpoSprintCollectionError("observed_at must be timezone-aware")
    observed = observed_at.astimezone(SHANGHAI)
    today = observed.date()
    start = start_date or today - timedelta(days=30)
    if start > today:
        raise IpoSprintCollectionError("start_date cannot be after observation date")
    start_text, end_text = start.strftime("%Y%m%d"), today.strftime("%Y%m%d")

    new_rows = _call(client, "new_share", start_date=start_text, end_date=end_text,
                     fields="ts_code,name,ipo_date,issue_date,price")
    basic_rows = _call(client, "stock_basic", exchange="", list_status="L",
                       fields="ts_code,name,market,exchange,list_date,delist_date")
    basic_by = _indexed_unique(basic_rows, "ts_code", "stock_basic")

    provisional: list[tuple[dict[str, Any], dict[str, Any], list[date], int]] = []
    excluded: list[dict[str, Any]] = []
    calendar_cache: dict[str, list[date]] = {}
    seen_symbols: set[str] = set()

    def exclude(issue: Mapping[str, Any], *reason_codes: str) -> None:
        excluded.append({
            "symbol": str(issue.get("ts_code") or ""),
            "name": str(issue.get("name") or ""),
            "route_origin": "IPO_SPRINT",
            "reason_codes": list(reason_codes),
        })

    for issue in sorted(new_rows, key=lambda row: str(row.get("ts_code", ""))):
        symbol = issue.get("ts_code")
        symbol_text = str(symbol or "")
        if symbol_text in seen_symbols:
            exclude(issue, "DUPLICATE_NEW_SHARE_IDENTITY")
            continue
        seen_symbols.add(symbol_text)
        match = re.fullmatch(r"(\d{6})\.(SH|SZ)", str(symbol or ""))
        listing = _day(issue.get("issue_date"))
        price = _finite(issue.get("price"))
        basic = basic_by.get(str(symbol))
        if match is None:
            exclude(issue, "UNSUPPORTED_SYMBOL")
            continue
        if listing is None or listing > today:
            exclude(issue, "LISTING_DATE_INVALID_OR_FUTURE")
            continue
        if price is None or price <= 0:
            exclude(issue, "ISSUE_PRICE_INVALID")
            continue
        if basic is None:
            exclude(issue, "STOCK_BASIC_MISSING")
            continue
        suffix = match.group(2)
        rule = RULES[suffix]
        if _day(basic.get("list_date")) != listing:
            exclude(issue, "IDENTITY_CONFLICT")
            continue
        if not _blank(basic.get("delist_date")):
            exclude(issue, "DELISTED")
            continue
        exchange = rule["exchange"]
        if exchange not in calendar_cache:
            rows = _call(client, "trade_cal", exchange=exchange, start_date=start_text, end_date=end_text,
                         is_open="1", fields="exchange,cal_date,is_open")
            parsed_days = {
                day for row in rows if str(row.get("is_open")) == "1"
                and (day := _day(row.get("cal_date"))) is not None
            }
            calendar_cache[exchange] = sorted(parsed_days)
        sessions = [day for day in calendar_cache[exchange] if listing <= day <= today]
        if not sessions or sessions[0] != listing:
            exclude(issue, "TRADING_CALENDAR_INCOMPLETE")
            continue
        if today not in sessions:
            exclude(issue, "OBSERVATION_DATE_NOT_OPEN_SESSION")
            continue
        session_number = sessions.index(today) + 1
        if session_number > int(rule["sessions"]):
            exclude(issue, "OUTSIDE_UNBOUNDED_LISTING_WINDOW")
            continue
        provisional.append((issue, basic, sessions, session_number))

    symbols = [str(issue["ts_code"]) for issue, _, _, _ in provisional]
    quotes = dict(quote_reader(symbols)) if symbols else {}
    stocks: list[dict[str, Any]] = []
    for issue, basic, sessions, session_number in provisional:
        symbol = str(issue["ts_code"])
        quote = quotes.get(symbol)
        if not isinstance(quote, Mapping):
            quote = {}
        listing = _day(issue["issue_date"])
        assert listing is not None
        issue_price = _finite(issue["price"])
        suffix = symbol.rsplit(".", 1)[1]
        rule = RULES[suffix]
        daily_rows = _call(client, "daily", ts_code=symbol, start_date=listing.strftime("%Y%m%d"),
                           end_date=end_text, fields="ts_code,trade_date,pre_close,close,amount")
        daily_basic_rows = _call(client, "daily_basic", ts_code=symbol, start_date=listing.strftime("%Y%m%d"),
                                 end_date=end_text, fields="ts_code,trade_date,turnover_rate,volume_ratio")
        announcement_rows = _call(
            client, "anns_d", ts_code=symbol, start_date=start_text, end_date=end_text,
            fields="ann_date,ts_code,name,title,url,rec_time",
        )
        closes = {
            day: close for row in daily_rows
            if (day := _day(row.get("trade_date"))) is not None
            and (close := _finite(row.get("close"))) is not None and day < today
        }
        latest_basic = sorted(
            (row for row in daily_basic_rows if _day(row.get("trade_date")) and _day(row.get("trade_date")) < today),
            key=lambda row: _day(row["trade_date"]), reverse=True,
        )
        basic_latest = latest_basic[0] if latest_basic else {}
        basic_as_of = _day(basic_latest.get("trade_date"))
        last = _finite(quote.get("last_price"))
        previous = _finite(quote.get("previous_close"))
        windows = None
        if issue_price and last and previous:
            windows = _return_windows(
                session_number=session_number, issue_price=issue_price, last_price=last,
                previous_close=previous, completed_closes=closes, sessions=sessions,
            )
        response_hash = _hash({"issue": issue, "basic": basic, "daily": daily_rows, "daily_basic": daily_basic_rows})
        evidence_ref = f"tushare://ipo-sprint/{symbol}#sha256={response_hash}"
        risk_review = _announcement_risk_review(
            symbol, announcement_rows, observed_at=observed, start_date=start, end_date=today,
        )
        realtime = {key: quote.get(key) for key in (
            "trade_date", "observed_at", "last_price", "bid_price", "ask_price",
            "bid_size", "ask_size", "suspended", "tradable", "price_locked", "source",
        )}
        stocks.append({
            "symbol": symbol,
            "name": str(basic.get("name") or issue.get("name") or ""),
            "listing_date": listing.isoformat(),
            "trading_sessions": [
                {"trade_date": day.isoformat(), "limit_regime": "UNBOUNDED"}
                for day in sessions[:session_number]
            ],
            "rule_evidence": {
                "unbounded_session_count": int(rule["sessions"]),
                "source": _source(f"{rule['exchange']}.trading-rules", str(rule["source"]), observed),
            },
            "identity_evidence": _source("tushare.new_share+stock_basic", evidence_ref, observed),
            "returns_pct": {
                "windows": windows or {},
                "source": _source("tushare.daily+sina.level1", evidence_ref + "/returns", observed),
            },
            "liquidity": {
                "turnover_amount": quote.get("turnover_amount"),
                "turnover_amount_source": quote.get("source"),
                "daily_basic_as_of_trade_date": basic_as_of.isoformat() if basic_as_of else None,
                "turnover_rate_pct": _finite(basic_latest.get("turnover_rate")),
                "volume_ratio": _finite(basic_latest.get("volume_ratio")),
                "daily_basic_source": _source(
                    "tushare.daily_basic",
                    evidence_ref + f"/liquidity?as_of={basic_as_of.isoformat() if basic_as_of else 'missing'}",
                    observed,
                ),
            },
            "risk_review": risk_review,
            "realtime": realtime,
        })
    summary = {
        "scanned_count": len(new_rows),
        "included_count": len(stocks),
        "excluded_count": len(excluded),
        "excluded": excluded,
    }
    body = {
        "schema_version": SNAPSHOT_SCHEMA,
        "snapshot_id": "",
        "stocks": stocks,
        "collection_summary": summary,
    }
    body["snapshot_id"] = _hash({
        "observed_at": observed.isoformat(), "stocks": stocks, "collection_summary": summary,
    })
    return body


__all__ = [
    "IpoSprintCollectionError", "collect_ipo_sprint_snapshot", "fetch_sina_quotes",
]
