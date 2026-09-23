"""Evaluate an independent IPO sprint snapshot without portfolio or order data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import math
from pathlib import Path
import re
from typing import Any, Mapping

import yaml

from contracts.symbol import CanonicalSymbol, SymbolContractError

SNAPSHOT_SCHEMA = "competition.ipo-sprint-snapshot.v1"
RESULT_SCHEMA = "competition.ipo-sprint-result.v1"
ROUTE_ORIGIN = "IPO_SPRINT"
SHANGHAI = timezone(timedelta(hours=8))
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "ipo-sprint.yaml"
_TOP_LEVEL_FIELDS = {"schema_version", "snapshot_id", "stocks", "collection_summary"}
_REQUIRED_TOP_LEVEL_FIELDS = _TOP_LEVEL_FIELDS
_STOCK_FIELDS = {
    "symbol", "name", "listing_date", "trading_sessions", "rule_evidence",
    "identity_evidence", "returns_pct", "liquidity", "risk_review", "realtime",
}
_REGIMES = {"UNBOUNDED", "LIMITED", "UNKNOWN"}
_RISK_STATUSES = {"CLEAR", "FLAGGED", "INCOMPLETE"}
_RISK_SEVERITIES = {"INFO", "LOW", "MEDIUM", "HIGH", "CRITICAL"}
_RISK_CATEGORIES = {"announcement", "regulatory", "reduction", "earnings"}
_CANONICAL_A_SHARE_PATTERN = re.compile(r"(?P<code>[0-9]{6})\.(?P<suffix>SH|SZ|BJ)\Z")
_RETURN_SOURCE_FIELDS = {"windows", "source"}
_LIQUIDITY_FIELDS = {
    "turnover_amount", "turnover_amount_source", "daily_basic_as_of_trade_date",
    "turnover_rate_pct", "volume_ratio", "daily_basic_source",
}
_REALTIME_FIELDS = {
    "trade_date", "observed_at", "last_price", "bid_price", "ask_price",
    "bid_size", "ask_size", "suspended", "tradable", "price_locked", "source",
}


class IpoSprintError(ValueError):
    """Raised when the independent snapshot or its policy is invalid."""


def _number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed


def _date(value: Any) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _is_canonical_a_share(value: Any) -> bool:
    if not isinstance(value, str) or value != value.strip():
        return False
    match = _CANONICAL_A_SHARE_PATTERN.fullmatch(value)
    if match is None or match.group("code") == "000000":
        return False
    if match.group("suffix") == "BJ":
        # The shared CanonicalSymbol contract currently covers Shanghai and
        # Shenzhen only; Beijing symbols still use the same six-digit core form.
        return True
    try:
        CanonicalSymbol.from_core(value)
    except SymbolContractError:
        return False
    return True


def _source_is_valid(
    value: Any, *, evaluated_at: datetime, max_age_seconds: int | None = None
) -> bool:
    if not isinstance(value, Mapping) or set(value) != {"source_name", "source_ref", "observed_at"}:
        return False
    observed_at = _timestamp(value.get("observed_at"))
    valid = (
        isinstance(value.get("source_name"), str)
        and bool(value["source_name"].strip())
        and isinstance(value.get("source_ref"), str)
        and bool(value["source_ref"].strip())
        and observed_at is not None
        and observed_at.astimezone(timezone.utc) <= evaluated_at
    )
    if not valid or max_age_seconds is None:
        return valid
    return (
        evaluated_at - observed_at.astimezone(timezone.utc)
    ).total_seconds() <= max_age_seconds


def _risk_source_is_valid(
    value: Any,
    *,
    symbol: str,
    category: str,
    evaluated_at: datetime,
    max_age_seconds: int,
) -> bool:
    if not _source_is_valid(
        value, evaluated_at=evaluated_at, max_age_seconds=max_age_seconds,
    ):
        return False
    return (
        value["source_name"] == "tushare.anns_d.classifier"
        and re.fullmatch(
            rf"tushare://anns_d/{re.escape(symbol)}/{category}"
            r"\?start=\d{8}&end=\d{8}&sha256=[0-9a-f]{64}",
            value["source_ref"],
        ) is not None
    )


def _is_continuous_auction(value: datetime) -> bool:
    local_time = value.astimezone(SHANGHAI).time().replace(tzinfo=None)
    return (
        time(9, 30) <= local_time < time(11, 30)
        or time(13, 0) <= local_time < time(14, 57)
    )


@dataclass(frozen=True)
class IpoSprintConfig:
    max_quote_age_seconds: int = 60
    max_risk_age_seconds: int = 86_400
    min_turnover_amount: float = 50_000_000
    min_turnover_rate_pct: float = 1.0
    min_volume_ratio: float = 1.0
    min_return_pct: float = 10.0

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "IpoSprintConfig":
        if not isinstance(value, Mapping) or set(value) != {
            "version", "quote", "risk", "liquidity", "momentum"
        }:
            raise IpoSprintError("invalid IPO sprint configuration fields")
        if value.get("version") != "ipo-sprint.v1":
            raise IpoSprintError("unsupported IPO sprint configuration version")
        sections = ("quote", "risk", "liquidity", "momentum")
        if any(not isinstance(value.get(section), Mapping) for section in sections):
            raise IpoSprintError("invalid IPO sprint configuration section")
        quote, risk, liquidity, momentum = (value[name] for name in sections)
        if set(quote) != {"max_age_seconds"}:
            raise IpoSprintError("invalid quote configuration fields")
        if set(risk) != {"max_age_seconds"}:
            raise IpoSprintError("invalid risk configuration fields")
        if set(liquidity) != {
            "minimum_turnover_amount", "minimum_turnover_rate_pct", "minimum_volume_ratio"
        }:
            raise IpoSprintError("invalid liquidity configuration fields")
        if set(momentum) != {"minimum_return_pct"}:
            raise IpoSprintError("invalid momentum configuration fields")

        age = quote["max_age_seconds"]
        risk_age = risk["max_age_seconds"]
        thresholds = (
            liquidity["minimum_turnover_amount"],
            liquidity["minimum_turnover_rate_pct"],
            liquidity["minimum_volume_ratio"],
            momentum["minimum_return_pct"],
        )
        if (
            type(age) is not int or age <= 0
            or type(risk_age) is not int or risk_age <= 0
            or not all(_number(item) and item >= 0 for item in thresholds)
        ):
            raise IpoSprintError("invalid IPO sprint threshold")
        return cls(age, risk_age, *map(float, thresholds))


def load_ipo_sprint_config(path: Path = DEFAULT_CONFIG_PATH) -> IpoSprintConfig:
    try:
        payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise IpoSprintError("could not load IPO sprint configuration") from exc
    return IpoSprintConfig.from_mapping(payload)


def _validate_snapshot(snapshot: Any) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    if (
        not isinstance(snapshot, Mapping)
        or not _REQUIRED_TOP_LEVEL_FIELDS.issubset(snapshot)
        or not set(snapshot).issubset(_TOP_LEVEL_FIELDS)
    ):
        raise IpoSprintError("snapshot contains invalid top-level fields")
    if snapshot.get("schema_version") != SNAPSHOT_SCHEMA:
        raise IpoSprintError("unsupported IPO sprint snapshot schema")
    if not isinstance(snapshot.get("snapshot_id"), str) or not snapshot["snapshot_id"].strip():
        raise IpoSprintError("snapshot_id is required")
    stocks = snapshot.get("stocks")
    if not isinstance(stocks, list):
        raise IpoSprintError("stocks must be a list")
    seen: set[str] = set()
    for stock in stocks:
        if not isinstance(stock, Mapping) or not set(stock).issubset(_STOCK_FIELDS):
            raise IpoSprintError("stock contains unsupported fields")
        symbol = stock.get("symbol")
        if not isinstance(symbol, str) or not symbol.strip():
            raise IpoSprintError("every stock requires a symbol")
        if symbol in seen:
            raise IpoSprintError("duplicate stock symbol")
        seen.add(symbol)
    summary = snapshot["collection_summary"]
    if (
        not isinstance(summary, Mapping)
        or set(summary) != {"scanned_count", "included_count", "excluded_count", "excluded"}
        or type(summary.get("scanned_count")) is not int
        or type(summary.get("included_count")) is not int
        or type(summary.get("excluded_count")) is not int
        or not isinstance(summary.get("excluded"), list)
        or summary["included_count"] != len(stocks)
        or summary["excluded_count"] != len(summary["excluded"])
        or summary["scanned_count"] != summary["included_count"] + summary["excluded_count"]
    ):
        raise IpoSprintError("collection_summary is invalid")
    for item in summary["excluded"]:
        if (
            not isinstance(item, Mapping)
            or set(item) != {"symbol", "name", "route_origin", "reason_codes"}
            or not isinstance(item.get("symbol"), str)
            or not isinstance(item.get("name"), str)
            or item.get("route_origin") != ROUTE_ORIGIN
            or not isinstance(item.get("reason_codes"), list)
            or not item["reason_codes"]
            or any(not isinstance(code, str) or not code for code in item["reason_codes"])
        ):
            raise IpoSprintError("collection exclusion is invalid")
    return stocks, list(summary["excluded"])


def _returns(
    payload: Any, session_number: int
) -> tuple[tuple[float, int, str], tuple[float, int, str]] | None:
    if not isinstance(payload, Mapping) or not set(payload).issubset(
        {"1d", "3d", "5d", "available_window"}
    ):
        return None
    windows: list[tuple[float, int, str]] = []
    for label, horizon in (("1d", 1), ("3d", 3), ("5d", 5)):
        value = payload.get(label)
        if value is None:
            continue
        if not _number(value) or horizon > session_number:
            return None
        windows.append((float(value), horizon, label))
    custom = payload.get("available_window")
    if custom is not None:
        if not isinstance(custom, Mapping) or set(custom) != {"sessions", "return_pct"}:
            return None
        horizon, value = custom["sessions"], custom["return_pct"]
        if type(horizon) is not int or horizon < 1 or horizon > session_number or not _number(value):
            return None
        windows.append((float(value), horizon, f"{horizon}d_available"))
    if not windows:
        return None
    best_return = max(windows, key=lambda item: (item[0], item[1], item[2]))
    strongest_momentum = max(
        windows, key=lambda item: (item[0] / item[1], item[0], item[1], item[2])
    )
    return best_return, strongest_momentum


def _evaluate_stock(
    stock: Mapping[str, Any], *, evaluated_at: datetime, config: IpoSprintConfig
) -> tuple[dict[str, Any] | None, list[str]]:
    symbol = stock["symbol"]
    if not _is_canonical_a_share(symbol):
        return None, ["SYMBOL_INVALID"]
    name = stock.get("name")
    if not isinstance(name, str) or not name.strip():
        return None, ["IDENTITY_MISSING"]

    failures: list[str] = []
    listing_date = _date(stock.get("listing_date"))
    if listing_date is None or not _source_is_valid(
        stock.get("identity_evidence"), evaluated_at=evaluated_at
    ):
        failures.append("IDENTITY_EVIDENCE_INVALID")

    sessions = stock.get("trading_sessions")
    rule = stock.get("rule_evidence")
    session_number: int | None = None
    trade_date: date | None = None
    expected_liquidity_date: date | None = None
    if (
        listing_date is None
        or not isinstance(sessions, list)
        or not sessions
        or not isinstance(rule, Mapping)
        or set(rule) != {"unbounded_session_count", "source"}
        or type(rule.get("unbounded_session_count")) is not int
        or rule.get("unbounded_session_count", 0) < 1
        or not _source_is_valid(rule.get("source"), evaluated_at=evaluated_at)
    ):
        failures.append("LISTING_RULE_EVIDENCE_INVALID")
    else:
        dates: list[date] = []
        regimes: list[str] = []
        valid_sequence = True
        for item in sessions:
            if not isinstance(item, Mapping) or set(item) != {"trade_date", "limit_regime"}:
                valid_sequence = False
                break
            session_date = _date(item.get("trade_date"))
            regime = item.get("limit_regime")
            if session_date is None or not isinstance(regime, str) or regime not in _REGIMES:
                valid_sequence = False
                break
            dates.append(session_date)
            regimes.append(regime)
        quote = stock.get("realtime")
        quoted_date = _date(quote.get("trade_date")) if isinstance(quote, Mapping) else None
        if (
            not valid_sequence
            or not dates
            or dates[0] != listing_date
            or dates != sorted(set(dates))
            or quoted_date not in dates
        ):
            failures.append("TRADING_SESSION_SEQUENCE_INVALID")
        else:
            session_number = dates.index(quoted_date) + 1
            trade_date = quoted_date
            if session_number > 1:
                expected_liquidity_date = dates[session_number - 2]
            window_size = rule["unbounded_session_count"]
            if session_number > window_size:
                failures.append("OUTSIDE_UNBOUNDED_LISTING_WINDOW")
            if any(regime != "UNBOUNDED" for regime in regimes[:min(session_number, window_size)]):
                failures.append("LISTING_WINDOW_REGIME_INVALID")
            if regimes[session_number - 1] != "UNBOUNDED":
                failures.append("LIMIT_REGIME_NOT_UNBOUNDED")

    return_data = stock.get("returns_pct")
    returns: tuple[tuple[float, int, str], tuple[float, int, str]] | None = None
    if (
        not isinstance(return_data, Mapping)
        or set(return_data) != _RETURN_SOURCE_FIELDS
        or not _source_is_valid(return_data.get("source"), evaluated_at=evaluated_at)
    ):
        failures.append("RETURN_EVIDENCE_INVALID")
    elif session_number is not None:
        returns = _returns(return_data.get("windows"), session_number)
    if returns is None:
        failures.append("RETURN_WINDOW_INVALID")
    elif returns[0][0] < config.min_return_pct:
        failures.append("MOMENTUM_BELOW_MINIMUM")

    liquidity = stock.get("liquidity")
    if not isinstance(liquidity, Mapping):
        failures.append("LIQUIDITY_FACTS_INVALID")
        liquidity_values = None
    else:
        if set(liquidity) != _LIQUIDITY_FIELDS:
            if (
                "turnover_amount_source" not in liquidity
                or "daily_basic_source" not in liquidity
            ):
                failures.append("LIQUIDITY_EVIDENCE_INVALID")
            failures.append("LIQUIDITY_FACTS_INVALID")
            liquidity_values = None
        elif not all(_number(liquidity.get(key)) for key in (
            "turnover_amount", "turnover_rate_pct", "volume_ratio"
        )):
            failures.append("LIQUIDITY_FACTS_INVALID")
            liquidity_values = None
        else:
            realtime_source = (
                stock["realtime"].get("source")
                if isinstance(stock.get("realtime"), Mapping) else None
            )
            if (
                not _source_is_valid(
                    liquidity.get("turnover_amount_source"), evaluated_at=evaluated_at,
                    max_age_seconds=config.max_quote_age_seconds,
                )
                or liquidity.get("turnover_amount_source") != realtime_source
                or not _source_is_valid(
                    liquidity.get("daily_basic_source"), evaluated_at=evaluated_at,
                )
            ):
                failures.append("LIQUIDITY_EVIDENCE_INVALID")
            liquidity_date = _date(liquidity.get("daily_basic_as_of_trade_date"))
            if liquidity_date is None or liquidity_date != expected_liquidity_date:
                failures.append("LIQUIDITY_AS_OF_DATE_INVALID")
            liquidity_values = {
                key: float(liquidity[key])
                for key in ("turnover_amount", "turnover_rate_pct", "volume_ratio")
            }
            if (
                liquidity_values["turnover_amount"] <= 0
                or liquidity_values["turnover_rate_pct"] <= 0
                or liquidity_values["volume_ratio"] <= 0
            ):
                failures.append("LIQUIDITY_FACTS_INVALID")
            elif (
                liquidity_values["turnover_amount"] < config.min_turnover_amount
                or liquidity_values["turnover_rate_pct"] < config.min_turnover_rate_pct
                or liquidity_values["volume_ratio"] < config.min_volume_ratio
            ):
                failures.append("LIQUIDITY_BELOW_MINIMUM")

    risk = stock.get("risk_review")
    if (
        not isinstance(risk, Mapping)
        or set(risk) != {"complete", "status", "categories", "facts"}
        or risk.get("complete") is not True
        or not isinstance(risk.get("status"), str)
        or risk.get("status") not in _RISK_STATUSES
        or not isinstance(risk.get("categories"), Mapping)
        or not isinstance(risk.get("facts"), list)
    ):
        failures.append("RISK_FACTS_INCOMPLETE")
    else:
        facts_valid = True
        has_blocking_fact = False
        category_sources: list[str] = []
        categories = risk["categories"]
        if set(categories) != _RISK_CATEGORIES:
            facts_valid = False
        for category in sorted(_RISK_CATEGORIES):
            review = categories.get(category)
            if (
                not isinstance(review, Mapping)
                or set(review) != {"status", "has_risk", "source"}
                or not isinstance(review.get("status"), str)
                or review.get("status") not in _RISK_STATUSES
                or type(review.get("has_risk")) is not bool
                or not _risk_source_is_valid(
                    review.get("source"), symbol=symbol, category=category,
                    evaluated_at=evaluated_at,
                    max_age_seconds=config.max_risk_age_seconds,
                )
            ):
                facts_valid = False
                continue
            source_ref = review["source"]["source_ref"]
            category_sources.append(source_ref)
            status = review["status"]
            has_risk = review["has_risk"]
            if status == "INCOMPLETE" or (status == "CLEAR") != (not has_risk):
                facts_valid = False
            if status == "FLAGGED" or has_risk:
                has_blocking_fact = True
        if len(category_sources) != len(set(category_sources)):
            facts_valid = False
        for fact in risk["facts"]:
            if (
                not isinstance(fact, Mapping)
                or set(fact) != {"category", "severity", "description", "source"}
                or not isinstance(fact.get("category"), str)
                or fact.get("category") not in _RISK_CATEGORIES
                or not isinstance(fact.get("severity"), str)
                or fact.get("severity") not in _RISK_SEVERITIES
                or not isinstance(fact.get("description"), str)
                or not fact["description"].strip()
                or not _source_is_valid(
                    fact.get("source"), evaluated_at=evaluated_at,
                    max_age_seconds=config.max_risk_age_seconds,
                )
            ):
                facts_valid = False
                break
            if fact["severity"] in {"MEDIUM", "HIGH", "CRITICAL"}:
                has_blocking_fact = True
        if not facts_valid:
            failures.append("RISK_FACTS_INCOMPLETE")
        elif risk["status"] == "INCOMPLETE":
            failures.append("RISK_FACTS_INCOMPLETE")
        elif risk["status"] != "CLEAR" or has_blocking_fact:
            failures.append("RISK_FLAGGED")

    quote = stock.get("realtime")
    if (
        not isinstance(quote, Mapping)
        or not set(quote).issubset(_REALTIME_FIELDS)
        or not (_REALTIME_FIELDS - {"source"}).issubset(quote)
    ):
        failures.append("REALTIME_FACTS_INVALID")
    else:
        if not _source_is_valid(quote.get("source"), evaluated_at=evaluated_at):
            failures.append("REALTIME_EVIDENCE_INVALID")
        else:
            source_at = _timestamp(quote["source"].get("observed_at"))
            if (
                source_at is not None
                and (evaluated_at - source_at.astimezone(timezone.utc)).total_seconds()
                > config.max_quote_age_seconds
            ):
                failures.append("REALTIME_EVIDENCE_STALE")
        observed_at = _timestamp(quote.get("observed_at"))
        if observed_at is None:
            failures.append("QUOTE_TIMESTAMP_INVALID")
        else:
            age_seconds = (evaluated_at - observed_at.astimezone(timezone.utc)).total_seconds()
            if age_seconds < 0:
                failures.append("QUOTE_FROM_FUTURE")
            elif age_seconds > config.max_quote_age_seconds:
                failures.append("QUOTE_STALE")
            if trade_date is not None and observed_at.astimezone(SHANGHAI).date() != trade_date:
                failures.append("QUOTE_TRADE_DATE_MISMATCH")
            if (
                not _is_continuous_auction(observed_at)
                or not _is_continuous_auction(evaluated_at)
            ):
                failures.append("OUTSIDE_CONTINUOUS_AUCTION")
        numeric_quote = ("last_price", "bid_price", "ask_price", "bid_size", "ask_size")
        if not all(_number(quote.get(key)) for key in numeric_quote):
            failures.append("ORDER_BOOK_INVALID")
        elif (
            quote["last_price"] <= 0
            or quote["bid_price"] <= 0
            or quote["ask_price"] <= 0
            or quote["bid_size"] <= 0
            or quote["ask_size"] <= 0
            or quote["bid_price"] >= quote["ask_price"]
        ):
            failures.append("ORDER_BOOK_INVALID")
        if quote.get("suspended") is not False:
            failures.append("SUSPENDED")
        if quote.get("tradable") is not True:
            failures.append("NOT_TRADABLE")
        if quote.get("price_locked") is not False:
            failures.append("PRICE_LOCKED")

    if failures:
        return None, list(dict.fromkeys(failures))

    assert trade_date is not None and session_number is not None
    assert returns is not None and liquidity_values is not None
    best_return, strongest_momentum = returns
    return {
        "symbol": symbol,
        "name": name,
        "route_origin": ROUTE_ORIGIN,
        "listing_date": listing_date.isoformat(),
        "trade_date": trade_date.isoformat(),
        "listing_session_number": session_number,
        "limit_regime": "UNBOUNDED",
        "best_return_pct": best_return[0],
        "best_return_window": best_return[2],
        "momentum_pct_per_session": strongest_momentum[0] / strongest_momentum[1],
        "turnover_amount": liquidity_values["turnover_amount"],
        "turnover_rate_pct": liquidity_values["turnover_rate_pct"],
        "volume_ratio": liquidity_values["volume_ratio"],
        "quote_observed_at": quote["observed_at"],
    }, []


def evaluate_ipo_sprint_snapshot(
    snapshot: Any,
    *,
    evaluated_at: datetime | None = None,
    config: IpoSprintConfig | None = None,
) -> dict[str, Any]:
    """Return only independent IPO sprint candidates and their rejected peers."""

    stocks, collection_excluded = _validate_snapshot(snapshot)
    now = evaluated_at if evaluated_at is not None else datetime.now(timezone.utc)
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise IpoSprintError("evaluated_at must include a timezone")
    now = now.astimezone(timezone.utc)
    if config is not None and not isinstance(config, IpoSprintConfig):
        raise IpoSprintError("config must be an IpoSprintConfig")
    policy = config if config is not None else load_ipo_sprint_config()
    candidates: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for stock in stocks:
        candidate, failures = _evaluate_stock(stock, evaluated_at=now, config=policy)
        if candidate is not None:
            candidates.append(candidate)
        else:
            rejected.append({
                "symbol": stock["symbol"],
                "name": stock.get("name") if isinstance(stock.get("name"), str) else None,
                "route_origin": ROUTE_ORIGIN,
                "reason_codes": failures,
            })
    candidates.sort(key=lambda row: (
        -row["momentum_pct_per_session"],
        -row["best_return_pct"],
        -row["turnover_amount"],
        row["symbol"],
    ))
    rejected.sort(key=lambda row: row["symbol"])
    return {
        "schema_version": RESULT_SCHEMA,
        "snapshot_id": snapshot["snapshot_id"],
        "evaluated_at": now.isoformat().replace("+00:00", "Z"),
        "route_origin": ROUTE_ORIGIN,
        "candidate_count": len(candidates),
        "rejected_count": len(rejected),
        "collection_excluded_count": len(collection_excluded),
        "candidates": candidates,
        "rejected": rejected,
        "collection_excluded": collection_excluded,
        "read_only": True,
        "execution_ready": False,
        "production_accepted": False,
        "orders_submitted": 0,
    }
