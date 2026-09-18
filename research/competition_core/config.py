"""Load and validate the small, non-secret competition configuration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import os
from pathlib import Path
import re
from types import MappingProxyType
from typing import Any, Mapping
from urllib.parse import urlparse

import yaml


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "competition.yaml"
RUNTIME_ENDPOINT_ENV = "COMPETITION_RUNTIME_ENDPOINT"

_TOP_LEVEL_KEYS = frozenset({"competition", "runtime"})
_COMPETITION_KEYS = frozenset(
    {
        "start_date",
        "end_date",
        "advertised_trading_days",
        "initial_capital",
        "trading_market",
        "exchanges",
        "allowed_boards",
        "default_cash_ratio",
        "snapshot_phase",
        "universe_exclusions",
        "leaderboard",
        "board_rules",
        "candidate_limits",
        "portfolio",
    }
)
_RUNTIME_KEYS = frozenset({"endpoint"})
_EXCLUSION_KEYS = frozenset({"st", "delisting_risk", "suspended", "untradeable"})
_LEADERBOARD_KEYS = frozenset({"top_n", "show_own_rank", "ranking"})
_RANKING_KEYS = frozenset({"metric", "direction"})
_BOARD_RULE_KEYS = frozenset(
    {"normal_price_limit", "st_price_limit", "new_listing_special_sessions"}
)
_PORTFOLIO_KEYS = frozenset(
    {"max_single_weight", "max_slippage_pct", "max_quote_age_seconds", "execution_mode"}
)
_BOARDS = frozenset({"main", "chinext", "star"})
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class ConfigurationError(ValueError):
    """Raised when the competition configuration is missing or invalid."""


@dataclass(frozen=True, slots=True)
class CompetitionConfig:
    """Validated values needed by the initial Competition Core skeleton."""

    start_date: date
    end_date: date
    advertised_trading_days: int
    initial_capital: Decimal
    trading_market: str
    exchanges: tuple[str, ...]
    allowed_boards: tuple[str, ...]
    default_cash_ratio: Decimal
    snapshot_phase: str
    universe_exclusions: Mapping[str, bool]
    leaderboard_top_n: int
    show_own_rank: bool
    ranking: tuple[tuple[str, str], ...]
    board_rules: Mapping[str, Mapping[str, Decimal | int]]
    candidate_limits: Mapping[str, int]
    max_single_weight: Decimal
    max_slippage_pct: Decimal
    max_quote_age_seconds: int
    execution_mode: str
    runtime_endpoint: str


def load_config(
    path: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> CompetitionConfig:
    """Load the YAML config and apply the runtime endpoint env override only.

    ``COMPETITION_RUNTIME_ENDPOINT`` is intentionally the only environment
    variable read here. Credentials and other secrets are not configuration
    fields and are not accepted by this skeleton.
    """

    config_path = Path(path) if path is not None else DEFAULT_CONFIG_PATH
    try:
        with config_path.open("r", encoding="utf-8") as config_file:
            raw = yaml.safe_load(config_file)
    except OSError as exc:
        raise ConfigurationError(f"cannot read config: {config_path}") from exc
    except yaml.YAMLError as exc:
        raise ConfigurationError(f"invalid YAML in config: {config_path}") from exc

    if not isinstance(raw, dict):
        raise ConfigurationError("config root must be a mapping")
    _reject_unknown_keys(raw, _TOP_LEVEL_KEYS, "config")

    competition = _require_mapping(raw, "competition")
    runtime = _require_mapping(raw, "runtime")
    _reject_unknown_keys(competition, _COMPETITION_KEYS, "competition")
    _reject_unknown_keys(runtime, _RUNTIME_KEYS, "runtime")

    start_date = _parse_date("start_date", _required(competition, "start_date"))
    end_date = _parse_date("end_date", _required(competition, "end_date"))
    if end_date < start_date:
        raise ConfigurationError("end_date must be on or after start_date")

    advertised_trading_days = _parse_positive_integer(
        "advertised_trading_days", _required(competition, "advertised_trading_days")
    )
    initial_capital = _parse_positive_decimal(
        "initial_capital", _required(competition, "initial_capital")
    )
    trading_market = _parse_non_empty_string(
        "trading_market", _required(competition, "trading_market")
    )
    if trading_market != "CN_A":
        raise ConfigurationError("trading_market must be CN_A")
    exchanges = _parse_exchanges(_required(competition, "exchanges"))
    allowed_boards = _parse_allowed_boards(
        _required(competition, "allowed_boards")
    )
    default_cash_ratio = _parse_cash_ratio(
        _required(competition, "default_cash_ratio")
    )
    snapshot_phase = _parse_snapshot_phase(
        _required(competition, "snapshot_phase")
    )
    universe_exclusions = _parse_universe_exclusions(
        _required(competition, "universe_exclusions")
    )
    (
        leaderboard_top_n,
        show_own_rank,
        ranking,
    ) = _parse_leaderboard(_required(competition, "leaderboard"))
    board_rules = _parse_board_rules(_required(competition, "board_rules"))
    candidate_limits = _parse_candidate_limits(
        _required(competition, "candidate_limits")
    )
    (
        max_single_weight,
        max_slippage_pct,
        max_quote_age_seconds,
        execution_mode,
    ) = _parse_portfolio(_required(competition, "portfolio"))

    environment = os.environ if environ is None else environ
    configured_endpoint = _required(runtime, "endpoint")
    endpoint_override = environment.get(RUNTIME_ENDPOINT_ENV)
    runtime_endpoint = configured_endpoint if endpoint_override is None else endpoint_override
    runtime_endpoint = _parse_runtime_endpoint(runtime_endpoint)

    return CompetitionConfig(
        start_date=start_date,
        end_date=end_date,
        advertised_trading_days=advertised_trading_days,
        initial_capital=initial_capital,
        trading_market=trading_market,
        exchanges=exchanges,
        allowed_boards=allowed_boards,
        default_cash_ratio=default_cash_ratio,
        snapshot_phase=snapshot_phase,
        universe_exclusions=MappingProxyType(universe_exclusions),
        leaderboard_top_n=leaderboard_top_n,
        show_own_rank=show_own_rank,
        ranking=ranking,
        board_rules=MappingProxyType(
            {name: MappingProxyType(rule) for name, rule in board_rules.items()}
        ),
        candidate_limits=MappingProxyType(candidate_limits),
        max_single_weight=max_single_weight,
        max_slippage_pct=max_slippage_pct,
        max_quote_age_seconds=max_quote_age_seconds,
        execution_mode=execution_mode,
        runtime_endpoint=runtime_endpoint,
    )


def parse_runtime_endpoint(endpoint: str) -> tuple[str, int]:
    """Return the host and port from a validated runtime endpoint."""

    parsed = urlparse(endpoint)
    try:
        port = parsed.port
    except ValueError as exc:
        raise ConfigurationError("runtime endpoint port must be an integer") from exc
    if parsed.scheme not in {"http", "https"} or parsed.hostname is None or port is None:
        raise ConfigurationError("runtime endpoint must be an http(s) URL with a port")
    return parsed.hostname, port


def _required(mapping: Mapping[str, Any], key: str) -> Any:
    if key not in mapping:
        raise ConfigurationError(f"missing required config value: {key}")
    return mapping[key]


def _require_mapping(mapping: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = _required(mapping, key)
    if not isinstance(value, dict):
        raise ConfigurationError(f"{key} must be a mapping")
    return value


def _reject_unknown_keys(mapping: Mapping[str, Any], allowed: set[str] | frozenset[str], label: str) -> None:
    unknown = sorted(set(mapping) - allowed)
    if unknown:
        raise ConfigurationError(f"unsupported {label} keys: {', '.join(unknown)}")


def _parse_date(name: str, value: Any) -> date:
    if isinstance(value, datetime) or not isinstance(value, (str, date)):
        raise ConfigurationError(f"{name} must be an ISO date (YYYY-MM-DD)")
    if isinstance(value, date):
        return value
    if not _DATE_PATTERN.fullmatch(value):
        raise ConfigurationError(f"{name} must be an ISO date (YYYY-MM-DD)")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be a valid calendar date") from exc


def _parse_positive_decimal(name: str, value: Any) -> Decimal:
    if isinstance(value, bool):
        raise ConfigurationError(f"{name} must be greater than zero")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ConfigurationError(f"{name} must be a number greater than zero") from None
    if not parsed.is_finite() or parsed <= 0:
        raise ConfigurationError(f"{name} must be greater than zero")
    return parsed


def _parse_positive_integer(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigurationError(f"{name} must be a positive integer")
    return value


def _parse_cash_ratio(value: Any) -> Decimal:
    if isinstance(value, bool):
        raise ConfigurationError("default_cash_ratio must be between 0 and 1")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ConfigurationError("default_cash_ratio must be between 0 and 1") from None
    if not parsed.is_finite() or not Decimal("0") <= parsed <= Decimal("1"):
        raise ConfigurationError("default_cash_ratio must be between 0 and 1")
    return parsed


def _parse_non_empty_string(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(f"{name} must be a non-empty string")
    return value.strip()


def _parse_allowed_boards(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ConfigurationError("allowed_boards must be a non-empty list")
    boards = tuple(_parse_non_empty_string("allowed_boards item", item) for item in value)
    if len(set(boards)) != len(boards):
        raise ConfigurationError("allowed_boards must not contain duplicates")
    return boards


def _parse_exchanges(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ConfigurationError("exchanges must be a non-empty list")
    exchanges = tuple(_parse_non_empty_string("exchanges item", item) for item in value)
    if len(set(exchanges)) != len(exchanges):
        raise ConfigurationError("exchanges must not contain duplicates")
    if set(exchanges) != {"SSE", "SZSE"}:
        raise ConfigurationError("exchanges must contain exactly SSE and SZSE")
    return exchanges


def _parse_candidate_limits(value: Any) -> dict[str, int]:
    if not isinstance(value, dict) or not value:
        raise ConfigurationError("candidate_limits must be a non-empty mapping")
    limits: dict[str, int] = {}
    for name, limit in value.items():
        if not isinstance(name, str) or not name.strip():
            raise ConfigurationError("candidate_limits keys must be non-empty strings")
        _parse_positive_integer(f"candidate_limits.{name}", limit)
        limits[name.strip()] = limit
    return limits


def _parse_snapshot_phase(value: Any) -> str:
    phase = _parse_non_empty_string("snapshot_phase", value)
    if phase not in {"preopen", "postclose"}:
        raise ConfigurationError("snapshot_phase must be preopen or postclose")
    return phase


def _parse_portfolio(value: Any) -> tuple[Decimal, Decimal, int, str]:
    if not isinstance(value, dict):
        raise ConfigurationError("portfolio must be a mapping")
    _reject_unknown_keys(value, _PORTFOLIO_KEYS, "portfolio")
    if set(value) != _PORTFOLIO_KEYS:
        raise ConfigurationError("portfolio must define all fields")
    max_single = _parse_cash_ratio(_required(value, "max_single_weight"))
    if max_single <= 0:
        raise ConfigurationError("portfolio.max_single_weight must be greater than zero")
    slippage = _parse_cash_ratio(_required(value, "max_slippage_pct"))
    if slippage > Decimal("0.1"):
        raise ConfigurationError("portfolio.max_slippage_pct must not exceed 0.1")
    age = _parse_positive_integer(
        "portfolio.max_quote_age_seconds", _required(value, "max_quote_age_seconds")
    )
    mode = _parse_non_empty_string("portfolio.execution_mode", _required(value, "execution_mode"))
    if mode != "MANUAL_CONFIRM":
        raise ConfigurationError("portfolio.execution_mode must be MANUAL_CONFIRM")
    return max_single, slippage, age, mode


def _parse_universe_exclusions(value: Any) -> dict[str, bool]:
    if not isinstance(value, dict):
        raise ConfigurationError("universe_exclusions must be a mapping")
    _reject_unknown_keys(value, _EXCLUSION_KEYS, "universe_exclusions")
    if set(value) != _EXCLUSION_KEYS:
        raise ConfigurationError("universe_exclusions must define all exclusion flags")
    if any(not isinstance(item, bool) for item in value.values()):
        raise ConfigurationError("universe_exclusions values must be booleans")
    return dict(value)


def _parse_leaderboard(value: Any) -> tuple[int, bool, tuple[tuple[str, str], ...]]:
    if not isinstance(value, dict):
        raise ConfigurationError("leaderboard must be a mapping")
    _reject_unknown_keys(value, _LEADERBOARD_KEYS, "leaderboard")
    top_n = _parse_positive_integer("leaderboard.top_n", _required(value, "top_n"))
    show_own_rank = _required(value, "show_own_rank")
    if not isinstance(show_own_rank, bool):
        raise ConfigurationError("leaderboard.show_own_rank must be a boolean")
    raw_ranking = _required(value, "ranking")
    if not isinstance(raw_ranking, list) or not raw_ranking:
        raise ConfigurationError("leaderboard.ranking must be a non-empty list")
    ranking: list[tuple[str, str]] = []
    for item in raw_ranking:
        if not isinstance(item, dict):
            raise ConfigurationError("leaderboard.ranking items must be mappings")
        _reject_unknown_keys(item, _RANKING_KEYS, "leaderboard.ranking item")
        metric = _parse_non_empty_string("ranking.metric", _required(item, "metric"))
        direction = _parse_non_empty_string(
            "ranking.direction", _required(item, "direction")
        )
        if direction not in {"ascending", "descending"}:
            raise ConfigurationError("ranking.direction must be ascending or descending")
        ranking.append((metric, direction))
    if ranking[0][0] != "total_assets":
        raise ConfigurationError("total_assets must be the primary ranking metric")
    return top_n, show_own_rank, tuple(ranking)


def _parse_board_rules(value: Any) -> dict[str, dict[str, Decimal | int]]:
    if not isinstance(value, dict) or set(value) != _BOARDS:
        raise ConfigurationError("board_rules must define main, chinext, and star")
    result: dict[str, dict[str, Decimal | int]] = {}
    for board, raw_rule in value.items():
        if not isinstance(raw_rule, dict):
            raise ConfigurationError(f"board_rules.{board} must be a mapping")
        _reject_unknown_keys(raw_rule, _BOARD_RULE_KEYS, f"board_rules.{board}")
        if set(raw_rule) != _BOARD_RULE_KEYS:
            raise ConfigurationError(f"board_rules.{board} must define all rule fields")
        normal = _parse_band(
            f"board_rules.{board}.normal_price_limit",
            raw_rule["normal_price_limit"],
        )
        st_limit = _parse_band(
            f"board_rules.{board}.st_price_limit", raw_rule["st_price_limit"]
        )
        sessions = _parse_positive_integer(
            f"board_rules.{board}.new_listing_special_sessions",
            raw_rule["new_listing_special_sessions"],
        )
        result[board] = {
            "normal_price_limit": normal,
            "st_price_limit": st_limit,
            "new_listing_special_sessions": sessions,
        }
    return result


def _parse_band(name: str, value: Any) -> Decimal:
    parsed = _parse_positive_decimal(name, value)
    if parsed >= 1:
        raise ConfigurationError(f"{name} must be less than 1")
    return parsed


def _parse_runtime_endpoint(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError("runtime endpoint must be a non-empty URL")
    endpoint = value.strip()
    parsed = urlparse(endpoint)
    if parsed.path not in {"", "/"} or parsed.params or parsed.query or parsed.fragment:
        raise ConfigurationError("runtime endpoint must not contain a path or query")
    parse_runtime_endpoint(endpoint)
    return endpoint.rstrip("/")
