"""Prepare and validate one point-in-time canonical dataset."""

from __future__ import annotations

import argparse
import ast
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
import hashlib
import importlib
import json
import math
import os
from pathlib import Path
import random
import re
import sys
from typing import Any, Callable
from urllib.parse import urlsplit

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from adapters.tushare.client import TushareConfigurationError, create_tushare_client
from competition_core.config import load_config
from competition_core.pipeline.artifacts import atomic_bytes, atomic_json, file_hash, content_hash
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.config import load_pipeline_config
from competition_core.pipeline.sources import ResearchDataClient
from competition_core.pipeline.time_context import ResearchContext, SHANGHAI
from competition_core.universe import CompetitionUniverse
from competition_core.universe.service import _market_data_reference_date
from contracts.symbol import CanonicalSymbol, SymbolContractError


_ALLOWED_ENV = frozenset({"TUSHARE_TOKEN", "TUSHARE_BASE_URL"})
_SNAPSHOT_CONTENT_FIELDS = (
    "schema_version",
    "competition_id",
    "as_of",
    "snapshot_phase",
    "market_data_reference_date",
    "members",
    "sources",
    "warnings",
)
_SNAPSHOT_TRANSPORT_FLAGS = frozenset({"stale", "fallback_used"})
_SECRET_KEY = re.compile(
    r"(?:token|secret|password|api[_-]?key|authorization)", re.IGNORECASE
)
_SECRET_ASSIGNMENT = re.compile(
    r"""(?P<prefix>(?:"|')?\b(?:token|secret|password|api[_-]?key|authorization)\b(?:"|')?\s*[:=]\s*)(?P<value>"(?:\\.|[^"])*"|'(?:\\.|[^'])*'|[^\s,}\]]+)""",
    re.IGNORECASE,
)
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_VALIDATION_FIXED = ("300750.SZ", "600519.SH", "000001.SZ")
_VALIDATION_SEED = 20260902


def _safe_text(value: object, secrets: Sequence[str] = ()) -> str:
    """Redact secret assignments, including quoted JSON-style assignments."""

    text = str(value)

    def replace_assignment(match: re.Match[str]) -> str:
        raw = match.group("value")
        if raw[:1] in {"'", '"'}:
            replacement = f"{raw[0]}[REDACTED]{raw[0]}"
        else:
            replacement = "[REDACTED]"
        return f"{match.group('prefix')}{replacement}"

    text = _SECRET_ASSIGNMENT.sub(replace_assignment, text)
    for secret in sorted(
        {item for item in secrets if isinstance(item, str) and len(item) >= 4},
        key=len,
        reverse=True,
    ):
        text = text.replace(secret, "[REDACTED]")
    return text


def _safe_details(value: object, secrets: Sequence[str] = ()) -> Any:
    """Convert report/error details to JSON-safe values without exposing keys."""

    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            raw_key = str(key)
            safe_key = _safe_text(key, secrets)
            result[safe_key] = (
                "[REDACTED]"
                if _SECRET_KEY.search(raw_key)
                else _safe_details(item, secrets)
            )
        return result
    if isinstance(value, (list, tuple, set)):
        return [_safe_details(item, secrets) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, str):
        return _safe_text(value, secrets)
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if hasattr(value, "item"):
        try:
            return _safe_details(value.item(), secrets)
        except (TypeError, ValueError):
            pass
    try:
        missing = pd.isna(value)
        if isinstance(missing, bool) and missing:
            return None
    except (TypeError, ValueError):
        pass
    return _safe_text(value, secrets)


def _parse_date(value: str) -> date:
    if not isinstance(value, str) or not _DATE_RE.fullmatch(value):
        raise PipelineError("INVALID_ARGUMENTS", "--date must be YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise PipelineError("INVALID_ARGUMENTS", "--date is not a valid calendar date") from exc


def _parse_as_of(value: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise PipelineError("INVALID_ARGUMENTS", "--as-of must be a timezone-aware ISO datetime")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise PipelineError("INVALID_ARGUMENTS", "--as-of must be a timezone-aware ISO datetime") from exc
    if parsed.utcoffset() is None:
        raise PipelineError("INVALID_ARGUMENTS", "--as-of must include a timezone offset")
    return parsed


def _parse_sessions(value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("sessions must be a positive integer") from exc
    if parsed <= 0 or parsed > 2000:
        raise argparse.ArgumentTypeError("sessions must be between 1 and 2000")
    return parsed


def _unquote_env_value(value: str) -> str:
    raw = value.strip()
    if not raw or raw[0] not in {"'", '"'}:
        return raw
    quote = raw[0]
    closing = None
    escaped = False
    for index in range(1, len(raw)):
        character = raw[index]
        if character == quote and not escaped:
            closing = index
            break
        escaped = character == "\\" and not escaped
        if character != "\\":
            escaped = False
    trailing = "" if closing is None else raw[closing + 1 :].strip()
    if closing is None or (trailing and not trailing.startswith("#")):
        raise PipelineError("INVALID_ARGUMENTS", "malformed quoted environment value")
    quoted = raw[: closing + 1]
    if quote == '"':
        try:
            decoded = ast.literal_eval(quoted)
        except (SyntaxError, ValueError) as exc:
            raise PipelineError("INVALID_ARGUMENTS", "malformed quoted environment value") from exc
        if not isinstance(decoded, str):
            raise PipelineError("INVALID_ARGUMENTS", "malformed quoted environment value")
        return decoded
    return raw[1:closing].replace("\\'", "'").replace("\\\\", "\\")


def _read_allowlisted_env(path: str | Path | None) -> dict[str, str]:
    """Read only the two supported variables; no dotenv dependency is needed."""

    if path is None:
        return {}
    target = Path(path)
    try:
        lines = target.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise PipelineError("NOT_CONFIGURED", "cannot read the explicit env file") from exc

    values: dict[str, str] = {}
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = re.match(r"^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=(.*)$", stripped)
        if match is None:
            continue
        key = match.group(1)
        if key in _ALLOWED_ENV:
            values[key] = _unquote_env_value(match.group(2))
    return values


def _credential_env(args: argparse.Namespace) -> dict[str, str]:
    values = _read_allowlisted_env(args.env_file)
    token_file = getattr(args, "token_file", None)
    if token_file is not None:
        try:
            token = Path(token_file).read_text(encoding="utf-8-sig").strip()
        except (OSError, UnicodeError) as exc:
            raise PipelineError("NOT_CONFIGURED", "cannot read the explicit token file") from exc
        if not token or any(character.isspace() for character in token):
            raise PipelineError("NOT_CONFIGURED", "token file must contain one nonempty token")
        values["TUSHARE_TOKEN"] = token
    base_url = getattr(args, "base_url", None)
    if base_url is not None:
        values["TUSHARE_BASE_URL"] = base_url.strip()
    return values


@contextmanager
def _temporary_env(values: Mapping[str, str]):
    previous = {key: os.environ.get(key) for key in _ALLOWED_ENV}
    try:
        for key, value in values.items():
            # Desktop secure storage intentionally leaves dotenv tokens blank
            # while injecting the real credential into the child environment.
            if key == "TUSHARE_TOKEN" and not value.strip() and previous.get(key):
                continue
            os.environ[key] = value
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _actual_now(clock: Callable[[], datetime]) -> datetime:
    try:
        observed_at = clock()
    except Exception as exc:  # noqa: BLE001 - clock is an injectable test seam
        raise PipelineError("NOT_READY", "observed clock is unavailable") from exc
    if not isinstance(observed_at, datetime) or observed_at.utcoffset() is None:
        raise PipelineError("NOT_READY", "observed clock must return a timezone-aware datetime")
    return observed_at


def _source_name() -> str:
    configured = os.environ.get("TUSHARE_BASE_URL", "").strip()
    base_url = configured or "https://api.tushare.pro"
    try:
        hostname = urlsplit(base_url).hostname
    except ValueError:
        hostname = None
    return hostname.lower() if hostname else "tushare"


def _coerce_day(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(SHANGHAI).date() if value.utcoffset() else value.date()
    if isinstance(value, date):
        return value
    try:
        missing = pd.isna(value)
        if isinstance(missing, bool) and missing:
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if not text or text.lower() in {"nan", "nat", "none"}:
        return None
    compact = text.replace("-", "")
    if len(compact) >= 8 and compact[:8].isdigit():
        try:
            return datetime.strptime(compact[:8], "%Y%m%d").date()
        except ValueError:
            return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _fetch_calendar(client: Any, research_date: date) -> pd.DataFrame:
    start_date = (research_date - timedelta(days=31)).strftime("%Y%m%d")
    end_date = research_date.strftime("%Y%m%d")
    try:
        calendar = client.trade_cal(
            exchange="SSE",
            start_date=start_date,
            end_date=end_date,
            fields="exchange,cal_date,is_open,pretrade_date",
        )
    except Exception as exc:  # noqa: BLE001 - provider details are not report-safe
        raise PipelineError("SOURCE_UNAVAILABLE", "trade_cal request failed", endpoint="trade_cal") from exc
    if not isinstance(calendar, pd.DataFrame) or calendar.empty:
        raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "trade_cal returned no calendar rows")
    if not {"cal_date", "is_open"} <= set(calendar.columns):
        raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "trade_cal response is missing calendar fields")
    for raw_day in calendar["cal_date"]:
        parsed = _coerce_day(raw_day)
        if parsed is None or parsed < research_date - timedelta(days=31) or parsed > research_date:
            raise PipelineError("STALE_DATA", "trade_cal returned a date outside the request")
    return calendar


def _universe_snapshot_id(snapshot: Mapping[str, Any]) -> str:
    allowed = set(_SNAPSHOT_CONTENT_FIELDS) | {"snapshot_id"} | set(_SNAPSHOT_TRANSPORT_FLAGS)
    unknown = sorted(set(snapshot) - allowed)
    if unknown:
        raise PipelineError(
            "STALE_DATA",
            "Universe snapshot contains unsupported top-level fields",
            fields=unknown,
        )
    missing = [field for field in _SNAPSHOT_CONTENT_FIELDS if field not in snapshot]
    if missing:
        raise PipelineError(
            "STALE_DATA",
            "Universe snapshot is missing content fields",
            fields=missing,
        )
    for field in _SNAPSHOT_TRANSPORT_FLAGS:
        if field in snapshot and type(snapshot[field]) is not bool:
            raise PipelineError(
                "STALE_DATA",
                "Universe snapshot transport flags must be boolean",
                field=field,
            )
    content = {key: snapshot[key] for key in _SNAPSHOT_CONTENT_FIELDS}
    payload = json.dumps(
        content,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _read_snapshot(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PipelineError("STALE_DATA", "cannot read the Universe snapshot") from exc
    if not isinstance(payload, dict):
        raise PipelineError("STALE_DATA", "Universe snapshot must be a JSON object")
    supplied = payload.get("snapshot_id")
    if not isinstance(supplied, str) or supplied != _universe_snapshot_id(payload):
        raise PipelineError("STALE_DATA", "Universe snapshot content hash does not match snapshot_id")
    return payload


def _eligible_symbols(snapshot: Mapping[str, Any]) -> list[str]:
    members = snapshot.get("members")
    if not isinstance(members, list):
        raise PipelineError("STALE_DATA", "Universe snapshot members must be a list")
    symbols: list[str] = []
    for member in members:
        if not isinstance(member, Mapping):
            raise PipelineError("STALE_DATA", "Universe snapshot contains an invalid member")
        if member.get("is_eligible") is not True:
            continue
        raw_symbol = member.get("ts_code")
        try:
            symbol = CanonicalSymbol.from_core(raw_symbol).symbol
        except (SymbolContractError, TypeError) as exc:
            raise PipelineError("STALE_DATA", "Universe snapshot contains an invalid eligible symbol") from exc
        symbols.append(symbol)
    if len(symbols) != len(set(symbols)):
        raise PipelineError("STALE_DATA", "Universe snapshot contains duplicate eligible symbols")
    return symbols


def _write_snapshot(
    output_root: Path,
    snapshot: Mapping[str, Any],
    *,
    source_name: str,
) -> tuple[Path, Path]:
    snapshot_id = str(snapshot["snapshot_id"])
    directory = output_root / "snapshots"
    artifact = directory / f"universe-{snapshot['as_of']}-{snapshot['snapshot_phase']}.json"
    directory.mkdir(parents=True, exist_ok=True)
    if artifact.exists():
        try:
            existing = json.loads(artifact.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise PipelineError("STALE_DATA", "existing Universe artifact is not valid JSON") from exc
        if existing != dict(snapshot):
            raise PipelineError("STALE_DATA", "immutable Universe artifact content conflict")
    else:
        atomic_json(artifact, dict(snapshot), immutable=True)

    meta = {
        "artifact_type": "competition_universe_snapshot",
        "snapshot_id": snapshot_id,
        "snapshot_id_algorithm": "sha256(_content_dict JSON UTF-8, no trailing newline)",
        "artifact_sha256": file_hash(artifact),
        "snapshot_path": str(artifact),
        "research_date": snapshot["as_of"],
        "snapshot_phase": snapshot["snapshot_phase"],
        "market_data_reference_date": snapshot["market_data_reference_date"],
        "source": source_name,
    }
    meta_path = artifact.with_suffix(".meta.json")
    atomic_json(meta_path, meta, immutable=True)
    return artifact, meta_path


def _resolve_dataset_class() -> Any:
    module_name = "competition_core.pipeline.dataset"
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        raise PipelineError(
            "NOT_READY",
            "CanonicalDataset implementation is not available",
            expected_module=module_name,
        ) from exc
    dataset_class = getattr(module, "CanonicalDataset", None)
    if dataset_class is None:
        raise PipelineError(
            "NOT_READY",
            "CanonicalDataset implementation is not available",
            expected_module=module_name,
        )
    return dataset_class


def _artifact_path(value: Any, output_root: Path, field: str) -> Path:
    if not isinstance(value, (str, Path)) or not str(value).strip():
        raise PipelineError("NOT_READY", f"CanonicalDataset manifest is missing {field}")
    path = Path(value)
    return path if path.is_absolute() else output_root / path


def _prepare_and_load(
    dataset_class: Any,
    output_root: Path,
    source_name: str,
    research_client: ResearchDataClient,
    context: ResearchContext,
    symbols: list[str],
    *,
    snapshot_id: str,
    sessions: int,
    publication_snapshot: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], pd.DataFrame, Path, Path, Path]:
    try:
        dataset = dataset_class(output_root, source_name=source_name)
        prepared = dataset.prepare(
            research_client,
            context,
            symbols,
            snapshot_id=snapshot_id,
            sessions=sessions,
            **({"publication_snapshot": publication_snapshot} if context.phase == "postclose" else {}),
        )
    except PipelineError:
        raise
    except Exception as exc:  # noqa: BLE001 - never echo provider/credential details
        raise PipelineError("SOURCE_UNAVAILABLE", "CanonicalDataset preparation failed") from exc
    if not isinstance(prepared, Mapping):
        raise PipelineError("NOT_READY", "CanonicalDataset.prepare must return a manifest mapping")
    required = {
        "manifest_path",
        "panel_path",
        "quality_path",
        "dataset_id",
        "market_data_reference_date",
        "sessions",
        "batch_refs",
    }
    missing = sorted(required - set(prepared))
    if missing:
        raise PipelineError("NOT_READY", "CanonicalDataset manifest is incomplete", missing=missing)
    if str(prepared["market_data_reference_date"]) != context.market_data_reference_date.isoformat():
        raise PipelineError("STALE_DATA", "CanonicalDataset manifest reference date does not match context")
    if int(prepared["sessions"]) != sessions:
        raise PipelineError("STALE_DATA", "CanonicalDataset manifest sessions do not match request")
    if prepared.get("snapshot_id") not in (None, snapshot_id):
        raise PipelineError("STALE_DATA", "CanonicalDataset manifest snapshot_id does not match Universe")
    if not isinstance(prepared["batch_refs"], (list, tuple)):
        raise PipelineError("NOT_READY", "CanonicalDataset manifest batch_refs must be a list")
    if not isinstance(prepared["dataset_id"], str) or not prepared["dataset_id"].strip():
        raise PipelineError("NOT_READY", "CanonicalDataset manifest dataset_id must be non-empty")

    manifest_path = _artifact_path(prepared["manifest_path"], output_root, "manifest_path")
    panel_path = _artifact_path(prepared["panel_path"], output_root, "panel_path")
    quality_path = _artifact_path(prepared["quality_path"], output_root, "quality_path")
    try:
        first = dataset.load(manifest_path)
        second = dataset.load(manifest_path)
    except PipelineError:
        raise
    except Exception as exc:  # noqa: BLE001 - load errors are reported structurally
        raise PipelineError("STALE_DATA", "CanonicalDataset.load failed manifest verification") from exc
    if not isinstance(first, pd.DataFrame) or not isinstance(second, pd.DataFrame):
        raise PipelineError("NOT_READY", "CanonicalDataset.load must return a pandas DataFrame")
    try:
        pd.testing.assert_frame_equal(first, second, check_exact=True, check_dtype=True)
    except AssertionError as exc:
        raise PipelineError("STALE_DATA", "loading one manifest twice was not deterministic") from exc
    return dict(prepared), first, manifest_path, panel_path, quality_path


def _find_column(frame: pd.DataFrame, *names: str) -> str | None:
    return next((name for name in names if name in frame.columns), None)


def _finite_numeric(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    numeric = pd.to_numeric(series, errors="coerce")
    finite = numeric.map(
        lambda value: bool(pd.notna(value)) and math.isfinite(float(value))
    )
    return numeric, finite


def _quality_report(
    frame: pd.DataFrame,
    symbols: Sequence[str],
    reference_date: date,
    sessions: int,
    minimum_full_history_ratio: float = 0.9,
) -> dict[str, Any]:
    row_count = int(len(frame))
    symbol_column = _find_column(frame, "symbol", "ts_code")
    date_column = _find_column(frame, "trade_date")
    structural_missing = [
        field
        for field, present in (("symbol", symbol_column), ("trade_date", date_column))
        if present is None
    ]
    working = frame.copy()
    if symbol_column is not None:
        working["_canonical_symbol"] = working[symbol_column].map(
            lambda value: ""
            if value is None or str(value).strip().lower() in {"", "nan", "none"}
            else str(value)
        )
    else:
        working["_canonical_symbol"] = ""
    if date_column is not None:
        working["_canonical_date"] = working[date_column].map(_coerce_day)
    else:
        working["_canonical_date"] = None
    valid_identity = working["_canonical_symbol"].ne("") & working["_canonical_date"].notna()
    invalid_date_count = int(working["_canonical_date"].isna().sum())
    date_leakage_count = int(
        (working.loc[valid_identity, "_canonical_date"] > reference_date).sum()
    )
    duplicate_count = (
        int(frame.duplicated([symbol_column, date_column]).sum())
        if symbol_column is not None and date_column is not None
        else None
    )

    requested = list(dict.fromkeys(str(symbol) for symbol in symbols))
    counts = (
        working.loc[valid_identity & (working["_canonical_date"] <= reference_date)]
        .groupby("_canonical_symbol")
        .size()
        if symbol_column is not None and date_column is not None
        else pd.Series(dtype="int64")
    )
    covered = int(sum(int(counts.get(symbol, 0)) >= sessions for symbol in requested))
    coverage_ratio = covered / len(requested) if requested else 0.0
    if "close_qfq" in working:
        qfq, finite_qfq = _finite_numeric(working["close_qfq"])
        valid_qfq_rows = valid_identity & finite_qfq & (qfq > 0)
        qfq_counts = working.loc[valid_qfq_rows].groupby("_canonical_symbol").size()
        qfq_covered = sum(int(qfq_counts.get(symbol, 0)) >= sessions for symbol in requested)
    else:
        qfq_covered = 0
    qfq_coverage_ratio = qfq_covered / len(requested) if requested else 0.0

    factor_column = _find_column(frame, "adj_factor")
    if factor_column is None:
        missing_factor_count = row_count
        invalid_factor_count = row_count
    else:
        factor, factor_finite = _finite_numeric(frame[factor_column])
        missing_factor_count = int(factor.isna().sum())
        invalid_factor_count = int((~factor_finite | (factor <= 0)).sum())

    volume_column = _find_column(frame, "volume_raw", "volume")
    if volume_column is None:
        volume_negative_count = None
        invalid_volume_count = row_count
    else:
        volume, volume_finite = _finite_numeric(frame[volume_column])
        volume_negative_count = int((volume_finite & (volume < 0)).sum())
        invalid_volume_count = int((~volume_finite).sum())

    ohlc_columns = {
        field: _find_column(frame, f"{field}_raw", field)
        for field in ("open", "high", "low", "close")
    }
    invalid_ohlc_count = row_count if any(value is None for value in ohlc_columns.values()) else 0
    if invalid_ohlc_count == 0:
        numeric = {field: _finite_numeric(frame[column]) for field, column in ohlc_columns.items()}
        valid_ohlc = pd.Series(True, index=frame.index)
        for values, finite in numeric.values():
            valid_ohlc &= finite
        valid_ohlc &= numeric["open"][0] > 0
        valid_ohlc &= numeric["high"][0] > 0
        valid_ohlc &= numeric["low"][0] > 0
        valid_ohlc &= numeric["close"][0] > 0
        high_floor = pd.concat(
            [numeric["open"][0], numeric["close"][0], numeric["low"][0]], axis=1
        ).max(axis=1)
        low_ceiling = pd.concat(
            [numeric["open"][0], numeric["close"][0], numeric["high"][0]], axis=1
        ).min(axis=1)
        valid_ohlc &= numeric["high"][0] >= high_floor
        valid_ohlc &= numeric["low"][0] <= low_ceiling
        invalid_ohlc_count = int((~valid_ohlc).sum())

    panel_symbols = working["_canonical_symbol"].replace("", pd.NA).dropna().nunique()
    quality_pass = bool(
        row_count > 0
        and not structural_missing
        and invalid_date_count == 0
        and date_leakage_count == 0
        and duplicate_count == 0
        and coverage_ratio >= minimum_full_history_ratio
        and qfq_coverage_ratio >= minimum_full_history_ratio
        and volume_negative_count == 0
        and invalid_volume_count == 0
        and invalid_ohlc_count == 0
    )
    return {
        "quality_pass": quality_pass,
        "panel_count": row_count,
        "eligible_symbol_count": len(requested),
        "panel_symbol_count": int(panel_symbols),
        "sessions_required": sessions,
        "symbols_with_at_least_sessions": covered,
        "coverage_ge_sessions_ratio": coverage_ratio,
        "full_qfq_history_ratio": qfq_coverage_ratio,
        "minimum_full_history_ratio": minimum_full_history_ratio,
        "missing_factor_count": missing_factor_count,
        "invalid_factor_count": invalid_factor_count,
        "volume_negative_count": volume_negative_count,
        "invalid_volume_count": invalid_volume_count,
        "invalid_ohlc_count": invalid_ohlc_count,
        "duplicate_count": duplicate_count,
        "invalid_trade_date_count": invalid_date_count,
        "date_leakage_count": date_leakage_count,
        "structural_missing": structural_missing,
    }


def _panel_row(frame: pd.DataFrame, symbol: str, reference_date: date) -> Mapping[str, Any] | None:
    symbol_column = _find_column(frame, "symbol", "ts_code")
    date_column = _find_column(frame, "trade_date")
    if symbol_column is None or date_column is None:
        return None
    mask = frame[symbol_column].map(str).eq(symbol) & frame[date_column].map(_coerce_day).eq(reference_date)
    rows = frame.loc[mask]
    return rows.iloc[0].to_dict() if not rows.empty else None


def _source_row(frame: pd.DataFrame | None, symbol: str, reference_date: date) -> Mapping[str, Any] | None:
    if not isinstance(frame, pd.DataFrame) or not {"ts_code", "trade_date"} <= set(frame.columns):
        return None
    mask = frame["ts_code"].map(str).str.upper().eq(symbol) & frame["trade_date"].map(_coerce_day).eq(reference_date)
    rows = frame.loc[mask]
    return rows.iloc[0].to_dict() if not rows.empty else None


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        missing = pd.isna(value)
        return isinstance(missing, bool) and missing
    except (TypeError, ValueError):
        return False


def _compare_values(actual: Any, expected: Any) -> tuple[Any, Any, Any, bool]:
    actual_safe = _safe_details(actual)
    expected_safe = _safe_details(expected)
    actual_missing = _is_missing(actual)
    expected_missing = _is_missing(expected)
    if actual_missing and expected_missing:
        return actual_safe, expected_safe, 0.0, True
    if actual_missing or expected_missing:
        return actual_safe, expected_safe, None, False
    try:
        actual_number = float(actual)
        expected_number = float(expected)
        if math.isfinite(actual_number) and math.isfinite(expected_number):
            difference = abs(actual_number - expected_number)
            return actual_safe, expected_safe, difference, math.isclose(
                actual_number, expected_number, rel_tol=0.0, abs_tol=1e-9
            )
    except (TypeError, ValueError):
        pass
    return actual_safe, expected_safe, None, actual == expected


def _validate_source(
    client: Any,
    context: ResearchContext,
    frame: pd.DataFrame,
    symbols: Sequence[str],
    *,
    source_name: str,
    now_clock: Callable[[], datetime],
) -> dict[str, Any]:
    available = set(symbols)
    fixed = [symbol for symbol in _VALIDATION_FIXED if symbol in available]
    skipped_fixed = [symbol for symbol in _VALIDATION_FIXED if symbol not in available]
    remaining = sorted(available - set(fixed))
    sampled = random.Random(_VALIDATION_SEED).sample(remaining, min(3, len(remaining)))
    selected = fixed + sampled
    result: dict[str, Any] = {
        "status": "not_run" if not selected else "failed",
        "source": source_name,
        "request_mode": "fresh_direct_endpoint",
        "cache_used": False,
        "seed": _VALIDATION_SEED,
        "reference_date": context.market_data_reference_date.isoformat(),
        "selected_symbols": selected,
        "skipped_fixed_symbols": skipped_fixed,
        "items": [],
    }
    if not selected:
        result["reason"] = "none of the fixed or sampled symbols is in the eligible Universe"
        return result

    validation_client = ResearchDataClient(client, context, observed_clock=now_clock)
    trade_date = context.market_data_reference_date.strftime("%Y%m%d")
    requests = {
        "daily": (
            "ts_code,trade_date,open,high,low,close,vol,amount",
            ("open", "high", "low", "close", "vol", "amount"),
        ),
        "adj_factor": ("ts_code,trade_date,adj_factor", ("adj_factor",)),
        "daily_basic": (
            "ts_code,trade_date,turnover_rate,volume_ratio,total_mv,circ_mv",
            ("turnover_rate", "volume_ratio", "total_mv", "circ_mv"),
        ),
    }
    raw_frames: dict[str, pd.DataFrame | None] = {}
    for endpoint, (fields, _required) in requests.items():
        try:
            returned = getattr(validation_client, endpoint)(
                ts_code=",".join(selected),
                trade_date=trade_date,
                fields=fields,
            )
            raw_frames[endpoint] = returned if isinstance(returned, pd.DataFrame) else None
        except Exception:  # noqa: BLE001 - do not put provider text in the report
            raw_frames[endpoint] = None

    field_mappings = {
        "daily": (
            ("open", "open_raw"),
            ("high", "high_raw"),
            ("low", "low_raw"),
            ("close", "close_raw"),
            ("vol", "volume_raw"),
            ("amount", "amount_raw"),
        ),
        "adj_factor": (("adj_factor", "adj_factor"),),
        "daily_basic": (
            ("turnover_rate", "turnover_rate"),
            ("volume_ratio", "volume_ratio"),
            ("total_mv", "total_mv"),
            ("circ_mv", "circ_mv"),
        ),
    }
    for endpoint, mappings in field_mappings.items():
        raw_frame = raw_frames[endpoint]
        for symbol in selected:
            panel = _panel_row(frame, symbol, context.market_data_reference_date)
            source = _source_row(raw_frame, symbol, context.market_data_reference_date)
            for source_field, panel_field in mappings:
                if (
                    endpoint == "daily_basic"
                    and source_field != "turnover_rate"
                    and source_field not in (raw_frame.columns if isinstance(raw_frame, pd.DataFrame) else set())
                    and panel_field not in frame.columns
                ):
                    continue
                if source is None:
                    result["items"].append({
                        "endpoint": endpoint,
                        "symbol": symbol,
                        "field": source_field,
                        "actual": None if panel is None else panel.get(panel_field),
                        "expected": None,
                        "diff": None,
                        "pass": False,
                        "error": "missing fresh source row",
                    })
                    continue
                if source_field not in source:
                    result["items"].append({
                        "endpoint": endpoint,
                        "symbol": symbol,
                        "field": source_field,
                        "actual": None if panel is None else panel.get(panel_field),
                        "expected": None,
                        "diff": None,
                        "pass": False,
                        "error": "missing source field",
                    })
                    continue
                actual = None if panel is None else panel.get(panel_field)
                expected = source.get(source_field)
                actual_safe, expected_safe, difference, passed = _compare_values(actual, expected)
                result["items"].append({
                    "endpoint": endpoint,
                    "symbol": symbol,
                    "field": source_field,
                    "actual": actual_safe,
                    "expected": expected_safe,
                    "diff": difference,
                    "pass": passed and panel is not None,
                })
    result["status"] = "passed" if result["items"] and all(item["pass"] for item in result["items"]) else "failed"
    result["pass"] = result["status"] == "passed"
    return result


def _cross_exdate_report(frame: pd.DataFrame, reference_date: date) -> dict[str, Any]:
    if "adj_factor" not in frame.columns:
        return {"status": "not_run", "reason": "panel has no adj_factor column"}
    symbol_column = _find_column(frame, "symbol", "ts_code")
    date_column = _find_column(frame, "trade_date")
    if symbol_column is None or date_column is None:
        return {"status": "not_run", "reason": "panel lacks symbol/trade_date columns"}
    raw_close = _find_column(frame, "close_raw", "close")
    qfq_close = _find_column(frame, "close_qfq")
    raw_volume = _find_column(frame, "volume_raw")
    scaled_volume = _find_column(frame, "volume_shares")
    if not all((raw_close, qfq_close, raw_volume, scaled_volume)):
        return {
            "status": "failed",
            "reason": "factor changes exist but qfq close or raw/scaled volume columns are missing",
        }
    working = frame[[symbol_column, date_column, "adj_factor", raw_close, qfq_close, raw_volume, scaled_volume]].copy()
    working["_symbol"] = working[symbol_column].map(str)
    working["_date"] = working[date_column].map(_coerce_day)
    factor, factor_finite = _finite_numeric(working["adj_factor"])
    working["_factor"] = factor
    valid = working["_date"].notna() & factor_finite & (factor > 0) & (working["_date"] <= reference_date)
    ordered = working.loc[valid].sort_values(["_symbol", "_date"])
    if ordered.empty:
        return {"status": "not_run", "reason": "no valid factor rows"}
    changed_counts = ordered.groupby("_symbol")["_factor"].nunique()
    changed_symbols = sorted(changed_counts[changed_counts > 1].index.tolist())
    if not changed_symbols:
        return {"status": "not_run", "reason": "no factor change is present in the panel"}
    changed = ordered[ordered["_symbol"].isin(changed_symbols)].copy()
    final_factor = changed.groupby("_symbol")["_factor"].transform("last")
    raw_close_values, raw_close_finite = _finite_numeric(changed[raw_close])
    qfq_values, qfq_finite = _finite_numeric(changed[qfq_close])
    raw_volume_values, raw_volume_finite = _finite_numeric(changed[raw_volume])
    scaled_volume_values, scaled_volume_finite = _finite_numeric(changed[scaled_volume])
    expected_qfq = raw_close_values * changed["_factor"] / final_factor
    qfq_ok = (
        raw_close_finite
        & qfq_finite
        & expected_qfq.map(lambda value: bool(pd.notna(value)) and math.isfinite(float(value)))
        & (qfq_values - expected_qfq).abs().le(1e-8)
    )
    volume_ok = (
        raw_volume_finite
        & scaled_volume_finite
        & (scaled_volume_values - raw_volume_values * 100.0).abs().le(1e-8)
    )
    qfq_failures = int((~qfq_ok).sum())
    volume_failures = int((~volume_ok).sum())
    passed = qfq_failures == 0 and volume_failures == 0
    return {
        "status": "passed" if passed else "failed",
        "checked_symbols": len(changed_symbols),
        "factor_changed_symbols": changed_symbols,
        "qfq_formula": "close_qfq = close_raw * adj_factor / final_known_factor",
        "qfq_formula_failures": qfq_failures,
        "volume_scaling_failures": volume_failures,
        "volume_rule": "volume_shares = volume_raw * 100; factor does not scale volume",
    }


def _markdown_report(report: Mapping[str, Any]) -> str:
    snapshot = report["snapshot"]
    calendar = report["calendar"]
    dataset = report["dataset"]
    quality = report["quality"]
    lines = [
        "# P10B canonical prepare / acceptance report",
        "",
        "工程结果：NOT_PRODUCTION_PASS。本报告只证明本次 P10B CLI 工程验收结果，"
        "不等同于 production PASS。",
        "",
        f"- production_pass: {report['production_pass']}",
        f"- ok: {report['ok']}",
        f"- source: {report['source']}",
        "",
        "## Snapshot",
        "",
        f"- snapshot_count: {snapshot['snapshot_count']}",
        f"- snapshot_id: {snapshot['snapshot_id']}",
        f"- path: {snapshot['path']}",
        f"- meta_path: {snapshot.get('meta_path')}",
        f"- members / eligible / passed to Dataset: {snapshot['member_count']} / {snapshot['eligible_count']} / {snapshot['symbols_passed_count']}",
        "",
        "## Calendar and dataset",
        "",
        f"- date / phase / Universe phase: {calendar['research_date']} / {calendar['phase']} / {calendar['universe_phase']}",
        f"- market_data_reference_date: {calendar['market_data_reference_date']}",
        f"- as_of / observed_at: {calendar['as_of']} / {calendar['observed_at']}",
        f"- panel rows: {quality['panel_count']}",
        f"- symbols with >= {quality['sessions_required']} bars: {quality['symbols_with_at_least_sessions']}/{quality['eligible_symbol_count']} ({quality['coverage_ge_sessions_ratio']:.3f})",
        f"- manifest: {dataset['manifest_path']}",
        f"- panel: {dataset['panel_path']}",
        f"- quality: {dataset['quality_path']}",
        f"- dataset_id: {dataset['dataset_id']}",
        f"- batch_refs: {dataset['batch_ref_count']}",
        "",
        "## Quality checks",
        "",
        "| check | result |",
        "| --- | ---: |",
        f"| missing factor | {quality['missing_factor_count']} |",
        f"| negative volume | {quality['volume_negative_count']} |",
        f"| invalid OHLC | {quality['invalid_ohlc_count']} |",
        f"| duplicate symbol/date | {quality['duplicate_count']} |",
        f"| date leakage | {quality['date_leakage_count']} |",
        f"| same-manifest load twice exact | {report['determinism']['same_manifest_load_exact']} |",
        f"| quality pass | {quality['quality_pass']} |",
        "",
        "## Optional live source validation",
        "",
        f"status: {report['source_validation']['status']}; fresh direct requests only; cache_used={report['source_validation'].get('cache_used')}.",
        "",
        "## Optional cross-ex-date validation",
        "",
        f"status: {report['cross_exdate']['status']}. If no factor change is present, this remains not_run; it is not claimed as covered.",
        "",
        "## Report files",
        "",
        f"- JSON: {report['report_paths']['json']}",
        f"- Markdown: {report['report_paths']['markdown']}",
        "",
    ]
    if report["source_validation"].get("items"):
        lines.extend(
            [
                "Source comparison items (actual=canonical panel, expected=fresh endpoint):",
                "",
                "JSON:",
                json.dumps(report["source_validation"]["items"], ensure_ascii=False, sort_keys=True, indent=2),
                "",
            ]
        )
    return "\n".join(lines)


def _write_reports(
    report: dict[str, Any],
    output_root: Path,
    secrets: Sequence[str],
) -> dict[str, Any]:
    report_dir = output_root / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_id = content_hash(_safe_details(report, secrets))
    json_path = report_dir / f"prepare-{report['calendar']['research_date']}-{report['calendar']['phase']}-{report_id}.json"
    markdown_path = json_path.with_suffix(".md")
    report["report_paths"] = {"json": str(json_path), "markdown": str(markdown_path)}
    safe_report = _safe_details(report, secrets)
    atomic_json(json_path, safe_report, immutable=True)
    atomic_bytes(markdown_path, _markdown_report(safe_report).encode("utf-8"), immutable=True)
    return safe_report


def _execute(
    args: argparse.Namespace,
    *,
    client_factory: Callable[[], Any],
    dataset_class: Any | None,
    now_clock: Callable[[], datetime],
    secrets: Sequence[str],
    snapshot_ready: Callable[[Mapping[str, Any]], None] | None = None,
) -> dict[str, Any]:
    research_date = _parse_date(args.date)
    as_of = _parse_as_of(args.as_of)
    output_root = Path(args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    observed_at = _actual_now(now_clock)
    if as_of.astimezone(SHANGHAI).date() != research_date:
        raise PipelineError("NOT_READY", "as_of local Shanghai date must equal --date")
    if as_of > observed_at:
        raise PipelineError("NOT_READY", "research cutoff is in the future")
    if args.phase == "postclose" and as_of.astimezone(SHANGHAI).time() < time(15):
        raise PipelineError("NOT_READY", "postclose requires a cutoff after session close")

    try:
        client = client_factory()
    except TushareConfigurationError as exc:
        raise PipelineError(
            "NOT_CONFIGURED",
            "新进程没有可用凭据；如使用桌面安全存储请从注入环境启动",
        ) from exc
    except Exception as exc:  # noqa: BLE001 - never echo client/provider details
        raise PipelineError("SOURCE_UNAVAILABLE", "cannot create TUSHARE client") from exc

    calendar = _fetch_calendar(client, research_date)
    if args.phase == "postclose":
        # Tentative context only; no current-day artifact is accepted without
        # independent publication coverage proof from CanonicalDataset below.
        reference = _market_data_reference_date(calendar, research_date, "postclose")
        context = ResearchContext(research_date, args.phase, reference, as_of)
    else:
        context = ResearchContext.resolve(
            research_date=research_date, phase=args.phase, as_of=as_of,
            calendar=calendar, observed_at=observed_at,
        )
    source_name = _source_name()
    research_client = ResearchDataClient(client, context, observed_clock=now_clock)

    snapshot_path: Path
    meta_path: Path | None
    if args.snapshot is not None:
        snapshot_path = Path(args.snapshot).resolve()
        snapshot = _read_snapshot(snapshot_path)
        context.validate_snapshot(snapshot)
        symbols = _eligible_symbols(snapshot)
        meta_path = None
    else:
        if observed_at.astimezone(SHANGHAI).date() != research_date:
            raise PipelineError(
                "NOT_READY",
                "without --snapshot, Universe construction is allowed only for actual today",
            )
        try:
            snapshot_object = CompetitionUniverse(research_client, load_config()).build(
                research_date, context.universe_phase
            )
        except PipelineError:
            raise
        except Exception as exc:  # noqa: BLE001 - provider details are not report-safe
            raise PipelineError("SOURCE_UNAVAILABLE", "CompetitionUniverse construction failed") from exc
        snapshot = snapshot_object.to_dict()
        if snapshot["snapshot_id"] != _universe_snapshot_id(snapshot):
            raise PipelineError("STALE_DATA", "new Universe snapshot content hash is inconsistent")
        context.validate_snapshot(snapshot)
        symbols = _eligible_symbols(snapshot)
        if args.phase != "postclose":
            snapshot_path, meta_path = _write_snapshot(
                output_root, snapshot, source_name=source_name
            )

    if snapshot_ready is not None:
        snapshot_ready(snapshot)
    if dataset_class is None:
        dataset_class = _resolve_dataset_class()
    prepared, frame, manifest_path, panel_path, quality_path = _prepare_and_load(
        dataset_class,
        output_root,
        source_name,
        research_client,
        context,
        symbols,
        snapshot_id=str(snapshot["snapshot_id"]),
        sessions=args.sessions,
        publication_snapshot=snapshot if args.phase == "postclose" else None,
    )
    if args.phase == "postclose":
        proof = prepared.get("publication_proof")
        if not isinstance(proof, dict) or proof.get("snapshot_id") != snapshot["snapshot_id"]:
            raise PipelineError("NOT_READY", "dataset did not supply snapshot-bound publication proof")
        context = ResearchContext.resolve(
            research_date=research_date, phase=args.phase, as_of=as_of, calendar=calendar,
            observed_at=_actual_now(now_clock), completed_daily_date=_parse_date(proof["completed_daily_date"]),
        )
        if args.snapshot is None:
            snapshot_path, meta_path = _write_snapshot(output_root, snapshot, source_name=source_name)
    quality = _quality_report(
        frame,
        symbols,
        context.market_data_reference_date,
        args.sessions,
        load_pipeline_config().section("dataset")["minimum_full_history_ratio"],
    )
    source_validation = (
        _validate_source(
            client,
            context,
            frame,
            symbols,
            source_name=source_name,
            now_clock=now_clock,
        )
        if args.validate_source
        else {
            "status": "not_run",
            "reason": "--validate-source was not requested",
            "cache_used": False,
            "items": [],
        }
    )
    cross_exdate = _cross_exdate_report(frame, context.market_data_reference_date)
    deterministic = {"same_manifest_load_exact": True}
    source_pass = source_validation["status"] in {"passed", "not_run"}
    cross_pass = cross_exdate["status"] in {"passed", "not_run"}
    report = {
        "ok": bool(quality["quality_pass"] and source_pass and cross_pass),
        "engineering_result": "NOT_PRODUCTION_PASS",
        "production_pass": False,
        "source": source_name,
        "snapshot": {
            "snapshot_count": 1,
            "snapshot_id": str(snapshot["snapshot_id"]),
            "path": str(snapshot_path),
            "meta_path": str(meta_path) if meta_path is not None else None,
            "member_count": len(snapshot.get("members", [])),
            "eligible_count": len(symbols),
            "symbols_passed_count": len(symbols),
        },
        "calendar": {
            "research_date": research_date.isoformat(),
            "phase": args.phase,
            "universe_phase": context.universe_phase,
            "market_data_reference_date": context.market_data_reference_date.isoformat(),
            "as_of": as_of.isoformat(),
            "observed_at": observed_at.isoformat(),
            "timezone": context.timezone,
        },
        "dataset": {
            "dataset_id": str(prepared["dataset_id"]),
            "manifest_path": str(manifest_path),
            "panel_path": str(panel_path),
            "quality_path": str(quality_path),
            "market_data_reference_date": str(prepared["market_data_reference_date"]),
            "sessions": int(prepared["sessions"]),
            "batch_ref_count": len(prepared["batch_refs"]),
            "publication_proof": prepared.get("publication_proof"),
        },
        "quality": quality,
        "determinism": deterministic,
        "source_validation": source_validation,
        "cross_exdate": cross_exdate,
    }
    return _write_reports(report, output_root, secrets)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare and validate a canonical P10B dataset")
    parser.add_argument("--date", required=True, help="research date, YYYY-MM-DD")
    parser.add_argument(
        "--phase",
        choices=("preopen", "intraday", "postclose"),
        default="preopen",
        help="research phase (postclose requires independent current-session publication coverage)",
    )
    parser.add_argument("--as-of", required=True, help="timezone-aware ISO cutoff on --date")
    parser.add_argument("--env-file", type=Path, default=None, help="explicit dotenv-style allowlisted file")
    parser.add_argument("--token-file", type=Path, default=None, help="UTF-8 single-token file; overrides dotenv token for this process only")
    parser.add_argument("--base-url", default=None, help="explicit compatible URL; no third-party default")
    parser.add_argument("--snapshot", type=Path, default=None, help="saved exact-date Universe JSON")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=PROJECT_ROOT / "data" / "canonical",
        help="canonical output root",
    )
    parser.add_argument("--sessions", type=_parse_sessions, default=120)
    parser.add_argument("--validate-source", action="store_true")
    return parser


def run(
    args: argparse.Namespace,
    *,
    client_factory: Callable[[], Any] = create_tushare_client,
    dataset_class: Any | None = None,
    now_clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Run the CLI body for tests or a caller that already parsed arguments."""

    clock = now_clock or (lambda: datetime.now(SHANGHAI))
    env_values = _credential_env(args)
    secrets = [
        value
        for value in (env_values.get("TUSHARE_TOKEN"), os.environ.get("TUSHARE_TOKEN"))
        if value
    ]
    with _temporary_env(env_values):
        return _execute(
            args,
            client_factory=client_factory,
            dataset_class=dataset_class,
            now_clock=clock,
            secrets=secrets,
        )


def main(
    argv: Sequence[str] | None = None,
    *,
    client_factory: Callable[[], Any] = create_tushare_client,
    dataset_class: Any | None = None,
    now_clock: Callable[[], datetime] | None = None,
) -> int:
    args = _parser().parse_args(argv)
    secrets: list[str] = []
    try:
        env_values = _credential_env(args)
        secrets = [
            value
            for value in (env_values.get("TUSHARE_TOKEN"), os.environ.get("TUSHARE_TOKEN"))
            if value
        ]
        with _temporary_env(env_values):
            report = _execute(
                args,
                client_factory=client_factory,
                dataset_class=dataset_class,
                now_clock=now_clock or (lambda: datetime.now(SHANGHAI)),
                secrets=secrets,
            )
    except PipelineError as exc:
        payload = {
            "ok": False,
            "engineering_result": "NOT_PRODUCTION_PASS",
            "production_pass": False,
            "error": {
                "code": exc.code,
                "message": _safe_text(str(exc), secrets),
                "details": _safe_details(exc.details, secrets),
            },
        }
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))
        return 2
    except Exception as exc:  # noqa: BLE001 - CLI errors must not echo credentials
        payload = {
            "ok": False,
            "engineering_result": "NOT_PRODUCTION_PASS",
            "production_pass": False,
            "error": {
                "code": "NOT_READY",
                "message": "canonical preparation failed",
                "details": {"exception_type": type(exc).__name__},
            },
        }
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))
        return 2
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
