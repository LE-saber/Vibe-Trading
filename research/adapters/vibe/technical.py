"""Core adapter for the isolated technical-indicator worker.

The adapter owns the Core-side point-in-time and identity checks.  It sends a
small deterministic CSV across the process boundary. Indicator math is the
frozen pure implementation migrated from Vibe; this module only orchestrates
the worker.
"""

from __future__ import annotations

import csv
from datetime import date, datetime
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from typing import Any, Mapping

import pandas as pd

from competition_core.pipeline.artifacts import atomic_json, file_hash
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.time_context import ResearchContext
from contracts.symbol import CanonicalSymbol, SymbolContractError


ROOT = Path(__file__).resolve().parents[2]


TECHNICAL_FIELDS = (
    "sma_5",
    "sma_10",
    "sma_20",
    "sma_20_lag5",
    "rsi_14",
    "macd_histogram",
    "bollinger_upper",
    "bollinger_middle",
    "bollinger_lower",
)
ROW_FIELDS = ("symbol", *TECHNICAL_FIELDS, "contiguous_sessions", "missing_fields")
PANEL_FIELDS = ("symbol", "trade_date", "close_qfq")
REQUEST_SCHEMA = "p10c.technical.v1"
WORKER_NAME = "vibe-technical-worker.py"
HELPER_RELATIVE_PATH = Path("competition_core") / "technical_indicators.py"


def compute_technical(
    panel: pd.DataFrame,
    *,
    context: ResearchContext,
    sessions: list[str],
    symbols: list[str],
    work_dir: Path,
    vibe_root: Path,
    timeout_seconds: int = 180,
) -> dict[str, Any]:
    """Compute Vibe's pure technical indicators for a Core panel.

    ``sessions`` is an explicit, ascending exchange-session list ending at the
    context reference date.  The worker reindexes every symbol to those exact
    positions; no missing day is dropped or filled.  The returned rows are
    always the exact requested symbols in sorted order.
    """

    if not isinstance(context, ResearchContext):
        raise PipelineError("INVALID_ARGUMENTS", "context must be a ResearchContext")
    normalized_symbols = _validate_symbols(symbols)
    normalized_sessions = _validate_sessions(sessions, context)
    normalized_panel = _validate_panel(panel, normalized_symbols, normalized_sessions, context)
    timeout = _validate_timeout(timeout_seconds)
    run_root = _prepare_work_root(work_dir)
    worker_path = Path(__file__).resolve().parent.parent.parent / "scripts" / WORKER_NAME
    helper_path = _helper_path()
    _require_file(worker_path, "technical worker")
    _require_file(helper_path, "technical helper")
    expected_worker_hash = _checked_file_hash(worker_path, "technical worker")
    expected_helper_hash = _checked_file_hash(helper_path, "technical helper")
    python_path = Path(sys.executable).resolve()

    temporary_path: Path | None = None
    try:
        try:
            temporary_path = Path(
                tempfile.mkdtemp(prefix=".vibe-technical-", dir=str(run_root))
            ).resolve()
        except OSError as exc:
            raise PipelineError(
                "SOURCE_UNAVAILABLE", "cannot create bounded technical worker directory"
            ) from None
        if not temporary_path.is_relative_to(run_root):
            raise PipelineError("INVALID_ARGUMENTS", "technical worker directory escaped work_dir")

        input_path = temporary_path / "input.csv"
        request_path = temporary_path / "request.json"
        output_path = temporary_path / "output.json"
        _write_deterministic_csv(input_path, normalized_panel)
        input_hash = _checked_file_hash(input_path, "technical input")
        request = {
            "schema_version": REQUEST_SCHEMA,
            "context": context.to_dict(),
            "sessions": normalized_sessions,
            "symbols": list(normalized_symbols),
            "input_csv": input_path.name,
            "input_sha256": input_hash,
        }
        try:
            atomic_json(request_path, request)
        except Exception as exc:  # noqa: BLE001 - the boundary must fail closed
            raise PipelineError(
                "SOURCE_UNAVAILABLE", "cannot write technical worker request"
            ) from None

        command = [
            str(python_path),
            str(worker_path),
            "--request",
            str(request_path),
            "--output",
            str(output_path),
            "--vibe-root",
            str(ROOT),
        ]
        try:
            completed = subprocess.run(
                command,
                cwd=str(temporary_path),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                shell=False,
                timeout=timeout,
                creationflags=_creation_flags(),
            )
        except subprocess.TimeoutExpired:
            raise PipelineError("TIMEOUT", "technical worker timed out") from None
        except (OSError, ValueError):
            raise PipelineError("SOURCE_UNAVAILABLE", "technical worker could not start") from None

        if getattr(completed, "returncode", 1) != 0:
            # Captured stdout/stderr are intentionally never surfaced.  They
            # may contain environment-specific paths or third-party secrets.
            raise PipelineError(
                "SOURCE_UNAVAILABLE",
                "technical worker failed",
                returncode=int(getattr(completed, "returncode", 1)),
            )

        actual_worker_hash = _checked_file_hash(worker_path, "technical worker")
        actual_helper_hash = _checked_file_hash(helper_path, "technical helper")
        if actual_worker_hash != expected_worker_hash or actual_helper_hash != expected_helper_hash:
            raise PipelineError("STALE_DATA", "technical worker source changed during execution")
        if not output_path.is_file():
            raise PipelineError("SOURCE_UNAVAILABLE", "technical worker produced no output")
        payload = _read_output(output_path)
        return _validate_output(
            payload,
            symbols=normalized_symbols,
            context=context,
            sessions=normalized_sessions,
            input_hash=input_hash,
            worker_hash=actual_worker_hash,
            helper_hash=actual_helper_hash,
        )
    finally:
        if temporary_path is not None:
            shutil.rmtree(temporary_path, ignore_errors=True)


def _validate_symbols(symbols: Any) -> list[str]:
    if not isinstance(symbols, list) or not symbols:
        raise PipelineError("INVALID_ARGUMENTS", "symbols must be a non-empty list")
    result: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        if not isinstance(symbol, str) or not symbol or symbol != symbol.strip():
            raise PipelineError("INVALID_ARGUMENTS", "symbols contain an invalid symbol")
        try:
            parsed = CanonicalSymbol.from_core(symbol)
        except (SymbolContractError, ValueError):
            raise PipelineError("INVALID_ARGUMENTS", "symbols contain an invalid symbol") from None
        if parsed.symbol != symbol:
            raise PipelineError("INVALID_ARGUMENTS", "symbols contain a non-canonical symbol")
        if symbol in seen:
            raise PipelineError("INVALID_ARGUMENTS", "symbols contain a duplicate symbol")
        seen.add(symbol)
        result.append(symbol)
    return result


def _validate_sessions(sessions: Any, context: ResearchContext) -> list[str]:
    if not isinstance(sessions, list) or not sessions:
        raise PipelineError("INVALID_ARGUMENTS", "sessions must be a non-empty list")
    parsed: list[date] = []
    seen: set[date] = set()
    for value in sessions:
        if not isinstance(value, str) or value != value.strip():
            raise PipelineError("INVALID_ARGUMENTS", "sessions contain an invalid date")
        parsed_date = _parse_iso_or_compact_date(value, "session date")
        if parsed_date in seen:
            raise PipelineError("INVALID_ARGUMENTS", "sessions contain a duplicate date")
        if parsed_date > context.market_data_reference_date:
            raise PipelineError("STALE_DATA", "sessions contain a future date")
        seen.add(parsed_date)
        parsed.append(parsed_date)
    if parsed != sorted(parsed):
        raise PipelineError("INVALID_ARGUMENTS", "sessions must be ascending")
    reference = context.market_data_reference_date
    if parsed[-1] != reference:
        raise PipelineError(
            "DATA_COVERAGE_INSUFFICIENT",
            "sessions must end at the market-data reference date",
        )
    return [item.isoformat() for item in parsed]


def _validate_panel(
    panel: Any,
    symbols: list[str],
    sessions: list[str],
    context: ResearchContext,
) -> list[tuple[str, str, float | None]]:
    if not isinstance(panel, pd.DataFrame):
        raise PipelineError("INVALID_ARGUMENTS", "panel must be a pandas DataFrame")
    if len(set(panel.columns)) != len(panel.columns):
        raise PipelineError("INVALID_ARGUMENTS", "panel contains duplicate columns")
    missing = [column for column in PANEL_FIELDS if column not in panel.columns]
    if missing:
        raise PipelineError("INVALID_ARGUMENTS", "panel is missing required columns", fields=missing)

    allowed = set(symbols)
    allowed_sessions = set(sessions)
    rows: list[tuple[str, str, float | None]] = []
    seen: set[tuple[str, date]] = set()
    for values in panel.loc[:, list(PANEL_FIELDS)].itertuples(index=False, name=None):
        raw_symbol, raw_date, raw_close = values
        if not isinstance(raw_symbol, str) or not raw_symbol or raw_symbol != raw_symbol.strip():
            raise PipelineError("INVALID_ARGUMENTS", "panel contains an invalid symbol")
        if raw_symbol not in allowed:
            raise PipelineError("IDENTITY_CONFLICT", "panel contains an unexpected symbol")
        parsed_date = _parse_panel_date(raw_date)
        if parsed_date > context.market_data_reference_date:
            raise PipelineError("STALE_DATA", "panel contains a future date")
        if parsed_date.isoformat() not in allowed_sessions:
            raise PipelineError("STALE_DATA", "panel contains a date outside sessions")
        key = (raw_symbol, parsed_date)
        if key in seen:
            raise PipelineError("STALE_DATA", "panel contains duplicate symbol/date rows")
        seen.add(key)
        rows.append((raw_symbol, parsed_date.isoformat(), _finite_close(raw_close)))
    return sorted(rows, key=lambda item: (item[0], item[1]))


def _parse_panel_date(value: Any) -> date:
    if _is_scalar_missing(value):
        raise PipelineError("STALE_DATA", "panel contains an invalid trade date")
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            raise PipelineError("STALE_DATA", "panel contains an invalid trade date")
        return value.date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return _parse_iso_or_compact_date(str(value).strip(), "panel trade date")


def _parse_iso_or_compact_date(value: str, label: str) -> date:
    if len(value) == 8 and value.isdigit():
        try:
            return datetime.strptime(value, "%Y%m%d").date()
        except ValueError:
            raise PipelineError("STALE_DATA", f"{label} is invalid") from None
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        raise PipelineError("STALE_DATA", f"{label} is invalid") from None
    if parsed.isoformat() != value:
        raise PipelineError("STALE_DATA", f"{label} is invalid")
    return parsed


def _finite_close(value: Any) -> float | None:
    if _is_scalar_missing(value):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def _is_scalar_missing(value: Any) -> bool:
    try:
        result = pd.isna(value)
    except (TypeError, ValueError):
        return False
    try:
        return bool(result)
    except (TypeError, ValueError):
        return False


def _validate_timeout(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise PipelineError("INVALID_ARGUMENTS", "timeout_seconds must be a positive integer")
    if value > 3600:
        raise PipelineError("INVALID_ARGUMENTS", "timeout_seconds exceeds the safe limit")
    return value


def _prepare_work_root(value: Any) -> Path:
    path = _resolve_directory(value, "work_dir", create=True)
    if path == Path(path.anchor):
        raise PipelineError("INVALID_ARGUMENTS", "work_dir must not be a filesystem root")
    return path


def _resolve_directory(value: Any, label: str, *, create: bool = False) -> Path:
    if not isinstance(value, (str, Path)) or not str(value).strip():
        raise PipelineError("INVALID_ARGUMENTS", f"{label} must be a directory")
    path = Path(value).expanduser().resolve()
    try:
        if create:
            path.mkdir(parents=True, exist_ok=True)
        if not path.is_dir():
            raise PipelineError("INVALID_ARGUMENTS", f"{label} must be a directory")
    except OSError:
        raise PipelineError("SOURCE_UNAVAILABLE", f"cannot access {label}") from None
    return path


def _helper_path() -> Path:
    helper = ROOT / HELPER_RELATIVE_PATH
    if helper.is_file():
        return helper.resolve()
    raise PipelineError("NOT_READY", "bundled technical helper is unavailable")


def _require_file(path: Path, label: str) -> None:
    if not path.is_file():
        raise PipelineError("NOT_READY", f"{label} is unavailable")


def _checked_file_hash(path: Path, label: str) -> str:
    try:
        return file_hash(path)
    except (OSError, ValueError):
        raise PipelineError("STALE_DATA", f"cannot hash {label}") from None


def _write_deterministic_csv(
    path: Path,
    rows: list[tuple[str, str, float | None]],
) -> None:
    try:
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(PANEL_FIELDS)
            for symbol, trade_date, close in rows:
                writer.writerow(
                    [symbol, trade_date, "" if close is None else format(close, ".17g")]
                )
            handle.flush()
            os.fsync(handle.fileno())
    except OSError:
        raise PipelineError("SOURCE_UNAVAILABLE", "cannot write technical input") from None


def _read_output(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        raise PipelineError("STALE_DATA", "technical worker output is invalid") from None


def _validate_output(
    payload: Any,
    *,
    symbols: list[str],
    context: ResearchContext,
    sessions: list[str],
    input_hash: str,
    worker_hash: str,
    helper_hash: str,
) -> dict[str, Any]:
    if not isinstance(payload, Mapping) or set(payload) != {"rows", "provenance"}:
        raise PipelineError("STALE_DATA", "technical worker output envelope is invalid")
    rows = payload["rows"]
    provenance = payload["provenance"]
    if not isinstance(rows, list) or not isinstance(provenance, Mapping):
        raise PipelineError("STALE_DATA", "technical worker output types are invalid")
    expected_symbols = sorted(symbols)
    if len(rows) != len(expected_symbols) or [row.get("symbol") if isinstance(row, Mapping) else None for row in rows] != expected_symbols:
        raise PipelineError("IDENTITY_CONFLICT", "technical worker symbols differ from input")

    clean_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping) or set(row) != set(ROW_FIELDS):
            raise PipelineError("STALE_DATA", "technical worker row schema is invalid")
        if not isinstance(row["symbol"], str) or row["symbol"] not in expected_symbols:
            raise PipelineError("IDENTITY_CONFLICT", "technical worker returned an invalid symbol")
        missing_fields = row["missing_fields"]
        if not isinstance(missing_fields, list) or len(set(missing_fields)) != len(missing_fields):
            raise PipelineError("STALE_DATA", "technical worker missing_fields is invalid")
        if any(field not in TECHNICAL_FIELDS for field in missing_fields):
            raise PipelineError("STALE_DATA", "technical worker named an unsupported missing field")
        for field in TECHNICAL_FIELDS:
            value = row[field]
            if value is not None:
                if field in missing_fields:
                    raise PipelineError("STALE_DATA", "technical worker valued a missing field")
                if isinstance(value, bool):
                    raise PipelineError("STALE_DATA", "technical worker returned a non-numeric value")
                try:
                    if not math.isfinite(float(value)):
                        raise PipelineError("STALE_DATA", "technical worker returned a non-finite value")
                except (TypeError, ValueError):
                    raise PipelineError("STALE_DATA", "technical worker returned a non-numeric value") from None
            elif field not in missing_fields:
                raise PipelineError("STALE_DATA", "technical worker omitted a missing-field declaration")
        contiguous = row["contiguous_sessions"]
        if isinstance(contiguous, bool) or not isinstance(contiguous, int) or not 0 <= contiguous <= len(sessions):
            raise PipelineError("STALE_DATA", "technical worker contiguous_sessions is invalid")
        clean_rows.append(dict(row))

    required_provenance = {
        "worker_sha256": worker_hash,
        "helper_sha256": helper_hash,
        "pandas_version": None,
        "input_sha256": input_hash,
        "context": context.to_dict(),
        "sessions": sessions,
        "symbols": expected_symbols,
    }
    for key, expected in required_provenance.items():
        if key not in provenance:
            raise PipelineError("STALE_DATA", "technical worker provenance is incomplete")
        if expected is not None and provenance[key] != expected:
            raise PipelineError("STALE_DATA", "technical worker provenance does not match input")
    if not isinstance(provenance["pandas_version"], str) or not provenance["pandas_version"].strip():
        raise PipelineError("STALE_DATA", "technical worker pandas version is invalid")

    return {"rows": clean_rows, "provenance": dict(provenance)}


def _creation_flags() -> int:
    return int(getattr(subprocess, "CREATE_NO_WINDOW", 0)) if os.name == "nt" else 0


__all__ = ["compute_technical"]
