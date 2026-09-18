"""Run Vibe's existing pure technical helpers on a Core-owned CSV input.

This process is intentionally small and offline.  It accepts only a hashed
request, validates the request and its bounded CSV reference, imports the
existing ``_compute_*`` functions directly, and emits a compact JSON result.
It never calls ``TechnicalIndicatorTool.execute`` or any loader/network/LLM.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, time
import hashlib
import json
import math
from pathlib import Path
import os
import re
import sys
import tempfile
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


_REQUIRED_REQUEST_FIELDS = frozenset(
    {"context", "sessions", "symbols", "input_csv", "input_sha256"}
)
_REQUIRED_CSV_FIELDS = ("symbol", "trade_date", "close_qfq")
_TECHNICAL_FIELDS = (
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
_ROW_FIELDS = ("symbol", *_TECHNICAL_FIELDS, "contiguous_sessions", "missing_fields")
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE = re.compile(r"^(?P<code>[0-9]{6})\.(?P<suffix>SH|SZ)$")


class _InvalidRequest(Exception):
    """Private control-flow exception; its message never crosses the CLI."""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--vibe-root", required=True)
    args = parser.parse_args(argv)

    try:
        request_path = _resolved_file(args.request)
        output_path = _bounded_output_path(args.output, request_path.parent)
        request = _read_json(request_path)
        context = _validate_context(request.get("context"))
        sessions = _validate_sessions(request.get("sessions"), context["reference"])
        symbols = _validate_symbols(request.get("symbols"))
        input_hash = _validate_hash(request.get("input_sha256"))
        input_path = _bounded_input_path(request.get("input_csv"), request_path.parent)
        if _sha256(input_path) != input_hash:
            raise _InvalidRequest
        frame = _read_input(input_path, symbols, sessions, context["reference"])

        helper_path = ROOT / "competition_core" / "technical_indicators.py"
        if not helper_path.is_file():
            raise _InvalidRequest
        helper_hash = _sha256(helper_path)
        # These are the migrated Vibe implementations. No period is redefined
        # here and no Tool.execute path is involved.
        from competition_core.technical_indicators import (  # noqa: PLC0415
            _compute_bollinger,
            _compute_macd,
            _compute_rsi,
            _compute_sma,
        )

        prices = _price_map(frame)
        rows, missing_reasons = _compute_rows(
            prices,
            sessions=sessions,
            symbols=symbols,
            compute_sma=_compute_sma,
            compute_rsi=_compute_rsi,
            compute_macd=_compute_macd,
            compute_bollinger=_compute_bollinger,
        )
        current_helper_hash = _sha256(helper_path)
        if current_helper_hash != helper_hash:
            raise _InvalidRequest
        provenance = {
            "schema_version": "p10c.technical.v1",
            "worker_sha256": _sha256(Path(__file__).resolve()),
            "helper_sha256": helper_hash,
            "pandas_version": pd.__version__,
            "input_sha256": input_hash,
            "context": context["payload"],
            "sessions": sessions,
            "symbols": sorted(symbols),
            "missing_reasons": missing_reasons,
        }
        _atomic_json(output_path, {"rows": rows, "provenance": provenance})
        return 0
    except Exception:  # noqa: BLE001 - no request/input details leave the worker
        return 2


def _resolved_file(value: Any) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise _InvalidRequest
    path = Path(value).expanduser().resolve()
    if not path.is_file():
        raise _InvalidRequest
    return path


def _resolved_directory(value: Any) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise _InvalidRequest
    path = Path(value).expanduser().resolve()
    if not path.is_dir():
        raise _InvalidRequest
    return path


def _bounded_output_path(value: Any, root: Path) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise _InvalidRequest
    candidate = Path(value)
    resolved = candidate.resolve() if candidate.is_absolute() else (root / candidate).resolve()
    if resolved == root or not resolved.is_relative_to(root):
        raise _InvalidRequest
    return resolved


def _bounded_input_path(value: Any, root: Path) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise _InvalidRequest
    candidate = Path(value)
    # A drive-relative path (``C:foo``) has an anchor on Windows and is not a
    # safe request-relative reference either.
    if candidate.is_absolute() or candidate.anchor:
        raise _InvalidRequest
    resolved = (root / candidate).resolve()
    if resolved == root or not resolved.is_relative_to(root) or not resolved.is_file():
        raise _InvalidRequest
    return resolved


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 - details stay inside the worker
        raise _InvalidRequest from exc
    if not isinstance(value, dict) or not _REQUIRED_REQUEST_FIELDS <= set(value):
        raise _InvalidRequest
    return value


def _validate_hash(value: Any) -> str:
    if not isinstance(value, str) or _HASH_RE.fullmatch(value) is None:
        raise _InvalidRequest
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise _InvalidRequest from exc
    return digest.hexdigest()


def _validate_context(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise _InvalidRequest
    required = {
        "research_date",
        "phase",
        "market_data_reference_date",
        "as_of",
        "timezone",
    }
    if not required <= set(value):
        raise _InvalidRequest
    research_date = _parse_iso_date(value["research_date"])
    reference = _parse_iso_date(value["market_data_reference_date"])
    phase = value["phase"]
    timezone = value["timezone"]
    if phase not in {"preopen", "intraday", "postclose"} or timezone != "Asia/Shanghai":
        raise _InvalidRequest
    as_of = _parse_datetime(value["as_of"])
    local = as_of.astimezone(_SHANGHAI)
    if local.date() != research_date:
        raise _InvalidRequest
    if phase == "postclose":
        if reference != research_date or local.time() < time(15):
            raise _InvalidRequest
    elif reference >= research_date:
        raise _InvalidRequest
    payload = {
        "research_date": research_date.isoformat(),
        "phase": phase,
        "market_data_reference_date": reference.isoformat(),
        "as_of": value["as_of"],
        "timezone": timezone,
    }
    return {"payload": payload, "reference": reference}


def _parse_datetime(value: Any) -> datetime:
    if not isinstance(value, str):
        raise _InvalidRequest
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise _InvalidRequest from exc
    if parsed.utcoffset() is None:
        raise _InvalidRequest
    return parsed


def _parse_iso_date(value: Any) -> date:
    if not isinstance(value, str) or value != value.strip():
        raise _InvalidRequest
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise _InvalidRequest from exc
    if parsed.isoformat() != value:
        raise _InvalidRequest
    return parsed


def _validate_sessions(value: Any, reference: date) -> list[str]:
    if not isinstance(value, list) or not value:
        raise _InvalidRequest
    dates = [_parse_iso_date(item) for item in value]
    if len(set(dates)) != len(dates) or dates != sorted(dates):
        raise _InvalidRequest
    if any(item > reference for item in dates) or dates[-1] != reference:
        raise _InvalidRequest
    return [item.isoformat() for item in dates]


def _validate_symbols(value: Any) -> list[str]:
    if not isinstance(value, list) or not value:
        raise _InvalidRequest
    result: list[str] = []
    seen: set[str] = set()
    for symbol in value:
        if not isinstance(symbol, str) or not symbol or symbol != symbol.strip():
            raise _InvalidRequest
        match = _SYMBOL_RE.fullmatch(symbol)
        if match is None or not _valid_board(match.group("code"), match.group("suffix")):
            raise _InvalidRequest
        if symbol in seen:
            raise _InvalidRequest
        seen.add(symbol)
        result.append(symbol)
    return result


def _valid_board(code: str, suffix: str) -> bool:
    if suffix == "SH":
        return code.startswith(("600", "601", "603", "605", "688", "689"))
    return code.startswith(("000", "001", "002", "003", "300", "301", "302"))


def _read_input(
    path: Path,
    symbols: list[str],
    sessions: list[str],
    reference: date,
) -> pd.DataFrame:
    try:
        frame = pd.read_csv(path, dtype="string", keep_default_na=False)
    except Exception as exc:  # noqa: BLE001 - malformed input is a batch failure
        raise _InvalidRequest from exc
    if len(set(frame.columns)) != len(frame.columns) or not set(_REQUIRED_CSV_FIELDS) <= set(frame.columns):
        raise _InvalidRequest
    frame = frame.loc[:, list(_REQUIRED_CSV_FIELDS)].copy()
    allowed = set(symbols)
    allowed_sessions = set(sessions)
    seen: set[tuple[str, date]] = set()
    normalized_dates: list[str] = []
    for raw_symbol, raw_date in frame.loc[:, ["symbol", "trade_date"]].itertuples(
        index=False, name=None
    ):
        if not isinstance(raw_symbol, str) or raw_symbol not in allowed:
            raise _InvalidRequest
        parsed_date = _parse_iso_date(raw_date)
        if parsed_date > reference:
            raise _InvalidRequest
        if parsed_date.isoformat() not in allowed_sessions:
            raise _InvalidRequest
        key = (raw_symbol, parsed_date)
        if key in seen:
            raise _InvalidRequest
        seen.add(key)
        normalized_dates.append(parsed_date.isoformat())
    frame["trade_date"] = normalized_dates
    normalized_closes: list[float | None] = []
    for value in frame["close_qfq"].tolist():
        number = _finite_float(value)
        normalized_closes.append(number if number is not None and number > 0 else None)
    frame["close_qfq"] = normalized_closes
    return frame


def _finite_float(value: Any) -> float | None:
    if value is None or _is_missing(value):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    # Indicator values are not prices: MACD can be negative or zero, and a
    # legitimate RSI can be zero. Price positivity is enforced at input only.
    return number if math.isfinite(number) else None


def _is_missing(value: Any) -> bool:
    try:
        result = pd.isna(value)
        return bool(result)
    except (TypeError, ValueError):
        return False


def _price_map(frame: pd.DataFrame) -> dict[tuple[str, str], float | None]:
    return {
        (symbol, trade_date): close
        for symbol, trade_date, close in frame.loc[:, list(_REQUIRED_CSV_FIELDS)].itertuples(
            index=False, name=None
        )
    }


def _compute_rows(
    prices: dict[tuple[str, str], float | None],
    *,
    sessions: list[str],
    symbols: list[str],
    compute_sma: Any,
    compute_rsi: Any,
    compute_macd: Any,
    compute_bollinger: Any,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, str]]]:
    rows: list[dict[str, Any]] = []
    reasons: dict[str, dict[str, str]] = {}
    for symbol in sorted(symbols):
        try:
            row, row_reasons = _compute_one(
                prices,
                symbol=symbol,
                sessions=sessions,
                compute_sma=compute_sma,
                compute_rsi=compute_rsi,
                compute_macd=compute_macd,
                compute_bollinger=compute_bollinger,
            )
        except Exception:  # noqa: BLE001 - one stock cannot poison the batch
            row = _empty_row(symbol, 0)
            row_reasons = {field: "stock_calculation_error" for field in _TECHNICAL_FIELDS}
        rows.append(row)
        reasons[symbol] = row_reasons
    return rows, reasons


def _compute_one(
    prices: dict[tuple[str, str], float | None],
    *,
    symbol: str,
    sessions: list[str],
    compute_sma: Any,
    compute_rsi: Any,
    compute_macd: Any,
    compute_bollinger: Any,
) -> tuple[dict[str, Any], dict[str, str]]:
    values = pd.Series(
        [prices.get((symbol, trade_date)) for trade_date in sessions],
        index=pd.Index(sessions, name="trade_date"),
        dtype="float64",
    )
    row: dict[str, Any] = {"symbol": symbol}
    row.update({field: None for field in _TECHNICAL_FIELDS})
    row["contiguous_sessions"] = _contiguous_length(values)
    reasons: dict[str, str] = {}

    for field, period in (("sma_5", 5), ("sma_10", 10), ("sma_20", 20)):
        window, reason = _complete_window(values, period)
        if window is None:
            reasons[field] = reason
            continue
        result = compute_sma(window, period)
        value = _finite_float(result)
        if value is None:
            reasons[field] = "helper_returned_missing"
        else:
            row[field] = value

    if len(values) < 25:
        reasons["sma_20_lag5"] = "insufficient_sessions"
    else:
        window = values.iloc[-25:-5]
        if window.isna().any():
            reasons["sma_20_lag5"] = "missing_close_window"
        else:
            result = compute_sma(window, 20)
            value = _finite_float(result)
            if value is None:
                reasons["sma_20_lag5"] = "helper_returned_missing"
            else:
                row["sma_20_lag5"] = value

    window, reason = _complete_window(values, 20)
    if window is None:
        reasons["bollinger_upper"] = reason
        reasons["bollinger_middle"] = reason
        reasons["bollinger_lower"] = reason
    else:
        result = compute_bollinger(window, 20)
        if not isinstance(result, Mapping):
            for field in ("bollinger_upper", "bollinger_middle", "bollinger_lower"):
                reasons[field] = "helper_returned_missing"
        else:
            for field, key in (
                ("bollinger_upper", "upper"),
                ("bollinger_middle", "middle"),
                ("bollinger_lower", "lower"),
            ):
                value = _finite_float(result.get(key))
                if value is None:
                    reasons[field] = "helper_returned_missing"
                else:
                    row[field] = value

    tail = _contiguous_tail(values)
    if len(tail) < 15:
        reasons["rsi_14"] = "insufficient_contiguous_sessions"
    else:
        value = _finite_float(compute_rsi(tail))
        if value is None:
            reasons["rsi_14"] = "helper_returned_missing"
        else:
            row["rsi_14"] = value

    if len(tail) < 35:
        reasons["macd_histogram"] = "insufficient_contiguous_sessions"
    else:
        result = compute_macd(tail)
        value = _finite_float(result.get("histogram") if isinstance(result, Mapping) else None)
        if value is None:
            reasons["macd_histogram"] = "helper_returned_missing"
        else:
            row["macd_histogram"] = value

    row["missing_fields"] = [field for field in _TECHNICAL_FIELDS if row[field] is None]
    return row, reasons


def _empty_row(symbol: str, contiguous: int) -> dict[str, Any]:
    row: dict[str, Any] = {"symbol": symbol}
    row.update({field: None for field in _TECHNICAL_FIELDS})
    row["contiguous_sessions"] = contiguous
    row["missing_fields"] = list(_TECHNICAL_FIELDS)
    return row


def _complete_window(values: pd.Series, period: int) -> tuple[pd.Series | None, str]:
    if len(values) < period:
        return None, "insufficient_sessions"
    window = values.iloc[-period:]
    if window.isna().any():
        return None, "missing_close_window"
    return window, ""


def _contiguous_tail(values: pd.Series) -> pd.Series:
    start = len(values)
    for index in range(len(values) - 1, -1, -1):
        value = values.iloc[index]
        if pd.notna(value) and math.isfinite(float(value)):
            start = index
            continue
        break
    return values.iloc[start:] if start < len(values) else values.iloc[0:0]


def _contiguous_length(values: pd.Series) -> int:
    return len(_contiguous_tail(values))


def _agent_root(vibe_root: Path) -> Path:
    candidate = vibe_root / "agent"
    if candidate.is_dir():
        return candidate.resolve()
    if (vibe_root / "src").is_dir():
        return vibe_root
    raise _InvalidRequest


def _atomic_json(path: Path, value: Any) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = (
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=path.parent,
                prefix=".partial-",
                delete=False,
            ) as handle:
                temporary = Path(handle.name)
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
    except Exception as exc:  # noqa: BLE001 - caller only receives failure code
        raise _InvalidRequest from exc


if __name__ == "__main__":
    raise SystemExit(main())
