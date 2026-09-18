"""Small, immutable cache for explicitly dated supplemental source queries."""

from __future__ import annotations

from datetime import date, datetime
import json
import math
from numbers import Integral, Real
from pathlib import Path
import re
from typing import Any, Mapping

import pandas as pd

from competition_core.pipeline.artifacts import atomic_json, confined_path, content_hash, json_bytes
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.sources import ResearchDataClient


PAGE_SIZE = 6000
MAX_PAGES = 100
MAX_ROWS = 600_000

_ENDPOINTS = frozenset({"index_daily", "stk_limit", "moneyflow", "top_list", "margin_detail"})
_RECORD_FIELDS = frozenset({
    "status", "endpoint", "request", "source", "context", "observed_at", "data",
    "error", "integrity_hash", "request_id",
})
_MISSING_ERROR_FIELDS = frozenset({"code", "class"})
_MAX_PARAMETERS = 64
_MAX_PARAMETER_STRING = 4096
_MAX_PARAMETER_NUMBER = 10**18
_SAFE_LABEL = re.compile(r"[A-Za-z0-9_.-]{1,64}")


class _MissingSource(Exception):
    def __init__(self, error: dict[str, str]) -> None:
        self.error = error


class _FieldError(Exception):
    pass


class _DateError(Exception):
    pass


class DatedSourceCache:
    """Freeze a small historical source response under a deterministic request id.

    The caller supplies a ``ResearchDataClient`` so date and observation-clock
    validation remains at the common source boundary.  Only the five
    supplemental endpoints used by S2/S3 are exposed here.
    """

    def __init__(self, root: Path, client: ResearchDataClient, *, source_name: str) -> None:
        if not isinstance(client, ResearchDataClient):
            raise PipelineError("INVALID_ARGUMENTS", "client must be a ResearchDataClient")
        if not isinstance(source_name, str) or not source_name.strip() or len(source_name.strip()) > 256:
            raise PipelineError("INVALID_ARGUMENTS", "source_name must be a bounded non-empty string")
        try:
            resolved_root = Path(root).resolve()
        except (TypeError, OSError) as exc:
            raise PipelineError("INVALID_ARGUMENTS", "cache root is invalid") from exc
        if resolved_root.exists() and not resolved_root.is_dir():
            raise PipelineError("INVALID_ARGUMENTS", "cache root must be a directory")
        self.root = resolved_root
        self.client = client
        self.source_name = source_name.strip()

    def get(self, endpoint: str, parameters: dict, *, key_fields: tuple[str, ...]) -> dict:
        """Get a frozen dated response, querying the source only on a cache miss."""
        endpoint = self._validate_endpoint(endpoint)
        request = self._validate_parameters(parameters)
        keys = self._validate_key_fields(key_fields)
        context = self._context_dict()
        request_id = content_hash({
            "source": self.source_name,
            "context": context,
            "endpoint": endpoint,
            "parameters": request,
            "key_fields": list(keys),
        })
        path = self._cache_path(request_id)

        if path.exists() or path.is_symlink():
            return self._load_cached(
                path,
                endpoint=endpoint,
                request=request,
                context=context,
                key_fields=keys,
                request_id=request_id,
            )

        observed_value = self._observed_datetime()
        context_as_of = _context_datetime(context, "as_of")
        if observed_value < context_as_of:
            raise PipelineError("NOT_READY", "source observation clock precedes context as_of")
        observed_at = observed_value.isoformat()
        records: list[dict[str, Any]] = []
        seen: set[str] = set()
        offset = 0
        total_rows = 0

        for _page_number in range(MAX_PAGES):
            page_request = {**request, "limit": PAGE_SIZE, "offset": offset}
            try:
                raw_page = self._request_page(endpoint, page_request)
            except _MissingSource as missing:
                return self._persist(
                    path,
                    self._record(
                        status="MISSING", endpoint=endpoint, request=request,
                        source=self.source_name, context=context, observed_at=observed_at,
                        data=[], error=missing.error, request_id=request_id,
                    ),
                )

            if raw_page.empty:
                records = sorted(records, key=lambda row: self._sort_key(row, keys))
                return self._persist(
                    path,
                    self._record(
                        status="SUCCESS", endpoint=endpoint, request=request,
                        source=self.source_name, context=context, observed_at=observed_at,
                        data=records, error=None, request_id=request_id,
                    ),
                )
            if len(raw_page) > PAGE_SIZE:
                raise PipelineError("STALE_DATA", "source page exceeds the dated cache page size", endpoint=endpoint)
            if total_rows + len(raw_page) > MAX_ROWS:
                raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "source pagination exceeded the dated cache safety limit", endpoint=endpoint)

            try:
                page_records = self._records_from_page(raw_page, request, keys)
            except _FieldError:
                return self._persist(
                    path,
                    self._record(
                        status="MISSING", endpoint=endpoint, request=request,
                        source=self.source_name, context=context, observed_at=observed_at,
                        data=[], error={"code": "FIELD_ERROR", "class": "FieldError"},
                        request_id=request_id,
                    ),
                )

            page_keys = [self._key_token(row, keys) for row in page_records]
            if len(set(page_keys)) != len(page_keys) or seen.intersection(page_keys):
                raise PipelineError("STALE_DATA", "source pagination contains duplicate identity keys", endpoint=endpoint)
            seen.update(page_keys)
            records.extend(page_records)
            total_rows += len(raw_page)
            offset += len(raw_page)
        else:
            raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "source pagination did not terminate", endpoint=endpoint)

    def get_cached(self, endpoint: str, parameters: dict, *, key_fields: tuple[str, ...]) -> dict:
        """Load an already frozen response; never fall through to the provider."""
        endpoint = self._validate_endpoint(endpoint)
        request = self._validate_parameters(parameters)
        keys = self._validate_key_fields(key_fields)
        context = self._context_dict()
        request_id = content_hash({"source": self.source_name, "context": context, "endpoint": endpoint,
                                   "parameters": request, "key_fields": list(keys)})
        path = self._cache_path(request_id)
        if not path.exists() or path.is_symlink():
            raise PipelineError("NOT_READY", "required frozen source response is unavailable", endpoint=endpoint)
        return self._load_cached(path, endpoint=endpoint, request=request, context=context,
                                 key_fields=keys, request_id=request_id)

    def _request_page(self, endpoint: str, parameters: dict[str, Any]) -> pd.DataFrame:
        try:
            frame = self.client.request(endpoint, **parameters)
        except PipelineError as exc:
            if str(exc.code).upper() in {"STALE_DATA", "NOT_READY"}:
                raise
            raise _MissingSource(self._source_error(exc)) from None
        except Exception as exc:  # noqa: BLE001 - provider details must not enter a frozen record
            raise _MissingSource(self._source_error(exc)) from None
        if not isinstance(frame, pd.DataFrame):
            raise _MissingSource({"code": "SOURCE_UNAVAILABLE", "class": "InvalidSourceResponse"})
        return frame

    def _records_from_page(
        self,
        frame: pd.DataFrame,
        request: dict[str, Any],
        key_fields: tuple[str, ...],
    ) -> list[dict[str, Any]]:
        try:
            columns = list(frame.columns)
        except Exception as exc:  # noqa: BLE001 - malformed provider frame is a field failure
            raise _FieldError from exc
        if len(columns) != len(set(columns)) or any(not isinstance(column, str) for column in columns):
            raise _FieldError
        field_names = _field_names(request["fields"])
        if "*" not in field_names and any(field not in columns for field in field_names):
            raise _FieldError
        if any(field not in columns for field in key_fields):
            raise _FieldError
        try:
            raw_records = frame.to_dict(orient="records")
        except Exception as exc:  # noqa: BLE001 - malformed provider frame is a field failure
            raise _FieldError from exc

        records: list[dict[str, Any]] = []
        for raw in raw_records:
            if not isinstance(raw, dict):
                raise _FieldError
            try:
                record = {column: _json_value(value) for column, value in raw.items()}
            except _FieldError:
                raise
            except Exception as exc:  # noqa: BLE001 - non-JSON provider dtype
                raise _FieldError from exc
            if any(field not in record for field in field_names if field != "*"):
                raise _FieldError
            for field in key_fields:
                if field not in record or _missing_key(record[field]):
                    raise _FieldError
                if isinstance(record[field], (dict, list)):
                    raise _FieldError
            try:
                json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
            except (TypeError, ValueError, OverflowError) as exc:
                raise _FieldError from exc
            records.append(record)
        return records

    def _load_cached(
        self,
        path: Path,
        *,
        endpoint: str,
        request: dict[str, Any],
        context: dict[str, Any],
        key_fields: tuple[str, ...],
        request_id: str,
    ) -> dict[str, Any]:
        record = _read_record(path)
        if set(record) != _RECORD_FIELDS:
            raise PipelineError("STALE_DATA", "dated cache record fields are invalid")
        if (
            record.get("request_id") != request_id
            or record.get("endpoint") != endpoint
            or record.get("source") != self.source_name
            or not _same_json(record.get("request"), request)
            or not _same_json(record.get("context"), context)
        ):
            raise PipelineError("STALE_DATA", "dated cache record identity differs from the request")
        observed_at = record.get("observed_at")
        if not _aware_iso(observed_at):
            raise PipelineError("STALE_DATA", "dated cache observation time is invalid")
        observed_value = datetime.fromisoformat(observed_at)
        context_as_of = _context_datetime(context, "as_of")
        current_observed = self._observed_datetime()
        if observed_value < context_as_of:
            raise PipelineError("STALE_DATA", "dated cache observation precedes context as_of")
        if observed_value > current_observed:
            raise PipelineError("STALE_DATA", "dated cache observation is in the future relative to observation clock")
        status = record.get("status")
        data = record.get("data")
        error = record.get("error")
        if status not in {"SUCCESS", "MISSING"} or not isinstance(data, list):
            raise PipelineError("STALE_DATA", "dated cache record status or data is invalid")
        if status == "SUCCESS" and error is not None:
            raise PipelineError("STALE_DATA", "successful dated cache record has an error")
        if status == "MISSING":
            if data or not _safe_error(error):
                raise PipelineError("STALE_DATA", "missing dated cache record is invalid")
        try:
            self._validate_cached_data(data, request, context, key_fields)
        except _DateError as exc:
            raise PipelineError("STALE_DATA", "dated cache row trade_date is outside request or context cutoff") from exc
        except _FieldError as exc:
            raise PipelineError("STALE_DATA", "dated cache identity or field data is invalid") from exc
        return record

    def _validate_cached_data(
        self,
        data: list[Any],
        request: dict[str, Any],
        context: dict[str, Any],
        key_fields: tuple[str, ...],
    ) -> None:
        if not data:
            return
        field_names = _field_names(request["fields"])
        request_start, request_end = _request_date_bounds(request)
        cutoff = _context_date(context, "market_data_reference_date")
        seen: set[str] = set()
        for record in data:
            if not isinstance(record, dict):
                raise _FieldError
            try:
                trade_date = _parse_date_value(record["trade_date"])
            except (KeyError, PipelineError) as exc:
                raise _DateError from exc
            if not request_start <= trade_date <= request_end or trade_date > cutoff:
                raise _DateError
            if "*" not in field_names and any(field not in record for field in field_names):
                raise _FieldError
            if any(field not in record or _missing_key(record[field]) for field in key_fields):
                raise _FieldError
            if any(isinstance(record[field], (dict, list)) for field in key_fields):
                raise _FieldError
            try:
                json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
                token = self._key_token(record, key_fields)
            except (TypeError, ValueError, OverflowError, _FieldError) as exc:
                raise _FieldError from exc
            if token in seen:
                raise _FieldError
            seen.add(token)
        expected = sorted(data, key=lambda row: self._sort_key(row, key_fields))
        if data != expected:
            raise _FieldError

    def _persist(self, path: Path, record: dict[str, Any]) -> dict[str, Any]:
        atomic_json(path, record, immutable=True)
        return record

    @staticmethod
    def _record(
        *,
        status: str,
        endpoint: str,
        request: dict[str, Any],
        source: str,
        context: dict[str, Any],
        observed_at: str,
        data: list[dict[str, Any]],
        error: dict[str, str] | None,
        request_id: str,
    ) -> dict[str, Any]:
        record: dict[str, Any] = {
            "status": status,
            "endpoint": endpoint,
            "request": request,
            "source": source,
            "context": context,
            "observed_at": observed_at,
            "data": data,
            "error": error,
            "request_id": request_id,
        }
        record["integrity_hash"] = content_hash(record)
        return record

    @staticmethod
    def _validate_endpoint(endpoint: str) -> str:
        if not isinstance(endpoint, str) or endpoint not in _ENDPOINTS:
            raise PipelineError("INVALID_ARGUMENTS", "endpoint is outside the dated source cache contract")
        return endpoint

    @staticmethod
    def _validate_key_fields(key_fields: tuple[str, ...]) -> tuple[str, ...]:
        if (
            not isinstance(key_fields, tuple)
            or not key_fields
            or len(key_fields) != len(set(key_fields))
            or any(not isinstance(field, str) or not field or len(field) > 256 for field in key_fields)
        ):
            raise PipelineError("INVALID_ARGUMENTS", "key_fields must be a unique non-empty tuple of strings")
        return key_fields

    @staticmethod
    def _validate_parameters(parameters: dict) -> dict[str, Any]:
        if not isinstance(parameters, dict) or not parameters or len(parameters) > _MAX_PARAMETERS:
            raise PipelineError("INVALID_ARGUMENTS", "dated source parameters are invalid or unbounded")
        normalized: dict[str, Any] = {}
        for key, value in parameters.items():
            if not isinstance(key, str) or not key or key.startswith("_") or len(key) > 128 or _forbidden_parameter(key):
                raise PipelineError("INVALID_ARGUMENTS", "dated source parameters contain a forbidden field")
            normalized[key] = _parameter_value(value)
        if "fields" not in normalized or not isinstance(normalized["fields"], str):
            raise PipelineError("INVALID_ARGUMENTS", "dated source parameters require fields")
        _field_names(normalized["fields"])
        has_trade_date = "trade_date" in normalized
        has_range = "start_date" in normalized or "end_date" in normalized
        if has_trade_date and has_range:
            raise PipelineError("INVALID_ARGUMENTS", "dated source parameters mix date forms")
        if not has_trade_date and set(normalized).intersection({"start_date", "end_date"}) != {"start_date", "end_date"}:
            raise PipelineError("INVALID_ARGUMENTS", "dated source parameters require an explicit date")
        if not has_trade_date and not {"start_date", "end_date"} <= set(normalized):
            raise PipelineError("INVALID_ARGUMENTS", "dated source range requires both dates")
        return normalized

    def _context_dict(self) -> dict[str, Any]:
        context = getattr(self.client, "context", None)
        to_dict = getattr(context, "to_dict", None)
        if not callable(to_dict):
            raise PipelineError("INVALID_ARGUMENTS", "ResearchDataClient context is invalid")
        try:
            raw = to_dict()
            if not isinstance(raw, dict):
                raise TypeError
            result = {key: _json_value(value) for key, value in raw.items()}
            json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
            return result
        except PipelineError:
            raise
        except Exception as exc:  # noqa: BLE001 - context is part of the cache identity
            raise PipelineError("INVALID_ARGUMENTS", "ResearchDataClient context is not JSON-compatible") from exc

    def _observed_at(self) -> str:
        return self._observed_datetime().isoformat()

    def _observed_datetime(self) -> datetime:
        clock = getattr(self.client, "observed_clock", None)
        if not callable(clock):
            raise PipelineError("NOT_READY", "source observation clock is unavailable")
        try:
            value = clock()
        except Exception as exc:  # noqa: BLE001 - clock details are not safe provenance
            raise PipelineError("NOT_READY", "source observation clock failed") from exc
        if not isinstance(value, datetime) or value.utcoffset() is None:
            raise PipelineError("NOT_READY", "source observation time must be timezone-aware")
        return value

    def _cache_path(self, request_id: str) -> Path:
        return confined_path(self.root, f"{request_id}.json")

    @staticmethod
    def _key_token(record: dict[str, Any], key_fields: tuple[str, ...]) -> str:
        try:
            values = [record[field] for field in key_fields]
            if any(isinstance(value, (dict, list)) for value in values):
                raise _FieldError
            return json.dumps(values, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        except _FieldError:
            raise
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise _FieldError from exc

    @staticmethod
    def _sort_key(record: dict[str, Any], key_fields: tuple[str, ...]) -> tuple[Any, ...]:
        try:
            return tuple(_sort_value(record[field]) for field in key_fields)
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise _FieldError from exc

    @staticmethod
    def _source_error(exc: Exception) -> dict[str, str]:
        if isinstance(exc, PipelineError):
            code = _safe_label(exc.code, "SOURCE_UNAVAILABLE")
        elif isinstance(exc, PermissionError):
            code = "PERMISSION_DENIED"
        elif isinstance(exc, (KeyError, LookupError, TypeError, ValueError)):
            code = "FIELD_ERROR"
        else:
            code = "SOURCE_UNAVAILABLE"
        return {"code": code, "class": _safe_label(type(exc).__name__, "SourceError")}


def _read_record(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001 - every malformed artifact is a pipeline error
        raise PipelineError("STALE_DATA", "dated cache artifact is corrupt") from exc
    if not isinstance(value, dict):
        raise PipelineError("STALE_DATA", "dated cache artifact is not an object")
    stored = value.get("integrity_hash")
    payload = dict(value)
    payload.pop("integrity_hash", None)
    try:
        valid_hash = isinstance(stored, str) and content_hash(payload) == stored
        canonical = raw == json_bytes(value)
    except Exception as exc:  # noqa: BLE001 - malformed JSON values fail closed
        raise PipelineError("STALE_DATA", "dated cache artifact hash is invalid") from exc
    if not valid_hash:
        raise PipelineError("STALE_DATA", "dated cache integrity hash mismatch")
    if not canonical:
        raise PipelineError("STALE_DATA", "dated cache serialization is not canonical")
    return value


def _json_value(value: Any) -> Any:
    if value is None:
        return None
    if _is_missing(value):
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return value
    if isinstance(value, Integral):
        return int(value)
    if isinstance(value, Real):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise _FieldError
            result[key] = _json_value(item)
        return result
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    item = getattr(value, "item", None)
    if callable(item):
        return _json_value(item())
    raise _FieldError


def _is_missing(value: Any) -> bool:
    try:
        result = pd.isna(value)
        return isinstance(result, bool) and result
    except (TypeError, ValueError):
        return False


def _missing_key(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _parameter_value(value: Any) -> str | int | float:
    if isinstance(value, str):
        if len(value) > _MAX_PARAMETER_STRING:
            raise PipelineError("INVALID_ARGUMENTS", "dated source parameter string is too long")
        return value
    if isinstance(value, bool) or not isinstance(value, (Integral, Real)):
        raise PipelineError("INVALID_ARGUMENTS", "dated source parameters must be strings or finite numbers")
    if isinstance(value, Integral):
        number = int(value)
        if abs(number) > _MAX_PARAMETER_NUMBER:
            raise PipelineError("INVALID_ARGUMENTS", "dated source parameter number is out of bounds")
        return number
    number = float(value)
    if not math.isfinite(number) or abs(number) > _MAX_PARAMETER_NUMBER:
        raise PipelineError("INVALID_ARGUMENTS", "dated source parameters must be finite and bounded")
    return number


def _field_names(fields: str) -> list[str]:
    if not isinstance(fields, str) or not fields.strip():
        raise PipelineError("INVALID_ARGUMENTS", "dated source fields must be a non-empty string")
    names = [field.strip() for field in fields.split(",")]
    if not names or any(not field or len(field) > 256 for field in names) or len(names) > _MAX_PARAMETERS:
        raise PipelineError("INVALID_ARGUMENTS", "dated source fields are invalid or unbounded")
    return names


def _forbidden_parameter(key: str) -> bool:
    lower = key.lower().replace("-", "_")
    parts = {part for part in re.split(r"[^a-z0-9]+", lower) if part}
    forbidden = {
        "offset", "limit", "token", "url", "secret", "password", "credential", "authorization",
        "apikey", "appkey", "auth", "header", "headers", "cookie", "cookies", "proxy", "proxies",
    }
    return bool(parts & forbidden) or "api_key" in lower


def _safe_label(value: Any, default: str) -> str:
    label = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value))[:64]
    return label if _SAFE_LABEL.fullmatch(label or "") else default


def _safe_error(value: Any) -> bool:
    if not isinstance(value, dict) or set(value) != _MISSING_ERROR_FIELDS:
        return False
    return all(isinstance(value[field], str) and _SAFE_LABEL.fullmatch(value[field]) for field in _MISSING_ERROR_FIELDS)


def _aware_iso(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return False
    return parsed.utcoffset() is not None


def _request_date_bounds(request: dict[str, Any]) -> tuple[date, date]:
    try:
        if "trade_date" in request:
            value = _parse_date_value(request["trade_date"])
            return value, value
        start = _parse_date_value(request["start_date"])
        end = _parse_date_value(request["end_date"])
    except (KeyError, PipelineError) as exc:
        raise PipelineError("STALE_DATA", "dated cache request date is invalid") from exc
    if start > end:
        raise PipelineError("STALE_DATA", "dated cache request date range is invalid")
    return start, end


def _context_date(context: dict[str, Any], field: str) -> date:
    try:
        return _parse_date_value(context[field])
    except (KeyError, PipelineError) as exc:
        raise PipelineError("STALE_DATA", "dated cache context date is invalid") from exc


def _context_datetime(context: dict[str, Any], field: str) -> datetime:
    value = context.get(field)
    if not isinstance(value, str):
        raise PipelineError("STALE_DATA", "dated cache context time is invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise PipelineError("STALE_DATA", "dated cache context time is invalid") from exc
    if parsed.utcoffset() is None:
        raise PipelineError("STALE_DATA", "dated cache context time must be timezone-aware")
    return parsed


def _parse_date_value(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, bool):
        raise PipelineError("STALE_DATA", "dated cache date is invalid")
    if isinstance(value, Integral):
        text = str(int(value))
    elif isinstance(value, str):
        text = value.strip().replace("-", "")
    else:
        raise PipelineError("STALE_DATA", "dated cache date is invalid")
    if len(text) != 8 or not text.isdigit():
        raise PipelineError("STALE_DATA", "dated cache date is invalid")
    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError as exc:
        raise PipelineError("STALE_DATA", "dated cache date is invalid") from exc


def _same_json(left: Any, right: Any) -> bool:
    try:
        return content_hash(left) == content_hash(right)
    except (TypeError, ValueError, OverflowError):
        return False


def _sort_value(value: Any) -> tuple[int, Any]:
    if isinstance(value, bool):
        return 0, int(value)
    if isinstance(value, (int, float)):
        return 1, value
    if isinstance(value, str):
        return 2, value
    return 3, json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


__all__ = ["DatedSourceCache", "MAX_PAGES", "MAX_ROWS", "PAGE_SIZE"]
