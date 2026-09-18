"""Immutable point-in-time canonical daily dataset for the research pipeline."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from io import BytesIO
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any

import pandas as pd

from competition_core.pipeline.artifacts import atomic_bytes, atomic_json, content_hash, file_hash
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.publication import (
    snapshot_publication_symbols, verify_completed_daily,
    verify_publication_frame, verify_snapshot_liquidity,
)
from competition_core.pipeline.sources import ResearchDataClient
from competition_core.pipeline.suspensions import SuspensionProofStore, placeholder_mask
from competition_core.pipeline.time_context import ResearchContext, SHANGHAI
from contracts.symbol import CanonicalSymbol, SymbolContractError

PAGE_SIZE = 6000
BATCH_SCHEMA_VERSION = "p10b.batch.v1"
QUALITY_SCHEMA_VERSION = "p10b.quality.v1"
MANIFEST_SCHEMA_VERSION = "p10b.manifest.v1"

RAW_COLUMNS: dict[str, tuple[str, ...]] = {
    "trade_calendar": ("exchange", "cal_date", "is_open", "pretrade_date"),
    "daily": ("ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"),
    "daily_basic": (
        "ts_code",
        "trade_date",
        "turnover_rate",
        "volume_ratio",
        "total_mv",
        "circ_mv",
    ),
    "adj_factor": ("ts_code", "trade_date", "adj_factor"),
}

PANEL_COLUMNS = (
    "symbol",
    "trade_date",
    "open_raw",
    "high_raw",
    "low_raw",
    "close_raw",
    "volume_raw",
    "amount_raw",
    "volume_shares",
    "amount_cny",
    "adj_factor",
    "open_qfq",
    "high_qfq",
    "low_qfq",
    "close_qfq",
    "turnover_rate",
    "volume_ratio",
    "total_mv",
    "circ_mv",
)


class CanonicalDataset:
    """Prepare and load a frozen canonical daily panel.

    Raw provider responses are cached by source, endpoint and requested date.
    A completed manifest is immutable and is the only input used by ``load``.
    """

    def __init__(self, root: Path, source_name: str = "tushare") -> None:
        if not isinstance(root, Path):
            root = Path(root)
        if not isinstance(source_name, str) or not source_name.strip():
            raise PipelineError("INVALID_ARGUMENTS", "source_name must be a non-empty string")
        self.root = root.resolve()
        self.source_name = source_name.strip()
        self._source_key = _safe_component(self.source_name)
        self.suspensions = SuspensionProofStore(self.root, self._source_key, self.source_name)

    def prepare(
        self,
        client: Any,
        context: ResearchContext,
        symbols: list[str],
        *,
        snapshot_id: str,
        sessions: int = 120,
        publication_snapshot: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_symbols = self._validate_inputs(context, symbols, snapshot_id, sessions)
        if not isinstance(client, ResearchDataClient):
            client = ResearchDataClient(client, context)
        elif client.context != context:
            raise PipelineError("STALE_DATA", "source context differs from dataset context")
        self._validate_observation_clock(client, context)
        publication_symbols = None
        if context.phase == "postclose":
            if not isinstance(publication_snapshot, dict):
                raise PipelineError("NOT_READY", "postclose dataset requires an independent PIT publication snapshot")
            publication_symbols = snapshot_publication_symbols(publication_snapshot, context)
            if publication_snapshot["snapshot_id"] != snapshot_id or normalized_symbols != sorted(
                row["ts_code"] for row in publication_snapshot["members"] if row.get("is_eligible") is True
            ):
                raise PipelineError("STALE_DATA", "publication snapshot identity or eligible Universe differs")
        reference_date = context.market_data_reference_date
        calendar_start = reference_date - timedelta(days=max(45, sessions * 3))
        dataset_id = self._dataset_id(
            context=context,
            symbols=normalized_symbols,
            snapshot_id=snapshot_id,
            sessions=sessions,
        )
        dataset_dir = self.root / "datasets" / dataset_id
        manifest_path = dataset_dir / "manifest.json"

        if manifest_path.exists():
            self.load(manifest_path)
            return _read_json(manifest_path, "manifest")
        # An interrupted final commit can leave valid panel/quality artifacts.
        # Reconstruct from frozen source batches; immutable writes check equality.

        calendar_request = {
            "exchange": "SSE",
            "start_date": calendar_start.strftime("%Y%m%d"),
            "end_date": reference_date.strftime("%Y%m%d"),
            "fields": ",".join(RAW_COLUMNS["trade_calendar"]),
        }
        calendar_key = f"{calendar_start:%Y%m%d}_{reference_date:%Y%m%d}"
        calendar, calendar_ref = self._get_or_fetch_batch(
            client,
            context=context,
            endpoint="trade_calendar",
            batch_key=calendar_key,
            request=calendar_request,
            requested_date=None,
            calendar_start=calendar_start,
        )
        session_dates = _completed_sessions(calendar, reference_date, sessions)

        raw: dict[tuple[str, str], pd.DataFrame] = {}
        batch_refs = [calendar_ref]
        suspension_refs = []
        for session_date in session_dates:
            date_key = session_date.strftime("%Y%m%d")
            for endpoint in ("daily", "daily_basic", "adj_factor"):
                request = {
                    "trade_date": date_key,
                    "fields": ",".join(RAW_COLUMNS[endpoint]),
                }
                frame, batch_ref = self._get_or_fetch_batch(
                    client,
                    context=context,
                    endpoint=endpoint,
                    batch_key=date_key,
                    request=request,
                    requested_date=session_date,
                    calendar_start=None,
                    publication_symbols=publication_symbols if session_date == reference_date else None,
                )
                raw[(endpoint, session_date.isoformat())] = frame
                batch_refs.append(batch_ref)
                if endpoint == "daily" and placeholder_mask(frame).any():
                    suspension_refs.append(self.suspensions.reference(session_date, frame))

        publication_proof = None
        publication_snapshot_path = None
        if publication_symbols is not None:
            current = {endpoint: raw[(endpoint, reference_date.isoformat())]
                       for endpoint in ("daily", "adj_factor", "daily_basic")}
            verify_snapshot_liquidity(publication_snapshot, current["daily"], publication_symbols)
            publication_proof = verify_completed_daily(
                reference_date=reference_date, as_of=context.as_of,
                expected_symbols=publication_symbols, **current, source=self.source_name,
                observed_at=datetime.fromisoformat(self._observed_at(client)), snapshot_id=snapshot_id,
            )
            publication_snapshot_path = dataset_dir / "publication-snapshot.json"
            atomic_json(publication_snapshot_path, publication_snapshot, immutable=True)
        panel, quality = self._build_panel(
            raw=raw,
            symbols=normalized_symbols,
            session_dates=session_dates,
            context=context,
            dataset_id=dataset_id,
        )
        panel_path = dataset_dir / "panel.parquet"
        quality_path = dataset_dir / "quality.json"
        atomic_bytes(panel_path, _parquet_bytes(panel), immutable=True)
        atomic_json(quality_path, quality, immutable=True)

        manifest_payload: dict[str, Any] = {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "dataset_id": dataset_id,
            "manifest_path": self._path_token(manifest_path),
            "panel_path": self._path_token(panel_path),
            "panel_hash": file_hash(panel_path),
            "panel_schema": list(PANEL_COLUMNS),
            "quality_path": self._path_token(quality_path),
            "quality_hash": file_hash(quality_path),
            "quality_schema_version": QUALITY_SCHEMA_VERSION,
            "source": self.source_name,
            "snapshot_id": snapshot_id,
            "research_date": context.research_date.isoformat(),
            "phase": context.phase,
            "market_data_reference_date": reference_date.isoformat(),
            "as_of": context.as_of.isoformat(),
            "timezone": context.timezone,
            "calendar_start_date": calendar_start.isoformat(),
            "session_dates": [value.isoformat() for value in session_dates],
            "sessions": sessions,
            "symbols": normalized_symbols,
            "symbols_hash": content_hash(normalized_symbols),
            "context": context.to_dict(),
            "batch_refs": batch_refs,
            "suspension_refs": suspension_refs,
            "publication_proof": publication_proof,
            "publication_snapshot_path": self._path_token(publication_snapshot_path) if publication_snapshot_path else None,
            "publication_snapshot_hash": file_hash(publication_snapshot_path) if publication_snapshot_path else None,
            "status": "COMPLETE",
            "complete": True,
        }
        manifest_payload["manifest_hash"] = content_hash(manifest_payload)
        atomic_json(manifest_path, manifest_payload, immutable=True)
        self.load(manifest_path)
        return _read_json(manifest_path, "manifest")

    def load(self, manifest_path: Path) -> pd.DataFrame:
        path = self._resolve_path(manifest_path, "manifest")
        manifest = _read_json(path, "manifest")
        self._validate_manifest_header(manifest, path)
        cutoff = _parse_date(manifest["market_data_reference_date"], "manifest cutoff")
        calendar_start = _parse_date(manifest["calendar_start_date"], "calendar start")
        session_dates = [
            _parse_date(value, "manifest session date") for value in manifest["session_dates"]
        ]
        expected_dates = {value.isoformat() for value in session_dates}
        refs = manifest["batch_refs"]
        if not isinstance(refs, list):
            raise PipelineError("STALE_DATA", "manifest batch_refs must be a list")
        expected_keys = {f"{endpoint}:{value:%Y%m%d}" for value in session_dates for endpoint in ("daily", "daily_basic", "adj_factor")}
        expected_keys.add(f"trade_calendar:{manifest['calendar_start_date'].replace('-', '')}_{cutoff:%Y%m%d}")
        actual_keys: set[str] = set()
        suspension_refs = []
        suspension_events = []
        for reference in refs:
            if not isinstance(reference, dict):
                raise PipelineError("STALE_DATA", "manifest batch reference is invalid")
            batch_key = reference.get("batch_key")
            if not isinstance(batch_key, str) or batch_key in actual_keys:
                raise PipelineError("STALE_DATA", "manifest batch keys are not unique")
            actual_keys.add(batch_key)
            verified_frame = self._validate_batch_reference(
                reference,
                cutoff=cutoff,
                calendar_start=calendar_start,
                expected_dates=expected_dates,
            )
            if reference["endpoint"] == "daily" and placeholder_mask(verified_frame).any():
                day = _parse_date(reference["trade_date"], "suspension day")
                suspension_refs.append(self.suspensions.reference(day, verified_frame))
                suspension_events.extend({"symbol": row.ts_code, "trade_date": day.isoformat()}
                                         for row in verified_frame.loc[placeholder_mask(verified_frame)].itertuples()
                                         if row.ts_code in manifest["symbols"])
        if actual_keys != expected_keys:
            raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "manifest does not freeze every required source batch")
        if suspension_refs != manifest.get("suspension_refs", []):
            raise PipelineError("STALE_DATA", "manifest suspension proof references differ from frozen daily rows")
        if manifest["phase"] == "postclose":
            self._validate_publication_proof(manifest, cutoff)

        panel_path = self._resolve_path(manifest["panel_path"], "panel")
        if file_hash(panel_path) != manifest["panel_hash"]:
            raise PipelineError("STALE_DATA", "panel hash mismatch", path=str(panel_path))
        try:
            panel = pd.read_parquet(panel_path, engine="pyarrow")
        except Exception as exc:  # noqa: BLE001 - corrupt artifacts fail closed
            raise PipelineError("STALE_DATA", "cannot read panel artifact", path=str(panel_path)) from exc
        self._validate_panel(panel, manifest["symbols"], expected_dates)

        quality_path = self._resolve_path(manifest["quality_path"], "quality")
        if file_hash(quality_path) != manifest["quality_hash"]:
            raise PipelineError("STALE_DATA", "quality hash mismatch", path=str(quality_path))
        quality = _read_json(quality_path, "quality")
        if quality.get("confirmed_suspension_events", []) != suspension_events:
            raise PipelineError("STALE_DATA", "quality suspension events differ from frozen source records")
        self._validate_quality(quality, manifest, panel, session_dates)
        return panel

    def _validate_inputs(
        self,
        context: ResearchContext,
        symbols: list[str],
        snapshot_id: str,
        sessions: int,
    ) -> list[str]:
        if not isinstance(context, ResearchContext):
            raise PipelineError("INVALID_ARGUMENTS", "context must be a ResearchContext")
        if isinstance(sessions, bool) or not isinstance(sessions, int) or sessions <= 0:
            raise PipelineError("INVALID_ARGUMENTS", "sessions must be a positive integer")
        if not isinstance(snapshot_id, str) or not snapshot_id.strip():
            raise PipelineError("INVALID_ARGUMENTS", "snapshot_id must be a non-empty string")
        if not isinstance(symbols, list) or not symbols:
            raise PipelineError("INVALID_ARGUMENTS", "symbols must be a non-empty list")
        normalized: list[str] = []
        for value in symbols:
            try:
                normalized.append(CanonicalSymbol.from_core(value).symbol)
            except SymbolContractError as exc:
                raise PipelineError("IDENTITY_CONFLICT", "symbols must be canonical Core identities") from exc
        if len(set(normalized)) != len(normalized):
            raise PipelineError("IDENTITY_CONFLICT", "symbols must be unique")
        return sorted(normalized)

    def _validate_publication_proof(self, manifest: dict[str, Any], cutoff: date) -> None:
        proof = manifest.get("publication_proof")
        if not isinstance(proof, dict) or not isinstance(proof.get("expected_symbols"), list):
            raise PipelineError("STALE_DATA", "postclose manifest lacks publication proof")
        try:
            snapshot_path = self._resolve_path(manifest.get("publication_snapshot_path"), "publication snapshot")
            if file_hash(snapshot_path) != manifest.get("publication_snapshot_hash"):
                raise PipelineError("STALE_DATA", "publication snapshot file hash mismatch")
            snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
            context = ResearchContext(_parse_date(manifest["research_date"], "research date"), "postclose",
                                      cutoff, datetime.fromisoformat(manifest["as_of"]))
            expected = snapshot_publication_symbols(snapshot, context)
            if snapshot["snapshot_id"] != manifest["snapshot_id"] or expected != proof["expected_symbols"]:
                raise PipelineError("STALE_DATA", "publication proof differs from independent snapshot membership")
            current = {ref["endpoint"]: pd.read_parquet(self._resolve_path(ref["path"], "publication batch"))
                       for ref in manifest["batch_refs"]
                       if ref.get("trade_date") == cutoff.isoformat() and ref["endpoint"] != "trade_calendar"}
            verified = verify_completed_daily(
                reference_date=cutoff, as_of=datetime.fromisoformat(manifest["as_of"]),
                expected_symbols=proof["expected_symbols"], **current, source=self.source_name,
                observed_at=datetime.fromisoformat(proof["observed_at"]), snapshot_id=manifest["snapshot_id"],
            )
            verify_snapshot_liquidity(snapshot, current["daily"], expected)
        except PipelineError:
            raise
        except (TypeError, ValueError, KeyError) as exc:
            raise PipelineError("STALE_DATA", "postclose publication proof is invalid") from exc
        if verified != proof:
            raise PipelineError("STALE_DATA", "postclose publication proof differs from frozen batches")

    def _dataset_id(
        self,
        *,
        context: ResearchContext,
        symbols: list[str],
        snapshot_id: str,
        sessions: int,
    ) -> str:
        identity = {
            "source": self.source_name,
            "snapshot_id": snapshot_id,
            "research_date": context.research_date.isoformat(),
            "phase": context.phase,
            "market_data_reference_date": context.market_data_reference_date.isoformat(),
            "as_of": context.as_of.isoformat(),
            "timezone": context.timezone,
            "sessions": sessions,
            "symbols": symbols,
        }
        return f"canonical-{content_hash(identity)[:24]}"

    def _get_or_fetch_batch(
        self,
        client: Any,
        *,
        context: ResearchContext,
        endpoint: str,
        batch_key: str,
        request: dict[str, Any],
        requested_date: date | None,
        calendar_start: date | None,
        publication_symbols: list[str] | None = None,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        path, meta_path = self._batch_paths(endpoint, batch_key)
        if path.exists() or meta_path.exists():
            if not path.exists() or not meta_path.exists():
                raise PipelineError("STALE_DATA", "cached batch is incomplete", path=str(path))
            frame, reference = self._load_cached_batch(
                path,
                meta_path,
                endpoint=endpoint,
                batch_key=batch_key,
                request=request,
                requested_date=requested_date,
                calendar_start=calendar_start,
                cutoff=context.market_data_reference_date,
            )
            if publication_symbols is not None:
                verify_publication_frame(endpoint=endpoint, frame=frame, reference_date=requested_date,
                                         expected_symbols=publication_symbols)
            return frame, reference

        if endpoint == "trade_calendar":
            frame, requests, observed_at = self._fetch_calendar(
                client,
                request=request,
                calendar_start=calendar_start,
                cutoff=context.market_data_reference_date,
            )
        else:
            frame, requests, observed_at = self._fetch_paged(
                client,
                endpoint=endpoint,
                batch_key=batch_key,
                request=request,
                requested_date=requested_date,
                cutoff=context.market_data_reference_date,
                cache_pages=publication_symbols is None,
            )
        if publication_symbols is not None:
            verify_publication_frame(endpoint=endpoint, frame=frame, reference_date=requested_date,
                                     expected_symbols=publication_symbols)
        return self._persist_batch(
            frame,
            endpoint=endpoint,
            batch_key=batch_key,
            request=requests,
            requested_date=requested_date,
            calendar_start=calendar_start,
            cutoff=context.market_data_reference_date,
            observed_at=observed_at,
        )

    def _fetch_calendar(
        self,
        client: Any,
        *,
        request: dict[str, Any],
        calendar_start: date | None,
        cutoff: date,
    ) -> tuple[pd.DataFrame, dict[str, Any], str]:
        if calendar_start is None:
            raise PipelineError("INVALID_ARGUMENTS", "calendar start is required")
        observed_at = self._observed_at(client)
        frame = self._call_endpoint(client, "trade_cal", request)
        normalized = self._validate_raw_frame(
            frame,
            endpoint="trade_calendar",
            requested_date=None,
            calendar_start=calendar_start,
            cutoff=cutoff,
        )
        return normalized, request, observed_at

    def _fetch_paged(
        self,
        client: Any,
        *,
        endpoint: str,
        batch_key: str,
        request: dict[str, Any],
        requested_date: date | None,
        cutoff: date,
        cache_pages: bool = True,
    ) -> tuple[pd.DataFrame, list[dict[str, Any]], str]:
        pages: list[pd.DataFrame] = []
        requests: list[dict[str, Any]] = []
        seen_keys: set[tuple[str, str]] = set()
        offset = 0
        last_observed = self._observed_at(client)
        while len(requests) < 100:
            page_request = {**request, "limit": PAGE_SIZE, "offset": offset}
            cached = self._load_partial_page(endpoint, batch_key, page_request, offset, cutoff) if cache_pages else None
            if cached is None:
                frame = self._call_endpoint(client, endpoint, page_request)
                observed_at = self._observed_at(client)
            else:
                frame, observed_at = cached
            last_observed = observed_at
            requests.append(
                {
                    "parameters": page_request,
                    "observed_at": observed_at,
                    "rowcount": len(frame),
                }
            )
            if frame.empty:
                if not pages:
                    raise PipelineError(
                        "DATA_COVERAGE_INSUFFICIENT",
                        "whole-market batch is empty",
                        endpoint=endpoint,
                        trade_date=request.get("trade_date"),
                    )
                break
            if endpoint == "daily" and placeholder_mask(frame).any():
                self.suspensions.ensure(client, requested_date, frame)
            normalized = self._validate_raw_frame(
                frame,
                endpoint=endpoint,
                requested_date=requested_date,
                calendar_start=None,
                cutoff=cutoff,
            )
            page_keys = {
                (str(row["ts_code"]), _date_key(row["trade_date"]))
                for row in normalized.to_dict("records")
            }
            if len(page_keys) != len(normalized):
                raise PipelineError("STALE_DATA", "source page contains duplicate keys", endpoint=endpoint)
            overlap = seen_keys & page_keys
            if overlap:
                raise PipelineError(
                    "STALE_DATA",
                    "source pagination pages overlap on canonical data keys",
                    endpoint=endpoint,
                )
            seen_keys.update(page_keys)
            pages.append(normalized)
            if cached is None and cache_pages:
                self._persist_partial_page(
                    normalized, endpoint=endpoint, batch_key=batch_key, request=page_request,
                    offset=offset, cutoff=cutoff, observed_at=observed_at,
                )
            offset += len(normalized)
        else:
            raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "source pagination did not terminate", endpoint=endpoint)

        combined = pd.concat(pages, ignore_index=True)
        combined = combined.sort_values(list(_key_columns(endpoint)), kind="mergesort").reset_index(drop=True)
        return combined, requests, last_observed

    def _call_endpoint(self, client: Any, endpoint: str, parameters: dict[str, Any]) -> pd.DataFrame:
        provider = getattr(client, endpoint, None)
        if not callable(provider):
            raise PipelineError("SOURCE_UNAVAILABLE", "required source endpoint is unavailable", endpoint=endpoint)
        try:
            frame = provider(**parameters)
        except PipelineError:
            raise
        except Exception as exc:  # noqa: BLE001 - do not expose provider details
            raise PipelineError("SOURCE_UNAVAILABLE", "source request failed", endpoint=endpoint) from exc
        if not isinstance(frame, pd.DataFrame):
            raise PipelineError("SOURCE_UNAVAILABLE", "source endpoint did not return a DataFrame", endpoint=endpoint)
        return frame

    def _persist_batch(
        self,
        frame: pd.DataFrame,
        *,
        endpoint: str,
        batch_key: str,
        request: Any,
        requested_date: date | None,
        calendar_start: date | None,
        cutoff: date,
        observed_at: str,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        path, meta_path = self._batch_paths(endpoint, batch_key)
        atomic_bytes(path, _parquet_bytes(frame), immutable=True)
        payload: dict[str, Any] = {
            "schema_version": BATCH_SCHEMA_VERSION,
            "endpoint": endpoint,
            "batch_key": f"{endpoint}:{batch_key}",
            "path": self._path_token(path),
            "meta_path": self._path_token(meta_path),
            "source": self.source_name,
            "request": request,
            "as_of": cutoff.isoformat(),
            "observed_at": observed_at,
            "rowcount": len(frame),
            "completion": True,
            "schema": list(frame.columns),
            "hash": file_hash(path),
            "trade_date": requested_date.isoformat() if requested_date else None,
            "calendar_start_date": calendar_start.isoformat() if calendar_start else None,
        }
        payload["integrity_hash"] = content_hash(payload)
        atomic_json(meta_path, payload, immutable=True)
        return frame, self._batch_reference(payload, meta_path)

    def _persist_partial_page(
        self,
        frame: pd.DataFrame,
        *,
        endpoint: str,
        batch_key: str,
        request: dict[str, Any],
        offset: int,
        cutoff: date,
        observed_at: str,
    ) -> None:
        partial_dir = self.root / "batches" / self._source_key / endpoint / "partial"
        path = partial_dir / f"{batch_key}__offset-{offset}.parquet"
        meta_path = partial_dir / f"{batch_key}__offset-{offset}.meta.json"
        atomic_bytes(path, _parquet_bytes(frame), immutable=True)
        payload: dict[str, Any] = {
            "schema_version": BATCH_SCHEMA_VERSION,
            "endpoint": endpoint,
            "batch_key": f"{endpoint}:{batch_key}:partial:{offset}",
            "path": self._path_token(path),
            "meta_path": self._path_token(meta_path),
            "source": self.source_name,
            "request": [{"parameters": request, "rowcount": len(frame)}],
            "as_of": cutoff.isoformat(),
            "observed_at": observed_at,
            "rowcount": len(frame),
            "completion": False,
            "schema": list(frame.columns),
            "hash": file_hash(path),
            "trade_date": request.get("trade_date"),
        }
        payload["integrity_hash"] = content_hash(payload)
        atomic_json(meta_path, payload, immutable=True)

    def _load_partial_page(self, endpoint: str, batch_key: str, request: dict[str, Any],
                           offset: int, cutoff: date) -> tuple[pd.DataFrame, str] | None:
        directory = self.root / "batches" / self._source_key / endpoint / "partial"
        path = directory / f"{batch_key}__offset-{offset}.parquet"
        meta_path = path.with_suffix(".meta.json")
        if not meta_path.exists():
            return None
        meta = _read_json(meta_path, "partial page metadata")
        if (meta.get("source") != self.source_name or meta.get("endpoint") != endpoint
                or meta.get("completion") is not False
                or _parse_date(meta.get("as_of"), "partial cutoff") > cutoff
                or meta.get("request") != [{"parameters": request, "rowcount": meta.get("rowcount")}]
                or self._resolve_path(meta.get("path"), "partial page") != path.resolve()
                or self._resolve_path(meta.get("meta_path"), "partial metadata") != meta_path.resolve()):
            raise PipelineError("STALE_DATA", "partial page identity mismatch")
        if not path.is_file() or file_hash(path) != meta.get("hash"):
            raise PipelineError("STALE_DATA", "partial page hash mismatch")
        observed = datetime.fromisoformat(meta["observed_at"])
        if observed.utcoffset() is None:
            raise PipelineError("STALE_DATA", "partial page observation is not timezone-aware")
        frame = pd.read_parquet(path)
        if len(frame) != meta["rowcount"]:
            raise PipelineError("STALE_DATA", "partial page row count mismatch")
        return frame, observed.isoformat()

    def _load_cached_batch(
        self,
        path: Path,
        meta_path: Path,
        *,
        endpoint: str,
        batch_key: str,
        request: dict[str, Any],
        requested_date: date | None,
        calendar_start: date | None,
        cutoff: date,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        meta = _read_json(meta_path, "batch metadata")
        if meta.get("schema_version") != BATCH_SCHEMA_VERSION:
            raise PipelineError("STALE_DATA", "cached batch schema version is unsupported", path=str(meta_path))
        if meta.get("endpoint") != endpoint or meta.get("batch_key") != f"{endpoint}:{batch_key}":
            raise PipelineError("STALE_DATA", "cached batch identity mismatch", path=str(meta_path))
        if meta.get("source") != self.source_name or _parse_date(meta.get("as_of"), "batch cutoff") > cutoff:
            raise PipelineError("STALE_DATA", "cached batch provenance mismatch", path=str(meta_path))
        if self._resolve_path(meta.get("path"), "cached batch") != path.resolve() or self._resolve_path(meta.get("meta_path"), "cached batch metadata") != meta_path.resolve():
            raise PipelineError("STALE_DATA", "cached batch path metadata mismatch", path=str(meta_path))
        if meta.get("completion") is not True or not _request_matches(meta.get("request"), request, endpoint):
            raise PipelineError("STALE_DATA", "cached batch is incomplete or request-mismatched", path=str(meta_path))
        if file_hash(path) != meta.get("hash"):
            raise PipelineError("STALE_DATA", "cached batch hash mismatch", path=str(path))
        try:
            frame = pd.read_parquet(path, engine="pyarrow")
        except Exception as exc:  # noqa: BLE001 - corrupt artifacts fail closed
            raise PipelineError("STALE_DATA", "cannot read cached batch", path=str(path)) from exc
        normalized = self._validate_raw_frame(
            frame,
            endpoint=endpoint,
            requested_date=requested_date,
            calendar_start=calendar_start,
            cutoff=cutoff,
        )
        if len(normalized) != meta.get("rowcount") or list(normalized.columns) != meta.get("schema"):
            raise PipelineError("STALE_DATA", "cached batch schema or row count mismatch", path=str(path))
        return normalized, self._batch_reference(meta, meta_path)

    def _batch_paths(self, endpoint: str, batch_key: str) -> tuple[Path, Path]:
        directory = self.root / "batches" / self._source_key / endpoint
        return directory / f"{batch_key}.parquet", directory / f"{batch_key}.meta.json"

    def _batch_reference(self, meta: dict[str, Any], meta_path: Path) -> dict[str, Any]:
        reference = dict(meta)
        reference["meta_hash"] = file_hash(meta_path)
        reference.pop("integrity_hash", None)
        return reference

    def _observed_at(self, client: Any) -> str:
        clock = getattr(client, "observed_clock", None)
        value = clock() if callable(clock) else datetime.now(SHANGHAI)
        if not isinstance(value, datetime) or value.utcoffset() is None:
            raise PipelineError("NOT_READY", "source observation time must be timezone-aware")
        return value.isoformat()

    def _validate_observation_clock(self, client: Any, context: ResearchContext) -> None:
        clock = getattr(client, "observed_clock", None)
        if not callable(clock):
            return
        observed = clock()
        if not isinstance(observed, datetime) or observed.utcoffset() is None:
            raise PipelineError("NOT_READY", "source observation time must be timezone-aware")
        if context.as_of > observed:
            raise PipelineError("NOT_READY", "research cutoff exceeds source observation clock")

    def _validate_raw_frame(
        self,
        frame: pd.DataFrame,
        *,
        endpoint: str,
        requested_date: date | None,
        calendar_start: date | None,
        cutoff: date,
    ) -> pd.DataFrame:
        if endpoint not in RAW_COLUMNS:
            raise PipelineError("INVALID_ARGUMENTS", "unknown canonical dataset endpoint", endpoint=endpoint)
        expected = RAW_COLUMNS[endpoint]
        if not isinstance(frame, pd.DataFrame) or set(frame.columns) != set(expected):
            raise PipelineError("STALE_DATA", "source schema does not match the canonical endpoint schema", endpoint=endpoint)
        normalized = frame.loc[:, list(expected)].copy()
        if normalized.empty:
            raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "source batch is empty", endpoint=endpoint)

        if endpoint == "trade_calendar":
            if calendar_start is None:
                raise PipelineError("INVALID_ARGUMENTS", "calendar start is required")
            dates = [_parse_date(value, "calendar date") for value in normalized["cal_date"]]
            if any(value < calendar_start or value > cutoff for value in dates):
                raise PipelineError("STALE_DATA", "trade calendar returned a date outside the requested cutoff")
            if len(set(dates)) != len(dates):
                raise PipelineError("STALE_DATA", "trade calendar contains duplicate dates")
            for value in normalized["is_open"]:
                if _open_flag(value) is None:
                    raise PipelineError("STALE_DATA", "trade calendar contains an invalid is_open value")
            return normalized.sort_values("cal_date", kind="mergesort").reset_index(drop=True)

        if requested_date is None:
            raise PipelineError("INVALID_ARGUMENTS", "requested trade date is required")
        dates = [_parse_date(value, "source trade date") for value in normalized["trade_date"]]
        if any(value != requested_date or value > cutoff for value in dates):
            raise PipelineError("STALE_DATA", "source returned a wrong or future trade date", endpoint=endpoint)
        symbols = [_text(value) for value in normalized["ts_code"]]
        if any(value is None for value in symbols):
            raise PipelineError("STALE_DATA", "source returned an empty symbol", endpoint=endpoint)
        normalized["ts_code"] = [value for value in symbols if value is not None]
        if normalized.duplicated(["ts_code", "trade_date"]).any():
            raise PipelineError("STALE_DATA", "source returned duplicate symbol/date rows", endpoint=endpoint)

        if endpoint == "daily":
            placeholders = placeholder_mask(normalized)
            if placeholders.any():
                self.suspensions.reference(requested_date, normalized)
            _validate_numeric(normalized.loc[~placeholders], ("open", "high", "low", "close"), positive=True, allow_missing=False, endpoint=endpoint)
            _validate_numeric(normalized, ("vol", "amount"), nonnegative=True, allow_missing=False, endpoint=endpoint)
            prices = normalized.loc[~placeholders, ["open", "high", "low", "close"]].apply(pd.to_numeric)
            if ((prices.high < prices[["open", "low", "close"]].max(axis=1)).any()
                    or (prices.low > prices[["open", "high", "close"]].min(axis=1)).any()):
                raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "inconsistent raw OHLC", endpoint=endpoint)
        elif endpoint == "daily_basic":
            _validate_numeric(
                normalized,
                ("turnover_rate", "volume_ratio", "total_mv", "circ_mv"),
                nonnegative=True,
                allow_missing=True,
                endpoint=endpoint,
            )
        else:
            # Invalid factors remain explicit missing qfq inputs; they are never
            # converted into a usable factor or used as a raw-price fallback.
            _validate_numeric(
                normalized,
                ("adj_factor",),
                allow_missing=True,
                allow_nonfinite=True,
                endpoint=endpoint,
            )
        return normalized.sort_values(["ts_code", "trade_date"], kind="mergesort").reset_index(drop=True)

    def _build_panel(
        self,
        *,
        raw: dict[tuple[str, str], pd.DataFrame],
        symbols: list[str],
        session_dates: list[date],
        context: ResearchContext,
        dataset_id: str,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        symbol_set = set(symbols)
        daily_parts: list[pd.DataFrame] = []
        basic_parts: list[pd.DataFrame] = []
        factor_parts: list[pd.DataFrame] = []
        suspension_events = []
        for session_date in session_dates:
            key = session_date.isoformat()
            daily = raw[("daily", key)].copy()
            daily["symbol"] = daily["ts_code"].map(_canonical_symbol)
            daily = daily[daily["symbol"].isin(symbol_set)].copy()
            placeholders = placeholder_mask(daily)
            suspension_events.extend({"symbol": row.symbol, "trade_date": session_date.isoformat()}
                                     for row in daily.loc[placeholders].itertuples())
            # A confirmed non-trading placeholder is a coverage gap, not a bar.
            daily = daily.loc[~placeholders].copy()
            if not daily.empty:
                daily["trade_date"] = session_date.isoformat()
                daily_parts.append(
                    daily.rename(
                        columns={
                            "open": "open_raw",
                            "high": "high_raw",
                            "low": "low_raw",
                            "close": "close_raw",
                            "vol": "volume_raw",
                            "amount": "amount_raw",
                        }
                    )[["symbol", "trade_date", "open_raw", "high_raw", "low_raw", "close_raw", "volume_raw", "amount_raw"]]
                )

            basic = raw[("daily_basic", key)].copy()
            basic["symbol"] = basic["ts_code"].map(_canonical_symbol)
            basic = basic[basic["symbol"].isin(symbol_set)].copy()
            if not basic.empty:
                basic["trade_date"] = session_date.isoformat()
                basic_parts.append(basic[["symbol", "trade_date", "turnover_rate", "volume_ratio", "total_mv", "circ_mv"]])

            factor = raw[("adj_factor", key)].copy()
            factor["symbol"] = factor["ts_code"].map(_canonical_symbol)
            factor = factor[factor["symbol"].isin(symbol_set)].copy()
            if not factor.empty:
                factor["trade_date"] = session_date.isoformat()
                factor["adj_factor"] = factor["adj_factor"].map(_positive_finite)
                factor_parts.append(factor[["symbol", "trade_date", "adj_factor"]])

        daily_frame = pd.concat(daily_parts, ignore_index=True) if daily_parts else pd.DataFrame(columns=["symbol", "trade_date", "open_raw", "high_raw", "low_raw", "close_raw", "volume_raw", "amount_raw"])
        if daily_frame.empty:
            raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "no canonical daily bars were downloaded")
        if daily_frame.duplicated(["symbol", "trade_date"]).any():
            raise PipelineError("STALE_DATA", "canonical daily panel has duplicate keys")
        basic_frame = pd.concat(basic_parts, ignore_index=True) if basic_parts else pd.DataFrame(columns=["symbol", "trade_date", "turnover_rate", "volume_ratio", "total_mv", "circ_mv"])
        factor_frame = pd.concat(factor_parts, ignore_index=True) if factor_parts else pd.DataFrame(columns=["symbol", "trade_date", "adj_factor"])
        if basic_frame.duplicated(["symbol", "trade_date"]).any() or factor_frame.duplicated(["symbol", "trade_date"]).any():
            raise PipelineError("STALE_DATA", "canonical source panel has duplicate keys")

        panel = daily_frame.merge(basic_frame, on=["symbol", "trade_date"], how="left", validate="one_to_one")
        panel = panel.merge(factor_frame, on=["symbol", "trade_date"], how="left", validate="one_to_one")
        panel["volume_shares"] = pd.to_numeric(panel["volume_raw"], errors="coerce") * 100.0
        panel["amount_cny"] = pd.to_numeric(panel["amount_raw"], errors="coerce") * 1000.0
        for column in ("open_qfq", "high_qfq", "low_qfq", "close_qfq"):
            panel[column] = float("nan")

        reference_key = context.market_data_reference_date.isoformat()
        anchors = factor_frame.loc[factor_frame.trade_date.eq(reference_key)].set_index("symbol").adj_factor
        anchor_values = pd.to_numeric(panel.symbol.map(anchors), errors="coerce")
        factors = pd.to_numeric(panel.adj_factor, errors="coerce")
        factors = factors.where(factors.gt(0) & ~factors.isin([float("inf"), -float("inf")]))
        # An unavailable anchor invalidates qfq, not an observed historical factor.
        panel["adj_factor"] = factors
        ratio = factors / anchor_values
        raw_columns = {
            "open_qfq": "open_raw",
            "high_qfq": "high_raw",
            "low_qfq": "low_raw",
            "close_qfq": "close_raw",
        }
        for qfq_column, raw_column in raw_columns.items():
            adjusted = pd.to_numeric(panel[raw_column], errors="coerce") * ratio
            panel[qfq_column] = adjusted.where(adjusted.gt(0) & ~adjusted.isin([float("inf"), -float("inf")]))

        panel = panel.loc[:, list(PANEL_COLUMNS)].sort_values(["symbol", "trade_date"], kind="mergesort").reset_index(drop=True)
        quality = self._quality(
            panel=panel,
            factor_frame=factor_frame,
            symbols=symbols,
            session_dates=session_dates,
            context=context,
            dataset_id=dataset_id,
            suspension_events=suspension_events,
        )
        return panel, quality

    def _quality(
        self,
        *,
        panel: pd.DataFrame,
        factor_frame: pd.DataFrame,
        symbols: list[str],
        session_dates: list[date],
        context: ResearchContext,
        dataset_id: str,
        suspension_events: list[dict[str, str]],
    ) -> dict[str, Any]:
        expected = [value.isoformat() for value in session_dates]
        quality_symbols: dict[str, Any] = {}
        panel_groups = dict(tuple(panel.groupby("symbol", sort=False)))
        factor_groups = dict(tuple(factor_frame.groupby("symbol", sort=False)))
        confirmed_by_symbol: dict[str, list[str]] = {}
        for event in suspension_events:
            confirmed_by_symbol.setdefault(event["symbol"], []).append(event["trade_date"])
        for symbol in symbols:
            selected = panel_groups.get(symbol, panel.iloc[:0])
            symbol_factors = factor_groups.get(symbol, factor_frame.iloc[:0])
            present_dates = set(selected["trade_date"].astype(str))
            missing_dates = [value for value in expected if value not in present_dates]
            factor_dates = set(
                symbol_factors["trade_date"].astype(str)
            )
            valid_factor_dates = set(
                symbol_factors.loc[
                    symbol_factors["adj_factor"].map(_positive_finite).notna(),
                    "trade_date",
                ].astype(str)
            )
            missing_factor_dates = [value for value in expected if value not in valid_factor_dates]
            qfq_count = int(selected["close_qfq"].map(_positive_finite).notna().sum())
            bar_count = len(selected)
            coverage = bar_count / len(expected) if expected else 0.0
            qfq_availability = qfq_count / bar_count if bar_count else 0.0
            reasons: list[str] = []
            if missing_dates:
                reasons.append("missing_daily_bar")
            if missing_factor_dates:
                reasons.append("missing_or_invalid_adj_factor")
            confirmed_dates = confirmed_by_symbol.get(symbol, [])
            if confirmed_dates:
                reasons.append("confirmed_suspension")
            quality_symbols[symbol] = {
                "bar_count": bar_count,
                "expected_sessions": len(expected),
                "coverage": coverage,
                "history_coverage": coverage,
                "missing_ratio": 1.0 - coverage,
                "missing_dates": missing_dates,
                "missing_factor_dates": missing_factor_dates,
                "missing_factor_count": len(missing_factor_dates),
                "factor_row_dates": sorted(factor_dates),
                "qfq_available": qfq_count == bar_count and bar_count > 0,
                "qfq_availability": qfq_availability,
                "qfq_available_bars": qfq_count,
                "qfq_bars": qfq_count,
                "source_confidence": coverage * (0.5 + 0.5 * qfq_availability),
                "gap_reasons": reasons,
                "confirmed_suspension_dates": confirmed_dates,
                "possible_suspension_or_listing_gap": bool(missing_dates),
                "new_listing_gap_unresolved": bool(missing_dates),
            }
        minimum = len(expected)
        with_minimum = sum(item["bar_count"] >= minimum for item in quality_symbols.values())
        symbol_count = len(symbols)
        summary = {
            "symbol_count": symbol_count,
            "minimum_history_sessions": minimum,
            "symbols_with_minimum_history": with_minimum,
            "majority_ratio": with_minimum / symbol_count if symbol_count else 0.0,
            "majority_has_minimum_history": with_minimum > symbol_count / 2,
            "majority_ge_sessions": with_minimum > symbol_count / 2,
            "symbols_with_at_least_120_bars": sum(item["bar_count"] >= 120 for item in quality_symbols.values()),
            "majority_ge_120": sum(item["bar_count"] >= 120 for item in quality_symbols.values()) > symbol_count / 2,
        }
        full_qfq = sum(
            item["bar_count"] >= minimum and item["qfq_bars"] == item["bar_count"]
            for item in quality_symbols.values()
        )
        quality = {
            "schema_version": QUALITY_SCHEMA_VERSION,
            "dataset_id": dataset_id,
            "source": self.source_name,
            "research_date": context.research_date.isoformat(),
            "phase": context.phase,
            "market_data_reference_date": context.market_data_reference_date.isoformat(),
            "as_of": context.as_of.isoformat(),
            "timezone": context.timezone,
            "session_dates": expected,
            "symbols": quality_symbols,
            "per_symbol": [
                {"symbol": symbol, **quality_symbols[symbol]} for symbol in symbols
            ],
            "full_qfq_history_ratio": full_qfq / symbol_count if symbol_count else 0.0,
            "summary": summary,
            "confirmed_suspension_events": suspension_events,
        }
        quality["integrity_hash"] = content_hash(quality)
        return quality

    def _validate_manifest_header(self, manifest: dict[str, Any], path: Path) -> None:
        if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
            raise PipelineError("STALE_DATA", "unsupported manifest schema")
        stored_hash = manifest.get("manifest_hash")
        payload = dict(manifest)
        payload.pop("manifest_hash", None)
        if not isinstance(stored_hash, str) or content_hash(payload) != stored_hash:
            raise PipelineError("STALE_DATA", "manifest integrity hash mismatch", path=str(path))
        if self._resolve_path(manifest.get("manifest_path"), "manifest") != path.resolve():
            raise PipelineError("STALE_DATA", "manifest path metadata mismatch", path=str(path))
        if manifest.get("source") != self.source_name:
            raise PipelineError("IDENTITY_CONFLICT", "manifest source does not match dataset source")
        required = {
            "dataset_id", "snapshot_id", "research_date", "phase", "market_data_reference_date",
            "as_of", "timezone", "calendar_start_date", "session_dates", "sessions", "symbols",
            "symbols_hash", "panel_path", "panel_hash", "panel_schema", "quality_path", "quality_hash",
            "batch_refs", "complete", "status",
        }
        if not required <= set(manifest):
            raise PipelineError("STALE_DATA", "manifest is missing required fields")
        if manifest["status"] != "COMPLETE" or manifest["complete"] is not True:
            raise PipelineError("STALE_DATA", "manifest is not complete")
        if manifest["timezone"] != "Asia/Shanghai":
            raise PipelineError("STALE_DATA", "manifest timezone is invalid")
        try:
            research_date = _parse_date(manifest["research_date"], "research date")
            cutoff = _parse_date(manifest["market_data_reference_date"], "market cutoff")
            as_of = datetime.fromisoformat(manifest["as_of"])
        except (TypeError, ValueError) as exc:
            raise PipelineError("STALE_DATA", "manifest time fields are invalid") from exc
        if as_of.utcoffset() is None or as_of.astimezone(SHANGHAI).date() != research_date:
            raise PipelineError("STALE_DATA", "manifest as_of is not an aware research-date timestamp")
        phase = manifest["phase"]
        if phase not in {"preopen", "intraday", "postclose"}:
            raise PipelineError("STALE_DATA", "manifest phase is invalid")
        if phase == "postclose" and cutoff != research_date:
            raise PipelineError("STALE_DATA", "postclose manifest cutoff is invalid")
        if phase != "postclose" and cutoff >= research_date:
            raise PipelineError("STALE_DATA", "preopen/intraday manifest cutoff is invalid")
        symbols = manifest["symbols"]
        if not isinstance(symbols, list) or symbols != sorted(symbols) or len(set(symbols)) != len(symbols):
            raise PipelineError("STALE_DATA", "manifest symbols are not a sorted unique list")
        for symbol in symbols:
            try:
                CanonicalSymbol.from_core(symbol)
            except SymbolContractError as exc:
                raise PipelineError("IDENTITY_CONFLICT", "manifest contains a non-canonical symbol") from exc
        if content_hash(symbols) != manifest["symbols_hash"]:
            raise PipelineError("STALE_DATA", "manifest symbol hash mismatch")
        if manifest["panel_schema"] != list(PANEL_COLUMNS):
            raise PipelineError("STALE_DATA", "manifest panel schema mismatch")
        if not isinstance(manifest["sessions"], int) or manifest["sessions"] != len(manifest["session_dates"]):
            raise PipelineError("STALE_DATA", "manifest session count mismatch")
        expected_id = self._dataset_id_from_manifest(manifest)
        if manifest["dataset_id"] != expected_id:
            raise PipelineError("STALE_DATA", "manifest dataset id mismatch")

    def _dataset_id_from_manifest(self, manifest: dict[str, Any]) -> str:
        identity = {
            "source": manifest["source"],
            "snapshot_id": manifest["snapshot_id"],
            "research_date": manifest["research_date"],
            "phase": manifest["phase"],
            "market_data_reference_date": manifest["market_data_reference_date"],
            "as_of": manifest["as_of"],
            "timezone": manifest["timezone"],
            "sessions": manifest["sessions"],
            "symbols": manifest["symbols"],
        }
        return f"canonical-{content_hash(identity)[:24]}"

    def _validate_batch_reference(
        self,
        reference: dict[str, Any],
        *,
        cutoff: date,
        calendar_start: date,
        expected_dates: set[str],
    ) -> pd.DataFrame:
        for field in ("batch_key", "endpoint", "path", "meta_path", "hash", "meta_hash", "source", "schema", "request", "as_of", "rowcount", "completion"):
            if field not in reference:
                raise PipelineError("STALE_DATA", "manifest batch reference is incomplete")
        endpoint = reference["endpoint"]
        if endpoint not in RAW_COLUMNS or reference["source"] != self.source_name:
            raise PipelineError("STALE_DATA", "manifest batch reference identity is invalid")
        if _parse_date(reference["as_of"], "batch cutoff") > cutoff or reference["completion"] is not True:
            raise PipelineError("STALE_DATA", "manifest batch reference is not point-in-time complete")
        path = self._resolve_path(reference["path"], "batch")
        meta_path = self._resolve_path(reference["meta_path"], "batch metadata")
        if file_hash(path) != reference["hash"] or file_hash(meta_path) != reference["meta_hash"]:
            raise PipelineError("STALE_DATA", "source batch hash mismatch", path=str(path))
        meta = _read_json(meta_path, "batch metadata")
        if meta.get("hash") != reference["hash"] or meta.get("integrity_hash") is None:
            raise PipelineError("STALE_DATA", "source batch metadata hash mismatch", path=str(meta_path))
        if any(meta.get(key) != value for key, value in reference.items() if key != "meta_hash"):
            raise PipelineError("STALE_DATA", "source batch provenance differs from manifest")
        if self._resolve_path(meta.get("path"), "batch") != path.resolve() or self._resolve_path(meta.get("meta_path"), "batch metadata") != meta_path.resolve():
            raise PipelineError("STALE_DATA", "source batch path metadata mismatch", path=str(meta_path))
        if meta.get("batch_key") != reference["batch_key"] or meta.get("request") != reference["request"]:
            raise PipelineError("STALE_DATA", "source batch manifest metadata mismatch", path=str(meta_path))
        try:
            frame = pd.read_parquet(path, engine="pyarrow")
        except Exception as exc:  # noqa: BLE001 - corrupt artifacts fail closed
            raise PipelineError("STALE_DATA", "cannot read source batch artifact", path=str(path)) from exc
        trade_date = reference.get("trade_date")
        requested = _parse_date(trade_date, "batch trade date") if trade_date else None
        if endpoint != "trade_calendar" and (requested is None or not _request_matches(
            reference["request"], {"trade_date": requested.strftime("%Y%m%d"),
                                   "fields": ",".join(RAW_COLUMNS[endpoint])}, endpoint
        )):
            raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "source batch lacks complete pagination evidence")
        normalized = self._validate_raw_frame(
            frame,
            endpoint=endpoint,
            requested_date=requested,
            calendar_start=calendar_start if endpoint == "trade_calendar" else None,
            cutoff=cutoff,
        )
        if len(normalized) != reference["rowcount"] or list(normalized.columns) != reference["schema"]:
            raise PipelineError("STALE_DATA", "source batch schema or row count mismatch", path=str(path))
        if endpoint != "trade_calendar" and requested is not None and requested.isoformat() not in expected_dates:
            raise PipelineError("STALE_DATA", "source batch date is outside the frozen session set")
        return normalized

    def _validate_panel(self, panel: pd.DataFrame, symbols: list[str], expected_dates: set[str]) -> None:
        if not isinstance(panel, pd.DataFrame) or list(panel.columns) != list(PANEL_COLUMNS):
            raise PipelineError("STALE_DATA", "panel schema mismatch")
        if panel.empty or panel.duplicated(["symbol", "trade_date"]).any():
            raise PipelineError("STALE_DATA", "panel is empty or has duplicate keys")
        symbol_set = set(symbols)
        if not set(panel["symbol"].astype(str)) <= symbol_set:
            raise PipelineError("IDENTITY_CONFLICT", "panel contains an unexpected symbol")
        if not set(panel["trade_date"].astype(str)) <= expected_dates:
            raise PipelineError("STALE_DATA", "panel contains a date outside the frozen session set")
        _validate_numeric(panel, ("open_raw", "high_raw", "low_raw", "close_raw"), positive=True, allow_missing=False, endpoint="panel")
        _validate_numeric(panel, ("volume_raw", "amount_raw", "volume_shares", "amount_cny"), nonnegative=True, allow_missing=False, endpoint="panel")
        _validate_numeric(panel, ("adj_factor", "open_qfq", "high_qfq", "low_qfq", "close_qfq"), positive=True, allow_missing=True, endpoint="panel")
        _validate_numeric(panel, ("turnover_rate", "volume_ratio", "total_mv", "circ_mv"), nonnegative=True, allow_missing=True, endpoint="panel")
        for raw, normalized, multiplier in (("volume_raw", "volume_shares", 100.0), ("amount_raw", "amount_cny", 1000.0)):
            for left, right in zip(panel[raw], panel[normalized]):
                if not math.isclose(float(left) * multiplier, float(right), rel_tol=1e-12, abs_tol=1e-9):
                    raise PipelineError("STALE_DATA", "panel unit normalization mismatch", field=normalized)

    def _validate_quality(
        self,
        quality: dict[str, Any],
        manifest: dict[str, Any],
        panel: pd.DataFrame,
        session_dates: list[date],
    ) -> None:
        if not isinstance(quality, dict) or quality.get("schema_version") != QUALITY_SCHEMA_VERSION:
            raise PipelineError("STALE_DATA", "quality schema mismatch")
        if quality.get("dataset_id") != manifest["dataset_id"] or quality.get("source") != self.source_name:
            raise PipelineError("STALE_DATA", "quality identity mismatch")
        symbols = manifest["symbols"]
        entries = quality.get("symbols")
        if not isinstance(entries, dict) or set(entries) != set(symbols):
            raise PipelineError("STALE_DATA", "quality symbol coverage mismatch")
        expected = [value.isoformat() for value in session_dates]
        groups = dict(tuple(panel.groupby("symbol", sort=False)))
        for symbol in symbols:
            item = entries[symbol]
            for field in ("bar_count", "coverage", "missing_ratio", "qfq_availability", "source_confidence", "missing_dates", "missing_factor_dates"):
                if field not in item:
                    raise PipelineError("STALE_DATA", "quality entry is incomplete", symbol=symbol)
            selected = groups.get(symbol, panel.iloc[:0])
            if item["bar_count"] != len(selected):
                raise PipelineError("STALE_DATA", "quality bar count mismatch", symbol=symbol)
            if not set(item["missing_dates"]) <= set(expected) or not set(item["missing_factor_dates"]) <= set(expected):
                raise PipelineError("STALE_DATA", "quality contains a future or unknown gap date", symbol=symbol)
            for field in ("coverage", "missing_ratio", "qfq_availability", "source_confidence"):
                value = _finite(item[field])
                if value is None or not 0 <= value <= 1:
                    raise PipelineError("STALE_DATA", "quality contains a non-finite metric", symbol=symbol)
        summary = quality.get("summary")
        if not isinstance(summary, dict) or summary.get("symbol_count") != len(symbols):
            raise PipelineError("STALE_DATA", "quality summary is invalid")

    def _resolve_path(self, value: Any, label: str) -> Path:
        if not isinstance(value, (str, Path)):
            raise PipelineError("STALE_DATA", f"{label} path is invalid")
        candidate = Path(value)
        path = (candidate if candidate.is_absolute() else self.root / candidate).resolve()
        if path == self.root or not path.is_relative_to(self.root):
            raise PipelineError("INVALID_ARGUMENTS", f"{label} path escapes dataset root", path=str(path))
        return path

    def _path_token(self, path: Path) -> str:
        resolved = path.resolve()
        if not resolved.is_relative_to(self.root) or resolved == self.root:
            raise PipelineError("INVALID_ARGUMENTS", "artifact path escapes dataset root", path=str(resolved))
        return resolved.relative_to(self.root).as_posix()


def _safe_component(value: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._") or "source"
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:10]
    return f"{clean[:64]}-{digest}"


def _key_columns(endpoint: str) -> tuple[str, ...]:
    return ("cal_date",) if endpoint == "trade_calendar" else ("ts_code", "trade_date")


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 - malformed artifacts fail closed
        raise PipelineError("STALE_DATA", f"cannot read {label}", path=str(path)) from exc
    if not isinstance(value, dict):
        raise PipelineError("STALE_DATA", f"{label} must be a JSON object", path=str(path))
    if label == "manifest":
        integrity_field = "manifest_hash"
    else:
        integrity_field = "integrity_hash"
    stored = value.get(integrity_field)
    payload = dict(value)
    payload.pop("manifest_hash", None)
    payload.pop("integrity_hash", None)
    if not isinstance(stored, str) or content_hash(payload) != stored:
        message = "manifest content hash mismatch" if label == "manifest" else f"{label} integrity hash mismatch"
        raise PipelineError("STALE_DATA", message, path=str(path))
    return value


def _file_hash_for_meta(path: Path) -> str:
    # Kept separate to make the cache validation call explicit at its boundary.
    return file_hash(path)


def _parquet_bytes(frame: pd.DataFrame) -> bytes:
    try:
        buffer = BytesIO()
        frame.to_parquet(buffer, engine="pyarrow", index=False, compression=None)
        return buffer.getvalue()
    except ImportError as exc:
        raise PipelineError("NOT_READY", "pyarrow is required for canonical Parquet artifacts") from exc
    except Exception as exc:  # noqa: BLE001 - serialization failure is not a valid dataset
        raise PipelineError("SOURCE_UNAVAILABLE", "cannot serialize canonical Parquet artifact") from exc


def _completed_sessions(calendar: pd.DataFrame, cutoff: date, sessions: int) -> list[date]:
    open_dates: list[date] = []
    for row in calendar.to_dict("records"):
        if _open_flag(row.get("is_open")) == 1:
            open_dates.append(_parse_date(row.get("cal_date"), "calendar date"))
    open_dates = sorted(set(value for value in open_dates if value <= cutoff))
    if len(open_dates) < sessions or not open_dates or open_dates[-1] != cutoff:
        raise PipelineError(
            "DATA_COVERAGE_INSUFFICIENT",
            "trade calendar does not provide the requested completed sessions",
            requested=sessions,
            available=len(open_dates),
        )
    return open_dates[-sessions:]


def _parse_date(value: Any, label: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        raise PipelineError("STALE_DATA", f"{label} is missing")
    if isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            raise PipelineError("STALE_DATA", f"{label} is invalid")
        value = int(value)
    text = str(value).strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise PipelineError("STALE_DATA", f"{label} is invalid")
    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError as exc:
        raise PipelineError("STALE_DATA", f"{label} is invalid") from exc


def _date_key(value: Any) -> str:
    return _parse_date(value, "trade date").strftime("%Y%m%d")


def _text(value: Any) -> str | None:
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip().upper()
    return text if text and text not in {"NAN", "NAT", "NONE", "<NA>"} else None


def _canonical_symbol(value: Any) -> str | None:
    text = _text(value)
    if text is None:
        return None
    try:
        return CanonicalSymbol.from_core(text).symbol
    except SymbolContractError:
        return None


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _positive_finite(value: Any) -> float | None:
    number = _finite(value)
    return number if number is not None and number > 0 else None


def _is_missing(value: Any) -> bool:
    try:
        result = pd.isna(value)
        return bool(result) if not hasattr(result, "__len__") else False
    except (TypeError, ValueError):
        return False


def _open_flag(value: Any) -> int | None:
    if _is_missing(value):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number not in {0.0, 1.0}:
        return None
    return int(number)


def _validate_numeric(
    frame: pd.DataFrame,
    columns: tuple[str, ...],
    *,
    positive: bool = False,
    nonnegative: bool = False,
    allow_missing: bool,
    allow_nonfinite: bool = False,
    endpoint: str,
) -> None:
    for column in columns:
        for value in frame[column].tolist():
            if _is_missing(value):
                if allow_missing:
                    continue
                raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "required numeric value is missing", endpoint=endpoint, field=column)
            try:
                number = float(value)
            except (TypeError, ValueError):
                raise PipelineError("STALE_DATA", "numeric value is invalid", endpoint=endpoint, field=column)
            if not math.isfinite(number):
                if allow_nonfinite:
                    continue
                raise PipelineError("STALE_DATA", "numeric value is invalid", endpoint=endpoint, field=column)
            if positive and number <= 0:
                raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "price must be positive", endpoint=endpoint, field=column)
            if nonnegative and number < 0:
                raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "volume/amount must be non-negative", endpoint=endpoint, field=column)


def _request_matches(actual: Any, base: dict[str, Any], endpoint: str) -> bool:
    if endpoint == "trade_calendar":
        parameters = actual.get("parameters", actual) if isinstance(actual, dict) else None
        return isinstance(parameters, dict) and all(parameters.get(key) == value for key, value in base.items())
    if not isinstance(actual, list) or not actual:
        return False
    expected_offset = 0
    for item in actual:
        if not isinstance(item, dict) or not isinstance(item.get("parameters"), dict):
            return False
        params = item["parameters"]
        if any(params.get(key) != value for key, value in base.items()):
            return False
        if params.get("limit") != PAGE_SIZE or not isinstance(params.get("offset"), int):
            return False
        rowcount = item.get("rowcount")
        if isinstance(rowcount, bool) or not isinstance(rowcount, int) or rowcount < 0:
            return False
        if params["offset"] != expected_offset:
            return False
        expected_offset += rowcount
    return expected_offset > 0 and actual[-1]["rowcount"] == 0


__all__ = ["CanonicalDataset", "PANEL_COLUMNS", "PAGE_SIZE"]
