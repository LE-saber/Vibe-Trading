"""Independently proven no-trade placeholders, never usable price bars."""

from datetime import date, datetime
import json
from pathlib import Path

import pandas as pd

from competition_core.pipeline.artifacts import atomic_json, content_hash, file_hash
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.sources import _date


def placeholder_mask(frame: pd.DataFrame) -> pd.Series:
    required = {"open", "high", "low", "close", "vol", "amount"}
    if not required <= set(frame):
        return pd.Series(False, index=frame.index)
    values = frame[sorted(required)].apply(pd.to_numeric, errors="coerce")
    return (values[["open", "high", "low", "vol", "amount"]].eq(0).all(axis=1)
            & values.close.gt(0) & ~values.close.isin([float("inf"), -float("inf")]))


class SuspensionProofStore:
    def __init__(self, root: Path, source_key: str, source: str):
        self.root = root
        self.directory = root / "batches" / source_key / "suspensions"
        self.source = source

    def _path(self, day: date) -> Path:
        return self.directory / f"{day:%Y%m%d}.json"

    def _validate(self, payload: dict, day: date, required: list[str]) -> None:
        if (not isinstance(payload, dict) or payload.get("source") != self.source or payload.get("trade_date") != day.isoformat()
                or payload.get("endpoint") != "suspend_d"
                or payload.get("integrity_hash") != content_hash({k: v for k, v in payload.items() if k != "integrity_hash"})):
            raise PipelineError("STALE_DATA", "suspension proof identity/hash mismatch")
        try:
            if datetime.fromisoformat(payload["observed_at"]).utcoffset() is None:
                raise ValueError
            rows = pd.DataFrame(payload["rows"])
            fields = {"ts_code", "trade_date", "suspend_type", "suspend_timing"}
            if (not fields <= set(rows) or not rows.trade_date.map(_date).eq(day).all()
                    or not rows.suspend_type.eq("S").all() or rows.ts_code.isna().any()):
                raise ValueError
            full_day = rows.loc[rows.suspend_timing.isna() | rows.suspend_timing.eq("")]
            absent = sorted(set(required) - set(full_day.ts_code))
        except (KeyError, ValueError, TypeError) as exc:
            raise PipelineError("STALE_DATA", "invalid dated suspension proof") from exc
        if absent:
            raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "zero-price row lacks full-day suspension proof",
                                trade_date=day.isoformat(), symbols=absent)

    def ensure(self, client, day: date, frame: pd.DataFrame) -> None:
        required = frame.loc[placeholder_mask(frame), "ts_code"].tolist()
        if not required:
            return
        if self._path(day).exists():
            self.reference(day, frame)
            return
        try:
            response = client.historical_suspensions(day.strftime("%Y%m%d"))
        except PipelineError:
            raise
        except Exception as exc:
            raise PipelineError("SOURCE_UNAVAILABLE", "historical suspension proof unavailable") from exc
        # Preserve the source rows and their nulls, without filling any price.
        rows = response.astype(object).where(pd.notna(response), None).to_dict("records")
        payload = {"source": self.source, "endpoint": "suspend_d", "trade_date": day.isoformat(),
                   "request": {"trade_date": day.strftime("%Y%m%d"), "suspend_type": "S",
                               "fields": "ts_code,trade_date,suspend_type,suspend_timing"},
                   "observed_at": client.observed_clock().isoformat(), "rows": rows}
        payload["integrity_hash"] = content_hash(payload)
        self._validate(payload, day, required)
        atomic_json(self._path(day), payload, immutable=True)

    def reference(self, day: date, frame: pd.DataFrame) -> dict:
        path = self._path(day)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise PipelineError("STALE_DATA", "suspension proof missing or corrupt") from exc
        self._validate(payload, day, frame.loc[placeholder_mask(frame), "ts_code"].tolist())
        return {"trade_date": day.isoformat(), "path": path.relative_to(self.root).as_posix(),
                "hash": file_hash(path), "source": self.source, "endpoint": "suspend_d"}
