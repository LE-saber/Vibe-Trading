"""Canonical Parquet to deterministic Qlib binary-store conversion."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
from pathlib import Path

import numpy as np
import pandas as pd

from qlib_research.models import ConversionManifest
from qlib_research.symbols import canonical_to_qlib

REQUIRED_COLUMNS = {
    "date",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "open_qfq",
    "high_qfq",
    "low_qfq",
    "close_qfq",
    "volume",
    "adj_factor",
}
OUTPUT_FIELDS = (
    "open",
    "high",
    "low",
    "close",
    "vwap",
    "volume",
    "factor",
    "raw_open",
    "raw_high",
    "raw_low",
    "raw_close",
    "adj_factor",
    "tradable",
)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def convert_canonical_parquet(source: str | Path, target: str | Path, cutoff) -> ConversionManifest:
    source_path = Path(source).resolve()
    target_path = Path(target).resolve()
    if not source_path.is_file():
        raise FileNotFoundError(source_path)
    if target_path.exists() and any(target_path.iterdir()):
        raise FileExistsError(f"Qlib store must be absent or empty: {target_path}")

    frame = pd.read_parquet(source_path)
    missing = sorted(REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"canonical parquet missing columns: {', '.join(missing)}")
    rows_read = len(frame)
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="raise").dt.normalize()
    cutoff_ts = pd.Timestamp(cutoff)
    frame = frame.loc[frame["date"] <= cutoff_ts].copy()
    if frame.empty:
        raise ValueError("canonical parquet has no rows on or before cutoff")
    if frame.duplicated(["date", "symbol"]).any():
        raise ValueError("canonical parquet contains duplicate date/symbol rows")

    numeric = sorted(REQUIRED_COLUMNS - {"date", "symbol"})
    for column in numeric:
        frame[column] = pd.to_numeric(frame[column], errors="raise")
        if not np.isfinite(frame[column].to_numpy(dtype=float)).all():
            raise ValueError(f"canonical parquet contains non-finite {column}")
    if (frame[["open", "high", "low", "close", "open_qfq", "high_qfq", "low_qfq", "close_qfq"]] <= 0).any().any():
        raise ValueError("prices must be positive")
    if (frame["volume"] < 0).any() or (frame["adj_factor"] <= 0).any():
        raise ValueError("volume must be non-negative and adj_factor positive")

    for symbol in frame["symbol"].unique():
        canonical_to_qlib(str(symbol))

    target_path.mkdir(parents=True, exist_ok=True)
    calendars_dir = target_path / "calendars"
    instruments_dir = target_path / "instruments"
    features_dir = target_path / "features"
    calendars_dir.mkdir()
    instruments_dir.mkdir()
    features_dir.mkdir()

    calendar = sorted(pd.DatetimeIndex(frame["date"].unique()))
    (calendars_dir / "day.txt").write_text(
        "".join(f"{value:%Y-%m-%d}\n" for value in calendar), encoding="utf-8"
    )
    instrument_lines: list[str] = []
    for symbol, group in frame.sort_values(["symbol", "date"]).groupby("symbol", sort=True):
        qlib_symbol = canonical_to_qlib(str(symbol))
        group = group.set_index("date").sort_index()
        start = int(calendar.index(group.index.min()))
        symbol_calendar = [
            value for value in calendar if group.index.min() <= value <= group.index.max()
        ]
        aligned = group.reindex(symbol_calendar)
        adjusted = pd.DataFrame(index=aligned.index)
        adjusted["open"] = aligned["open_qfq"]
        adjusted["high"] = aligned["high_qfq"]
        adjusted["low"] = aligned["low_qfq"]
        adjusted["close"] = aligned["close_qfq"]
        adjusted["volume"] = aligned["volume"]
        adjusted["raw_open"] = aligned["open"]
        adjusted["raw_high"] = aligned["high"]
        adjusted["raw_low"] = aligned["low"]
        adjusted["raw_close"] = aligned["close"]
        adjusted["adj_factor"] = aligned["adj_factor"]
        adjusted["factor"] = aligned["close_qfq"] / aligned["close"]
        adjusted["tradable"] = aligned.get("tradable", pd.Series(1.0, index=aligned.index)).astype(float)
        if "amount" in aligned and (aligned["volume"] > 0).any():
            raw_vwap = aligned["amount"] / aligned["volume"].replace(0, np.nan)
            adjusted["vwap"] = raw_vwap * adjusted["factor"]
        else:
            adjusted["vwap"] = adjusted["close"]

        symbol_dir = features_dir / qlib_symbol.lower()
        symbol_dir.mkdir()
        for field in OUTPUT_FIELDS:
            values = adjusted[field].to_numpy(dtype="<f4")
            np.hstack((np.array([start], dtype="<f4"), values)).tofile(
                symbol_dir / f"{field}.day.bin"
            )
        instrument_lines.append(
            f"{qlib_symbol}\t{group.index.min():%Y-%m-%d}\t{group.index.max():%Y-%m-%d}\n"
        )
    (instruments_dir / "all.txt").write_text("".join(instrument_lines), encoding="utf-8")

    manifest = ConversionManifest(
        source_sha256=file_sha256(source_path),
        cutoff=cutoff_ts.date(),
        rows_read=rows_read,
        rows_written=len(frame),
        symbols=frame["symbol"].nunique(),
        calendar_days=len(calendar),
        qlib_version=importlib.metadata.version("pyqlib"),
    )
    (target_path / "conversion-manifest.json").write_text(
        json.dumps(manifest.model_dump(mode="json"), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return manifest
