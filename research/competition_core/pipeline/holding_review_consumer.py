"""Validate supplemental holding reviews without granting portfolio authority."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import re

from competition_core.pipeline.account import AccountSnapshot
from competition_core.pipeline.artifacts import confined_path, content_hash, read_json
from competition_core.pipeline.errors import PipelineError


@dataclass(frozen=True, slots=True)
class HoldingDisposition:
    symbol: str
    verdict: str | None
    disposition: str
    attempt_id: str | None
    blocker: str | None

    def to_dict(self) -> dict[str, Any]:
        return {"symbol": self.symbol, "verdict": self.verdict, "disposition": self.disposition,
                "attempt_id": self.attempt_id, "blocker": self.blocker}


def disposition_for(*, symbol: str, verdict: str | None, attempt_id: str | None,
                    status: str) -> HoldingDisposition:
    """Map a validated research verdict to a non-order holding disposition."""
    if status != "SUCCESS":
        return HoldingDisposition(symbol, verdict, "BLOCKED", attempt_id,
                                  f"MISSING_HOLDING_RESEARCH_COVERAGE:{symbol}")
    if verdict == "KEEP":
        return HoldingDisposition(symbol, verdict, "RETAIN", attempt_id, None)
    if verdict == "WATCH":
        return HoldingDisposition(symbol, verdict, "RETAIN_WATCH", attempt_id, None)
    if verdict == "EXCLUDE":
        return HoldingDisposition(symbol, verdict, "EXIT_REVIEW_REQUIRED", attempt_id,
                                  f"HOLDING_EXIT_REQUIRES_HUMAN_REVIEW:{symbol}")
    return HoldingDisposition(symbol, verdict, "BLOCKED", attempt_id,
                              f"HOLDING_REVIEW_UNRESOLVED:{symbol}")


def validate_holding_review_index(*, repository, account: AccountSnapshot,
                                  account_source_hash: str) -> dict[str, Any]:
    """Read and bind the current index to the exact account and S1/S4/S5 artifacts."""
    if not isinstance(account, AccountSnapshot) or not isinstance(account_source_hash, str):
        raise PipelineError("INVALID_ARGUMENTS", "account and source hash are required")
    if not re.fullmatch(r"[0-9a-f]{64}", account_source_hash):
        raise PipelineError("INVALID_ARGUMENTS", "account source hash is invalid")
    stages = {stage: repository.load_stage(stage) for stage in ("S1", "S4", "S5")}
    if any(value is None for value in stages.values()):
        raise PipelineError("NOT_READY", "holding review validation requires S1, S4 and S5")
    outside = [position for position in account.positions
               if position.symbol not in set(stages["S4"]["output_symbols"])]
    pointer_path = repository.directory / "holding-review-current.json"
    if not pointer_path.is_file():
        dispositions = [disposition_for(symbol=p.symbol, verdict=None, attempt_id=None, status="MISSING")
                        for p in outside]
        return _summary(account, account_source_hash, dispositions, None)
    pointer = read_json(pointer_path)
    if set(pointer) != {"index_hash"} or not re.fullmatch(r"[0-9a-f]{64}", str(pointer["index_hash"])):
        raise PipelineError("STALE_DATA", "holding review pointer is invalid")
    index_path = confined_path(repository.directory,
        f"holding-review-indexes/{pointer['index_hash']}.json")
    index = read_json(index_path, expected_hash=pointer["index_hash"])
    if (index.get("run_id") != repository.run_id or index.get("account_hash") != account.snapshot_hash
            or index.get("account_source_hash") != account_source_hash
            or index.get("portfolio_authority") is not False
            or index.get("input_symbols") != [position.symbol for position in outside]):
        raise PipelineError("STALE_DATA", "holding review index differs from current account/run")
    records = index.get("records")
    if not isinstance(records, list) or len({row.get("symbol") for row in records if isinstance(row, dict)}) != len(records):
        raise PipelineError("STALE_DATA", "holding review index records are invalid")
    by_symbol = {row["symbol"]: row for row in records}
    dispositions = []
    for position in outside:
        record = by_symbol.get(position.symbol)
        if record is None:
            dispositions.append(disposition_for(symbol=position.symbol, verdict=None,
                                                attempt_id=None, status="MISSING"))
            continue
        attempt_id = record.get("attempt_id")
        if not isinstance(attempt_id, str) or not re.fullmatch(r"holding-[0-9a-f]{32}", attempt_id):
            raise PipelineError("STALE_DATA", "holding review attempt identity is invalid")
        root = confined_path(repository.directory, f"holding-review-attempts/{attempt_id}")
        inputs = read_json(root / "inputs.json")
        packet = read_json(root / "packet.json")
        packet_hash = packet.pop("packet_hash", None)
        checkpoint = read_json(root / "reviews" / position.symbol / "checkpoint.json")
        checkpoint_integrity = checkpoint.pop("integrity_hash", None)
        sealed = read_json(root / "result.json")
        integrity = sealed.pop("integrity_hash", None)
        if (integrity != content_hash(sealed) or checkpoint_integrity != content_hash(checkpoint)
                or packet_hash != content_hash(packet)):
            raise PipelineError("STALE_DATA", "holding review artifact hash mismatch")
        expected_holding_hash = content_hash({"account_hash": account.snapshot_hash,
            "account_source_hash": account_source_hash, "position": position.to_dict()})
        if (inputs.get("run_id") != repository.run_id or inputs.get("symbol") != position.symbol
                or inputs.get("holding_source_hash") != expected_holding_hash
                or inputs.get("stage_hashes") != {stage: value["artifact_hash"] for stage, value in stages.items()}
                or sealed.get("portfolio_authority") is not False or sealed.get("research_role") != "existing_holding"
                or sealed.get("attempt_id") != attempt_id or sealed.get("symbol") != position.symbol
                or sealed.get("input_hash") != content_hash(inputs)
                or inputs.get("packet_hash") != packet_hash or sealed.get("packet_hash") != packet_hash
                or packet.get("research_role") != "existing_holding"
                or checkpoint.get("input_hash") != sealed.get("input_hash")
                or checkpoint.get("packet_hash") != packet_hash or checkpoint.get("symbol") != position.symbol
                or checkpoint.get("status") != sealed.get("status") or checkpoint.get("review") != sealed.get("review")
                or any(record.get(key) != sealed.get(key) for key in ("symbol", "status", "input_hash", "packet_hash"))):
            raise PipelineError("STALE_DATA", "holding review attempt binding is invalid")
        review = sealed.get("review")
        verdict = review.get("verdict") if isinstance(review, dict) else None
        dispositions.append(disposition_for(symbol=position.symbol, verdict=verdict,
                                            attempt_id=attempt_id, status=sealed.get("status")))
    expected_blockers = sorted(item.blocker for item in dispositions if item.blocker)
    if index.get("blockers") != [reason for reason in expected_blockers
                                 if reason.startswith("MISSING_HOLDING_RESEARCH_COVERAGE:")]:
        raise PipelineError("STALE_DATA", "holding review index blockers differ from validated attempts")
    return _summary(account, account_source_hash, dispositions, pointer["index_hash"])


def _summary(account, account_source_hash, dispositions, index_hash):
    return {"schema_version": "p10.holding_review_consumption.v1", "account_hash": account.snapshot_hash,
            "account_source_hash": account_source_hash, "index_hash": index_hash,
            "dispositions": [item.to_dict() for item in dispositions],
            "blocking_reasons": sorted(item.blocker for item in dispositions if item.blocker),
            "portfolio_authority": False}


__all__ = ["HoldingDisposition", "disposition_for", "validate_holding_review_index"]
