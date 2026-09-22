"""Pure S4/S5/S5.5 candidate observation adapter.

This module is deliberately a boundary object. It accepts stage payloads
which the caller has already loaded and verified, maps only facts explicitly
present in those payloads, and returns provider-neutral
``CandidateObservation`` values. It never reads files, calls a provider, or
creates a snapshot, order, quantity, or execution decision.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
import re
from typing import Any
from zoneinfo import ZoneInfo

from contracts.symbol import CanonicalSymbol, SymbolContractError
from competition_core.pipeline.artifacts import content_hash

from .candidate_funnel import CandidateObservation


SHANGHAI = ZoneInfo("Asia/Shanghai")
_SHA256 = re.compile(r"^[0-9a-fA-F]{64}$")
_DEFAULT_MAX_QUOTE_AGE_SECONDS = Decimal("60")


class PipelineAdapterError(ValueError):
    """Raised only by the strict convenience wrapper."""


class PipelineAdapterBlocker(StrEnum):
    """Fail-closed reasons emitted by the candidate boundary."""

    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    OBSERVED_AT_INVALID = "OBSERVED_AT_INVALID"
    STAGE_SCHEMA_INVALID = "STAGE_SCHEMA_INVALID"
    STAGE_HASH_INVALID = "STAGE_HASH_INVALID"
    STAGE_HASH_MISMATCH = "STAGE_HASH_MISMATCH"
    STAGE_STATUS_NOT_SUCCESS = "STAGE_STATUS_NOT_SUCCESS"
    STAGE_IDENTITY_INVALID = "STAGE_IDENTITY_INVALID"
    STAGE_RELATION_INVALID = "STAGE_RELATION_INVALID"
    STAGE_CONTEXT_INVALID = "STAGE_CONTEXT_INVALID"
    STAGE_FUTURE = "STAGE_FUTURE"
    ROW_SCHEMA_INVALID = "ROW_SCHEMA_INVALID"
    ROW_SYMBOL_MISMATCH = "ROW_SYMBOL_MISMATCH"
    DUPLICATE_SYMBOL = "DUPLICATE_SYMBOL"
    SYMBOL_INVALID = "SYMBOL_INVALID"
    ACCOUNT_FACTS_INVALID = "ACCOUNT_FACTS_INVALID"
    SAME_DAY_INPUT_MISSING = "SAME_DAY_INPUT_MISSING"
    SAME_DAY_INPUT_INVALID = "SAME_DAY_INPUT_INVALID"
    QUOTE_MISSING = "QUOTE_MISSING"
    QUOTE_STALE = "QUOTE_STALE"
    QUOTE_FUTURE = "QUOTE_FUTURE"
    QUOTE_DATE_INVALID = "QUOTE_DATE_INVALID"
    FIELD_INVALID = "FIELD_INVALID"
    CANDIDATE_MAPPING_BLOCKED = "CANDIDATE_MAPPING_BLOCKED"


@dataclass(frozen=True, slots=True)
class PipelineAdapterDiagnostics:
    """Auditable result metadata; all collections are deterministic."""

    blockers: tuple[str, ...] = ()
    missing_fields: tuple[str, ...] = ()
    sources: tuple[tuple[str, str], ...] = ()
    warnings: tuple[str, ...] = ()
    stage_hashes: tuple[tuple[str, str], ...] = ()
    input_hash: str = ""
    output_hash: str = ""

    @property
    def ready(self) -> bool:
        return not self.blockers

    @property
    def blocked(self) -> bool:
        return bool(self.blockers)

    @property
    def status(self) -> str:
        return "READY" if self.ready else "BLOCKED"

    @property
    def missing(self) -> tuple[str, ...]:
        return self.missing_fields

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "blockers": list(self.blockers),
            "missing_fields": list(self.missing_fields),
            "sources": {field: source for field, source in self.sources},
            "warnings": list(self.warnings),
            "stage_hashes": {name: digest for name, digest in self.stage_hashes},
            "input_hash": self.input_hash,
            "output_hash": self.output_hash,
        }


@dataclass(frozen=True, slots=True)
class CandidateAdapterResult:
    """Candidate tuple plus diagnostics, with tuple-unpacking support."""

    candidates: tuple[CandidateObservation, ...]
    diagnostics: PipelineAdapterDiagnostics

    def __iter__(self) -> Iterator[Any]:
        yield self.candidates
        yield self.diagnostics

    @property
    def output_hash(self) -> str:
        return self.diagnostics.output_hash

    @property
    def execution_ready(self) -> bool:
        return False

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "diagnostics": self.diagnostics.to_dict(),
            "execution_ready": False,
        }


AdapterDiagnostics = PipelineAdapterDiagnostics
AdapterResult = CandidateAdapterResult
CandidateAdapterDiagnostics = PipelineAdapterDiagnostics
CandidateAdapterBlocker = PipelineAdapterBlocker


@dataclass(slots=True)
class _DiagnosticBuilder:
    blockers: list[str]
    missing_fields: set[str]
    sources: dict[str, str]
    warnings: set[str]

    def __init__(self) -> None:
        self.blockers = []
        self.missing_fields = set()
        self.sources = {}
        self.warnings = set()

    def blocker(self, reason: PipelineAdapterBlocker | str, detail: str | None = None) -> None:
        value = reason.value if isinstance(reason, PipelineAdapterBlocker) else str(reason)
        if value not in self.blockers:
            self.blockers.append(value)
        if detail:
            self.warnings.add(detail)

    def missing(self, field: str) -> None:
        self.missing_fields.add(field)

    def source(self, field: str, source: str) -> None:
        self.sources[field] = source

    def freeze(self, *, stages: tuple[tuple[str, str], ...], input_hash: str, output_hash: str) -> PipelineAdapterDiagnostics:
        return PipelineAdapterDiagnostics(
            blockers=tuple(self.blockers),
            missing_fields=tuple(sorted(self.missing_fields)),
            sources=tuple(sorted(self.sources.items())),
            warnings=tuple(sorted(self.warnings)),
            stage_hashes=stages,
            input_hash=input_hash,
            output_hash=output_hash,
        )


@dataclass(frozen=True, slots=True)
class _Stage:
    name: str
    payload: Mapping[str, Any]
    artifact_hash: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    rows: Mapping[str, Mapping[str, Any]]


def adapt_pipeline_candidates(
    s4_stage: Mapping[str, Any],
    s5_stage: Mapping[str, Any],
    s5_5_stage: Mapping[str, Any],
    account_position_facts: Mapping[str, Mapping[str, Any]] | None,
    observed_at: datetime,
    same_day_exit_unavailable_symbols: Iterable[str] | None,
    *,
    max_quote_age_seconds: int | Decimal = _DEFAULT_MAX_QUOTE_AGE_SECONDS,
) -> CandidateAdapterResult:
    """Map verified research stages into deterministic candidate observations.

    Explicit caller facts are never inferred from symbols, prices, or labels.
    Missing facts remain ``None`` and are recorded so the downstream funnel's
    hard gates can reject them.
    """

    collector = _DiagnosticBuilder()
    observed = _aware_datetime(observed_at)
    if observed is None:
        collector.blocker(PipelineAdapterBlocker.OBSERVED_AT_INVALID)
        return _empty_result(collector, (s4_stage, s5_stage, s5_5_stage), None, account_position_facts, max_quote_age_seconds)
    max_age = _decimal(max_quote_age_seconds, "max_quote_age_seconds")
    if max_age is None or max_age < 0:
        collector.blocker(PipelineAdapterBlocker.INVALID_ARGUMENT)
        return _empty_result(collector, (s4_stage, s5_stage, s5_5_stage), observed, account_position_facts, max_quote_age_seconds)

    stages = (
        _load_stage("S4", s4_stage, collector),
        _load_stage("S5", s5_stage, collector),
        _load_stage("S5_5", s5_5_stage, collector),
    )
    same_day = _normalise_same_day(same_day_exit_unavailable_symbols, collector)
    account_facts = _normalise_account_facts(account_position_facts, collector)
    if not all(stage is not None for stage in stages):
        return _empty_result(collector, (s4_stage, s5_stage, s5_5_stage), observed, account_facts, max_age)
    s4, s5, s55 = stages  # type: ignore[misc]
    assert s4 is not None and s5 is not None and s55 is not None
    if not (s4.outputs == s5.inputs == s5.outputs == s55.inputs):
        collector.blocker(PipelineAdapterBlocker.STAGE_RELATION_INVALID)
    _validate_stage_context((s4, s5, s55), observed, collector)
    if any(
        blocker in collector.blockers
        for blocker in (
            PipelineAdapterBlocker.STAGE_RELATION_INVALID.value,
            PipelineAdapterBlocker.STAGE_CONTEXT_INVALID.value,
            PipelineAdapterBlocker.STAGE_FUTURE.value,
            PipelineAdapterBlocker.STAGE_STATUS_NOT_SUCCESS.value,
            PipelineAdapterBlocker.STAGE_IDENTITY_INVALID.value,
            PipelineAdapterBlocker.DUPLICATE_SYMBOL.value,
            PipelineAdapterBlocker.ROW_SCHEMA_INVALID.value,
            PipelineAdapterBlocker.ROW_SYMBOL_MISMATCH.value,
        )
    ):
        return _empty_result(collector, (s4_stage, s5_stage, s5_5_stage), observed, account_facts, max_age)

    candidates: list[CandidateObservation] = []
    for symbol in s4.outputs:
        s4_row, s5_row, s55_row = s4.rows.get(symbol), s5.rows.get(symbol), s55.rows.get(symbol)
        if s4_row is None or s5_row is None or s55_row is None:
            collector.blocker(PipelineAdapterBlocker.ROW_SYMBOL_MISMATCH, symbol)
            continue
        candidate = _map_candidate(
            symbol, s4_row, s5_row, s55_row,
            account_facts.get(symbol) if account_facts is not None else None,
            observed, same_day, max_age, collector,
        )
        if candidate is not None:
            candidates.append(candidate)

    candidates_tuple = tuple(sorted(candidates, key=lambda item: item.symbol))
    input_hash = _safe_hash({
        "s4": _canonical_stage_for_hash(s4_stage),
        "s5": _canonical_stage_for_hash(s5_stage),
        "s5_5": _canonical_stage_for_hash(s5_5_stage),
        "account_position_facts": _json_safe(account_position_facts),
        "observed_at": observed.isoformat(),
        "same_day_exit_unavailable_symbols": None if same_day is None else list(same_day),
        "max_quote_age_seconds": str(max_age),
    })
    output_hash = _safe_hash([candidate.to_dict() for candidate in candidates_tuple])
    diagnostics = collector.freeze(
        stages=_stage_hashes((s4, s5, s55)), input_hash=input_hash, output_hash=output_hash,
    )
    return CandidateAdapterResult(candidates_tuple, diagnostics)


def adapt_verified_pipeline_candidates(*args: Any, **kwargs: Any) -> CandidateAdapterResult:
    """Explicit alias for callers that already verified the stages."""

    return adapt_pipeline_candidates(*args, **kwargs)


def build_pipeline_candidates(*args: Any, **kwargs: Any) -> tuple[CandidateObservation, ...]:
    """Strict wrapper returning candidates only when diagnostics are clear."""

    result = adapt_pipeline_candidates(*args, **kwargs)
    if result.diagnostics.blocked:
        raise PipelineAdapterError(",".join(result.diagnostics.blockers))
    return result.candidates


def _load_stage(name: str, payload: Mapping[str, Any], collector: _DiagnosticBuilder) -> _Stage | None:
    if not isinstance(payload, Mapping):
        collector.blocker(PipelineAdapterBlocker.STAGE_SCHEMA_INVALID, name)
        return None
    artifact = payload.get("artifact_hash")
    if not isinstance(artifact, str) or _SHA256.fullmatch(artifact) is None:
        collector.blocker(PipelineAdapterBlocker.STAGE_HASH_INVALID, name)
        return None
    body = {key: value for key, value in payload.items() if key != "artifact_hash"}
    try:
        expected = content_hash(body)
    except (TypeError, ValueError):
        collector.blocker(PipelineAdapterBlocker.STAGE_SCHEMA_INVALID, name)
        return None
    if expected.lower() != artifact.lower():
        collector.blocker(PipelineAdapterBlocker.STAGE_HASH_MISMATCH, name)
        return None
    if payload.get("stage") != name:
        collector.blocker(PipelineAdapterBlocker.STAGE_IDENTITY_INVALID, name)
    allowed_statuses = {"SUCCESS", "PARTIAL"} if name == "S5_5" else {"SUCCESS"}
    if payload.get("status") not in allowed_statuses:
        collector.blocker(PipelineAdapterBlocker.STAGE_STATUS_NOT_SUCCESS, name)
    inputs = _symbols(payload.get("input_symbols"), collector, PipelineAdapterBlocker.STAGE_IDENTITY_INVALID)
    outputs = _symbols(payload.get("output_symbols"), collector, PipelineAdapterBlocker.STAGE_IDENTITY_INVALID)
    raw_rows = payload.get("rows")
    if inputs is None or outputs is None or not isinstance(raw_rows, list):
        collector.blocker(PipelineAdapterBlocker.STAGE_SCHEMA_INVALID, name)
        return None
    rows: dict[str, Mapping[str, Any]] = {}
    for row in raw_rows:
        if not isinstance(row, Mapping):
            collector.blocker(PipelineAdapterBlocker.ROW_SCHEMA_INVALID, name)
            continue
        symbol = _canonical(row.get("symbol"))
        if symbol is None:
            collector.blocker(PipelineAdapterBlocker.SYMBOL_INVALID, name)
            continue
        if symbol in rows:
            collector.blocker(PipelineAdapterBlocker.DUPLICATE_SYMBOL, symbol)
            continue
        if symbol not in inputs:
            collector.blocker(PipelineAdapterBlocker.ROW_SYMBOL_MISMATCH, f"{name}:{symbol}")
            continue
        rows[symbol] = row
    return _Stage(name, payload, artifact.lower(), inputs, outputs, rows)


def _validate_stage_context(stages: tuple[_Stage, ...], observed: datetime, collector: _DiagnosticBuilder) -> None:
    for key in ("snapshot_id", "config_hash", "research_date", "market_data_reference_date", "phase"):
        values = [stage.payload.get(key) for stage in stages if key in stage.payload]
        if values and any(value != values[0] for value in values[1:]):
            collector.blocker(PipelineAdapterBlocker.STAGE_CONTEXT_INVALID, key)
    for stage in stages:
        if "as_of" not in stage.payload:
            continue
        as_of = _aware_datetime(stage.payload.get("as_of"))
        if as_of is None:
            collector.blocker(PipelineAdapterBlocker.STAGE_CONTEXT_INVALID, f"{stage.name}.as_of")
        elif as_of > observed:
            collector.blocker(PipelineAdapterBlocker.STAGE_FUTURE, stage.name)


def _map_candidate(
    symbol: str,
    s4: Mapping[str, Any],
    s5: Mapping[str, Any],
    s55: Mapping[str, Any],
    account: Mapping[str, Any] | None,
    observed: datetime,
    same_day: tuple[str, ...] | None,
    max_age: Decimal,
    collector: _DiagnosticBuilder,
) -> CandidateObservation | None:
    prefix = f"{symbol}."
    features, sector = _mapping(s4.get("features")), _mapping(s4.get("sector_metadata"))
    evidence, risks = _mapping(s5.get("evidence")), _mapping(s5.get("risk_facts"))
    quote, metrics = _mapping(s55.get("quote")), _mapping(s55.get("metrics"))
    sector_facts = {"industry": sector.get("value")} if sector.get("value") is not None else {}
    quote_facts = {
        "current_price": quote.get("price"),
        "previous_close": quote.get("prev_close", quote.get("pre_close")),
    }
    metric_facts = {
        "opening_gap": metrics.get("gap"),
        "close_strength": metrics.get("position_in_intraday_range"),
        "intraday_max_drawdown": metrics.get("drawdown_from_high"),
    }

    def value(field: str, candidates: tuple[tuple[str, Mapping[str, Any]], ...]) -> Any:
        for source, mapping in candidates:
            if field in mapping and mapping[field] is not None:
                collector.source(prefix + field, source + "." + field)
                return mapping[field]
        collector.missing(prefix + field)
        return None

    def decimal(field: str, candidates: tuple[tuple[str, Mapping[str, Any]], ...]) -> Decimal | None:
        raw = value(field, candidates)
        if raw is None:
            return None
        parsed = _decimal(raw, prefix + field)
        if parsed is None:
            collector.blocker(PipelineAdapterBlocker.FIELD_INVALID, prefix + field)
        return parsed

    def boolean(field: str, candidates: tuple[tuple[str, Mapping[str, Any]], ...]) -> bool | None:
        raw = value(field, candidates)
        if raw is None:
            return None
        if type(raw) is not bool:
            collector.blocker(PipelineAdapterBlocker.FIELD_INVALID, prefix + field)
            return None
        return raw

    def integer(field: str, candidates: tuple[tuple[str, Mapping[str, Any]], ...]) -> int | None:
        raw = value(field, candidates)
        if raw is None:
            return None
        if type(raw) is not int or raw < 0:
            collector.blocker(PipelineAdapterBlocker.FIELD_INVALID, prefix + field)
            return None
        return raw

    def text(field: str, candidates: tuple[tuple[str, Mapping[str, Any]], ...]) -> str | None:
        raw = value(field, candidates)
        if raw is None:
            return None
        if not isinstance(raw, str) or not raw.strip():
            collector.blocker(PipelineAdapterBlocker.FIELD_INVALID, prefix + field)
            return None
        return raw.strip()

    relative_sources = (("S4", s4), ("S4.features", features))
    # Exact relative-strength fields only: momentum_5d and selection_score are
    # intentionally not aliases for the 1/3/5-day facts.
    relative_1d = decimal("relative_strength_1d", relative_sources)
    relative_3d = decimal("relative_strength_3d", relative_sources)
    relative_5d = decimal("relative_strength_5d", relative_sources)
    benchmark_1d = decimal("benchmark_relative_1d", relative_sources)
    benchmark_3d = decimal("benchmark_relative_3d", relative_sources)
    benchmark_5d = decimal("benchmark_relative_5d", relative_sources)

    quote_as_of = _quote_datetime(quote, prefix, collector)
    quote_trade_date = _quote_date(quote, prefix, collector)
    if quote_trade_date is not None and quote_trade_date != observed.astimezone(SHANGHAI).date():
        collector.blocker(PipelineAdapterBlocker.QUOTE_DATE_INVALID, symbol)
    quote_age = decimal("quote_age_seconds", (("S5_5.metrics", metrics), ("S5_5", s55)))
    if quote_as_of is not None:
        age = Decimal(str((observed - quote_as_of).total_seconds()))
        if age < 0:
            collector.blocker(PipelineAdapterBlocker.QUOTE_FUTURE, symbol)
        elif age > max_age:
            collector.blocker(PipelineAdapterBlocker.QUOTE_STALE, symbol)
        if quote_age is None:
            quote_age = age
    else:
        collector.blocker(PipelineAdapterBlocker.QUOTE_MISSING, symbol)
    if quote_age is not None and quote_age > max_age:
        collector.blocker(PipelineAdapterBlocker.QUOTE_STALE, symbol)
    if quote_age is not None and quote_age < 0:
        collector.blocker(PipelineAdapterBlocker.QUOTE_FUTURE, symbol)

    bid = decimal("bid_price", (("S5_5.quote", quote),))
    if bid is None and "bid1" in quote:
        bid = _decimal(quote.get("bid1"), prefix + "bid_price")
        collector.source(prefix + "bid_price", "S5_5.quote.bid1")
    ask = decimal("ask_price", (("S5_5.quote", quote),))
    if ask is None and "ask1" in quote:
        ask = _decimal(quote.get("ask1"), prefix + "ask_price")
        collector.source(prefix + "ask_price", "S5_5.quote.ask1")
    has_valid_bid_ask = None if bid is None or ask is None else bid > 0 and ask > 0 and ask >= bid

    same_day_value = None if same_day is None else symbol in same_day
    if same_day is None:
        collector.missing(prefix + "same_day_exit_unavailable")
    else:
        collector.source(prefix + "same_day_exit_unavailable", "caller.same_day_exit_unavailable_symbols")

    holding = sellable = reconciled = None
    if account is None:
        for field in ("holding_quantity", "sellable_quantity", "orders_reconciled"):
            collector.missing(prefix + field)
    else:
        holding = _account_int(account.get("holding_quantity"), prefix + "holding_quantity", collector)
        sellable = _account_int(account.get("sellable_quantity"), prefix + "sellable_quantity", collector)
        reconciled = _account_bool(account.get("orders_reconciled"), prefix + "orders_reconciled", collector)
        for field in ("holding_quantity", "sellable_quantity", "orders_reconciled"):
            if field in account and account[field] is not None:
                collector.source(prefix + field, "caller.account_position_facts." + field)
            else:
                collector.missing(prefix + field)
        if holding is not None and sellable is not None and sellable > holding:
            collector.blocker(PipelineAdapterBlocker.ACCOUNT_FACTS_INVALID, symbol)

    correlation_raw = value("correlation_to_holdings", (("S4", s4), ("S4.features", features)))
    correlation = _correlations(correlation_raw, prefix, collector)
    evidence_missing = _string_tuple(value("missing", (("S5.evidence", evidence),)))
    evidence_sources = _string_tuple(value("sources", (("S5.evidence", evidence),)))
    evidence_id = value("evidence_id", (("S5", s5), ("S5.evidence", evidence)))
    if evidence_id is not None and isinstance(evidence_id, str) and evidence_id.strip():
        evidence_sources = tuple(sorted(set(evidence_sources + (evidence_id.strip(),))))
        collector.source(prefix + "evidence_id", "S5.evidence_id")

    try:
        return CandidateObservation(
            symbol=symbol,
            as_of=observed,
            trade_date=observed.astimezone(SHANGHAI).date(),
            relative_strength_1d=relative_1d,
            relative_strength_3d=relative_3d,
            relative_strength_5d=relative_5d,
            benchmark_relative_1d=benchmark_1d,
            benchmark_relative_3d=benchmark_3d,
            benchmark_relative_5d=benchmark_5d,
            amount_cny=decimal("amount_cny", (("S4", s4), ("S4.features", features), ("S5_5.metrics", metrics))),
            turnover_rate=decimal("turnover_rate", relative_sources),
            volume_ratio=decimal("volume_ratio", relative_sources),
            liquidity_score=decimal("liquidity_score", (("S4", s4), ("S4.features", features))),
            opening_gap=decimal("opening_gap", (("S4", s4), ("S4.features", features), ("S5_5.metrics", metric_facts))),
            close_strength=decimal("close_strength", (("S4", s4), ("S4.features", features), ("S5_5.metrics", metric_facts))),
            intraday_max_drawdown=decimal("intraday_max_drawdown", (("S4", s4), ("S4.features", features), ("S5_5.metrics", metric_facts))),
            trend_continuity=decimal("trend_continuity", (("S4", s4), ("S4.features", features))),
            consecutive_strong_sessions=integer("consecutive_strong_sessions", (("S4", s4), ("S4.features", features))),
            single_day_pulse=boolean("single_day_pulse", (("S4", s4), ("S4.features", features))),
            industry=text("industry", (("S4", s4), ("S4.sector_metadata", sector_facts))),
            correlation_cluster=text("correlation_cluster", (("S4", s4), ("S4.features", features))),
            correlation_to_holdings=correlation,
            announcement_risk=boolean("announcement_risk", (("S5.risk_facts", risks), ("S5", s5))),
            regulatory_risk=boolean("regulatory_risk", (("S5.risk_facts", risks), ("S5", s5))),
            reduction_risk=boolean("reduction_risk", (("S5.risk_facts", risks), ("S5", s5))),
            earnings_risk=boolean("earnings_risk", (("S5.risk_facts", risks), ("S5", s5))),
            risk_facts_complete=boolean("complete", (("S5.risk_facts", risks),)),
            overheat_score=decimal("overheat_score", (("S4", s4), ("S4.features", features))),
            evidence_complete=boolean("complete", (("S5.evidence", evidence),)),
            evidence_quality=decimal("quality", (("S5.evidence", evidence),)),
            evidence_missing=evidence_missing,
            evidence_sources=evidence_sources,
            quote_as_of=quote_as_of,
            quote_trade_date=quote_trade_date,
            quote_age_seconds=quote_age,
            is_suspended=boolean("is_suspended", (("S5_5.quote", quote), ("S5_5", s55))),
            is_tradeable=boolean("tradeable", (("S5_5.quote", quote), ("S5_5", s55))),
            price_locked=boolean("price_locked", (("S5_5.quote", quote), ("S5_5", s55))),
            executable_quote=boolean("execution_eligible", (("S5_5", s55),)),
            has_valid_bid_ask=has_valid_bid_ask,
            bid_price=bid,
            ask_price=ask,
            current_price=decimal("current_price", (("S5_5.quote", quote_facts),)),
            previous_close=decimal("previous_close", (("S5_5.quote", quote_facts),)),
            simulated_fill_rate=decimal("simulated_fill_rate", (("S5_5.metrics", metrics), ("S5_5", s55))),
            simulated_slippage_p95=decimal("simulated_slippage_p95", (("S5_5.metrics", metrics), ("S5_5", s55))),
            simulated_failure_reasons=_string_tuple(value("simulated_failure_reasons", (("S5_5.metrics", metrics), ("S5_5", s55)))),
            board=text("board", (("S4", s4), ("S4.features", features))),
            listed_sessions=integer("listed_sessions", (("S4", s4), ("S4.features", features))),
            is_new_listing=boolean("is_new_listing", (("S4", s4), ("S4.features", features))),
            price_limit_pct=decimal("price_limit_pct", (("S4", s4), ("S4.features", features))),
            price_limit_rule_known=boolean("price_limit_rule_known", (("S4", s4), ("S4.features", features))),
            new_listing_rule_known=boolean("new_listing_rule_known", (("S4", s4), ("S4.features", features))),
            holding_quantity=holding,
            sellable_quantity=sellable,
            same_day_exit_unavailable=same_day_value,
            orders_reconciled=reconciled,
            trigger_price=decimal("trigger_price", (("S5", s5),)),
            invalidation_price=decimal("invalidation_price", (("S5", s5),)),
            limit_price=decimal("limit_price", (("S5", s5),)),
            max_weight=decimal("max_weight", (("S5", s5),)),
            source_hash=_safe_hash({"S4": _json_safe(s4), "S5": _json_safe(s5), "S5_5": _json_safe(s55), "account": _json_safe(account)}),
        )
    except (ValueError, TypeError, InvalidOperation) as exc:
        collector.blocker(PipelineAdapterBlocker.CANDIDATE_MAPPING_BLOCKED, f"{symbol}:{type(exc).__name__}")
        return None


def _normalise_account_facts(values: Mapping[str, Mapping[str, Any]] | None, collector: _DiagnosticBuilder) -> dict[str, Mapping[str, Any]] | None:
    if values is None or not isinstance(values, Mapping):
        collector.blocker(PipelineAdapterBlocker.ACCOUNT_FACTS_INVALID)
        return None
    result: dict[str, Mapping[str, Any]] = {}
    for raw_symbol, facts in values.items():
        symbol = _canonical(raw_symbol)
        if symbol is None or not isinstance(facts, Mapping):
            collector.blocker(PipelineAdapterBlocker.ACCOUNT_FACTS_INVALID)
            continue
        if symbol in result:
            collector.blocker(PipelineAdapterBlocker.DUPLICATE_SYMBOL, symbol)
        result[symbol] = facts
    return result


def _normalise_same_day(values: Iterable[str] | None, collector: _DiagnosticBuilder) -> tuple[str, ...] | None:
    if values is None:
        collector.blocker(PipelineAdapterBlocker.SAME_DAY_INPUT_MISSING)
        return None
    if isinstance(values, (str, bytes)):
        collector.blocker(PipelineAdapterBlocker.SAME_DAY_INPUT_INVALID)
        return None
    try:
        raw = tuple(values)
    except TypeError:
        collector.blocker(PipelineAdapterBlocker.SAME_DAY_INPUT_INVALID)
        return None
    result: list[str] = []
    for value in raw:
        symbol = _canonical(value)
        if symbol is None:
            collector.blocker(PipelineAdapterBlocker.SAME_DAY_INPUT_INVALID)
            continue
        result.append(symbol)
    if len(result) != len(set(result)):
        collector.blocker(PipelineAdapterBlocker.DUPLICATE_SYMBOL, "same_day")
    return tuple(sorted(set(result)))


def _symbols(value: Any, collector: _DiagnosticBuilder, reason: PipelineAdapterBlocker) -> tuple[str, ...] | None:
    if not isinstance(value, list):
        collector.blocker(reason)
        return None
    result: list[str] = []
    for raw in value:
        symbol = _canonical(raw)
        if symbol is None:
            collector.blocker(PipelineAdapterBlocker.SYMBOL_INVALID)
            continue
        if symbol in result:
            collector.blocker(PipelineAdapterBlocker.DUPLICATE_SYMBOL, symbol)
        result.append(symbol)
    if len(result) != len(set(result)):
        return None
    return tuple(result)


def _canonical(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return CanonicalSymbol.from_core(value.strip()).symbol
    except (SymbolContractError, TypeError, ValueError):
        return None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _aware_datetime(value: Any) -> datetime | None:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return None
    if not isinstance(value, datetime) or value.utcoffset() is None:
        return None
    return value.astimezone(SHANGHAI)


def _quote_datetime(quote: Mapping[str, Any], prefix: str, collector: _DiagnosticBuilder) -> datetime | None:
    raw = quote.get("server_time", quote.get("as_of", quote.get("quote_as_of")))
    if raw is None:
        collector.missing(prefix + "quote_as_of")
        return None
    value = _aware_datetime(raw)
    if value is None:
        collector.blocker(PipelineAdapterBlocker.QUOTE_DATE_INVALID, prefix + "quote_as_of")
    else:
        collector.source(prefix + "quote_as_of", "S5_5.quote.server_time")
    return value


def _quote_date(quote: Mapping[str, Any], prefix: str, collector: _DiagnosticBuilder) -> date | None:
    raw = quote.get("trade_date", quote.get("quote_trade_date"))
    if raw is None:
        collector.missing(prefix + "quote_trade_date")
        return None
    if isinstance(raw, datetime):
        value = raw.date()
    elif isinstance(raw, date):
        value = raw
    elif isinstance(raw, str):
        try:
            value = date.fromisoformat(raw)
        except ValueError:
            value = None
    else:
        value = None
    if value is None:
        collector.blocker(PipelineAdapterBlocker.QUOTE_DATE_INVALID, prefix + "quote_trade_date")
    else:
        collector.source(prefix + "quote_trade_date", "S5_5.quote.trade_date")
    return value


def _decimal(value: Any, field: str) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return result if result.is_finite() else None


def _account_int(value: Any, field: str, collector: _DiagnosticBuilder) -> int | None:
    if value is None:
        return None
    if type(value) is not int or value < 0:
        collector.blocker(PipelineAdapterBlocker.ACCOUNT_FACTS_INVALID, field)
        return None
    return value


def _account_bool(value: Any, field: str, collector: _DiagnosticBuilder) -> bool | None:
    if value is None:
        return None
    if type(value) is not bool:
        collector.blocker(PipelineAdapterBlocker.ACCOUNT_FACTS_INVALID, field)
        return None
    return value


def _correlations(value: Any, prefix: str, collector: _DiagnosticBuilder) -> Mapping[str, Decimal] | None:
    if value is None:
        collector.missing(prefix + "correlation_to_holdings")
        return None
    if not isinstance(value, Mapping):
        collector.blocker(PipelineAdapterBlocker.FIELD_INVALID, prefix + "correlation_to_holdings")
        return None
    result: dict[str, Decimal] = {}
    for raw_symbol, raw_value in value.items():
        symbol = _canonical(raw_symbol) or (raw_symbol.strip() if isinstance(raw_symbol, str) and raw_symbol.strip() else None)
        parsed = _decimal(raw_value, prefix + "correlation_to_holdings")
        if symbol is None or parsed is None or not Decimal("0") <= parsed <= Decimal("1"):
            collector.blocker(PipelineAdapterBlocker.FIELD_INVALID, prefix + "correlation_to_holdings")
            continue
        result[symbol] = parsed
    return result


def _string_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value.strip(),) if value.strip() else ()
    if not isinstance(value, (list, tuple, set, frozenset)):
        return ()
    return tuple(sorted({item.strip() for item in value if isinstance(item, str) and item.strip()}))


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (set, frozenset)):
        return sorted((_json_safe(item) for item in value), key=repr)
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _safe_hash(value: Any) -> str:
    return content_hash(_json_safe(value))


def _canonical_stage_for_hash(value: Any) -> Any:
    if not isinstance(value, Mapping):
        return _json_safe(value)
    rows = value.get("rows")
    body = {key: item for key, item in value.items() if key != "rows"}
    if isinstance(rows, list):
        body["rows"] = sorted(
            (_json_safe(row) for row in rows),
            key=lambda row: str(row.get("symbol", "")) if isinstance(row, Mapping) else str(row),
        )
    return _json_safe(body)


def _stage_hashes(stages: tuple[_Stage | None, ...]) -> tuple[tuple[str, str], ...]:
    return tuple((stage.name, stage.artifact_hash) for stage in stages if stage is not None)


def _empty_result(
    collector: _DiagnosticBuilder,
    stages: tuple[Any, ...],
    observed: datetime | None,
    account: Any,
    max_age: Any,
) -> CandidateAdapterResult:
    input_hash = _safe_hash({
        "stages": [_canonical_stage_for_hash(stage) for stage in stages],
        "observed_at": observed.isoformat() if observed else None,
        "account_position_facts": _json_safe(account),
        "max_quote_age_seconds": _json_safe(max_age),
    })
    return CandidateAdapterResult(
        (), collector.freeze(stages=(), input_hash=input_hash, output_hash=_safe_hash([])),
    )


__all__ = [
    "AdapterDiagnostics",
    "AdapterResult",
    "CandidateAdapterBlocker",
    "CandidateAdapterDiagnostics",
    "CandidateAdapterResult",
    "PipelineAdapterBlocker",
    "PipelineAdapterDiagnostics",
    "PipelineAdapterError",
    "adapt_pipeline_candidates",
    "adapt_verified_pipeline_candidates",
    "build_pipeline_candidates",
]
