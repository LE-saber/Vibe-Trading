"""Build one deterministic, source-traceable S2 EvidenceBundle."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, time
from enum import Enum
import math
from numbers import Number
from typing import Any

from competition_core.pipeline.time_context import ResearchContext, SHANGHAI
from contracts.evidence import EvidenceBundle, EvidenceContractError, Provenance
from contracts.symbol import CanonicalSymbol


_SUPPLEMENT_NAMES = ("flow", "dragon_tiger", "margin")
_QUANT_COMPONENTS = (
    "trend",
    "momentum",
    "relative_strength",
    "liquidity",
    "volatility_quality",
    "data_quality",
)
_PRICE_FIELDS = (
    "open_raw",
    "high_raw",
    "low_raw",
    "close_raw",
    "open_qfq",
    "high_qfq",
    "low_qfq",
    "close_qfq",
    "adj_factor",
)
_RAW_FEATURES = frozenset(
    {
        "latest_close_raw",
        "latest_volume_raw",
        "latest_volume_shares",
        "latest_amount_raw",
        "latest_amount_cny",
    }
)
_DAILY_BASIC_FEATURES = frozenset(
    {"turnover_rate", "volume_ratio", "total_mv", "circ_mv"}
)
_INDEX_FEATURES = frozenset(
    {
        "benchmark_return_20d",
        "benchmark_return_60d",
        "benchmark_relative_20d",
        "benchmark_relative_60d",
    }
)
_LIMIT_FEATURES = frozenset(
    {
        "recent_limit_up_count",
        "recent_limit_down_count",
        "latest_limit_up",
        "latest_limit_down",
    }
)
_QUALITY_FEATURES = frozenset(
    {"history_coverage", "missing_ratio", "factor_coverage", "source_confidence"}
)
_HELPER_FEATURES = frozenset(
    {
        "sma_5",
        "sma_10",
        "sma_20",
        "sma_20_lag5",
        "rsi_14",
        "macd_histogram",
        "bollinger_upper",
        "bollinger_middle",
        "bollinger_lower",
        "technical_contiguous_sessions",
        "technical_missing_fields",
    }
)
_SUPPLEMENT_AUDIT_FIELDS = frozenset(
    {"data", "error", "context", "request", "observed_at", "request_id", "integrity_hash"}
)
_MEMBER_NON_IDENTITY_FIELDS = frozenset(
    {"field_sources", "source", "sources", "observed_at", "request_id", "integrity_hash"}
)
_MISSING = object()


def build_evidence(
    *,
    candidate: dict,
    member: dict,
    context: ResearchContext,
    snapshot_id: str,
    source: str,
    supplements: dict,
    quant_source: str = "competition_pipeline.s2",
) -> EvidenceBundle:
    """Build an immutable EvidenceBundle from already validated S2 inputs.

    The function deliberately only maps values.  It does not fetch data,
    recalculate S2 features, alter the candidate, or persist an artifact.
    ``reference_daily`` is preferred when present because the S2 feature row
    does not carry all of the canonical reference-day fields.
    """

    _require_mapping(candidate, "candidate")
    _require_mapping(member, "member")
    _require_mapping(supplements, "supplements")
    if not isinstance(context, ResearchContext):
        raise EvidenceContractError("context must be a ResearchContext")
    if not isinstance(snapshot_id, str) or not snapshot_id.strip():
        raise EvidenceContractError("snapshot_id must not be empty")
    if not isinstance(source, str) or not source.strip():
        raise EvidenceContractError("source must not be empty")
    if not isinstance(quant_source, str) or not quant_source.strip():
        raise EvidenceContractError("quant_source must not be empty")

    _validate_finite(candidate, "candidate")
    candidate_symbol = candidate.get("symbol")
    if not isinstance(candidate_symbol, str):
        raise EvidenceContractError("candidate.symbol must be a canonical symbol")
    identity = CanonicalSymbol.from_core(candidate_symbol)

    member_symbol = member.get("ts_code", _MISSING)
    if member_symbol != candidate_symbol:
        raise EvidenceContractError("candidate.symbol and member.ts_code do not match")
    if "exchange" in member and member["exchange"] is not None:
        try:
            CanonicalSymbol.from_core(candidate_symbol, exchange=member["exchange"])
        except (TypeError, ValueError) as exc:
            raise EvidenceContractError("candidate/member exchange identity mismatch") from exc
    _validate_member_pit_dates(member, context)

    features = candidate.get("features")
    if not isinstance(features, Mapping):
        raise EvidenceContractError("candidate.features must be a mapping")
    reference_daily = candidate.get("reference_daily")
    if reference_daily is None:
        reference_daily = {}
    elif not isinstance(reference_daily, Mapping):
        raise EvidenceContractError("candidate.reference_daily must be a mapping")
    _validate_reference_daily(reference_daily, candidate_symbol, context)

    supplement_values, supplement_sources = _validate_supplements(supplements, context)

    source = source.strip()
    daily_source = f"{source}.daily"
    daily_adjusted_source = f"{source}.daily+adj_factor"
    daily_basic_source = f"{source}.daily_basic"
    index_source = f"{source}.index_daily+daily+adj_factor"
    limit_source = f"{source}.stk_limit+daily"
    quality_source = "competition_pipeline.canonical_quality"
    helper_source = "pure_vibe_helpers"
    quant_source = quant_source.strip()
    context_source = "research_context"
    universe_source = f"competition_universe({snapshot_id})"

    reference_as_of = datetime.combine(
        context.market_data_reference_date, time(15, 0), tzinfo=SHANGHAI
    )

    identity_values = {
        key: _json_ready(value)
        for key, value in member.items()
        if key not in _MEMBER_NON_IDENTITY_FIELDS
    }
    snapshot_values = {"snapshot_id": snapshot_id}
    context_values = context.to_dict()

    prices = {
        field: _prefer(
            reference_daily,
            (field,),
            features,
            (f"latest_{field}", field),
        )
        for field in _PRICE_FIELDS
    }
    reference_values = {
        "price": _prefer(reference_daily, ("close_qfq",), features, ("latest_close_qfq",)),
        "price_raw": _prefer(reference_daily, ("close_raw",), features, ("latest_close_raw",)),
        "volume_raw": _prefer(
            reference_daily, ("volume_raw", "vol"), features, ("latest_volume_raw", "volume_raw")
        ),
        "volume_shares": _prefer(
            reference_daily,
            ("volume_shares",),
            features,
            ("latest_volume_shares", "volume_shares"),
        ),
        "amount_raw": _prefer(
            reference_daily, ("amount_raw", "amount"), features, ("latest_amount_raw", "amount_raw")
        ),
        "amount_cny": _prefer(
            reference_daily,
            ("amount_cny",),
            features,
            ("latest_amount_cny", "amount_cny"),
        ),
        "price_unit": "CNY",
        "volume_raw_unit": "lots",
        "volume_shares_unit": "shares",
        "amount_raw_unit": "thousand_CNY",
        "amount_cny_unit": "CNY",
    }
    fundamental = {
        "total_mv": _prefer(reference_daily, ("total_mv",), features, ("total_mv",)),
        "circ_mv": _prefer(reference_daily, ("circ_mv",), features, ("circ_mv",)),
        "market_cap_unit": "万元",
        "turnover_rate": _prefer(
            reference_daily, ("turnover_rate",), features, ("turnover_rate", "latest_turnover_rate")
        ),
        "volume_ratio": _prefer(
            reference_daily, ("volume_ratio",), features, ("volume_ratio", "latest_volume_ratio")
        ),
    }

    quant = {
        "quant_score": candidate.get("quant_score"),
        "component_scores": candidate.get("component_scores"),
        "feature_percentiles": candidate.get("feature_percentiles"),
        "effective_weights": candidate.get("effective_weights"),
        "s2rank": _prefer_candidate(candidate, "s2rank", "rank"),
    }
    for component in _QUANT_COMPONENTS:
        field = f"{component}_score"
        if field in candidate:
            quant[field] = candidate[field]
    for field in ("available_component_weight", "feature_completeness", "missing_fields"):
        if field in candidate:
            quant[field] = candidate[field]
    technical_features = dict(features)

    flags, flags_source = _risk_flags(
        supplement_values, supplements, supplement_sources, context_source
    )
    risk = {
        "overheat_score": candidate.get("overheat_score"),
        "data_confidence": candidate.get("data_confidence"),
        "flags": flags,
    }
    events = {"news": None, "coverage": "not_requested"}

    sections = {
        "market": {
            "identity": identity_values,
            "snapshot": snapshot_values,
            "context": context_values,
            "prices": prices,
            "reference": reference_values,
        },
        "technical": {"quant": quant, "features": technical_features},
        "flow": supplement_values["flow"],
        "margin": supplement_values["margin"],
        "dragon_tiger": supplement_values["dragon_tiger"],
        "fundamental": fundamental,
        "events": events,
        "risk": risk,
    }

    missing: list[str] = []
    provenance: list[Provenance] = []

    _collect("market.identity", identity_values, universe_source, context.as_of, provenance, missing)
    _collect("market.snapshot", snapshot_values, context_source, context.as_of, provenance, missing)
    _collect("market.context", context_values, context_source, context.as_of, provenance, missing)
    for field, value in prices.items():
        price_source = daily_adjusted_source if field.endswith("_qfq") or field == "adj_factor" else daily_source
        _collect(f"market.prices.{field}", value, price_source, reference_as_of, provenance, missing)
    for field, value in reference_values.items():
        if field in {"price", "price_unit"}:
            field_source = daily_adjusted_source if field == "price" else context_source
        elif field == "price_raw":
            field_source = daily_source
        elif field.endswith("_unit"):
            field_source = context_source
        else:
            field_source = daily_source
        _collect(f"market.reference.{field}", value, field_source, reference_as_of, provenance, missing)

    _collect("technical.quant", quant, quant_source, context.as_of, provenance, missing)
    for field, value in technical_features.items():
        if field in _INDEX_FEATURES:
            field_source = index_source
        elif field in _LIMIT_FEATURES:
            field_source = limit_source
        elif field in _QUALITY_FEATURES:
            field_source = quality_source
        elif field in _HELPER_FEATURES:
            field_source = helper_source
        elif field in _DAILY_BASIC_FEATURES:
            field_source = daily_basic_source
        elif field in _RAW_FEATURES:
            field_source = daily_source
        else:
            field_source = daily_adjusted_source
        _collect(
            f"technical.features.{field}",
            value,
            field_source,
            reference_as_of,
            provenance,
            missing,
        )

    for name in _SUPPLEMENT_NAMES:
        _collect(
            name,
            supplement_values[name],
            supplement_sources[name],
            context.as_of,
            provenance,
            missing,
        )
    for field, value in fundamental.items():
        field_source = daily_basic_source if field != "market_cap_unit" else context_source
        _collect(f"fundamental.{field}", value, field_source, reference_as_of, provenance, missing)
    _collect("events", events, context_source, context.as_of, provenance, missing)
    _collect("risk.overheat_score", risk["overheat_score"], quant_source, context.as_of, provenance, missing)
    _collect("risk.data_confidence", risk["data_confidence"], quant_source, context.as_of, provenance, missing)
    _collect("risk.flags", flags, flags_source, context.as_of, provenance, missing)

    provenance.sort(key=lambda item: item.field)
    missing_tuple = tuple(sorted(set(missing)))
    return EvidenceBundle(
        symbol=identity,
        as_of=context.as_of,
        snapshot_id=snapshot_id,
        sections=sections,
        missing=missing_tuple,
        provenance=tuple(provenance),
    )


def _require_mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise EvidenceContractError(f"{name} must be a mapping")
    return value


def _validate_finite(value: Any, path: str) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            _validate_finite(item, f"{path}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _validate_finite(item, f"{path}[{index}]")
        return
    if isinstance(value, Number) and not isinstance(value, bool):
        try:
            finite = math.isfinite(float(value))
        except (TypeError, ValueError, OverflowError) as exc:
            raise EvidenceContractError(f"{path} must be finite numeric") from exc
        if not finite:
            raise EvidenceContractError(f"{path} must be finite numeric")


def _validate_member_pit_dates(member: Mapping[str, Any], context: ResearchContext) -> None:
    value = member.get("list_date")
    if value is None:
        return
    parsed = _date_value(value, "member.list_date")
    if parsed > context.research_date:
        raise EvidenceContractError("member.list_date is after the research date")


def _validate_reference_daily(
    reference_daily: Mapping[str, Any], symbol: str, context: ResearchContext
) -> None:
    for key in ("symbol", "ts_code"):
        if key in reference_daily and reference_daily[key] is not None and reference_daily[key] != symbol:
            raise EvidenceContractError("candidate.reference_daily identity mismatch")
    if "trade_date" in reference_daily and reference_daily["trade_date"] is not None:
        parsed = _date_value(reference_daily["trade_date"], "candidate.reference_daily.trade_date")
        if parsed != context.market_data_reference_date:
            raise EvidenceContractError("candidate.reference_daily trade_date mismatch")


def _validate_supplements(
    supplements: Mapping[str, Any], context: ResearchContext
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    values: dict[str, dict[str, Any]] = {}
    sources: dict[str, str] = {}
    reference = context.market_data_reference_date
    for name in _SUPPLEMENT_NAMES:
        payload = supplements.get(name)
        if not isinstance(payload, Mapping):
            raise EvidenceContractError(f"supplements.{name} must be a mapping")
        if "as_of" not in payload:
            raise EvidenceContractError(f"supplements.{name}.as_of is required")
        as_of = _parse_iso_date(payload["as_of"], f"supplements.{name}.as_of")
        if as_of != reference:
            raise EvidenceContractError(f"supplements.{name}.as_of differs from market data reference date")
        source = payload.get("source")
        if not isinstance(source, str) or not source.strip():
            raise EvidenceContractError(f"supplements.{name}.source must be explicit")
        if "coverage" not in payload:
            raise EvidenceContractError(f"supplements.{name}.coverage is required")
        if "risk_flags" in payload and not isinstance(payload["risk_flags"], list):
            raise EvidenceContractError(f"supplements.{name}.risk_flags must be a list")
        values[name] = {
            key: value for key, value in payload.items() if key not in _SUPPLEMENT_AUDIT_FIELDS
        }
        sources[name] = source.strip()

    top_flags = supplements.get("risk_flags")
    if top_flags is not None and not isinstance(top_flags, list):
        raise EvidenceContractError("supplements.risk_flags must be a list")
    return values, sources


def _risk_flags(
    supplements: Mapping[str, dict[str, Any]],
    raw_supplements: Mapping[str, Any],
    sources: Mapping[str, str],
    default_source: str,
) -> tuple[list[Any], str]:
    top = raw_supplements.get("risk_flags", _MISSING)
    if top is not _MISSING:
        flags = list(top)
    else:
        flags = []
        for name in _SUPPLEMENT_NAMES:
            value = supplements[name].get("risk_flags", ())
            flags.extend(value)
    flag_sources = [sources[name] for name in _SUPPLEMENT_NAMES if sources.get(name)]
    return flags, ",".join(sorted(set(flag_sources))) if flag_sources else default_source


def _date_value(value: Any, name: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        if len(value) == 8 and value.isdigit():
            try:
                return datetime.strptime(value, "%Y%m%d").date()
            except ValueError:
                pass
        try:
            parsed = date.fromisoformat(value)
        except ValueError as exc:
            raise EvidenceContractError(f"{name} must be an ISO date") from exc
        if parsed.isoformat() != value:
            raise EvidenceContractError(f"{name} must be an ISO date")
        return parsed
    raise EvidenceContractError(f"{name} must be an ISO date")


def _parse_iso_date(value: Any, name: str) -> date:
    if not isinstance(value, str) or value != value.strip():
        raise EvidenceContractError(f"{name} must be an ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise EvidenceContractError(f"{name} must be an ISO date") from exc
    if parsed.isoformat() != value:
        raise EvidenceContractError(f"{name} must be an ISO date")
    return parsed


def _prefer(
    primary: Mapping[str, Any],
    primary_keys: tuple[str, ...],
    fallback: Mapping[str, Any],
    fallback_keys: tuple[str, ...],
) -> Any:
    for key in primary_keys:
        if key in primary:
            return primary[key]
    for key in fallback_keys:
        if key in fallback:
            return fallback[key]
    return None


def _prefer_candidate(candidate: Mapping[str, Any], primary: str, fallback: str) -> Any:
    if primary in candidate:
        return candidate[primary]
    return candidate.get(fallback)


def _json_ready(value: Any) -> Any:
    if isinstance(value, Enum):
        return _json_ready(value.value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value


def _collect(
    path: str,
    value: Any,
    source: str,
    as_of: datetime,
    provenance: list[Provenance],
    missing: list[str],
) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise EvidenceContractError(f"{path} keys must be non-empty strings")
            _collect(f"{path}.{key}", item, source, as_of, provenance, missing)
        return
    if value is None:
        missing.append(path)
        return
    provenance.append(Provenance(field=path, source=source, as_of=as_of))


__all__ = ["build_evidence"]
