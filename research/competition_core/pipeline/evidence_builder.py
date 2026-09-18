"""S3: dated market-wide inputs, durable per-stock evidence, bounded refinement."""

from __future__ import annotations

from collections import Counter
import copy
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from competition_core.pipeline.artifacts import atomic_json, content_hash, file_hash, read_json
from competition_core.pipeline.config import PipelineConfig
from competition_core.pipeline.dated_cache import DatedSourceCache
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.features import finite
from competition_core.pipeline.persistence import PipelineRepository
from competition_core.pipeline.scoring import percentile
from competition_core.pipeline.time_context import ResearchContext
from competition_core.storage.evidence import EvidenceRepository
from contracts.evidence import EvidenceBundle

EVIDENCE_VERSION = "p10d.evidence.v1"
FLOW_FIELDS = "ts_code,trade_date,buy_lg_amount,sell_lg_amount,buy_elg_amount,sell_elg_amount,net_mf_amount"
DRAGON_FIELDS = "ts_code,trade_date,reason,net_amount,l_buy,l_sell"
MARGIN_FIELDS = "ts_code,trade_date,rzye,rqye,rzmre,rzche,rqyl,rzrqye"


def build_holding_evidence(*, candidate: dict[str, Any], member: dict[str, Any], panel: pd.DataFrame,
                           sessions: list[str], context: ResearchContext, snapshot_id: str,
                           source: str, batches: dict[str, Any]) -> EvidenceBundle:
    """Build one S1-holding bundle from frozen inputs without fetching or persistence."""
    from competition_core.pipeline.evidence_bundle import build_evidence
    from competition_core.pipeline.evidence_fields import supplement_sections

    if not isinstance(candidate, dict) or not isinstance(member, dict) or not isinstance(batches, dict):
        raise PipelineError("INVALID_ARGUMENTS", "holding evidence inputs must be mappings")
    symbol = candidate.get("symbol")
    if member.get("ts_code") != symbol:
        raise PipelineError("IDENTITY_CONFLICT", "holding candidate and Universe member differ")
    if not isinstance(panel, pd.DataFrame) or not {"symbol", "trade_date", "amount_cny"} <= set(panel.columns):
        raise PipelineError("INVALID_ARGUMENTS", "holding evidence panel is incomplete")
    dates = sessions[-5:]
    if (len(dates) != 5 or dates != sorted(set(dates))
            or dates[-1] != context.market_data_reference_date.isoformat()):
        raise PipelineError("STALE_DATA", "holding evidence needs five frozen completed sessions")
    symbol_rows = panel.loc[panel["symbol"].eq(symbol)]
    observed_dates = pd.to_datetime(symbol_rows["trade_date"], format="%Y-%m-%d", errors="coerce")
    if observed_dates.isna().any() or (observed_dates.dt.date > context.market_data_reference_date).any():
        raise PipelineError("STALE_DATA", "holding panel contains an invalid or future date")
    rows = symbol_rows.loc[symbol_rows["trade_date"].isin(dates)].copy()
    if rows["trade_date"].duplicated().any() or not rows["trade_date"].isin(dates).all():
        raise PipelineError("STALE_DATA", "holding panel identity/date is ambiguous")
    reference_rows = rows.loc[rows["trade_date"].eq(dates[-1])]
    if len(reference_rows) != 1:
        raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "holding reference daily row is unavailable")
    required = {"flow", "margin", "dragon"}
    if not required <= set(batches) or not isinstance(batches["flow"], list) or not isinstance(batches["margin"], list):
        raise PipelineError("INVALID_ARGUMENTS", "holding supplement batches are incomplete")
    amount_by_date = {day: None for day in dates}
    for row in rows.to_dict("records"):
        amount_by_date[row["trade_date"]] = finite(row.get("amount_cny"))
    supplements = supplement_sections(symbol=symbol, sessions=dates, flow_batches=batches["flow"],
        dragon_batch=batches["dragon"], margin_batches=batches["margin"], amount_by_date=amount_by_date)
    reference = {key: value if isinstance(value, str) else finite(value)
                 for key, value in reference_rows.iloc[0].to_dict().items()}
    holding_candidate = copy.deepcopy(candidate)
    holding_candidate["reference_daily"] = reference
    return build_evidence(candidate=holding_candidate, member=copy.deepcopy(member), context=context,
        snapshot_id=snapshot_id, source=source, supplements=supplements,
        quant_source="competition_pipeline.s1_holding")


def evidence_policy(config: PipelineConfig) -> dict[str, Any]:
    if "evidence" not in config.to_dict():
        raise PipelineError("NOT_CONFIGURED", "S3 requires explicit versioned evidence settings; start a new configured run")
    return config.section("evidence")


def fetch_supplements(cache: DatedSourceCache, sessions: list[str], progress=None,
                      *, cache_only: bool = False) -> dict[str, Any]:
    """One logical whole-market query per date/type, with audited pagination."""
    getter = cache.get_cached if cache_only else cache.get
    output = {"flow": [], "margin": []}
    for kind, endpoint, fields in (("flow", "moneyflow", FLOW_FIELDS), ("margin", "margin_detail", MARGIN_FIELDS)):
        for day in sessions:
            batch = getter(endpoint, {"trade_date": day.replace("-", ""), "fields": fields},
                           key_fields=("ts_code", "trade_date"))
            output[kind].append(batch)
            if progress:
                progress({"stage": "S3", "source_endpoint": endpoint, "trade_date": day,
                          "source_status": batch["status"], "source_rows": len(batch["data"])})
    output["dragon"] = getter("top_list", {"trade_date": sessions[-1].replace("-", ""), "fields": DRAGON_FIELDS},
                              key_fields=("ts_code", "trade_date", "reason"))
    return output


def _evidence_checkpoint(*, repository: PipelineRepository, store: EvidenceRepository,
                         candidate: dict[str, Any], member: dict[str, Any], context: ResearchContext,
                         snapshot_id: str, source: str, supplements: dict[str, Any], inputs_hash: str
                         ) -> tuple[EvidenceBundle, list[dict[str, Any]], bool]:
    from competition_core.pipeline.evidence_bundle import build_evidence

    symbol = candidate["symbol"]
    path = repository.directory / "evidence" / f"{symbol}.checkpoint.json"
    expected = {"symbol": symbol, "inputs_hash": inputs_hash, "candidate_hash": content_hash(candidate),
                "member_hash": content_hash(member)}
    reused = path.exists()
    if reused:
        record = read_json(path)
        integrity = record.pop("integrity_hash", None)
        if integrity != content_hash(record) or any(record.get(key) != value for key, value in expected.items()):
            raise PipelineError("STALE_DATA", "per-stock evidence checkpoint input/hash mismatch", symbol=symbol)
        record["integrity_hash"] = integrity
        if record.get("body_path") != f"evidence/{record.get('evidence_id')}.json":
            raise PipelineError("STALE_DATA", "invalid evidence body reference", symbol=symbol)
        from competition_core.pipeline.artifacts import confined_path
        body_path = confined_path(repository.directory, record["body_path"])
        bundle = EvidenceBundle.from_dict(read_json(body_path, expected_hash=record["body_hash"]))
        if (bundle.evidence_id != record["evidence_id"] or bundle.symbol.symbol != symbol
                or bundle.snapshot_id != snapshot_id or bundle.as_of != context.as_of):
            raise PipelineError("STALE_DATA", "evidence identity differs from checkpoint", symbol=symbol)
        if store.get(bundle.evidence_id).to_dict() != bundle.to_dict():
            raise PipelineError("STALE_DATA", "Core Evidence Store differs from run body", symbol=symbol)
    else:
        bundle = build_evidence(candidate=candidate, member=member, context=context, snapshot_id=snapshot_id,
                                source=source, supplements=supplements)
        store.put(bundle)
        body_path = repository.directory / "evidence" / f"{bundle.evidence_id}.json"
        atomic_json(body_path, bundle.to_dict(), immutable=True)
        record = {**expected, "evidence_id": bundle.evidence_id, "body_path": f"evidence/{body_path.name}",
                  "body_hash": file_hash(body_path)}
        record["integrity_hash"] = content_hash(record)
        atomic_json(path, record, immutable=True)
    references = [{"path": f"evidence/{path.name}", "sha256": file_hash(path)},
                  {"path": record["body_path"], "sha256": record["body_hash"], "evidence_id": bundle.evidence_id}]
    return bundle, references, reused


def select_evidence(candidates: list[dict[str, Any]], bundles: dict[str, EvidenceBundle],
                    config: PipelineConfig) -> dict[str, Any]:
    """Leave QuantScore intact. Flow is a bounded, separately reported baseline."""
    policy = evidence_policy(config)
    if (len({row["symbol"] for row in candidates}) != len(candidates)
            or {row["symbol"] for row in candidates} != set(bundles)):
        raise PipelineError("IDENTITY_CONFLICT", "S3 evidence and candidate identities differ")
    ratio = pd.Series({symbol: bundle.sections["flow"].get("main_flow_ratio_5d") for symbol, bundle in bundles.items()}, dtype=float)
    eligible = {row["symbol"] for row in candidates if finite(row.get("data_confidence")) is not None
                and row["data_confidence"] >= policy["minimum_data_confidence"]
                and finite(row.get("quant_score")) is not None}
    # Net active flow cannot exceed total actually traded amount. Retain the
    # observed ratio in evidence, but do not reward an inconsistent unit/value.
    ratio_for_score = ratio.where(ratio.between(-1.000000001, 1.000000001))
    flow_scores = percentile(ratio_for_score.reindex(sorted(eligible))).reindex(ratio.index)
    weights = {"quant": policy["quant_weight"], "flow": policy["flow_weight"], "quality": policy["quality_weight"]}
    rows = []
    for candidate in sorted(candidates, key=lambda row: row["symbol"]):
        symbol = candidate["symbol"]
        bundle = bundles[symbol]
        coverages = {name: finite(bundle.sections[name].get("coverage")) for name in ("flow", "margin", "dragon_tiger")}
        if any(value is None or not 0 <= value <= 1 for value in coverages.values()):
            raise PipelineError("STALE_DATA", "supplement coverage must be explicit and bounded", symbol=symbol)
        confidence = finite(candidate.get("data_confidence"))
        evidence_confidence = min(confidence, sum(coverages.values()) / len(coverages)) if confidence is not None else None
        components = {"quant": finite(candidate.get("quant_score")), "flow": finite(flow_scores.get(symbol)),
                      "quality": evidence_confidence * 100 if evidence_confidence is not None else None}
        denominator = sum(weights[key] for key, value in components.items() if value is not None)
        score = sum(value * weights[key] for key, value in components.items() if value is not None) / denominator if denominator else None
        reasons = []
        if confidence is None or confidence < policy["minimum_data_confidence"]:
            reasons.append("data_confidence_below_s3_minimum")
        if components["quant"] is None:
            reasons.append("missing_s2_quant_score")
        row = copy.deepcopy(candidate)
        risk_flags = list(bundle.sections["risk"].get("flags", ()))
        if finite(ratio.get(symbol)) is not None and abs(ratio[symbol]) > 1.000000001:
            risk_flags.append("flow_ratio_outside_actual_amount_bound")
        row.update(s2_rank=candidate.get("rank"), rank=None, selected=False, selection_score=score,
                   s3_components=components, s3_effective_weights={key: weights[key] / denominator if value is not None and denominator else None
                                                               for key, value in components.items()},
                   evidence_id=bundle.evidence_id, evidence_confidence=evidence_confidence,
                   evidence_coverage=coverages, evidence_missing_fields=list(bundle.missing),
                   main_flow_ratio_5d=finite(ratio.get(symbol)),
                   risk_flags=sorted(set(risk_flags)),
                   reject_reasons=reasons, keep_reasons=[])
        rows.append(row)
    ranked = sorted((row for row in rows if not row["reject_reasons"] and row["selection_score"] is not None),
                    key=lambda row: (-row["selection_score"], row["symbol"]))
    top_n = config.section("stages")["s3_top_n"]
    for rank, row in enumerate(ranked, 1):
        row["rank"], row["selected"] = rank, rank <= top_n
        if row["selected"]:
            row["keep_reasons"] = [f"s3_evidence_rank_{rank}_within_top{top_n}", "quant_score_preserved"]
        else:
            row["reject_reasons"] = [f"rank_below_s3_top{top_n}"]
    selected = [row["symbol"] for row in ranked[:top_n]]
    return {"stage": "S3", "scoring_version": EVIDENCE_VERSION,
            "status": "SUCCESS" if len(selected) == top_n else "PARTIAL",
            "input_symbols": [row["symbol"] for row in rows], "output_symbols": selected, "rows": rows,
            "summary": {"input_count": len(rows), "selected_count": len(selected), "requested_count": top_n,
                        "evidence_count": len(bundles), "low_confidence_count": sum("data_confidence_below_s3_minimum" in row["reject_reasons"] for row in rows),
                        "missing_counts": dict(sorted(Counter(field for row in rows for field in row["evidence_missing_fields"]).items())),
                        "coverage_incomplete_counts": {name: sum(row["evidence_coverage"][name] < 1 for row in rows) for name in ("flow", "margin", "dragon_tiger")},
                        "llm_calls": 0, "agent_tool_calls": 0}}


def build_evidence_stage(*, s2: dict[str, Any], snapshot: dict[str, Any], panel: pd.DataFrame,
                         sessions: list[str], context: ResearchContext, config: PipelineConfig,
                         repository: PipelineRepository, store: EvidenceRepository, cache: DatedSourceCache,
                         source: str, progress: Callable[[dict[str, Any]], None] | None = None
                         ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    from competition_core.pipeline.evidence_fields import supplement_sections

    policy = evidence_policy(config)
    dates = sessions[-policy["lookback_sessions"]:]
    if len(dates) != 5 or dates[-1] != context.market_data_reference_date.isoformat():
        raise PipelineError("STALE_DATA", "S3 needs exactly five frozen completed sessions")
    candidates_by_symbol = {row["symbol"]: row for row in s2["rows"]}
    candidates = [candidates_by_symbol[symbol] for symbol in s2["output_symbols"]]
    if not candidates:
        raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "S3 has no S2 candidates")
    members = {row["ts_code"]: row for row in snapshot["members"]}
    batches = fetch_supplements(cache, dates, progress)
    batch_list = [*batches["flow"], *batches["margin"], batches["dragon"]]
    references = [{key: batch[key] for key in ("endpoint", "request_id", "source", "observed_at", "integrity_hash", "status")} for batch in batch_list]
    input_hash = content_hash({"specification_hash": repository.state()["specification_hash"],
                               "s2_semantic_hash": s2["semantic_hash"], "batches": references})
    groups = {symbol: frame.set_index("trade_date") for symbol, frame in panel.loc[panel.symbol.isin(s2["output_symbols"])].groupby("symbol", sort=False)}
    bundles = {}
    for number, candidate in enumerate(candidates, 1):
        symbol = candidate["symbol"]
        frame = groups[symbol]
        amount = {day: finite(frame.loc[day, "amount_cny"]) if day in frame.index else None for day in dates}
        supplements = supplement_sections(symbol=symbol, sessions=dates, flow_batches=batches["flow"],
            dragon_batch=batches["dragon"], margin_batches=batches["margin"], amount_by_date=amount)
        reference = frame.loc[dates[-1]].to_dict() if dates[-1] in frame.index else {}
        reference = {key: value if isinstance(value, str) else finite(value) for key, value in reference.items()}
        bundle, evidence_refs, reused = _evidence_checkpoint(repository=repository, store=store,
            candidate={**candidate, "reference_daily": reference}, member=members[symbol], context=context,
            snapshot_id=snapshot["snapshot_id"], source=source, supplements=supplements, inputs_hash=input_hash)
        bundles[symbol] = bundle
        references.extend(evidence_refs)
        # This atomic progress point follows both Core-store and run-body commit.
        # A killed worker can reuse all completed evidence and source batches.
        repository.update(stage_progress={"stage": "S3", "completed": number, "total": len(candidates),
                                          "last_symbol": symbol, "checkpoint_reused": reused})
        if progress:
            progress({"stage": "S3", "evidence_completed": number, "evidence_total": len(candidates),
                      "symbol": symbol, "checkpoint_reused": reused})
    result = select_evidence(candidates, bundles, config)
    result["summary"]["source_status"] = {"flow": [batch["status"] for batch in batches["flow"]],
                                          "margin": [batch["status"] for batch in batches["margin"]],
                                          "dragon_tiger": batches["dragon"]["status"]}
    return result, references
