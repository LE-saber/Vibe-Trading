"""Deterministic shortlist constraints; scores are preserved, never AI-adjusted."""

from collections import Counter
import copy
from datetime import datetime
import math
from typing import Any

import pandas as pd

from competition_core.pipeline.config import PipelineConfig
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.time_context import ResearchContext
from contracts.symbol import CanonicalSymbol


def _number(value):
    return float(value) if not isinstance(value, bool) and isinstance(value, (float, int)) and math.isfinite(value) else None


def _sector(member: dict, context: ResearchContext) -> dict:
    """Optional future PIT metadata interface, not name-based industry inference.

    A snapshot may carry sector_metadata with value/classification/source/as_of.
    Current Universe does not; its board must NOT substitute for industry.
    """
    unavailable = {"value": None, "classification": None, "source": None, "as_of": None,
                   "status": "unavailable"}
    value = member.get("sector_metadata")
    if value is None:
        return unavailable
    if not isinstance(value, dict) or any(not isinstance(value.get(key), str) or not value[key].strip()
                                          for key in ("value", "classification", "source", "as_of")):
        return {**unavailable, "status": "unverified_metadata"}
    try:
        observed = datetime.fromisoformat(value["as_of"])
        if observed.utcoffset() is None or observed > context.as_of:
            raise ValueError("future or naive sector metadata")
    except ValueError as exc:
        raise PipelineError("STALE_DATA", "sector metadata requires a nonfuture aware as_of") from exc
    return {key: value[key] for key in ("value", "classification", "source", "as_of")} | {"status": "available"}


def select_finalists(*, candidates: list[dict[str, Any]], members: dict[str, dict],
                     correlation: dict, context: ResearchContext, config: PipelineConfig) -> dict:
    """Greedy, score-ordered constrained selection. Never relax constraints to pad."""
    symbols = [row["symbol"] for row in candidates]
    if len(symbols) != len(set(symbols)) or set(symbols) != set(correlation["symbols"]):
        raise PipelineError("IDENTITY_CONFLICT", "S4 correlation/candidate identities differ")
    cluster_by_symbol = {}
    for cluster in correlation["clusters"]:
        for symbol in cluster["symbols"]:
            if symbol in cluster_by_symbol:
                raise PipelineError("IDENTITY_CONFLICT", "duplicate S4 cluster member")
            cluster_by_symbol[symbol] = cluster["cluster_id"]
    if set(cluster_by_symbol) != set(symbols):
        raise PipelineError("IDENTITY_CONFLICT", "S4 cluster membership is incomplete")
    if correlation["reference_date"] != context.market_data_reference_date.isoformat():
        raise PipelineError("STALE_DATA", "S4 correlation reference date differs")
    policy, target = config.section("risk"), config.section("stages")["s4_target_n"]
    rows = []
    for candidate in candidates:
        symbol = candidate["symbol"]
        identity = CanonicalSymbol.from_core(symbol)
        member = members.get(symbol, {})
        if member.get("ts_code") != symbol or member.get("exchange") != identity.exchange or member.get("is_eligible") is not True:
            raise PipelineError("IDENTITY_CONFLICT", "S4 requires the frozen eligible identity", symbol=symbol)
        row = copy.deepcopy(candidate)
        sector = _sector(member, context)
        heat = _number(row.get("overheat_score"))
        score = _number(row.get("selection_score"))
        reasons = []
        if score is None:
            reasons.append("missing_s3_selection_score")
        if heat is None or not 0 <= heat <= 100:
            reasons.append("missing_or_invalid_overheat_score")
        row.update(s3_rank=row.get("rank"), rank=None, selected=False, sector_metadata=sector,
                   board_metadata={"board": member.get("board"), "exchange": member["exchange"],
                                   "source": member.get("field_sources", {}).get("identity")},
                   correlation_cluster=cluster_by_symbol[symbol],
                   overheated=heat >= policy["overheat_threshold"] if heat is not None else None,
                   style_metadata={"overheat_score": heat, "overheat_threshold": policy["overheat_threshold"],
                                   "inputs": {key: row.get("features", {}).get(key) for key in
                                              ("turnover_rate", "rsi_14", "volume_ratio", "momentum_5d", "distance_ma20")}},
                   reject_reasons=reasons, keep_reasons=[], correlation_to_selected={})
        rows.append(row)
    ranked = sorted(rows, key=lambda row: (-(_number(row.get("selection_score"))
                         if _number(row.get("selection_score")) is not None else -math.inf), row["symbol"]))
    clusters, sectors, boards = Counter(), Counter(), Counter()
    selected, hot_count = [], 0
    for order, row in enumerate(ranked, 1):
        row["rank"] = order
        symbol, cluster, sector = row["symbol"], row["correlation_cluster"], row["sector_metadata"]
        sector_key = (sector["source"], sector["classification"], sector["value"]) if sector["status"] == "available" else None
        comparisons = {other: correlation["correlation_matrix"][symbol][other] for other in selected}
        row["correlation_to_selected"] = comparisons
        # A missing self-correlation means too little history or constant returns.
        # An unknown pair cannot be declared independent and used for diversification.
        if correlation["correlation_matrix"][symbol][symbol] is None:
            row["reject_reasons"].append("insufficient_or_constant_return_history")
        if any(value is None for value in comparisons.values()):
            row["reject_reasons"].append("unknown_correlation_to_selected")
        if clusters[cluster] >= policy["max_candidates_per_cluster"]:
            row["reject_reasons"].append("correlation_cluster_cap")
        if sector_key is not None and sectors[sector_key] >= policy["max_candidates_per_sector"]:
            row["reject_reasons"].append("verified_sector_cap")
        if row["overheated"] and hot_count >= policy["max_overheated_candidates"]:
            row["reject_reasons"].append("overheated_style_cap")
        if len(selected) >= target:
            row["reject_reasons"].append(f"rank_below_constrained_top{target}")
        if row["reject_reasons"]:
            continue
        selected.append(symbol)
        row["selected"] = True
        row["keep_reasons"] = ["highest_remaining_s3_score_within_constraints", "correlation_cluster_within_cap",
                               "overheated_style_within_cap", "quant_and_s3_scores_preserved"]
        row["keep_reasons"].append("verified_sector_within_cap" if sector_key else "sector_unavailable_not_inferred")
        clusters[cluster] += 1
        if sector_key:
            sectors[sector_key] += 1
        boards[str(row["board_metadata"]["board"])] += 1
        hot_count += int(row["overheated"])
    naive = [row["symbol"] for row in ranked[:target]]
    return {"stage": "S4", "scoring_version": "diversification.v1",
            "status": "SUCCESS" if 10 <= len(selected) <= target else "PARTIAL",
            "input_symbols": sorted(symbols), "output_symbols": selected, "rows": sorted(rows, key=lambda row: row["symbol"]),
            "correlation": correlation,
            "summary": {"input_count": len(rows), "selected_count": len(selected), "requested_count": target,
                        "shortfall": max(0, target - len(selected)), "constraints_relaxed": False,
                        "naive_top_symbols": naive, "differs_from_naive_top": selected != naive,
                        "cluster_counts": {str(key): value for key, value in sorted(clusters.items())},
                        "sector_counts": [{"source": key[0], "classification": key[1], "value": key[2], "count": count}
                                          for key, count in sorted(sectors.items())],
                        "sector_unavailable_count": sum(row["sector_metadata"]["status"] != "available" for row in rows),
                        "selected_board_counts": dict(sorted(boards.items())), "selected_overheated_count": hot_count,
                        "rejection_counts": dict(sorted(Counter(reason for row in rows for reason in row["reject_reasons"]).items())),
                        "policy": {key: policy[key] for key in ("corr_threshold", "correlation_sessions", "min_pair_observations",
                                   "max_candidates_per_cluster", "max_candidates_per_sector", "max_overheated_candidates", "overheat_threshold")},
                        "llm_calls": 0, "agent_tool_calls": 0}}


def build_diversified_stage(*, s3: dict, snapshot: dict, panel: pd.DataFrame, sessions: list[str],
                            context: ResearchContext, config: PipelineConfig) -> dict:
    from competition_core.pipeline.correlation import build_return_correlation

    by_symbol = {row["symbol"]: row for row in s3["rows"]}
    candidates = [by_symbol[symbol] for symbol in s3["output_symbols"]]
    correlation = build_return_correlation(panel, symbols=s3["output_symbols"], sessions=sessions, context=context, config=config)
    return select_finalists(candidates=candidates, members={row["ts_code"]: row for row in snapshot["members"]},
                            correlation=correlation, context=context, config=config)
