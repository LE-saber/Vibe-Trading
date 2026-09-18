"""Independent per-stock, budgeted AI reviews of an already committed S4 artifact."""

from concurrent.futures import ThreadPoolExecutor, as_completed
import copy
from datetime import datetime, timezone
from pathlib import Path
import re
import time
import uuid

from competition_core.pipeline.artifacts import atomic_json, content_hash, file_hash, read_json
from competition_core.pipeline.config import PipelineConfig
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.persistence import PipelineRepository
from competition_core.pipeline.review_schema import validate_review
from competition_core.pipeline.time_context import ResearchContext
from contracts.evidence import EvidenceBundle

ROOT = Path(__file__).resolve().parents[2]


def _fingerprint(vibe_root):
    own = ["competition_core/pipeline/reviews.py", "competition_core/pipeline/review_schema.py",
           "competition_core/pipeline/research_packet.py", "competition_core/pipeline/artifacts.py",
           "competition_core/pipeline/config.py", "competition_core/pipeline/time_context.py",
           "competition_core/pipeline/persistence.py", "competition_core/storage/evidence.py",
           "adapters/vibe/review.py", "adapters/ai/openai_compatible.py",
           "scripts/vibe-review-worker.py", "contracts/evidence.py", "contracts/symbol.py"]
    return {path: file_hash(ROOT / path) for path in own}


def _seal(path, value):
    atomic_json(path, value | {"integrity_hash": content_hash(value)}, immutable=True)


def _read_sealed(path):
    value = read_json(path)
    expected = value.pop("integrity_hash", None)
    if content_hash(value) != expected:
        raise PipelineError("STALE_DATA", "AI review checkpoint hash mismatch")
    return value


def _validate_payload(payload, packet, runtime):
    if (payload.get("ok") is not True or payload.get("runtime") != runtime
            or payload.get("packet_hash") != packet["packet_hash"] or payload.get("tool_calls")
            or payload.get("invalid_tool_calls") or payload.get("finish_reason") in {"length", "content_filter", "tool_calls"}):
        raise PipelineError("AI_INVALID_RESPONSE", "AI response runtime/packet/tool contract is invalid")
    reported = payload.get("response_model")
    allowed = {runtime["model"], runtime["model"].split("/")[-1], f"{runtime['provider']}/{runtime['model']}"}
    if reported is not None and reported not in allowed:
        raise PipelineError("AI_INVALID_RESPONSE", "provider response model differs from configured model")
    return validate_review(payload.get("content"), packet)


def _retry_feedback(attempts):
    if not attempts:
        return ""
    error = attempts[-1].get("error") or {}
    field = (error.get("details") or {}).get("field")
    if field not in {"symbol", "verdict", "strengths", "risks", "evidence_conflicts", "missing_critical_info", "summary"}:
        field = "JSON"
    # Only our validator's whitelisted field name enters the model prompt.
    # Never echo untrusted model output or potentially sensitive provider errors.
    message = error.get("message")
    hints = {
        "review claim must cite supplied fields": "summary及每条陈述都必须有引用；方括号内逐字填fields已有键，不得无引用或写自然语言",
        "review prose must be qualitative, without forecasts or quantities": "正文删阿拉伯/中文数字及“为/高于/低于/超过/不足/达到/百分之+数词”；引用键保持原样，改定性词，S4/S5.5可留",
        "missing-information claim cites only available values": "缺失只引null、coverage<1或空新闻；非空不算缺失",
    }
    hint = hints.get(message, "按系统约束使用fields引用")
    return (f"上次{field}校验失败。返回七键JSON；{hint}；新闻用events.news；"
            "其他陈述引非空值，不加字段。")


def _review_one(*, packet, runtime, root, identity_hash, policy, vibe_root, invoker):
    directory = root / "reviews" / packet["symbol"]
    directory.mkdir(parents=True, exist_ok=True)
    checkpoint = directory / "checkpoint.json"
    identity = {"input_hash": identity_hash, "packet_hash": packet["packet_hash"], "symbol": packet["symbol"]}
    if checkpoint.exists():
        stored = _read_sealed(checkpoint)
        if any(stored.get(key) != value for key, value in identity.items()):
            raise PipelineError("STALE_DATA", "review checkpoint belongs to different inputs")
        for attempt in stored["attempts"]:
            response_path = directory / f"attempt-{attempt['attempt']}" / "response.json"
            if attempt.get("response_hash") and file_hash(response_path) != attempt["response_hash"]:
                raise PipelineError("STALE_DATA", "review raw response changed after checkpoint")
        if stored["status"] == "SUCCESS":
            payload = read_json(directory / f"attempt-{stored['successful_attempt']}" / "response.json")
            if _validate_payload(payload, packet, runtime) != stored["review"]:
                raise PipelineError("STALE_DATA", "review checkpoint differs from validated raw response")
        return stored, True
    attempts, accepted, successful = [], None, None
    for number in range(1, policy["max_attempts"] + 1):
        attempt_dir = directory / f"attempt-{number}"
        attempt_dir.mkdir(parents=True, exist_ok=True)
        start, response, result = attempt_dir / "started.json", attempt_dir / "response.json", attempt_dir / "result.json"
        if result.exists():
            attempt = _read_sealed(result)
            if attempt["packet_hash"] != packet["packet_hash"] or attempt["attempt"] != number:
                raise PipelineError("STALE_DATA", "AI attempt identity mismatch")
            if attempt.get("response_hash") and file_hash(response) != attempt["response_hash"]:
                raise PipelineError("STALE_DATA", "AI attempt response hash mismatch")
            if attempt["status"] == "SUCCESS":
                accepted = _validate_payload(read_json(response), packet, runtime)
                successful = number
            attempts.append(attempt)
            if accepted is not None:
                break
            continue
        attempt = {"attempt": number, "packet_hash": packet["packet_hash"], "status": "FAILED",
                   "error": None, "response_hash": None, "llm_calls_confirmed": 0, "llm_call_count_unknown": True}
        try:
            if response.exists():
                payload = read_json(response)  # Recover durable response after a controller interruption.
            elif start.exists():
                # The interrupted request may already have reached the provider.
                # Consume that budget slot; never silently resend the same attempt.
                raise PipelineError("AI_ATTEMPT_INTERRUPTED", "previous request has no durable response")
            else:
                atomic_json(start, identity | {"attempt": number, "started_at": datetime.now(timezone.utc).isoformat()}, immutable=True)
                payload = invoker(packet=packet, runtime=runtime, directory=attempt_dir, vibe_root=vibe_root,
                                  timeout_seconds=policy["timeout_seconds"], packet_max_chars=policy["packet_max_chars"],
                                  reasoning_effort=policy["reasoning_effort"], max_output_tokens=policy["max_output_tokens"],
                                  retry_feedback=_retry_feedback(attempts))
                atomic_json(response, payload, immutable=True)
            attempt["llm_calls_confirmed"] = int(payload.get("llm_calls", 0))
            attempt["llm_call_count_unknown"] = "llm_calls" not in payload
            accepted = _validate_payload(payload, packet, runtime)
            successful, attempt["status"] = number, "SUCCESS"
        except PipelineError as exc:
            if exc.code == "STALE_DATA":
                raise
            attempt["error"] = exc.to_dict()
        except Exception as exc:
            attempt["error"] = {"code": "AI_PROVIDER_ERROR", "exception_type": type(exc).__name__}
        if response.exists():
            attempt["response_hash"] = file_hash(response)
        _seal(result, attempt)
        attempts.append(attempt)
        if accepted is not None:
            break
    record = identity | {"status": "SUCCESS" if accepted is not None else "FAILED", "review": accepted,
                         "successful_attempt": successful, "attempts": attempts}
    _seal(checkpoint, record)
    return record, False


def run_holding_review_attempt(*, repository: PipelineRepository, attempt_id: str, candidate: dict,
                               bundle: EvidenceBundle, context: ResearchContext, config: PipelineConfig,
                               holding_source_hash: str, news_context: dict, invoker=None, probe=None):
    """Persist one isolated review of an existing holding without changing S4/S5.

    The caller must build the holding EvidenceBundle from frozen S1 inputs first.
    This function deliberately does not alter portfolio blockers or map review
    verdicts to rebalance actions.
    """
    from adapters.vibe.review import effective_runtime, invoke_review, probe_vibe
    from competition_core.pipeline.research_packet import build_research_packet

    if not isinstance(attempt_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,95}", attempt_id):
        raise PipelineError("INVALID_ARGUMENTS", "invalid holding review attempt ID")
    if not isinstance(holding_source_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", holding_source_hash):
        raise PipelineError("INVALID_ARGUMENTS", "holding source hash is invalid")
    if not isinstance(bundle, EvidenceBundle) or not isinstance(candidate, dict):
        raise PipelineError("INVALID_ARGUMENTS", "holding candidate and Evidence are required")
    if candidate.get("symbol") != bundle.symbol.symbol:
        raise PipelineError("IDENTITY_CONFLICT", "holding candidate and Evidence identity differ")
    if candidate.get("evidence_id") != bundle.evidence_id:
        raise PipelineError("STALE_DATA", "holding Evidence is not bound to this run")

    invoker, probe = invoker or invoke_review, probe or probe_vibe
    with repository.lock():
        inputs = read_json(repository.directory / "inputs.json")
        if (content_hash({key: value for key, value in inputs.items() if key != "specification_hash"})
                != inputs.get("specification_hash")
                or repository.state().get("specification_hash") != inputs.get("specification_hash")
                or bundle.snapshot_id != repository.state().get("snapshot_id")):
            raise PipelineError("STALE_DATA", "frozen research input specification changed")
        if config.config_hash != inputs.get("config_hash") or context.to_dict() != inputs.get("context"):
            raise PipelineError("STALE_DATA", "holding review context/configuration differs from the run")
        stages = {stage: repository.load_stage(stage) for stage in ("S1", "S4", "S5")}
        if any(value is None for value in stages.values()):
            raise PipelineError("NOT_READY", "holding review requires committed S1, S4 and S5")
        if any(stage.get("config_hash") != inputs["config_hash"] or stage.get("snapshot_id") != inputs["snapshot_id"]
               or any(stage.get(key) != value for key, value in inputs["context"].items())
               for stage in stages.values()):
            raise PipelineError("STALE_DATA", "holding review stages differ from the frozen run")
        symbol = candidate["symbol"]
        if symbol in stages["S4"]["output_symbols"] or symbol not in {row["symbol"] for row in stages["S1"]["rows"]}:
            raise PipelineError("IDENTITY_CONFLICT", "supplemental review is only for S1 holdings outside S4")
        runtime = effective_runtime(probe(directory=repository.directory / "holding-review-preflight" / uuid.uuid4().hex,
                                          vibe_root=Path(inputs["vibe_root"])), config.section("ai"))
        packet = build_research_packet(candidate=candidate, bundle=bundle, context=context, config=config,
                                       news_context=news_context, research_role="existing_holding")
        code_hashes = _fingerprint(Path(inputs["vibe_root"]))
        specification = {
            "schema_version": "p10.holding_review_attempt.v1", "attempt_id": attempt_id,
            "run_id": repository.run_id, "symbol": symbol, "context": context.to_dict(),
            "snapshot_id": inputs["snapshot_id"], "config_hash": config.config_hash,
            "holding_source_hash": holding_source_hash, "evidence_id": bundle.evidence_id,
            "packet_hash": packet["packet_hash"], "news_context_hash": news_context.get("snapshot_hash"),
            "stage_hashes": {stage: value["artifact_hash"] for stage, value in stages.items()},
            "runtime": runtime, "code_hashes": code_hashes,
        }
        root = repository.directory / "holding-review-attempts" / attempt_id
        atomic_json(root / "inputs.json", specification, immutable=True)
        atomic_json(root / "evidence.json", bundle.to_dict(), immutable=True)
        atomic_json(root / "packet.json", packet, immutable=True)
        atomic_json(root / "news-context.json", news_context, immutable=True)
        identity_hash = content_hash(specification)
        record, _ = _review_one(packet=packet, runtime=runtime, root=root, identity_hash=identity_hash,
                                policy=config.section("ai"), vibe_root=Path(inputs["vibe_root"]), invoker=invoker)
        if code_hashes != _fingerprint(Path(inputs["vibe_root"])):
            raise PipelineError("STALE_DATA", "AI review code changed while running")
        result = {"schema_version": "p10.holding_review_result.v1", "attempt_id": attempt_id,
                  "run_id": repository.run_id, "symbol": symbol, "research_role": "existing_holding",
                  "status": record["status"], "review": record["review"], "packet_hash": packet["packet_hash"],
                  "input_hash": identity_hash, "holding_source_hash": holding_source_hash,
                  "s4_unchanged": True, "portfolio_authority": False}
        _seal(root / "result.json", result)
        return result


def run_holding_reviews(*, repository: PipelineRepository, account, account_source_hash: str,
                        snapshot: dict, panel, sessions: list[str], context: ResearchContext,
                        config: PipelineConfig, source_client, source_name: str, news_context: dict,
                        invoker=None, probe=None, progress=None):
    """Review all S1-covered holdings outside S4 from frozen run inputs only.

    Existing holdings are an independent research scope. They may be present
    in the frozen S1 rows without having survived the S1 Top-N output; that
    must not turn the supplemental review into a new buy-selection path.
    """
    from competition_core.pipeline.account import AccountSnapshot
    from competition_core.pipeline.dated_cache import DatedSourceCache
    from competition_core.pipeline.evidence_builder import build_holding_evidence, fetch_supplements

    if not isinstance(account, AccountSnapshot) or not isinstance(account_source_hash, str):
        raise PipelineError("INVALID_ARGUMENTS", "frozen account input is required for holding reviews")
    if not re.fullmatch(r"[0-9a-f]{64}", account_source_hash):
        raise PipelineError("INVALID_ARGUMENTS", "account source hash is invalid")
    stages = {stage: repository.load_stage(stage) for stage in ("S1", "S4", "S5")}
    if any(value is None for value in stages.values()):
        raise PipelineError("NOT_READY", "holding reviews require committed S1, S4 and S5")
    s4_symbols = set(stages["S4"]["output_symbols"])
    positions = [position for position in account.positions if position.symbol not in s4_symbols]
    s1_by = {row["symbol"]: row for row in stages["S1"]["rows"]}
    members = {row["ts_code"]: row for row in snapshot.get("members", []) if isinstance(row, dict) and "ts_code" in row}
    reviewable = [position for position in positions
                  if position.symbol in s1_by and position.symbol in members]
    blockers = [f"MISSING_HOLDING_RESEARCH_COVERAGE:{position.symbol}"
                for position in positions if position not in reviewable]
    batches = None
    if reviewable:
        cache = DatedSourceCache(repository.directory / "sources", source_client, source_name=source_name)
        try:
            batches = fetch_supplements(cache, sessions[-5:], cache_only=True)
        except PipelineError as exc:
            if exc.code != "NOT_READY":
                raise
            blockers.extend(f"MISSING_HOLDING_RESEARCH_COVERAGE:{position.symbol}" for position in reviewable)
            reviewable = []
    records = []
    for position in reviewable:
        candidate = copy.deepcopy(s1_by[position.symbol])
        bundle = build_holding_evidence(candidate=candidate, member=members[position.symbol], panel=panel,
            sessions=sessions, context=context, snapshot_id=snapshot["snapshot_id"], source=source_name,
            batches=batches)
        candidate["evidence_id"] = bundle.evidence_id
        holding_source_hash = content_hash({"account_hash": account.snapshot_hash,
            "account_source_hash": account_source_hash, "position": position.to_dict()})
        attempt_key = content_hash({"run_id": repository.run_id, "symbol": position.symbol,
            "holding_source_hash": holding_source_hash, "evidence_id": bundle.evidence_id,
            "news_context_hash": news_context.get("snapshot_hash"),
            "s5_hash": stages["S5"]["artifact_hash"]})
        result = run_holding_review_attempt(repository=repository, attempt_id=f"holding-{attempt_key[:32]}",
            candidate=candidate, bundle=bundle, context=context, config=config,
            holding_source_hash=holding_source_hash, news_context=news_context,
            invoker=invoker, probe=probe)
        records.append({key: result[key] for key in ("attempt_id", "symbol", "status", "input_hash", "packet_hash")})
        if result["status"] != "SUCCESS":
            blockers.append(f"MISSING_HOLDING_RESEARCH_COVERAGE:{position.symbol}")
        if progress:
            progress({"stage": "S5", "holding_review_completed": len(records),
                      "holding_review_total": len(reviewable), "symbol": position.symbol})
    result = {"schema_version": "p10.holding_review_index.v1", "run_id": repository.run_id,
              "account_hash": account.snapshot_hash, "account_source_hash": account_source_hash,
              "input_symbols": [position.symbol for position in positions],
              "reviewed_symbols": [record["symbol"] for record in records if record["status"] == "SUCCESS"],
              "records": records, "blockers": sorted(set(blockers)), "portfolio_authority": False}
    digest = content_hash(result)
    atomic_json(repository.directory / "holding-review-indexes" / f"{digest}.json", result, immutable=True)
    atomic_json(repository.directory / "holding-review-current.json", {"index_hash": digest})
    return result


def run_reviews(*, repository: PipelineRepository, evidence_store=None, invoker=None, probe=None,
                progress=None, news_context=None):
    from datetime import date
    from adapters.vibe.review import effective_runtime, invoke_review, probe_vibe
    from competition_core.pipeline.research_packet import build_research_packet
    from competition_core.storage.evidence import EvidenceRepository

    invoker, probe = invoker or invoke_review, probe or probe_vibe
    evidence_store = evidence_store or EvidenceRepository()
    with repository.lock():
        inputs = read_json(repository.directory / "inputs.json")
        if (content_hash({key: value for key, value in inputs.items() if key != "specification_hash"}) != inputs.get("specification_hash")
                or repository.state()["specification_hash"] != inputs.get("specification_hash")):
            raise PipelineError("STALE_DATA", "frozen research input specification changed")
        config = PipelineConfig.from_dict(inputs["config"])
        policy = config.section("ai")
        data = inputs["context"]
        context = ResearchContext(date.fromisoformat(data["research_date"]), data["phase"],
            date.fromisoformat(data["market_data_reference_date"]), datetime.fromisoformat(data["as_of"]))
        vibe_root = Path(inputs["vibe_root"])
        stages = {stage: repository.load_stage(stage) for stage in ("S0", "S1", "S2", "S3", "S4")}
        if any(value is None for value in stages.values()) or stages["S4"]["status"] != "SUCCESS" or not 10 <= len(stages["S4"]["output_symbols"]) <= 15:
            raise PipelineError("NOT_READY", "AI review requires a completed ten-to-fifteen-name S4 artifact")
        if (config.config_hash != inputs["config_hash"] or any(
                stage.get("config_hash") != inputs["config_hash"] or stage.get("snapshot_id") != inputs["snapshot_id"]
                or any(stage.get(key) != value for key, value in data.items()) for stage in stages.values())):
            raise PipelineError("STALE_DATA", "AI inputs differ from committed stage context/configuration")
        if file_hash(inputs["manifest_path"]) != inputs["manifest_hash"]:
            raise PipelineError("STALE_DATA", "dataset manifest changed before AI review")
        runtime = effective_runtime(probe(directory=repository.directory / "review-preflight" / uuid.uuid4().hex, vibe_root=vibe_root), policy)
        code_hashes = _fingerprint(vibe_root)
        specification = {"context": data, "config_hash": inputs["config_hash"], "runtime": runtime,
                         "stage_hashes": {stage: value["artifact_hash"] for stage, value in stages.items()}, "code_hashes": code_hashes,
                         "news_context_hash": news_context.get("snapshot_hash") if news_context is not None else None}
        identity_hash = content_hash(specification)
        if news_context is not None:
            atomic_json(repository.directory / "news-context.json", news_context, immutable=True)
        atomic_json(repository.directory / "review-inputs.json", specification, immutable=True)
        previous = repository.recover_checkpoint("S5")
        if previous is not None:
            return repository.update(status="PARTIAL", completed_through="S5", pending_stage="S5_5", error=None)
        candidates = {row["symbol"]: row for row in stages["S4"]["rows"]}
        packets, references = {}, []
        for symbol in stages["S4"]["output_symbols"]:
            candidate = candidates[symbol]
            bundle = evidence_store.get(candidate["evidence_id"])
            local = read_json(repository.directory / "evidence" / f"{bundle.evidence_id}.json")
            if local != bundle.to_dict() or bundle.snapshot_id != inputs["snapshot_id"]:
                raise PipelineError("STALE_DATA", "Core Evidence differs from frozen S3 evidence")
            packet = build_research_packet(candidate=candidate, bundle=bundle, context=context, config=config,
                                           news_context=news_context)
            path = repository.directory / "packets" / f"{symbol}.json"
            atomic_json(path, packet, immutable=True)
            packets[symbol] = packet
            references.append({"path": f"packets/{symbol}.json", "sha256": file_hash(path)})
        repository.start_stage("S5")
        started, records = time.monotonic(), {}
        try:
            with ThreadPoolExecutor(max_workers=policy["concurrency"]) as pool:
                futures = {pool.submit(_review_one, packet=packet, runtime=runtime, root=repository.directory,
                            identity_hash=identity_hash, policy=policy, vibe_root=vibe_root, invoker=invoker): symbol
                           for symbol, packet in packets.items()}
                for future in as_completed(futures):
                    symbol = futures[future]
                    record, reused = future.result()
                    records[symbol] = record
                    event = {"stage": "S5", "completed": len(records), "total": len(packets), "symbol": symbol,
                             "status": record["status"], "checkpoint_reused": reused}
                    repository.update(stage_progress=event)
                    if progress:
                        progress(event)
            if code_hashes != _fingerprint(vibe_root):
                raise PipelineError("STALE_DATA", "AI review code changed while running")
            rows = []
            for symbol in stages["S4"]["output_symbols"]:
                record = records[symbol]
                review = record["review"]
                verdict = review["verdict"] if review else "REVIEW"
                row = copy.deepcopy(candidates[symbol])
                row.update(selected=True, ai_review=review, ai_status=record["status"], ai_verdict=verdict,
                           ai_eligible=record["status"] == "SUCCESS" and verdict in {"KEEP", "WATCH"},
                           review_origin="model" if review else "system_failure", packet_hash=packets[symbol]["packet_hash"],
                           keep_reasons=["carried_for_review_audit_not_order_approval"],
                           reject_reasons=[] if review else ["ai_review_not_validated"])
                rows.append(row)
                folder = repository.directory / "reviews" / symbol
                for path in sorted(folder.rglob("*.json")):
                    references.append({"path": path.relative_to(repository.directory).as_posix(), "sha256": file_hash(path)})
            attempts = [attempt for record in records.values() for attempt in record["attempts"]]
            result = {"stage": "S5", "status": "SUCCESS" if all(record["status"] == "SUCCESS" for record in records.values()) else "PARTIAL",
                      "input_symbols": stages["S4"]["output_symbols"], "output_symbols": stages["S4"]["output_symbols"], "rows": rows,
                      "summary": {"input_count": len(rows), "reviewed_count": sum(row["ai_status"] == "SUCCESS" for row in rows),
                                  "failed_count": sum(row["ai_status"] != "SUCCESS" for row in rows),
                                  "verdict_counts": {verdict: sum(row["ai_verdict"] == verdict for row in rows) for verdict in ("KEEP", "WATCH", "EXCLUDE", "REVIEW")},
                                  "output_semantics": "original_finalists_with_review_states_not_buy_approval",
                                  "eligible_symbols": [row["symbol"] for row in rows if row["ai_eligible"]],
                                  "llm_calls_confirmed": sum(attempt["llm_calls_confirmed"] for attempt in attempts),
                                  "llm_calls_unknown_attempts": sum(attempt["llm_call_count_unknown"] for attempt in attempts),
                                  "attempts": len(attempts), "agent_tool_calls": 0, "runtime": runtime}}
            references.append({"path": "review-inputs.json", "sha256": file_hash(repository.directory / "review-inputs.json")})
            if news_context is not None:
                references.append({"path": "news-context.json", "sha256": file_hash(repository.directory / "news-context.json")})
            metadata = data | {"snapshot_id": inputs["snapshot_id"], "config_hash": inputs["config_hash"], "source_provenance": references}
            repository.commit_stage("S5", result, metadata=metadata, elapsed=time.monotonic() - started)
            return repository.update(status="PARTIAL", completed_through="S5", pending_stage="S5_5", error=None)
        except Exception as exc:
            repository.update(status="FAILED", error=exc.to_dict() if isinstance(exc, PipelineError) else {"code": "AI_REVIEW_FAILED", "exception_type": type(exc).__name__})
            raise
