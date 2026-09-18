"""Strict advisory review schema. No score, position or tool-call authority."""

import json
import re
from collections.abc import Mapping

from competition_core.pipeline.errors import PipelineError

KEYS = {"symbol", "verdict", "strengths", "risks", "evidence_conflicts", "missing_critical_info", "summary"}
LIST_FIELDS = ("strengths", "risks", "evidence_conflicts", "missing_critical_info")
VERDICTS = {"KEEP", "WATCH", "EXCLUDE", "REVIEW"}
CITATION = re.compile(r"\[([^\]]*)\]")
FORBIDDEN = re.compile(r"目标价|目标股数|目标仓位|胜率|预期收益|未来收益|预计盈利|保证盈利|target\s*(?:price|quantity|weight)|win\s*rate|expected\s*return", re.I)
_UNRESOLVED = object()
_STAGE_LABEL = re.compile(r"(?<![A-Za-z0-9_])S(?:[0-4]|5(?:[._]5)?|6)(?![A-Za-z0-9_])")


def _invalid_constant(value):
    raise ValueError("nonfinite JSON constant")


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _field_value(fields, path):
    """Resolve an exact packet field or a child of a structured field.

    Packets deliberately expose bounded top-level keys such as
    ``events.news``. A model may cite a child path for a structured value, but
    it may not invent a new root or use an arbitrary data source.
    """
    if path in fields:
        return fields[path]
    parts = path.split(".")
    for split in range(len(parts) - 1, 0, -1):
        prefix = ".".join(parts[:split])
        if prefix not in fields:
            continue
        value = fields[prefix]
        for part in parts[split:]:
            if not isinstance(value, Mapping) or part not in value:
                return _UNRESOLVED
            value = value[part]
        return value
    return _UNRESOLVED


def _is_missing(path, value):
    if value is None:
        return True
    if path == "events.news" or path.startswith("events.news."):
        if isinstance(value, (list, tuple, dict)) and not value:
            return True
        if (isinstance(value, Mapping) and value.get("status") in {"EMPTY_FOR_SYMBOL", "EMPTY_VERIFIED"}
                and not value.get("items")):
            return True
    return path.endswith(".coverage") and isinstance(value, (int, float)) and value < 1


def validate_review(raw: str, packet: dict) -> dict:
    """JSON only, canonical identity, grounded citations and qualitative prose.

    This cannot prove semantic truth. The source packet and raw response are
    retained for human inspection; it prevents structurally ungrounded approval.
    """
    try:
        value = json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
    except (ValueError, TypeError) as exc:
        raise PipelineError("AI_INVALID_RESPONSE", "review must be strict JSON without duplicate keys") from exc
    if not isinstance(value, dict) or set(value) != KEYS:
        raise PipelineError("AI_INVALID_RESPONSE", "review has missing or forbidden schema fields")
    if value["symbol"] != packet["symbol"] or not isinstance(value["verdict"], str) or value["verdict"] not in VERDICTS:
        raise PipelineError("AI_INVALID_RESPONSE", "review identity/verdict is invalid")
    fields = packet["fields"]
    for key in LIST_FIELDS:
        if not isinstance(value[key], list) or len(value[key]) > 8:
            raise PipelineError("AI_INVALID_RESPONSE", "review claims must be bounded lists", field=key)
    if not isinstance(value["summary"], str) or not value["summary"].strip():
        raise PipelineError("AI_INVALID_RESPONSE", "review needs a summary")
    for key in (*LIST_FIELDS, "summary"):
        texts = [value[key]] if key == "summary" else value[key]
        for text in texts:
            if not isinstance(text, str) or not text.strip() or len(text) > 400:
                raise PipelineError("AI_INVALID_RESPONSE", "review claim is empty or oversized", field=key)
            # `fields` is the packet's container, not an alternative data source.
            # Accept its explicit prefix only when the remaining exact key exists.
            references = [path.strip().removeprefix("fields.")
                          for group in CITATION.findall(text) for path in group.split(",")]
            if not references or any(not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.]*", path)
                                     or _field_value(fields, path) is _UNRESOLVED for path in references):
                raise PipelineError("AI_INVALID_RESPONSE", "review claim must cite supplied fields", field=key)
            if key in {"strengths", "risks", "evidence_conflicts"} and any(_field_value(fields, path) is None for path in references):
                raise PipelineError("AI_INVALID_RESPONSE", "factual review claim cites missing data", field=key)
            if key == "missing_critical_info" and not any(
                    _is_missing(path, _field_value(fields, path)) for path in references):
                raise PipelineError("AI_INVALID_RESPONSE", "missing-information claim cites only available values", field=key)
            prose = _STAGE_LABEL.sub("", CITATION.sub("", text).replace(packet["symbol"], ""))
            if (re.search(r"[0-9０-９]", prose) or FORBIDDEN.search(prose)
                    or re.search(r"[零〇一二三四五六七八九十百千万两]+(?:股|手|成仓)", prose)
                    or re.search(r"[零〇一二三四五六七八九十百千万两]+点[零〇一二三四五六七八九]+", prose)
                    or re.search(r"(?:为|高于|低于|超过|不足|达到|百分之)[零〇一二三四五六七八九十百千万两]+", prose)):
                raise PipelineError("AI_INVALID_RESPONSE", "review prose must be qualitative, without forecasts or quantities", field=key)
            if re.search(r"[0-9]{6}\.(?:SH|SZ|BJ)", prose):
                raise PipelineError("AI_INVALID_RESPONSE", "review introduced another security")
    if value["verdict"] == "KEEP" and not value["strengths"]:
        raise PipelineError("AI_INVALID_RESPONSE", "KEEP requires evidence-supported strengths")
    if value["verdict"] == "EXCLUDE" and not (value["risks"] or value["evidence_conflicts"]):
        raise PipelineError("AI_INVALID_RESPONSE", "EXCLUDE requires evidence-supported risks/conflicts")
    if value["verdict"] in {"WATCH", "REVIEW"} and not (value["risks"] or value["missing_critical_info"] or value["evidence_conflicts"]):
        raise PipelineError("AI_INVALID_RESPONSE", "uncertain verdict requires a reason")
    return value
