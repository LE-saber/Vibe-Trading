import json

import pytest

from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.review_schema import validate_review


PACKET = {"symbol": "300750.SZ", "fields": {"technical.features.momentum_5d": .02, "margin.coverage": 0., "margin.net": None}}


def response():
    return {"symbol": "300750.SZ", "verdict": "KEEP", "strengths": ["动量为正[technical.features.momentum_5d]"],
            "risks": [], "evidence_conflicts": [], "missing_critical_info": [], "summary": "保留研究资格[technical.features.momentum_5d]"}


def test_valid_schema():
    value = response()
    assert validate_review(json.dumps(value), PACKET) == value


def test_multiple_citations_are_each_checked_without_relaxing_grounding():
    value = response()
    value["summary"] = "综合复核[technical.features.momentum_5d, margin.coverage]"
    assert validate_review(json.dumps(value), PACKET) == value
    value["summary"] = "综合复核[technical.features.momentum_5d, invented.field]"
    with pytest.raises(PipelineError):
        validate_review(json.dumps(value), PACKET)


def test_explicit_fields_container_prefix_still_requires_exact_provided_key():
    value = response()
    value["summary"] = "综合复核[fields.technical.features.momentum_5d, fields.margin.coverage]"
    assert validate_review(json.dumps(value), PACKET) == value
    for path in ("fields.invented", "fields.fields.margin.coverage", "other.margin.coverage"):
        value["summary"] = f"综合复核[{path}]"
        with pytest.raises(PipelineError):
            validate_review(json.dumps(value), PACKET)


@pytest.mark.parametrize("fault", ["new_symbol", "score", "tool_calls", "quantity", "forecast", "unknown_citation", "no_citation",
                                   "only_missing", "exclude_without_reason", "keep_without_strength", "non_string", "duplicate", "markdown"])
def test_ungrounded_or_unauthorized_output_is_rejected(fault):
    value = response()
    if fault == "new_symbol":
        value["symbol"] = "600519.SH"
    elif fault in {"score", "tool_calls"}:
        value[fault] = 42
    elif fault == "quantity":
        value["summary"] = "买入一百股，目标股数明确[technical.features.momentum_5d]"
    elif fault == "forecast":
        value["summary"] = "未来收益很好[technical.features.momentum_5d]"
    elif fault == "unknown_citation":
        value["summary"] = "趋势良好[unknown.field]"
    elif fault == "no_citation":
        value["summary"] = "趋势良好"
    elif fault == "only_missing":
        value["strengths"] = ["融资增长[margin.net]"]
    elif fault == "exclude_without_reason":
        value["verdict"] = "EXCLUDE"
    elif fault == "keep_without_strength":
        value["strengths"] = []
    elif fault == "non_string":
        value["risks"] = [123]
    raw = json.dumps(value)
    if fault == "duplicate":
        raw = raw[:-1] + ', "verdict":"KEEP"}'
    if fault == "markdown":
        raw = '```json\n' + raw + '\n```'
    with pytest.raises(PipelineError):
        validate_review(raw, PACKET)


def test_missing_operational_input_supports_review_not_fake_fact():
    value = response()
    value.update(verdict="REVIEW", strengths=[], missing_critical_info=["融资覆盖缺失[margin.net]"], summary="等待补充信息[margin.net]")
    assert validate_review(json.dumps(value), PACKET)["verdict"] == "REVIEW"


@pytest.mark.parametrize("prose", ["比率为三十二点九九", "评分为七十二", "评分高于七十", "涨幅百分之三"])
def test_chinese_numbers_cannot_bypass_qualitative_only_protocol(prose):
    value = response()
    value["summary"] = prose + "[technical.features.momentum_5d]"
    with pytest.raises(PipelineError):
        validate_review(json.dumps(value), PACKET)


def test_period_names_and_qualitative_direction_are_not_numeric_claims():
    value = response()
    value["summary"] = "五日动量为正；二十日均线仍需另查[technical.features.momentum_5d]"
    assert validate_review(json.dumps(value), PACKET) == value


def test_structured_field_children_are_resolved_without_accepting_new_roots():
    value = response()
    value["missing_critical_info"] = ["新闻快照缺失[events.news.items]"]
    value["summary"] = "等待新闻补充[events.news.items]"
    packet = {"symbol": PACKET["symbol"], "fields": PACKET["fields"] | {"events.news": {"items": None}}}
    assert validate_review(json.dumps(value), packet) == value


def test_verified_empty_symbol_news_can_be_reported_as_missing_information():
    value = response()
    value["missing_critical_info"] = ["没有匹配新闻[events.news.items]"]
    value["summary"] = "等待相关新闻补充[events.news.items]"
    packet = {"symbol": PACKET["symbol"], "fields": PACKET["fields"] | {
        "events.news": {"items": [], "status": "EMPTY_FOR_SYMBOL"}
    }}
    assert validate_review(json.dumps(value), packet) == value


def test_stage_labels_are_not_treated_as_numeric_quantities():
    value = response()
    value["summary"] = "S4阶段仅保留研究资格[technical.features.momentum_5d]"
    assert validate_review(json.dumps(value), PACKET) == value
