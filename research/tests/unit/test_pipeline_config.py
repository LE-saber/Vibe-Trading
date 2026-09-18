import copy

import pytest

from competition_core.pipeline.config import PipelineConfig, load_pipeline_config
from competition_core.pipeline.errors import PipelineError


def test_defaults_match_v2_baseline_and_are_immutable():
    config = load_pipeline_config()
    assert config.section("stages") == dict(s1_top_n=300, s2_top_n=60, s3_top_n=20, s4_target_n=12)
    first_hash = config.config_hash
    changed = config.to_dict()
    changed["risk"]["cash_buffer"] = 0.3
    assert config.section("risk")["cash_buffer"] == 0.2
    assert config.config_hash == first_hash
    assert PipelineConfig.from_dict(changed).config_hash != first_hash


@pytest.mark.parametrize("section,key,value", [
    ("dataset", "warmup_sessions", 119), ("risk", "cash_buffer", -1),
    ("risk", "max_positions", 9), ("stages", "s4_target_n", 20),
    ("score", "trend", 0.5), ("ai", "packet_max_chars", 5001),
    ("portfolio", "execution_mode", "AUTO"), ("portfolio", "core_share", float("nan")),
    ("shadow", "qlib", "false"),
])
def test_unsafe_or_inconsistent_configuration_rejected(section, key, value):
    payload = copy.deepcopy(load_pipeline_config().to_dict())
    payload[section][key] = value
    with pytest.raises(PipelineError):
        PipelineConfig.from_dict(payload)


def test_frozen_pre_evidence_config_remains_readable_without_invented_weights():
    payload = load_pipeline_config().to_dict()
    payload.pop("evidence")
    payload["version"] = "v1.1"
    frozen = PipelineConfig.from_dict(payload)
    assert frozen.to_dict() == payload
    assert "evidence" not in frozen.to_dict()


@pytest.mark.parametrize("key,value", [("lookback_sessions", 4), ("quant_weight", 0),
                                      ("flow_weight", .9), ("minimum_data_confidence", -1)])
def test_invalid_evidence_policy_rejected(key, value):
    payload = load_pipeline_config().to_dict()
    payload["evidence"][key] = value
    with pytest.raises(PipelineError):
        PipelineConfig.from_dict(payload)


@pytest.mark.parametrize("key,value", [("reasoning_effort", "invalid"), ("reasoning_effort", []),
                                     ("max_output_tokens", 511), ("max_output_tokens", 16385),
                                     ("max_output_tokens", True)])
def test_invalid_ai_generation_policy_rejected(key, value):
    payload = load_pipeline_config().to_dict()
    payload["ai"][key] = value
    with pytest.raises(PipelineError):
        PipelineConfig.from_dict(payload)


def test_legacy_ai_config_is_not_silently_upgraded():
    payload = load_pipeline_config().to_dict()
    payload["ai"].pop("reasoning_effort")
    payload["ai"].pop("max_output_tokens")
    payload["version"] = "v1.2"
    assert PipelineConfig.from_dict(payload).to_dict() == payload
