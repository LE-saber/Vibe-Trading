from datetime import datetime, time

import pytest
import yaml

from competition_core.daily_workflow import (
    DailyWorkflowConfigError,
    DecisionState,
    load_daily_workflow_config,
    validate_transition,
)
from competition_core.pipeline.time_context import SHANGHAI


def test_schedule_preflights_before_0900_and_finishes_decision_with_buffer():
    policy = load_daily_workflow_config()
    assert policy.preflight_start == time(8, 45)
    assert policy.preopen_start == time(9, 0)
    assert policy.preopen_end == time(9, 25)
    assert policy.intraday_checkpoints[-3:] == (time(13, 35), time(14, 15), time(14, 35))
    assert policy.execution_target == time(14, 50)
    assert policy.ordinary_order_cutoff == time(14, 57)
    assert policy.execution_mode == "MANUAL_CONFIRM"
    assert policy.automatic_orders is False
    assert policy.unknown_policy == "AUTHORITATIVE_READBACK_NEVER_RETRY"
    assert policy.phase_at(datetime(2026, 9, 17, 8, 44, tzinfo=SHANGHAI)) == "BEFORE_PREFLIGHT"
    assert policy.phase_at(datetime(2026, 9, 17, 8, 45, tzinfo=SHANGHAI)) == "PREFLIGHT"
    assert policy.phase_at(datetime(2026, 9, 17, 8, 59, tzinfo=SHANGHAI)) == "PREFLIGHT"
    assert policy.phase_at(datetime(2026, 9, 17, 9, 0, tzinfo=SHANGHAI)) == "PREOPEN"
    assert policy.phase_at(datetime(2026, 9, 17, 14, 58, tzinfo=SHANGHAI)) == "ORDER_CUTOFF"
    assert policy.phase_at(datetime(2026, 9, 17, 15, 5, tzinfo=SHANGHAI)) == "POSTCLOSE"


def test_decision_state_machine_requires_reconciliation_before_close():
    assert validate_transition(DecisionState.APPROVED, DecisionState.SENDING) is DecisionState.SENDING
    assert validate_transition("SENDING", "RECONCILIATION_REQUIRED") is DecisionState.RECONCILIATION_REQUIRED
    assert validate_transition("RECONCILIATION_REQUIRED", "RECONCILED") is DecisionState.RECONCILED
    with pytest.raises(DailyWorkflowConfigError, match="unsafe"):
        validate_transition("SENDING", "CLOSED")


def test_unsafe_execution_policy_is_rejected(tmp_path):
    policy = load_daily_workflow_config().to_dict()
    policy["execution"]["mode"] = "AUTO"
    policy["execution"]["automatic_orders"] = True
    path = tmp_path / "daily-workflow.yaml"
    path.write_text(yaml.safe_dump(policy, sort_keys=False), encoding="utf-8")
    with pytest.raises(DailyWorkflowConfigError, match="manual-confirm"):
        load_daily_workflow_config(path)


def test_execution_target_must_leave_cutoff_buffer(tmp_path):
    policy = load_daily_workflow_config().to_dict()
    policy["schedule"]["execution_target"] = "14:57"
    path = tmp_path / "daily-workflow.yaml"
    path.write_text(yaml.safe_dump(policy, sort_keys=False), encoding="utf-8")
    with pytest.raises(DailyWorkflowConfigError, match="execution target"):
        load_daily_workflow_config(path)
