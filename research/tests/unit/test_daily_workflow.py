from datetime import datetime, time

import pytest
import yaml

from competition_core.daily_workflow import (
    DailyWorkflowConfigError,
    DecisionState,
    RiskAction,
    WorkflowAction,
    evaluate_holiday_gap_risk,
    load_daily_workflow_config,
    validate_transition,
)
from competition_core.pipeline.time_context import SHANGHAI


def _at(hour: int, minute: int) -> datetime:
    return datetime(2026, 9, 22, hour, minute, tzinfo=SHANGHAI)


def _write_config(tmp_path, policy: dict) -> object:
    path = tmp_path / "daily-workflow.yaml"
    path.write_text(yaml.safe_dump(policy, sort_keys=False), encoding="utf-8")
    return path


def test_schedule_exposes_terminal_decision_and_freeze_points():
    policy = load_daily_workflow_config()
    assert policy.timezone == "Asia/Shanghai"
    assert policy.preflight_start == time(8, 45)
    assert policy.preopen_start == time(9, 0)
    assert policy.preopen_end == time(9, 25)
    assert policy.refresh_at == time(13, 15)
    assert policy.primary_decision == time(13, 30)
    assert policy.final_revision == time(14, 10)
    assert policy.main_order_deadline == time(14, 25)
    assert policy.risk_freeze == time(14, 35)
    assert policy.ordinary_order_cutoff == time(14, 57)
    assert policy.postclose_start == time(15, 5)
    # Existing read-only preopen workflow uses these semantic aliases.
    schedule = policy.to_dict()["schedule"]
    assert schedule["refresh_at"] == "13:15"
    assert schedule["primary_decision"] == "13:30"
    assert schedule["risk_freeze"] == "14:35"
    assert "execution_target" not in schedule


def test_phase_boundaries_and_timezone_are_preserved():
    policy = load_daily_workflow_config()
    assert policy.phase_at(_at(8, 44)) == "BEFORE_PREFLIGHT"
    assert policy.phase_at(_at(8, 45)) == "PREFLIGHT"
    assert policy.phase_at(_at(8, 59)) == "PREFLIGHT"
    assert policy.phase_at(_at(9, 0)) == "PREOPEN"
    assert policy.phase_at(_at(9, 25)) == "PREOPEN"
    assert policy.phase_at(_at(9, 26)) == "INTRADAY_AM"
    assert policy.phase_at(_at(12, 0)) == "MIDDAY_BREAK"
    assert policy.phase_at(_at(13, 15)) == "INTRADAY_PM"
    assert policy.phase_at(_at(14, 58)) == "ORDER_CUTOFF"
    assert policy.phase_at(_at(15, 5)) == "POSTCLOSE"
    assert policy.risk_action_at(_at(14, 24)) is RiskAction.NORMAL_RISK
    assert policy.risk_action_at(_at(14, 25)) is RiskAction.REDUCE_ONLY
    assert policy.risk_action_at(_at(14, 35)) is RiskAction.FROZEN
    assert policy.risk_action_at(_at(14, 45)) is RiskAction.FROZEN
    assert policy.risk_action_at(_at(15, 5)) is RiskAction.POSTCLOSE
    with pytest.raises(DailyWorkflowConfigError, match="timezone-aware"):
        policy.phase_at(datetime(2026, 9, 22, 13, 15))


def test_decision_state_machine_requires_reconciliation_before_close():
    assert validate_transition(DecisionState.APPROVED, DecisionState.SENDING) is DecisionState.SENDING
    assert validate_transition("SENDING", "RECONCILIATION_REQUIRED") is DecisionState.RECONCILIATION_REQUIRED
    assert validate_transition("RECONCILIATION_REQUIRED", "RECONCILED") is DecisionState.RECONCILED
    with pytest.raises(DailyWorkflowConfigError, match="unsafe"):
        validate_transition("SENDING", "CLOSED")


def test_risk_gate_exact_boundaries_and_verification_checkpoint():
    policy = load_daily_workflow_config()

    before_deadline = policy.risk_action_at(_at(14, 24), WorkflowAction.SUBMIT_APPROVED_MAIN, approved=True)
    assert before_deadline.allowed is True
    assert before_deadline.can_add_risk is True
    assert before_deadline.execution_ready is False

    at_deadline = policy.risk_action_at(_at(14, 25), WorkflowAction.SUBMIT_APPROVED_MAIN, approved=True)
    assert at_deadline.allowed is False
    assert at_deadline.can_add_risk is False
    assert policy.risk_action_at(_at(14, 25), WorkflowAction.REPAIR).allowed is True
    assert policy.risk_action_at(_at(14, 34), WorkflowAction.DE_RISK).allowed is True
    assert policy.risk_action_at(_at(14, 30), WorkflowAction.CANCEL).allowed is False

    at_freeze = policy.risk_action_at(_at(14, 35), WorkflowAction.SUBMIT_APPROVED_MAIN, approved=True)
    assert at_freeze.allowed is False
    assert at_freeze.window == "RISK_FREEZE"
    assert policy.risk_action_at(_at(14, 35), WorkflowAction.DE_RISK).allowed is True

    at_verify = policy.risk_action_at(_at(14, 45), WorkflowAction.VERIFY)
    assert at_verify.allowed is True
    assert at_verify.window == "RISK_FREEZE"
    assert at_verify.decision_point is False
    assert policy.risk_action_at(_at(14, 45), WorkflowAction.SUBMIT_APPROVED_MAIN, approved=True).allowed is False
    assert policy.risk_action_at(_at(14, 57), WorkflowAction.CANCEL).allowed is True
    assert policy.risk_action_at(_at(15, 0), WorkflowAction.SUBMIT_APPROVED_MAIN, approved=True).allowed is False


def test_manual_confirmation_is_required_before_main_order_submission():
    policy = load_daily_workflow_config()
    result = policy.risk_action_at(_at(13, 30), "SUBMIT_MAIN_ORDER", approved=False)
    assert result.allowed is False
    assert result.reason_codes == ("MANUAL_CONFIRM_REQUIRED",)
    assert policy.automatic_orders is False
    assert policy.unknown_policy == "AUTHORITATIVE_READBACK_NEVER_RETRY"


def test_old_execution_target_and_unknown_schedule_fields_are_rejected(tmp_path):
    policy = load_daily_workflow_config().to_dict()
    policy["schedule"]["execution_target"] = "14:50"
    path = _write_config(tmp_path, policy)
    with pytest.raises(DailyWorkflowConfigError, match="unknown fields"):
        load_daily_workflow_config(path)


def test_terminal_schedule_ordering_is_fail_closed(tmp_path):
    policy = load_daily_workflow_config().to_dict()
    policy["schedule"]["primary_decision"] = "14:40"
    path = _write_config(tmp_path, policy)
    with pytest.raises(DailyWorkflowConfigError, match="strictly ordered"):
        load_daily_workflow_config(path)


def test_timezone_and_naive_schedule_values_are_rejected(tmp_path):
    timezone_policy = load_daily_workflow_config().to_dict()
    timezone_policy["timezone"] = "UTC"
    with pytest.raises(DailyWorkflowConfigError, match="Asia/Shanghai"):
        load_daily_workflow_config(_write_config(tmp_path, timezone_policy))

    clock_policy = load_daily_workflow_config().to_dict()
    clock_policy["schedule"]["primary_decision"] = "13:30+08:00"
    with pytest.raises(DailyWorkflowConfigError, match="must not include a timezone"):
        load_daily_workflow_config(_write_config(tmp_path, clock_policy))


def test_holiday_gap_gate_requires_authoritative_calendar():
    unavailable = evaluate_holiday_gap_risk(authoritative_calendar=False)
    assert unavailable.available is False
    assert unavailable.holiday_gap_risk is None
    assert unavailable.reason_codes == ("AUTHORITATIVE_TRADING_CALENDAR_REQUIRED",)
    incomplete = evaluate_holiday_gap_risk(authoritative_calendar=True)
    assert incomplete.available is False
    assert incomplete.reason_codes == ("CROSS_SESSION_STATUS_REQUIRED",)
    assert evaluate_holiday_gap_risk(authoritative_calendar=True, cross_session=True).holiday_gap_risk is True
