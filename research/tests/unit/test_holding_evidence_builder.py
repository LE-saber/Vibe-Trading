from copy import deepcopy

import pandas as pd
import pytest

from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.evidence_builder import build_holding_evidence
from tests.unit.test_pipeline_evidence_bundle import _candidate, _context, _member, _reference_daily, SNAPSHOT_ID


SESSIONS = ["2026-08-26", "2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01"]


def panel():
    reference = _reference_daily()
    return pd.DataFrame([reference | {"trade_date": day, "amount_cny": 456000.0} for day in SESSIONS])


def batches():
    def missing(endpoint, day):
        fields = ("ts_code,trade_date,buy_lg_amount,sell_lg_amount,buy_elg_amount,sell_elg_amount,net_mf_amount"
                  if endpoint == "moneyflow" else "ts_code,trade_date,rzye,rzmre,rzche,rqye,rqyl,rzrqye")
        return {"status": "MISSING", "data": [], "source": "tushare", "endpoint": endpoint,
                "request": {"trade_date": day.replace("-", ""), "fields": fields}}
    dragon = {"status": "MISSING", "data": [], "source": "tushare", "endpoint": "top_list",
              "request": {"trade_date": "20260901", "fields": "ts_code,trade_date,reason,net_amount,l_buy,l_sell"}}
    return {"flow": [missing("moneyflow", day) for day in SESSIONS],
            "margin": [missing("margin_detail", day) for day in SESSIONS], "dragon": dragon}


def build(**changes):
    values = {"candidate": _candidate(), "member": _member(), "panel": panel(), "sessions": SESSIONS,
              "context": _context(), "snapshot_id": SNAPSHOT_ID, "source": "tushare", "batches": batches()}
    values.update(changes)
    return build_holding_evidence(**values)


def test_frozen_s1_holding_build_is_deterministic_and_does_not_mutate_inputs():
    candidate, frame = _candidate(), panel()
    before_candidate, before_frame = deepcopy(candidate), frame.copy(deep=True)
    first = build(candidate=candidate, panel=frame)
    second = build(candidate=candidate, panel=frame)

    assert first.evidence_id == second.evidence_id
    assert first.sections["market"]["prices"]["close_raw"] == 100.0
    assert next(item.source for item in first.provenance if item.field == "technical.quant.quant_score") == "competition_pipeline.s1_holding"
    assert candidate == before_candidate
    pd.testing.assert_frame_equal(frame, before_frame)


@pytest.mark.parametrize("change,match", [
    ({"sessions": SESSIONS[:-1]}, "five frozen"),
    ({"sessions": SESSIONS[:-1] + ["2026-09-02"]}, "five frozen"),
    ({"member": _member() | {"ts_code": "600000.SH"}}, "differ"),
])
def test_missing_dates_future_cutoff_and_identity_conflict_fail_closed(change, match):
    with pytest.raises(PipelineError, match=match):
        build(**change)


def test_missing_reference_daily_fails_instead_of_using_account_price():
    frame = panel().loc[lambda value: value.trade_date.ne(SESSIONS[-1])]
    with pytest.raises(PipelineError, match="reference daily"):
        build(panel=frame)


def test_future_panel_row_is_not_silently_ignored():
    frame = pd.concat([panel(), pd.DataFrame([_reference_daily() | {"trade_date": "2026-09-02"}])], ignore_index=True)
    with pytest.raises(PipelineError, match="future date"):
        build(panel=frame)
