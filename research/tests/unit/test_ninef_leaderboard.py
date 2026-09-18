from datetime import datetime

import pytest

from adapters.ninef.account import NinefAccountAdapterError
from adapters.ninef.leaderboard import fetch_ninef_leaderboard
from competition_core.pipeline.time_context import SHANGHAI


NOW = datetime(2026, 9, 4, 10, 30, tzinfo=SHANGHAI)


def requester(calls, *, own_rank=88):
    def read(token, name, params=None):
        calls.append((name, dict(params or {})))
        if name == "competitions":
            return {"code": 0, "data": {"items": [{"competitionId": "competition-fixture"}]}}
        rows = [{"userId": f"p-{rank}", "displayName": f"匿名{rank}", "rank": rank,
                 "metricText": f"{40-rank / 10}%"} for rank in range(1, 51)]
        return {"code": 0, "data": {"leaderboard": {"updatedAt": "2026-09-04T02:00:00+00:00",
            "items": rows, "currentUser": {"userId": "private-owner", "displayName": "本人",
            "rank": own_rank, "metricText": "1.25%"}}}}
    return read


def test_top_30_and_own_rank_are_independent_and_identifiers_are_pseudonymized():
    calls = []
    board = fetch_ninef_leaderboard("secret-fixture", requester=requester(calls), clock=lambda: NOW)
    payload = board.to_dict()
    assert len(payload["entries"]) == 30 and payload["entries"][-1]["rank"] == 30
    assert payload["own"]["rank"] == 88
    assert payload["competition_id"] == "competition-fixture"
    assert "private-owner" not in str(payload)
    assert calls == [("competitions", {}), ("competition_status", {"competitionId": "competition-fixture"})]


def test_explicit_competition_id_avoids_ambiguous_discovery():
    calls = []
    board = fetch_ninef_leaderboard("secret-fixture", competition_id="selected",
                                    requester=requester(calls), clock=lambda: NOW)
    assert board.competition_id == "selected"
    assert [name for name, _ in calls] == ["competition_status"]


def test_optional_capital_and_risk_fields_are_preserved_when_provider_supplies_them():
    def read(token, name, params=None):
        row = {
            "userId": "owner", "displayName": "本人", "rank": 7, "metricText": "3.2%",
            "totalAssets": "1032000.50", "dayReturnPct": "1.1%", "tradeWinRate": "62.5%",
            "tradeCount": 12, "maxDrawdown": "-4.8%",
        }
        return {"code": 0, "data": {"leaderboard": {
            "updatedAt": "2026-09-04T02:00:00+00:00", "items": [row],
            "currentUser": row, "totalParticipants": 123,
        }}}
    board = fetch_ninef_leaderboard(
        "secret-fixture", competition_id="selected", requester=read, clock=lambda: NOW
    ).to_dict()
    assert board["own"]["total_assets"] == "1032000.50"
    assert board["own"]["day_return_pct"] == "1.1"
    assert board["own"]["trade_win_rate"] == "62.5"
    assert board["own"]["trade_count"] == 12
    assert board["total_participants"] == 123


def test_naive_provider_timestamp_is_shanghai_time_and_future_is_rejected():
    def read(timestamp):
        def response(token, name, params=None):
            row = {"userId": "one", "displayName": "匿名", "rank": 1, "metricText": "1%"}
            return {"code": 0, "data": {"leaderboard": {
                "updatedAt": timestamp, "items": [row], "currentUser": row}}}
        return response
    board = fetch_ninef_leaderboard("secret-fixture", competition_id="selected",
        requester=read("2026-09-04T10:20:00"), clock=lambda: NOW)
    assert board.as_of.isoformat() == "2026-09-04T10:20:00+08:00"
    with pytest.raises(NinefAccountAdapterError, match="future"):
        fetch_ninef_leaderboard("secret-fixture", competition_id="selected",
            requester=read("2026-09-04T10:31:00"), clock=lambda: NOW)


def test_ambiguous_competitions_and_duplicate_participants_fail_closed():
    def ambiguous(token, name, params=None):
        return {"code": 0, "data": {"items": [{"competitionId": "one"}, {"competitionId": "two"}]}}
    with pytest.raises(NinefAccountAdapterError, match="required"):
        fetch_ninef_leaderboard("secret-fixture", requester=ambiguous, clock=lambda: NOW)

    def duplicate(token, name, params=None):
        row = {"userId": "same", "displayName": "匿名", "rank": 1, "metricText": "1%"}
        return {"code": 0, "data": {"leaderboard": {"items": [row, row]}}}
    with pytest.raises(NinefAccountAdapterError, match="duplicate"):
        fetch_ninef_leaderboard("secret-fixture", competition_id="selected",
                                requester=duplicate, clock=lambda: NOW)
