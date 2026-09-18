from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from competition_core.config import load_config
from competition_core.universe import CompetitionUniverse
from competition_core.universe.ranking import _momentum_by_symbol, screen_competition_universe
from tests.fakes import FakeTushare


AS_OF = date(2026, 9, 1)
TRADE_DATES = [
    "20260825",
    "20260826",
    "20260827",
    "20260828",
    "20260831",
    "20260901",
]
SYMBOL = "300750.SZ"


class QfqMomentumClient:
    def __init__(
        self,
        *,
        factors: dict[str, float] | None = None,
        factor_date_overrides: dict[str, str] | None = None,
        daily_date_overrides: dict[str, str] | None = None,
        include_future_factor: bool = False,
    ) -> None:
        self.closes = {
            "20260825": 100.0,
            "20260826": 101.0,
            "20260827": 99.0,
            "20260828": 98.0,
            "20260831": 52.0,
            "20260901": 55.0,
        }
        self.factors = factors or {trade_date: 1.0 for trade_date in TRADE_DATES}
        self.factor_date_overrides = factor_date_overrides or {}
        self.daily_date_overrides = daily_date_overrides or {}
        self.include_future_factor = include_future_factor
        self.trade_cal_calls: list[dict] = []
        self.daily_calls: list[dict] = []
        self.adj_factor_calls: list[dict] = []

    def trade_cal(self, **kwargs) -> pd.DataFrame:
        self.trade_cal_calls.append(kwargs)
        return pd.DataFrame(
            [
                {
                    "exchange": "SSE",
                    "cal_date": trade_date,
                    "is_open": 1,
                    "pretrade_date": "",
                }
                for trade_date in TRADE_DATES + ["20260902", "20260903"]
            ]
        )

    def daily(self, **kwargs) -> pd.DataFrame:
        self.daily_calls.append(kwargs)
        requested_date = kwargs["trade_date"]
        if requested_date not in self.closes:
            return pd.DataFrame(columns=["ts_code", "trade_date", "close"])
        return pd.DataFrame(
            [
                {
                    "ts_code": SYMBOL,
                    "trade_date": self.daily_date_overrides.get(
                        requested_date, requested_date
                    ),
                    "close": self.closes[requested_date],
                }
            ]
        )

    def adj_factor(self, **kwargs) -> pd.DataFrame:
        self.adj_factor_calls.append(kwargs)
        requested_date = kwargs["trade_date"]
        rows = []
        if requested_date in self.factors:
            rows.append(
                {
                    "ts_code": SYMBOL,
                    "trade_date": self.factor_date_overrides.get(
                        requested_date, requested_date
                    ),
                    "adj_factor": self.factors[requested_date],
                }
            )
        if self.include_future_factor:
            rows.append(
                {
                    "ts_code": SYMBOL,
                    "trade_date": "20260902",
                    "adj_factor": 999.0,
                }
            )
        return pd.DataFrame(
            rows, columns=["ts_code", "trade_date", "adj_factor"]
        )


def test_qfq_momentum_uses_adjusted_return_across_300750_ex_right_day() -> None:
    client = QfqMomentumClient(
        factors={
            **{trade_date: 1.0 for trade_date in TRADE_DATES[:4]},
            "20260831": 2.0,
            "20260901": 2.0,
        }
    )

    result = _momentum_by_symbol(client, AS_OF, max_window=5)

    # The raw close return is -45%; qfq maps 100 before the split to 50.
    assert result[SYMBOL]["momentum_5d"] == pytest.approx(0.10)
    assert result[SYMBOL]["momentum_5d"] != pytest.approx(55.0 / 100.0 - 1.0)


def test_as_of_cuts_daily_and_future_adjustment_rows() -> None:
    client = QfqMomentumClient(
        factors={trade_date: 1.0 for trade_date in TRADE_DATES},
        include_future_factor=True,
    )
    client.factors["20260831"] = 2.0
    client.factors["20260901"] = 2.0

    result = _momentum_by_symbol(client, AS_OF, max_window=5)

    assert result[SYMBOL]["momentum_5d"] == pytest.approx(0.10)
    assert client.trade_cal_calls[0]["end_date"] == "20260901"
    assert all(call["trade_date"] <= "20260901" for call in client.daily_calls)
    assert all(
        call["trade_date"] <= "20260901" for call in client.adj_factor_calls
    )
    assert "20260902" not in {call["trade_date"] for call in client.daily_calls}
    assert "20260902" not in {
        call["trade_date"] for call in client.adj_factor_calls
    }


@pytest.mark.parametrize("failure", ["missing", "nan", "inf", "misaligned"])
def test_invalid_or_incomplete_adjustment_data_fails_closed(failure: str) -> None:
    factors = {trade_date: 1.0 for trade_date in TRADE_DATES}
    factor_date_overrides = {}
    if failure == "missing":
        del factors["20260831"]
    elif failure == "nan":
        factors["20260831"] = float("nan")
    elif failure == "inf":
        factors["20260831"] = float("inf")
    else:
        factor_date_overrides["20260831"] = "20260830"

    client = QfqMomentumClient(
        factors=factors, factor_date_overrides=factor_date_overrides
    )

    result = _momentum_by_symbol(client, AS_OF, max_window=5)

    assert result[SYMBOL]["momentum_5d"] is None


def test_daily_date_mismatch_also_fails_closed_without_raw_fallback() -> None:
    client = QfqMomentumClient(
        daily_date_overrides={"20260831": "20260830"},
        factors={trade_date: 1.0 for trade_date in TRADE_DATES},
    )

    result = _momentum_by_symbol(client, AS_OF, max_window=5)

    assert result[SYMBOL]["momentum_5d"] is None


def test_missing_adj_factor_endpoint_fails_closed() -> None:
    class NoAdjFactorClient(QfqMomentumClient):
        adj_factor = None

    client = NoAdjFactorClient()

    result = _momentum_by_symbol(client, AS_OF, max_window=5)

    assert result[SYMBOL]["momentum_5d"] is None


def test_unavailable_momentum_is_not_returned_as_fake_ranked_candidates() -> None:
    class RankingClient(QfqMomentumClient):
        adj_factor = None

        def stock_basic(self, **_kwargs) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {
                        "ts_code": SYMBOL,
                        "symbol": "300750",
                        "name": "宁德时代",
                        "market": "创业板",
                        "exchange": "SZSE",
                        "list_status": "L",
                        "list_date": "20180611",
                        "delist_date": None,
                    }
                ]
            )

        def daily(self, **kwargs) -> pd.DataFrame:
            if "amount" in kwargs.get("fields", ""):
                return pd.DataFrame(
                    [
                        {
                            "ts_code": SYMBOL,
                            "trade_date": kwargs["trade_date"],
                            "amount": 100_000.0,
                            "vol": 600_000.0,
                        }
                    ]
                )
            return super().daily(**kwargs)

        def daily_basic(self, **kwargs) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {
                        "ts_code": SYMBOL,
                        "trade_date": kwargs["trade_date"],
                        "turnover_rate": 2.0,
                        "volume_ratio": 1.2,
                    }
                ]
            )

        def suspend_d(self, **_kwargs) -> pd.DataFrame:
            return pd.DataFrame(columns=["ts_code", "trade_date", "suspend_type"])

    result = screen_competition_universe(
        client=RankingClient(),
        config=load_config(),
        as_of="2026-09-02",
        phase="preopen",
        sort_by="momentum_5d",
        limit=1,
    )

    assert result["returned"] == 0
    assert result["rows"] == []
    assert "missing_momentum=1" in result["warnings"]


def test_universe_volume_remains_raw_lots() -> None:
    snapshot = CompetitionUniverse(FakeTushare(), load_config()).build(
        "2026-08-31", "postclose"
    )

    member = next(item for item in snapshot.members if item.ts_code == SYMBOL)

    assert member.recent_volume_lots == 600_000.0
