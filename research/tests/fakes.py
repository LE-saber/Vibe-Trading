from __future__ import annotations

import pandas as pd


class FakeTushare:
    def __init__(self, *, suspend_error: Exception | None = None) -> None:
        self.suspend_error = suspend_error
        self.daily_trade_dates: list[str] = []
        self.suspend_trade_dates: list[str] = []

    def trade_cal(self, **_kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"exchange": "SSE", "cal_date": "20260828", "is_open": 1, "pretrade_date": "20260827"},
                {"exchange": "SSE", "cal_date": "20260831", "is_open": 1, "pretrade_date": "20260828"},
            ]
        )

    def stock_basic(self, **_kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            [
                basic("600000.SH", "浦发银行", "主板", "SSE", "19991110"),
                basic("300750.SZ", "宁德时代", "创业板", "SZSE", "20180611"),
                basic("688001.SH", "华兴源创", "科创板", "SSE", "20190722"),
                basic("000001.SZ", "ST平安", "主板", "SZSE", "19910403"),
                basic("000002.SZ", "测试退", "主板", "SZSE", "19910129"),
                basic("000003.SZ", "停牌股份", "主板", "SZSE", "19910129"),
                basic("000004.SZ", "无行情股", "主板", "SZSE", "19910129"),
                basic("920001.BJ", "北交股票", "北交所", "BSE", "20200101"),
                basic("510300.SH", "沪深ETF", "ETF", "SSE", "20120528"),
                basic("600999.SH", "未来上市", "主板", "SSE", "20260901"),
            ]
        )

    def daily(self, **kwargs) -> pd.DataFrame:
        self.daily_trade_dates.append(kwargs["trade_date"])
        if "close" in kwargs.get("fields", ""):
            return pd.DataFrame(
                [
                    {"ts_code": code, "trade_date": kwargs["trade_date"], "close": close}
                    for code, close in (("600000.SH", 10.0), ("300750.SZ", 20.0), ("688001.SH", 30.0))
                ]
            )
        return pd.DataFrame(
            [
                daily("600000.SH", 100_000.0, 500_000.0),
                daily("300750.SZ", 200_000.0, 600_000.0),
                daily("688001.SH", 300_000.0, 700_000.0),
                daily("000001.SZ", 40_000.0, 80_000.0),
                daily("000002.SZ", 50_000.0, 90_000.0),
                daily("000003.SZ", 60_000.0, 100_000.0),
            ]
        )

    def daily_basic(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"ts_code": "600000.SH", "trade_date": kwargs["trade_date"], "turnover_rate": 1.0, "volume_ratio": 0.8},
                {"ts_code": "300750.SZ", "trade_date": kwargs["trade_date"], "turnover_rate": 2.0, "volume_ratio": 1.2},
                {"ts_code": "688001.SH", "trade_date": kwargs["trade_date"], "turnover_rate": 3.0, "volume_ratio": 1.5},
            ]
        )

    def suspend_d(self, **kwargs) -> pd.DataFrame:
        self.suspend_trade_dates.append(kwargs["trade_date"])
        if self.suspend_error:
            raise self.suspend_error
        return pd.DataFrame(
            [{"ts_code": "000003.SZ", "trade_date": "20260831", "suspend_type": "S"}]
        )


class LargeCompetitionClient:
    """Frozen 5,216/5,007 legacy migration fixture."""

    def __init__(self) -> None:
        sz = [f"{number:06d}.SZ" for number in range(1, 3001)]
        sh = (
            [f"{number:06d}.SH" for number in range(600001, 601000)]
            + [f"{number:06d}.SH" for number in range(601001, 602000)]
            + [f"{number:06d}.SH" for number in range(603001, 603219)]
        )
        self.symbols = sz + sh
        self.eligible_symbols = self.symbols[:5007]
        self.open_dates = [
            "20260803", "20260804", "20260805", "20260806", "20260807",
            "20260810", "20260811", "20260812", "20260813", "20260814",
            "20260817", "20260818", "20260819", "20260820", "20260821",
            "20260824", "20260825", "20260826", "20260827", "20260828",
            "20260831", "20260901",
        ]

    def trade_cal(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"exchange": "SSE", "cal_date": trade_date, "is_open": 1, "pretrade_date": ""}
                for trade_date in self.open_dates
                if kwargs["start_date"] <= trade_date <= kwargs["end_date"]
            ]
        )

    def stock_basic(self, **_kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            [
                basic(
                    symbol,
                    f"测试{symbol[:6]}",
                    "主板",
                    "SZSE" if symbol.endswith(".SZ") else "SSE",
                    "20000101",
                )
                for symbol in self.symbols
            ]
        )

    def daily(self, **kwargs) -> pd.DataFrame:
        trade_date = kwargs["trade_date"]
        if "amount" in kwargs.get("fields", ""):
            return pd.DataFrame(
                [
                    {
                        "ts_code": symbol,
                        "trade_date": trade_date,
                        "amount": float(index + 1),
                        "vol": float((index + 1) * 10),
                    }
                    for index, symbol in enumerate(self.eligible_symbols)
                ]
            )
        day_index = self.open_dates.index(trade_date)
        return pd.DataFrame(
            [
                {
                    "ts_code": symbol,
                    "trade_date": trade_date,
                    "close": 10.0 + index * 0.001 + day_index * (index % 17) * 0.01,
                }
                for index, symbol in enumerate(self.eligible_symbols)
            ]
        )

    def daily_basic(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "ts_code": symbol,
                    "trade_date": kwargs["trade_date"],
                    "turnover_rate": float(index % 100) / 10.0,
                    "volume_ratio": 0.5 + float(index % 50) / 10.0,
                }
                for index, symbol in enumerate(self.eligible_symbols)
            ]
        )

    def adj_factor(self, **kwargs) -> pd.DataFrame:
        # Test-only unity factors keep this legacy fixture's momentum available.
        trade_date = kwargs["trade_date"]
        return pd.DataFrame(
            [
                {
                    "ts_code": symbol,
                    "trade_date": trade_date,
                    "adj_factor": 1.0,
                }
                for symbol in self.eligible_symbols
            ]
        )

    def suspend_d(self, **_kwargs) -> pd.DataFrame:
        return pd.DataFrame(columns=["ts_code", "trade_date", "suspend_type"])


def basic(ts_code: str, name: str, market: str, exchange: str, list_date: str) -> dict:
    return {
        "ts_code": ts_code,
        "symbol": ts_code.split(".")[0],
        "name": name,
        "market": market,
        "exchange": exchange,
        "list_status": "L",
        "list_date": list_date,
        "delist_date": None,
    }


def daily(ts_code: str, amount: float, volume: float) -> dict:
    return {"ts_code": ts_code, "trade_date": "20260831", "amount": amount, "vol": volume}
