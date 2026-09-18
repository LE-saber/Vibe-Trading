"""Tushare-backed, point-in-time Shanghai/Shenzhen A-share Universe service."""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd

from competition_core.config import CompetitionConfig
from competition_core.pipeline.errors import PipelineError
from competition_core.universe.models import (
    Board,
    CompetitionUniverseSnapshot,
    UniverseMember,
    UniverseSource,
)
from contracts.symbol import CanonicalSymbol, SymbolContractError

_STOCK_BASIC_FIELDS = "ts_code,symbol,name,market,exchange,list_status,list_date,delist_date"
_DAILY_FIELDS = "ts_code,trade_date,amount,vol"
_SUSPEND_FIELDS = "ts_code,trade_date,suspend_type,suspend_timing"
_MARKET_TO_BOARD = {
    "主板": Board.MAIN,
    "中小板": Board.MAIN,
    "创业板": Board.CHINEXT,
    "科创板": Board.STAR,
}
_BOARD_CONFIG_KEY = {
    Board.MAIN: "main",
    Board.CHINEXT: "chinext",
    Board.STAR: "star",
}
_CANONICAL_BOARD = {
    Board.MAIN: "MAIN",
    Board.CHINEXT: "CHINEXT",
    Board.STAR: "STAR",
}


class CompetitionUniverse:
    """Build one deterministic Competition Universe snapshot."""

    def __init__(self, client: Any, config: CompetitionConfig) -> None:
        self.client = client
        self.config = config

    def build(self, as_of: date | str, phase: str | None = None) -> CompetitionUniverseSnapshot:
        snapshot_date = _parse_date(as_of)
        if not self.config.start_date <= snapshot_date <= self.config.end_date:
            raise ValueError("snapshot date is outside the configured competition period")
        snapshot_phase = self.config.snapshot_phase if phase is None else phase
        if snapshot_phase not in {"preopen", "postclose"}:
            raise ValueError("snapshot phase must be preopen or postclose")

        snapshot_trade_date = snapshot_date.strftime("%Y%m%d")
        calendar_start = (snapshot_date - timedelta(days=31)).strftime("%Y%m%d")
        calendar = self.client.trade_cal(
            exchange="SSE",
            start_date=calendar_start,
            end_date=snapshot_trade_date,
            fields="exchange,cal_date,is_open,pretrade_date",
        )
        reference_date = _market_data_reference_date(
            calendar, snapshot_date, snapshot_phase
        )
        reference_trade_date = reference_date.strftime("%Y%m%d")

        basic = self.client.stock_basic(
            exchange="", list_status="L", fields=_STOCK_BASIC_FIELDS
        )
        daily = self.client.daily(
            trade_date=reference_trade_date, fields=_DAILY_FIELDS
        )
        if basic is None or basic.empty:
            raise RuntimeError("tushare.stock_basic returned no listed stocks")
        if daily is None or daily.empty:
            raise RuntimeError(f"tushare.daily returned no bars for {reference_trade_date}")

        warnings: list[str] = []
        suspension_source = "tushare.suspend_d"
        try:
            suspended_frame = self.client.suspend_d(
                trade_date=snapshot_trade_date,
                suspend_type="S",
                fields=_SUSPEND_FIELDS,
            )
            suspended_codes = _code_set(suspended_frame)
        except Exception as exc:  # noqa: BLE001 - recorded provider degradation
            if snapshot_phase == "preopen":
                raise RuntimeError(
                    "suspend_d is required for a preopen snapshot because prior-day "
                    "daily data cannot determine current-day suspension"
                ) from exc
            suspended_frame = None
            suspended_codes = set()
            suspension_source = "tushare.daily_presence"
            warnings.append(
                f"suspend_d unavailable ({type(exc).__name__}); suspension derived "
                "from absence in tushare.daily"
            )

        daily_rows = _daily_by_code(daily)
        members: list[UniverseMember] = []
        for row in basic.to_dict("records"):
            member = self._member_from_row(
                row=row,
                as_of=snapshot_date,
                reference_date=reference_date,
                daily_row=daily_rows.get(str(row.get("ts_code", "")).upper()),
                suspended_codes=suspended_codes,
                suspension_source=suspension_source,
                suspension_derived=suspended_frame is None,
            )
            if member is not None:
                members.append(member)
        members.sort(key=lambda item: item.ts_code)

        sources = (
            UniverseSource(
                "tushare.stock_basic",
                tuple(_STOCK_BASIC_FIELDS.split(",")),
                (("exchange", ""), ("list_status", "L")),
            ),
            UniverseSource(
                "tushare.trade_cal",
                ("exchange", "cal_date", "is_open", "pretrade_date"),
                (
                    ("exchange", "SSE"),
                    ("start_date", calendar_start),
                    ("end_date", snapshot_trade_date),
                ),
            ),
            UniverseSource(
                "tushare.daily",
                tuple(_DAILY_FIELDS.split(",")),
                (("trade_date", reference_trade_date),),
            ),
            UniverseSource(
                suspension_source,
                tuple(_SUSPEND_FIELDS.split(",")) if suspended_frame is not None else (),
                (("trade_date", snapshot_trade_date),),
            ),
        )
        return CompetitionUniverseSnapshot(
            competition_id="cn-a-share-2026-09",
            as_of=snapshot_date,
            snapshot_phase=snapshot_phase,
            market_data_reference_date=reference_date,
            members=tuple(members),
            sources=sources,
            warnings=tuple(warnings),
        )

    def _member_from_row(
        self,
        *,
        row: dict[str, Any],
        as_of: date,
        reference_date: date,
        daily_row: dict[str, Any] | None,
        suspended_codes: set[str],
        suspension_source: str,
        suspension_derived: bool,
    ) -> UniverseMember | None:
        ts_code = str(row.get("ts_code", "")).upper().strip()
        exchange = str(row.get("exchange", "")).upper().strip()
        board = _MARKET_TO_BOARD.get(str(row.get("market", "")).strip())
        if exchange not in self.config.exchanges or board is None:
            return None
        try:
            identity = CanonicalSymbol.from_tushare(ts_code, exchange=exchange)
        except SymbolContractError as exc:
            raise RuntimeError(
                f"stock_basic identity conflict for {ts_code or '<empty>'}: {exc}"
            ) from exc
        if identity.board != _CANONICAL_BOARD[board]:
            raise RuntimeError(
                f"stock_basic board conflict for {ts_code}: "
                f"market={board.value}, code={identity.board}"
            )

        list_status = str(row.get("list_status", "")).upper().strip()
        if list_status != "L":
            return None
        list_date = _parse_tushare_date(row.get("list_date"))
        if list_date is None or list_date > as_of:
            return None
        delist_date = _parse_tushare_date(row.get("delist_date"))

        name = str(row.get("name", "")).strip()
        compact_name = name.upper().replace(" ", "")
        is_st = "ST" in compact_name
        is_delisting_risk = "退市" in name or name.endswith("退") or delist_date is not None
        has_daily_bar = daily_row is not None
        is_suspended = ts_code in suspended_codes or (
            suspension_derived and not has_daily_bar
        )
        amount = _finite_float(daily_row.get("amount")) if daily_row else None
        volume = _finite_float(daily_row.get("vol")) if daily_row else None
        is_tradeable = has_daily_bar and not is_suspended and amount is not None and amount > 0

        exclusions = self.config.universe_exclusions
        reasons: list[str] = []
        if exclusions["st"] and is_st:
            reasons.append("st")
        if exclusions["delisting_risk"] and is_delisting_risk:
            reasons.append("delisting_risk")
        if exclusions["suspended"] and is_suspended:
            reasons.append("suspended")
        if exclusions["untradeable"] and not is_tradeable:
            reasons.append("untradeable")

        board_key = _BOARD_CONFIG_KEY[board]
        rule = self.config.board_rules[board_key]
        return UniverseMember(
            ts_code=ts_code,
            name=name,
            exchange=exchange,
            board=board,
            list_date=list_date,
            delist_date=delist_date,
            list_status=list_status,
            is_st=is_st,
            is_delisting_risk=is_delisting_risk,
            is_suspended=is_suspended,
            is_tradeable=is_tradeable,
            is_eligible=not reasons,
            exclusion_reasons=tuple(reasons),
            recent_amount_cny=amount * 1000.0 if amount is not None else None,
            recent_volume_lots=volume,
            liquidity_date=reference_date if daily_row is not None else None,
            trading_rule_key=board.value,
            normal_price_limit=float(rule["normal_price_limit"]),
            new_listing_special_sessions=int(rule["new_listing_special_sessions"]),
            field_sources=(
                ("identity", "tushare.stock_basic"),
                ("listing_status", "tushare.stock_basic"),
                ("suspension", suspension_source),
                ("tradeability", "tushare.daily"),
                ("liquidity", "tushare.daily"),
                ("trading_rule", "competition.config"),
            ),
        )


def _parse_date(value: date | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def _parse_tushare_date(value: Any) -> date | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip().replace("-", "")
    if not text or text.lower() in {"nan", "nat", "none"}:
        return None
    return datetime.strptime(text, "%Y%m%d").date()


def _market_data_reference_date(
    calendar: pd.DataFrame | None, snapshot_date: date, phase: str
) -> date:
    if calendar is None or calendar.empty:
        raise RuntimeError("tushare.trade_cal returned no calendar rows")
    if not {"cal_date", "is_open"}.issubset(calendar.columns):
        raise RuntimeError("tushare.trade_cal response is missing cal_date/is_open")
    open_dates = sorted(
        parsed
        for row in calendar.to_dict("records")
        if int(row.get("is_open", 0)) == 1
        if (parsed := _parse_tushare_date(row.get("cal_date"))) is not None
    )
    if snapshot_date not in open_dates:
        raise PipelineError("NON_TRADING_DAY", "research date is not an open SSE trading day")
    if phase == "postclose":
        return snapshot_date
    previous = [item for item in open_dates if item < snapshot_date]
    if not previous:
        raise RuntimeError("no completed trading day is available before snapshot date")
    return previous[-1]


def _finite_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _code_set(frame: pd.DataFrame | None) -> set[str]:
    if frame is None or frame.empty or "ts_code" not in frame.columns:
        return set()
    return {str(value).upper() for value in frame["ts_code"].dropna()}


def _daily_by_code(frame: pd.DataFrame | None) -> dict[str, dict[str, Any]]:
    if frame is None or frame.empty or "ts_code" not in frame.columns:
        return {}
    return {
        str(row["ts_code"]).upper(): row
        for row in frame.to_dict("records")
        if row.get("ts_code")
    }
