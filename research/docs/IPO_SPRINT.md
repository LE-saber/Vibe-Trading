# IPO Sprint Pool

For the competition workflow, operational checkpoints and Chinese command
examples, see [新股冲刺池：流程与使用手册](IPO_SPRINT_GUIDE_ZH.md).

The IPO sprint collector and evaluator form an independent, read-only route.
The collector builds a standalone `competition.ipo-sprint-snapshot.v1` JSON
file from Tushare identity/calendar/daily/announcement data and a bounded Sina
level-1 quote request. The evaluator writes one
`competition.ipo-sprint-result.v1` JSON file. It does not load S4, S5, S6 or
terminal-strategy candidates. Unknown fields are rejected, and the snapshot
must contain its own `stocks` array.

Each input stock carries its identity source, listing date, ordered trading
sessions, the explicit `UNBOUNDED`/`LIMITED`/`UNKNOWN` regime for each session,
the rule source and size of the unbounded listing window, sourced 1/3/5-day or
custom-window returns, sourced turnover amount/rate/volume ratio, a sourced
complete risk review, and a sourced fresh real-time quote with a two-sided
order book. The risk review must separately cover announcements, regulatory
issues, shareholder reductions, and earnings; each category has its own
`status`, `has_risk` boolean, and distinct source reference. The
evaluator derives the listing session number from the session sequence and
requires the quoted session to lie within the rule-sourced unbounded window.
It does not accept a numeric price-limit percentage as a substitute for the
explicit regime.

The collector performs a conservative title-based review over the complete
Tushare `anns_d` response for announcement, regulatory, reduction, and earnings
risk. Any matched risk title blocks the stock; unavailable or malformed
announcement data aborts collection rather than being treated as clear.

The top-level snapshot contains `schema_version`, `snapshot_id`, `stocks` and
`collection_summary`. The summary records all `new_share` rows scanned and any
pre-quote exclusions with reason codes. Each stock has `symbol`, `name`,
`listing_date`, `trading_sessions`,
`rule_evidence`, `identity_evidence`, `returns_pct`, `liquidity`,
`risk_review` and `realtime`. Dates use `YYYY-MM-DD`; all source and quote
timestamps must be ISO 8601 with a timezone. Each evidence source has
`source_name`, `source_ref` and `observed_at`. Return values are percentage
points (`15.0` means a 15% gain), with `null` used for unavailable standard
windows. `returns_pct` has `windows` and `source`. `liquidity` keeps current
intraday `turnover_amount` with its quote source separate from
`daily_basic_as_of_trade_date`, turnover rate, volume ratio, and their daily
source. `realtime` includes its quote fields and source.
A custom return window is `{ "sessions": 2, "return_pct": 30.0 }`. Symbols
must use canonical six-digit A-share spelling such as `688001.SH`, `300001.SZ`
or `920001.BJ`.

```json
{
  "schema_version": "competition.ipo-sprint-snapshot.v1",
  "snapshot_id": "feed-20260923-001",
  "stocks": [{
    "symbol": "688001.SH",
    "name": "示例新股",
    "listing_date": "2026-09-22",
    "trading_sessions": [
      {"trade_date": "2026-09-22", "limit_regime": "UNBOUNDED"},
      {"trade_date": "2026-09-23", "limit_regime": "UNBOUNDED"}
    ],
    "rule_evidence": {
      "unbounded_session_count": 5,
      "source": {"source_name": "exchange-rule", "source_ref": "rule-123", "observed_at": "2026-09-23T12:00:00+08:00"}
    },
    "identity_evidence": {"source_name": "listing-feed", "source_ref": "ipo-123", "observed_at": "2026-09-22T08:00:00+08:00"},
    "returns_pct": {
      "windows": {"1d": 18.0, "3d": null, "5d": null, "available_window": {"sessions": 2, "return_pct": 31.0}},
      "source": {"source_name": "return-feed", "source_ref": "return-123", "observed_at": "2026-09-23T13:29:00+08:00"}
    },
    "liquidity": {
      "turnover_amount": 85000000,
      "turnover_amount_source": {"source_name": "quote-feed", "source_ref": "quote-123", "observed_at": "2026-09-23T13:29:30+08:00"},
      "daily_basic_as_of_trade_date": "2026-09-22",
      "turnover_rate_pct": 12.0, "volume_ratio": 2.3,
      "daily_basic_source": {"source_name": "daily-basic-feed", "source_ref": "daily-basic-123", "observed_at": "2026-09-23T13:29:00+08:00"}
    },
    "risk_review": {
      "complete": true,
      "status": "CLEAR",
      "categories": {
        "announcement": {"status": "CLEAR", "has_risk": false, "source": {"source_name": "risk-feed", "source_ref": "announcement-123", "observed_at": "2026-09-23T13:29:00+08:00"}},
        "regulatory": {"status": "CLEAR", "has_risk": false, "source": {"source_name": "risk-feed", "source_ref": "regulatory-123", "observed_at": "2026-09-23T13:29:00+08:00"}},
        "reduction": {"status": "CLEAR", "has_risk": false, "source": {"source_name": "risk-feed", "source_ref": "reduction-123", "observed_at": "2026-09-23T13:29:00+08:00"}},
        "earnings": {"status": "CLEAR", "has_risk": false, "source": {"source_name": "risk-feed", "source_ref": "earnings-123", "observed_at": "2026-09-23T13:29:00+08:00"}}
      },
      "facts": []
    },
    "realtime": {
      "trade_date": "2026-09-23", "observed_at": "2026-09-23T13:29:30+08:00",
      "last_price": 25.0, "bid_price": 24.99, "ask_price": 25.01,
      "bid_size": 1000, "ask_size": 1200, "suspended": false,
      "tradable": true, "price_locked": false,
      "source": {"source_name": "quote-feed", "source_ref": "quote-123", "observed_at": "2026-09-23T13:29:30+08:00"}
    }
  }],
  "collection_summary": {
    "scanned_count": 1, "included_count": 1, "excluded_count": 0, "excluded": []
  }
}
```

Permanent gates reject missing identity or rule evidence, incomplete or flagged
risk review, quote data older than the configured age or from the future,
non-tradable or suspended securities, price-locked securities, invalid or
one-sided books, insufficient liquidity, and returns below the configured
growth threshold. Survivors are ranked by strongest per-session return, then
best cumulative return, turnover amount, and symbol. Every candidate and
rejection is tagged `route_origin: IPO_SPRINT`.

The default policy in `research/config/ipo-sprint.yaml` requires a return of at
least 10%, turnover amount of at least CNY 50 million, turnover rate of at
least 1%, volume ratio of at least 1, a quote no older than 60 seconds, and risk
evidence no older than 24 hours.
These are independent policy thresholds and are configurable through the CLI.
The result contains only this route's `candidates`, evaluated `rejected`, and
pre-evaluation `collection_excluded` arrays, and declares `read_only: true`,
`execution_ready: false`,
`production_accepted: false` and `orders_submitted: 0`.

The collector supports Shanghai and Shenzhen listings only. It verifies
`new_share` against `stock_basic`, derives the listing-session number from the
exchange calendar, and only includes sessions 1-5. The rule evidence is bound
to the official 2026 SSE/SZSE trading rules. It reads daily history and
`daily_basic` directly. Its actual date is preserved as
`daily_basic_as_of_trade_date` and must equal the previous completed trading
session; missing or older facts are
rejected. The Sina reader requires a dated status-00 quote and positive
buy-one/sell-one price and size. Quotes are accepted only during continuous
auction (09:30–11:30 and 13:00–14:57 Shanghai time).

Collect and evaluate without starting the Web application:

```powershell
Push-Location research
python scripts/ipo-sprint.py collect --token-file ..\Tushare-token.txt `
  --snapshot-output .\ipo-sprint-snapshot.json
python scripts/ipo-sprint.py evaluate `
  --input .\ipo-sprint-snapshot.json `
  --output .\ipo-sprint-result.json
Pop-Location
```

`--output` is required and must differ from `--input`. Input and output are
JSON files; policy is YAML. `TUSHARE_TOKEN` can replace `--token-file`.
Collection failures, unavailable `anns_d` permission, incomplete or stale risk
evidence, missing daily-basic facts, unknown quote status, one-sided books,
closed-session quotes, or stale quotes fail closed. Neither
command reads or writes the normal research `runs` directory.
