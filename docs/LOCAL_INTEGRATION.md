# Local ninef and research integration

This document records the implemented local boundary. It contains no credential,
account value, private ranking name or raw response.

## ninef source mapping

The adapter was checked against the 2026-09-04 local ninef release source, notably
`program/ninef_adapter.py`, `program/export_account_snapshot.py`,
`program/execute_advice_buy.py`, `program/verify_sell_order.py`, and
`program/ninef_core/service.py`.

`vibe_trading.adapters.ninef.NinefProvider` maps these observed endpoints:

| Contract | ninef endpoint / field ownership |
| --- | --- |
| Assets | `GET /api/v1/assets/overview`: `totalAssets`, `availableFunds`, `frozenFunds` |
| Positions | `GET /api/v1/positions`: `qty`, `sellableQty`, `frozenQty`, `marketValue` |
| Orders | all cursor pages of `GET /api/v1/record/order-activity`; exact `id`, status, cumulative quantities, execution price and cost breakdown |
| Quotes | `GET /api/v1/data/quotes/batch`: official price, limits and source timestamp |
| Session date | `GET /api/v1/market/trading-day`; Shanghai continuous-auction windows are evaluated locally |
| Ranking | `GET /api/v1/mp/competition-gate/status`: first 30 `leaderboard.items`, with `currentUser` mapped independently |
| News | all cursor pages of `GET /api/v1/data/hot-topics`: stable pseudonymous ID, source, publication/fetch times, body and related A-share symbols |
| Submit | one `POST /api/v1/stock/buy` or `/stock/sell` transport attempt for the exact approved order |
| Read order | all order pages, then exact authoritative ID equality; absence is an error |
| Cancel | one `POST /api/v1/stock/cancel-order` attempt for the exact stored order ID |

Observed order states are normalized in `vibe_trading.reconciliation.normalize_status`.
Pending, unfilled, submitted, partial and cancel-pending states remain active.
Unrecognized values remain `UNKNOWN` and block planning.

The live quote response observed during integration did not contain an authoritative
suspension/tradability/buy-permission/sell-permission field. The adapter therefore
does not infer tradability merely from the presence of a price. Likewise, fee and
lot/tick values are not invented. Without a verified market profile, the context is
valid for account display but portfolio planning reports `UNVERIFIED_FEES`,
`UNVERIFIED_RULE` and/or `NOT_TRADABLE`.

## Local configuration

Keep configuration outside Git. The minimum read-only doctor setup is:

```powershell
$env:NINEF_API_TOKEN = '<loaded locally; do not paste into Web UI>'
$env:NINEF_ACCOUNT_KEY = '<stable local alias; keep unchanged when rotating the token>'
$env:PYTHONPATH = 'src'
python -m vibe_trading.cli doctor --mode local --provider examples.local_provider:create_provider
```

Optional read-only variables are `NINEF_COMPETITION_ID`, `NINEF_TIMEOUT_SECONDS`,
`NINEF_MAX_PAGES`, `NINEF_MARKET_PROFILE`, `NINEF_VERIFICATION_REF`, and
`VIBE_RESEARCH_PROVIDER`.

The API does not expose a stable account ID in the verified account responses.
`NINEF_ACCOUNT_KEY` therefore supplies a credential-independent local lock identity;
only its hash-derived alias is persisted. It is mandatory before any write capability
can be marked verified. If omitted, reads use one conservative unconfigured-account
lock identity and all write capabilities remain false.

`NINEF_MARKET_PROFILE` is a local JSON file with a runtime `FeeSchedule` under
`fees` and prefix-selected runtime `InstrumentRule` objects under
`instrument_rules`. Each rule adds a `prefix` key. Every effective date and
`source_ref` must be explicit; only independently verified entries may set
`verified: true`. This file is intentionally not supplied with assumed production
rates or rules.

`NINEF_BUY_VERIFIED`, `NINEF_SELL_VERIFIED` and `NINEF_CANCEL_VERIFIED` are false
unless exactly set to `1`, and are ineffective without `NINEF_VERIFICATION_REF`.
The application-level `VIBE_ENABLE_NINEF_WRITES` gate is separate. None of these
write switches was enabled during implementation or verification.

## Real research boundary

`VIBE_RESEARCH_PROVIDER=module:factory` must resolve to an object implementing:

```python
research(request: ResearchRequest, report: Progress,
         cancelled: Cancelled) -> ResearchResult
```

The result must use the request run ID, report real measured stage counts, review
both candidates and every current holding, cite only supplied evidence, and bind
the `review_context` news hash. The ninef adapter deliberately raises when this
backend is absent. The existing local Vibe repository exposes S0-S6 only through a
loopback proxy; its actual pipeline is not present there, and the separately
excluded project was not inspected or migrated.

## Execution and recovery boundary

The integrated Web API has no arbitrary buy or sell route. Static audit is provided
by `python scripts/audit_write_entrypoints.py`. It requires all direct ninef write
paths to remain in `adapters/ninef.py` and all provider submit/cancel calls to remain
in `ExecutionService`.

An ambiguous submission remains locked. `POST
/api/batches/{batch_id}/intents/{intent_id}/resolve` accepts explicit operator
consent, an exact broker order ID and a local evidence reference. The backend then
performs an authoritative exact-ID read. Only a receipt whose broker-provided
`client_order_id` equals the immutable local intent ID, and whose fields match the
approved order, can create an `OperatorResolution` record. Failed/absent/UNKNOWN
evidence leaves the account blocked; the operation never resubmits or cancels.
