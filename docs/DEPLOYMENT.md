# Deployment

## Supported local deployment

- Windows 10/11, Git and Python 3.13
- one loopback backend on `127.0.0.1`
- one local operator and one ninef simulation account

Run `scripts/setup.ps1`, copy `.env.example` to `.env`, then run
`scripts/start.ps1`. Paths are derived from the clone; no fixed `D:\AI` layout
is required.

## Configuration

`VIBE_UI_PASSWORD` is mandatory in local mode and must contain at least 12
characters. The example research provider and paths normally remain unchanged.

Tushare accepts either `TUSHARE_TOKEN` or an explicit `TUSHARE_TOKEN_FILE`.
`TUSHARE_BASE_URL` is optional. Empty preserves the official SDK URL; a value is
applied only after `ts.pro_api(token)`.

ninef read-only integration requires `NINEF_API_TOKEN`. `NINEF_ACCOUNT_KEY` is
a stable local identity required before any write capability can be verified.
`NINEF_MARKET_PROFILE` points to a locally maintained authoritative fee/rule
JSON; missing facts become S6 blockers rather than guessed defaults.

S5 requires `RESEARCH_LLM_MODEL`, `RESEARCH_LLM_BASE_URL` and
`RESEARCH_LLM_API_KEY`. The endpoint must implement OpenAI-compatible Chat
Completions. Only HTTPS is accepted except explicit localhost HTTP. The adapter
does one bounded request, follows no redirect, binds no tools and stores no key.

## Modes

`--mode demo` is an offline UI/contract demonstration. `scripts/start.ps1` uses
local mode, reads real providers and clears the external-write switch. No cloud,
multi-user or non-loopback deployment is supported.

## Optional sidecars

Packages under `research/services/` have separate manifests because Qlib,
TradingAgents and vn.py use different dependency constraints. Install them only
for their optional shadow/research workflows; the canonical launcher does not
start them.
