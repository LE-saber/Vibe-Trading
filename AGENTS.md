# Project execution rules

Goal: one Web workflow joining stock research, visible progress, ninef account/holdings/orders, own rank/top-30, platform news in final AI review, account-aware portfolio planning, explicit consent and controlled execution.

This repository is the canonical `LE-saber/Vibe-Trading` source. Do not depend on sibling checkouts or fixed local paths. `research/` is the maintained migrated research package; historical local projects are not runtime dependencies.

Read `docs/ARCHITECTURE.md`, `docs/DEVELOPMENT.md`, `docs/SECURITY.md` and `docs/VERIFICATION.md`. Do not turn assistant summaries into verified source facts.

Credentials remain local. Never print/upload tokens, cookies, Authorization headers, raw capture databases or real account/ranking data. No external buy/sell/cancel, write-switch enablement or deployment during implementation/testing without separate explicit owner authorization. Tests default to DEMO. Do not remove blockers or assert verified capabilities merely to make the UI green.

Every integrated Web/agent write must enter the approval/execution service. No raw broker capability in AI/news/progress code. No blind retries after ambiguous writes, heuristic order matching, or database deletion to unlock an unknown batch. Inspect existing write bypasses locally; the new safe API alone does not disable old ones.

Use review branches and focused commits, preserve regression tests, report actual commands and unrun checks. Request a fresh-context reviewer before enabling ninef writes. Passing fabricated fixtures is not proof that the real adapter or legacy entrypoints are safe.
