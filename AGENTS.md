# Project execution rules

Goal: one Web workflow joining stock research, visible progress, ninef account/holdings/orders, own rank/top-30, platform news in final AI review, account-aware buys/sells, explicit consent and controlled automatic execution.

Target source repository: LE-saber/Vibe-Trading. Do not modify the orchestrator methodology repository. Do not investigate/migrate/build on stock-competition-platform by default: the owner explicitly excluded it. Existing local Vibe-Trading is an integration source, not permission for a wholesale strategy rewrite.

Read docs/ARCHITECTURE.md, docs/LOCAL_CODEX_TASK.md and docs/VERIFICATION.md. Do not turn earlier assistant summaries into verified source facts. Historical uploads were unreadable in the execution environment; alleged exact old schemas/defects were not independently verified.

Credentials remain local. Never print/upload tokens, cookies, Authorization headers, raw capture databases or real account/ranking data. No external buy/sell/cancel, write-switch enablement or deployment during implementation/testing without separate explicit owner authorization. Tests default to DEMO. Do not remove blockers or assert verified capabilities merely to make the UI green.

Every integrated Web/agent write must enter the approval/execution service. No raw broker capability in AI/news/progress code. No blind retries after ambiguous writes, heuristic order matching, or database deletion to unlock an unknown batch. Inspect existing write bypasses locally; the new safe API alone does not disable old ones.

Use review branches and focused commits, preserve regression tests, report actual commands and unrun checks. Request a fresh-context reviewer before enabling ninef writes. Passing fabricated fixtures is not proof that the real adapter or legacy entrypoints are safe.
