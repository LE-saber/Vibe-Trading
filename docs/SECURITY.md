# Security boundary

## Default posture

The normal launcher is loopback-only and clears `VIBE_ENABLE_NINEF_WRITES`.
Research can read configured data and produce a portfolio draft, but cannot buy,
sell or cancel. The migrated research runtime is permanently wired to a disabled
execution provider.

## Secrets and sensitive data

Keep `.env`, token files, cookies, authorization headers, account exports,
captures, SQLite databases and raw provider responses outside Git. Logs and
artifacts must use pseudonymous account aliases. The repository contains only
empty placeholders in `.env.example`.

## Execution invariants

The canonical execution path requires a READY plan, exact immutable plan hash,
unexpired human approval, verified provider capability, stable account identity
and a fresh pre-send context. It persists SENDING before the one permitted
network attempt. Ambiguous outcomes become UNKNOWN and cannot be retried without
authoritative reconciliation.

No browser route accepts an arbitrary buy/sell payload. The static entrypoint
audit checks that broker writes remain inside the canonical adapter and execution
service.

Terminal Tournament Strategy outputs are permanently shadow-only. Its CLI can
read and content-address account, fee, order, leaderboard and quote evidence,
but exposes no approve, execute, submit, cancel, force-mode or ignore-gate
command. Terminal candidates never enter canonical `ResearchResult.targets` or
the canonical `Plan` namespace. Both the terminal contracts and research
execution-plan builder reject attempts to turn shadow output into executable
orders.

Only local single-operator use on `127.0.0.1` is supported. TLS termination,
multi-user authorization, remote hosting and unattended trading are outside this
release.
