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

The terminal-strategy package has no public API or command-line entrypoint. It
is not wired to S6, portfolio construction, approval, or execution, and its
results cannot enter canonical `ResearchResult.targets` or the `Plan`
namespace.

The IPO sprint CLI is also read-only, but remains a separate route rather than
an S6 input. It writes only standalone snapshot/result JSON, labels every row
`IPO_SPRINT`, and declares outputs non-executable. Missing rule, identity, risk,
liquidity or fresh two-sided quote evidence fails closed. It neither reads nor
writes the normal research `runs` directory and contains no broker operation.

Only local single-operator use on `127.0.0.1` is supported. TLS termination,
multi-user authorization, remote hosting and unattended trading are outside this
release.
