# Canonical platform consolidation plan

Status: approved by the repository-consolidation request; implementation is
performed only in an isolated clone on branch
`codex/canonical-platform-consolidation`.

## 1. Observed source environments

| Source | Current role | Migration decision |
| --- | --- | --- |
| integrated-workbench source | Small typed workbench with account, portfolio, approval, reconciliation and guarded ninef boundaries | Canonical application base |
| competition-platform source | Actual Competition Universe, S0-S5.5 research pipeline, evidence, Tushare, market/AI/Qlib sidecars and tests | Migrate the maintained source and tests into `research/` |
| legacy Vibe source | Upstream Vibe product plus local competition proxy/UI and historical experiments | Do not copy wholesale; retain only behavior required by an explicit adapter or test |

The source worktrees are dirty and remain untouched. Their runtime data is not a
source of truth for the new repository.

## 2. Target repository shape

```text
src/vibe_trading/        canonical Web/API, contracts and guarded execution
research/                independently installable deterministic research system
  competition_core/      Competition Universe and S0-S5.5 pipeline
  adapters/              Tushare, market-runtime, AI and read-only ninef adapters
  contracts/             research/evidence contracts
  services/              optional market, AI-review and Qlib sidecars
  workflows/             preopen/intraday/postclose orchestration
  scripts/               research and audit commands
  tests/                 research unit/integration tests
config/                  non-secret canonical examples
scripts/                 repository setup/start/stop/verify commands
docs/                    architecture, deployment, use, development and verification
```

The workbench is the only portfolio approval and execution authority. The
research package may produce candidates, evidence, reviews and realtime-gate
facts, but its legacy execution service is not exposed by the canonical
launcher. There must be no second browser-accessible order path.

## 3. Inclusion rules

Include only:

- source required for Competition Universe and S0-S5.5;
- the Tushare compatible-base-URL client factory;
- read-only account/news/leaderboard inputs required by research;
- optional market-runtime, AI-decision and Qlib sidecars behind explicit
  availability checks;
- deterministic configuration, anonymous fixtures, tests and operator docs;
- the guarded canonical ninef adapter and its existing approval/reconciliation
  contracts.

Exclude:

- `.env`, token files, cookies, headers, account exports and captured broker
  payloads;
- `runs/`, `data/`, `storage/`, caches, databases, logs and generated reports;
- local virtual environments, build outputs and vendor checkouts;
- release ZIPs, backups, screenshots, one-off acceptance artifacts and obsolete
  planning documents;
- the upstream Vibe desktop/product tree and Alpha Zoo code not imported by the
  maintained research pipeline;
- live order capability enablement or any automatic execution setting.

## 4. Implementation phases and acceptance

1. **Inventory and freeze**
   - Record source revisions, dirty-file inventory and inclusion/exclusion map.
   - Establish a credential-safe `.gitignore` and placeholder-only env example.
2. **Research package migration**
   - Copy the maintained research packages, configs, scripts, sidecars and tests
     into `research/` without runtime artifacts.
   - Remove absolute machine paths from launch/setup code.
   - Keep optional frameworks isolated; failure of an optional sidecar must not
     disable deterministic research.
3. **Canonical integration**
   - Add one explicit research-provider adapter implementing the workbench
     `ResearchBackend` contract.
   - Map frozen S4/S5 output to `vibe.research.v1`; bind artifact and news hashes.
   - Keep S6 planning, review, approval and order reconciliation in
     `src/vibe_trading` only.
4. **Newcomer operation**
   - Provide repository-relative Windows setup/start/stop scripts and portable
     Python commands.
   - Document DEMO first-run, real read-only configuration, research workflow,
     optional sidecars and the separately gated execution boundary.
5. **Verification and publication**
   - Run kernel tests, selected/full research tests, package builds, browser
     smoke tests, write-entrypoint audit and credential/path scans.
   - Inspect the final tracked-file list and repository size.
   - Commit focused changes and push the review branch to the SSH GitHub remote.

Completion means a clean clone can run DEMO immediately, install deterministic
research from repository files, run its tests, and understand exactly which
local secrets and optional third-party runtimes are needed for real data. It
does not mean that live order submission is enabled.

## 5. Non-goals

- Rewriting Alpha definitions or the China-A backtest engine.
- Importing historical research outputs as source code.
- Treating DEMO results as real research.
- Deploying services or submitting/cancelling broker orders.
- Deleting or cleaning the three original local source directories.
