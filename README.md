# Vibe-Trading

[English](README.md) · [简体中文](README.zh-CN.md) · [繁體中文](README.zh-TW.md) · [日本語](README.ja.md)

**A local research, competition dashboard and approval-controlled execution workbench.**

## Project status

The default `main` branch currently contains the project introduction. Application code is developed on separate review branches; checking out `main` alone does not provide an installable application.

| Location | What to expect |
| --- | --- |
| [`main`](https://github.com/LE-saber/Vibe-Trading/tree/main) | Project overview and language editions |
| [`codex/canonical-platform-consolidation`](https://github.com/LE-saber/Vibe-Trading/tree/codex/canonical-platform-consolidation) | Research integration, local Web UI and approval/execution code |
| [Kernel review: PR #1](https://github.com/LE-saber/Vibe-Trading/pull/1) | The earlier integrated workbench proposal and its review history |

These are development references, not a claim that the project has been deployed or enabled against an external account.

## What the implementation branch contains

- A Python backend and a local browser interface for research progress, account context and portfolio proposals.
- A research package for universe selection, staged screening, evidence and AI review.
- Explicit provider interfaces for account context, rankings, news and order operations.
- Plan approval, order state tracking and reconciliation boundaries.
- A synthetic demonstration mode and a separately configured local simulation-provider mode.

The normal launcher on the consolidation branch clears the write-enablement setting. Credentials alone do not enable account actions; any write capability requires a separately verified configuration and the application's approval gates. Synthetic demo prices, accounts and results must never be presented as live NineF data.

## Explore the development version

Read the branch's [README](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/README.md) and [deployment guide](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/DEPLOYMENT.md) first. That branch specifies **Python 3.13** and provides Windows setup/start scripts.

To inspect the intended code rather than the documentation-only default branch:

```bash
git clone --branch codex/canonical-platform-consolidation https://github.com/LE-saber/Vibe-Trading.git
cd Vibe-Trading
```

Follow the checked-out branch's setup instructions. Do not copy its startup commands into a `main` checkout, where those files are absent.

## Documentation

- [User guide](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/USER_GUIDE.md)
- [Architecture](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/ARCHITECTURE.md)
- [Local integration](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/LOCAL_INTEGRATION.md)
- [Security boundaries](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/SECURITY.md)
- [Branch verification record](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/VERIFICATION.md)

Keep credentials, account exports and runtime databases local. There is no license file on the default branch; this introduction does not grant a software license.
