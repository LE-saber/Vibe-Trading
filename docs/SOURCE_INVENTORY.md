# Source inventory

The consolidation was built in an isolated clone. Original local directories
were treated as read-only migration sources and were neither deleted nor cleaned.

| Source | Retained capability | Excluded material |
| --- | --- | --- |
| Integrated workbench | typed account/market contracts, S6, approval, guarded execution, reconciliation, local Web UI | generated state and credentials |
| Competition research platform | Universe, Tushare, S0-S5, evidence, AI review, data-quality tests and optional sidecar interfaces | runs, data, storage, virtual environments, historical reports and fixed-path launchers |
| Legacy Vibe checkout | behavior needed by explicit technical-indicator compatibility tests | Electron/product tree, caches, archives, unrelated experiments and local secrets |

Bundled technical-indicator and AI-review adapters remove runtime imports from
other checkouts. The repository can therefore be cloned to an arbitrary path.
Optional Qlib, TradingAgents and market-runtime packages remain isolated under
`research/services/` and are not silently substituted for the canonical path.
