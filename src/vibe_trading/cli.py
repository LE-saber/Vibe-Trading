"""Supported deployment: one local loopback process, no cloud token bridge."""
import argparse
import importlib
import json
import os
from pathlib import Path

from pydantic import TypeAdapter

from .models import Leaderboard, NewsItem, TradingContext, canonical
from .store import Store


def load_provider(spec: str):
    module, separator, attribute = spec.partition(":")
    if not separator or not module or not attribute:
        raise ValueError("Provider must be a trusted installed module:factory, not a URL.")
    return getattr(importlib.import_module(module), attribute)()


def main() -> None:
    parser = argparse.ArgumentParser(description="Local-first Vibe-Trading workbench")
    parser.add_argument("command", choices=["serve", "doctor"], nargs="?", default="serve")
    parser.add_argument("--mode", choices=["demo", "local"], default="demo")
    parser.add_argument("--provider", default=os.environ.get("VIBE_PROVIDER", ""))
    parser.add_argument("--db", default=None)
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    from .demo import DemoProvider
    store = Store(Path(args.db or f"runtime/{args.mode}.db"))
    if args.mode == "demo":
        provider = DemoProvider(store)
    else:
        if not args.provider:
            parser.error("Local mode requires --provider module:factory. No ninef HTTP routes are guessed.")
        provider = load_provider(args.provider)
        if provider.capabilities().environment != "NINEF_SIMULATION":
            parser.error("Local provider must identify NINEF_SIMULATION, not DEMO or real-money trading.")
    if args.command == "doctor":
        report = {"capabilities": json.loads(canonical(provider.capabilities())), "checks": {}, "writes_attempted": 0}
        for name, function, schema in (("account_context", lambda: provider.context([]), TradingContext),
                                       ("rankings", provider.leaderboard, Leaderboard),
                                       ("news", provider.news, list[NewsItem])):
            try:
                canonical(TypeAdapter(schema).validate_python(function()))
                report["checks"][name] = "READ_AND_CONTRACT_OK"
            except Exception:
                report["checks"][name] = "FAILED_INSPECT_LOCALLY"
        print(json.dumps(report, indent=2))
        return
    password = os.environ.get("VIBE_UI_PASSWORD", "demo" if args.mode == "demo" else "")
    if args.mode == "local" and len(password) < 12:
        parser.error("Set a separate VIBE_UI_PASSWORD (at least 12 characters); never use the ninef token.")
    enabled = args.mode == "demo" or os.environ.get("VIBE_ENABLE_NINEF_WRITES") == "I_HAVE_VERIFIED_THE_LOCAL_ADAPTER"
    from .api import WebSettings, create_app
    from .service import Workbench
    workbench = Workbench(store, provider, enabled=enabled, poll_seconds=0.6 if args.mode == "demo" else 2)
    app = create_app(workbench, WebSettings(password=password, demo=args.mode == "demo"))
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=args.port, workers=1, access_log=False)


if __name__ == "__main__":
    main()
