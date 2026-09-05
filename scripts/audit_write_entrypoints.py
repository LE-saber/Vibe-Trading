"""Fail when an integrated write path bypasses the execution boundary."""
from __future__ import annotations

import ast
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "src" / "vibe_trading"
TRANSPORT_OWNER = SOURCE / "adapters" / "ninef.py"
EXECUTION_OWNER = SOURCE / "execution.py"
WRITE_PATHS = {
    "/api/v1/stock/buy", "/api/v1/stock/sell", "/api/v1/stock/cancel-order",
}


def audit() -> dict[str, object]:
    violations: list[str] = []
    transport_paths: list[str] = []
    provider_write_calls: list[str] = []
    for path in SOURCE.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        relative = path.relative_to(ROOT).as_posix()
        for endpoint in WRITE_PATHS:
            if endpoint in text:
                transport_paths.append(f"{relative}:{endpoint}")
                if path != TRANSPORT_OWNER:
                    violations.append(f"direct transport path outside adapter: {relative}:{endpoint}")
        tree = ast.parse(text, filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr not in {"submit", "cancel"}:
                continue
            if isinstance(node.func.value, ast.Attribute) and node.func.value.attr == "provider":
                provider_write_calls.append(f"{relative}:{node.lineno}:{node.func.attr}")
                if path != EXECUTION_OWNER:
                    violations.append(
                        f"provider write call outside ExecutionService: {relative}:{node.lineno}"
                    )
    required = {f"src/vibe_trading/adapters/ninef.py:{item}" for item in WRITE_PATHS}
    if set(transport_paths) != required:
        violations.append("ninef write endpoint inventory changed")
    return {
        "ok": not violations,
        "transport_paths": sorted(transport_paths),
        "provider_write_calls": sorted(provider_write_calls),
        "violations": violations,
    }


def main() -> int:
    result = audit()
    print(json.dumps(result, indent=2))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
