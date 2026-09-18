"""Review a committed S4 shortlist with the current configured non-tool Vibe model."""

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.persistence import PipelineRepository
from competition_core.pipeline.reviews import run_reviews


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    try:
        result = run_reviews(repository=PipelineRepository(ROOT / "runs", args.run_id),
                             progress=lambda event: print(json.dumps(event, ensure_ascii=False), flush=True))
        print(json.dumps({key: result.get(key) for key in ("run_id", "status", "completed_through", "pending_stage", "execution_ready")}), flush=True)
    except Exception as exc:
        print(json.dumps({"ok": False, "error": exc.to_dict() if isinstance(exc, PipelineError) else
                          {"code": "AI_REVIEW_FAILED", "exception_type": type(exc).__name__}}), flush=True)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
