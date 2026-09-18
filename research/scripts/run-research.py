"""Run or resume the deterministic research stages using explicit frozen inputs."""

import argparse
from datetime import date, datetime
import importlib.util
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from adapters.tushare.client import create_tushare_client
from competition_core.pipeline.artifacts import atomic_bytes, read_json
from competition_core.pipeline.config import PipelineConfig, load_pipeline_config
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.persistence import PipelineRepository
from competition_core.pipeline.runner import run_screening
from competition_core.pipeline.sources import ResearchDataClient
from competition_core.pipeline.time_context import ResearchContext


class _LazySource:
    def __init__(self, *, frozen_only=False):
        self.client = None
        self.frozen_only = frozen_only

    def __getattr__(self, endpoint):
        def call(**parameters):
            if self.frozen_only:
                raise PipelineError("NOT_READY", "frozen-sources-only run attempted an uncached source request", endpoint=endpoint)
            if self.client is None:
                self.client = create_tushare_client()
            return getattr(self.client, endpoint)(**parameters)
        return call


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--snapshot", type=Path)
    parser.add_argument("--dataset-root", type=Path, default=ROOT / "data/canonical")
    parser.add_argument("--runs-root", type=Path, default=ROOT / "runs")
    parser.add_argument("--vibe-root", type=Path, default=ROOT.parent / "Vibe-Trading")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--run-id")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--resume")
    mode.add_argument("--replay-of", help="Recompute stages using another run's frozen source inputs, not its outputs")
    parser.add_argument("--through-stage", choices=("S0", "S1", "S2", "S3", "S4"), default="S4")
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--token-file", type=Path)
    parser.add_argument("--base-url")
    parser.add_argument("--frozen-sources-only", action="store_true", help="Fail closed if any historical source request would need network access")
    args = parser.parse_args()
    # Reuse the already-tested credential scoping/redaction implementation.
    module_spec = importlib.util.spec_from_file_location("canonical_cli_credentials", ROOT / "scripts/prepare-canonical.py")
    credentials = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(credentials)
    secrets = []
    try:
        environment = credentials._credential_env(args)
        secrets = [environment.get("TUSHARE_TOKEN", "")]
        if args.resume or args.replay_of:
            if args.manifest or args.snapshot or args.config or (args.resume and args.run_id):
                raise PipelineError("INVALID_ARGUMENTS", "resume/replay uses the original frozen input specification")
            original = PipelineRepository(args.runs_root, args.resume or args.replay_of)
            frozen = read_json(original.directory / "inputs.json")
            args.manifest, args.dataset_root = Path(frozen["manifest_path"]), Path(frozen["dataset_root"])
            args.vibe_root = Path(frozen["vibe_root"])
            snapshot = read_json(original.directory / "snapshot.json")
            context_data = frozen["context"]
            config = PipelineConfig.from_dict(frozen["config"])
            repository = original if args.resume else PipelineRepository(args.runs_root, args.run_id)
            if args.replay_of:
                for path in sorted((original.directory / "sources").glob("*.json")):
                    atomic_bytes(repository.directory / "sources" / path.name, path.read_bytes(), immutable=True)
        else:
            if args.manifest is None or args.snapshot is None:
                raise PipelineError("INVALID_ARGUMENTS", "new research requires --manifest and --snapshot")
            config = load_pipeline_config(args.config) if args.config else load_pipeline_config()
            snapshot = read_json(args.snapshot)
            context_data = read_json(args.manifest)["context"]
            repository = PipelineRepository(args.runs_root, args.run_id)
        context = ResearchContext(date.fromisoformat(context_data["research_date"]), context_data["phase"],
            date.fromisoformat(context_data["market_data_reference_date"]), datetime.fromisoformat(context_data["as_of"]))
        with credentials._temporary_env(environment):
            source = ResearchDataClient(_LazySource(frozen_only=args.frozen_sources_only), context)
            result = run_screening(dataset_root=args.dataset_root, manifest_path=args.manifest, snapshot=snapshot,
                context=context, config=config, repository=repository, source_client=source, vibe_root=args.vibe_root,
                through_stage=args.through_stage,
                progress=lambda event: print(json.dumps(event, ensure_ascii=False), flush=True))
        print(json.dumps({"run_id": repository.run_id, "path": str(repository.directory), "status": result["status"],
                          "completed_through": result["completed_through"], "stages": result["stages"],
                          "production_accepted": False, "execution_ready": False}, ensure_ascii=False), flush=True)
    except Exception as exc:
        error = exc.to_dict() if isinstance(exc, PipelineError) else {"code": "NOT_READY", "message": "research command failed", "details": {"exception_type": type(exc).__name__}}
        print(json.dumps(credentials._safe_details({"ok": False, "error": error}, secrets), ensure_ascii=False), flush=True)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
