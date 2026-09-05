import importlib.util
from pathlib import Path


def test_all_integrated_write_paths_are_owned_by_execution_service():
    path = Path(__file__).resolve().parents[1] / "scripts" / "audit_write_entrypoints.py"
    spec = importlib.util.spec_from_file_location("write_audit", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    result = module.audit()
    assert result["ok"], result["violations"]
    assert len(result["transport_paths"]) == 3
    assert result["provider_write_calls"]
