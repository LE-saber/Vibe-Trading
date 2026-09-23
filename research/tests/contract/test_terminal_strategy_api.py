"""Contract confirming terminal strategy stays outside the original pipeline API."""

from competition_core.research_api import router


def test_terminal_strategy_routes_are_not_registered_on_pipeline_api():
    paths = [getattr(route, "path", "") for route in router.routes]
    assert not any("terminal" in path for path in paths)
