import pytest

from vibe_trading.demo import DemoProvider
from vibe_trading.service import Workbench
from vibe_trading.store import Store


@pytest.fixture
def bench(tmp_path):
    store = Store(tmp_path / 'workbench.db')
    workbench = Workbench(store, DemoProvider(store, delay=0), enabled=True)
    yield workbench
    workbench.close()


@pytest.fixture
def planned(bench):
    run_id = bench.create_run(asynchronous=False)
    run = bench.store.get('run', run_id)
    assert run['state'] == 'COMPLETED', run
    return bench, bench.execution.plan(run['plan_id'])
