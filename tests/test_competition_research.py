from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import sys

from vibe_trading.models import ResearchRequest, now_utc
from vibe_trading.research.competition import CompetitionResearchBackend, _phase


RESEARCH_ROOT = Path(__file__).resolve().parents[1] / "research"
if str(RESEARCH_ROOT) not in sys.path:
    sys.path.insert(0, str(RESEARCH_ROOT))


class Config:
    def section(self, name):
        assert name == "risk"
        return {"cash_buffer": 0.20, "single_name_cap": 0.20, "max_positions": 8}

    def to_dict(self):
        return {"version": "test"}


def test_phase_uses_shanghai_clock():
    assert _phase(datetime(2026, 9, 1, 1, 0, tzinfo=timezone.utc)) == "preopen"
    assert _phase(datetime(2026, 9, 1, 3, 0, tzinfo=timezone.utc)) == "intraday"
    assert _phase(datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)) == "postclose"


def test_result_maps_research_without_creating_execution_authority(bench, tmp_path):
    account = bench.provider.context([]).account
    held = account.positions[0]
    request = ResearchRequest(request_id="run-test", requested_at=now_utc(),
                              account=account, news=bench.provider.news(), competition=None)
    symbols = ["600001.SH", held.symbol, "300750.SZ"]
    symbols = list(dict.fromkeys(symbols))
    while len(symbols) < 3:
        symbols.append(f"60000{len(symbols) + 2}.SH")
    s4 = {
        "output_symbols": symbols,
        "rows": [{"symbol": symbol, "identity": {"name": f"name-{symbol}"}}
                 for symbol in symbols],
    }
    verdicts = ["KEEP", "WATCH", "EXCLUDE"]
    s5 = {
        "artifact_hash": "a" * 64,
        "rows": [{
            "symbol": symbol,
            "ai_verdict": verdict,
            "evidence_id": f"evidence-{index}",
            "ai_review": {"summary": f"review-{verdict}"},
        } for index, (symbol, verdict) in enumerate(zip(symbols, verdicts))],
    }
    backend = CompetitionResearchBackend.__new__(CompetitionResearchBackend)
    backend.runs_root = tmp_path
    result = backend._result(request, s4, s5, None, "b" * 64, Config())

    by_symbol = {target.symbol: target for target in result.targets}
    assert by_symbol[symbols[0]].weight <= Decimal("0.20")
    assert by_symbol[symbols[2]].weight == 0
    assert by_symbol[symbols[2]].verdict == "REJECT"
    assert by_symbol[held.symbol].verdict in {"KEEP", "WATCH"}
    assert sum(target.weight for target in result.targets) <= 1
    assert result.news_context_hash == "b" * 64
    assert result.demo is False
