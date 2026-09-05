"""Generate schemas directly from the runtime contracts, not duplicated handwritten JSON."""
import json
from pathlib import Path

from vibe_trading.models import (
    AccountSnapshot, Approval, BrokerOrder, Capabilities, FeeSchedule, InstrumentRule,
    Leaderboard, NewsItem, Plan, Policy, ResearchRequest, ResearchResult, TradingContext,
)

output = Path(__file__).resolve().parents[1] / 'schemas'
output.mkdir(exist_ok=True)
for model in (AccountSnapshot, Approval, BrokerOrder, Capabilities, FeeSchedule, InstrumentRule,
              Leaderboard, NewsItem, Plan, Policy, ResearchRequest, ResearchResult, TradingContext):
    (output/(model.__name__+'.json')).write_text(json.dumps(model.model_json_schema(), indent=2)+'\n')
print('Exported 13 runtime contract schemas.')
