from datetime import timedelta
from decimal import Decimal

import pytest

from vibe_trading.models import Policy, ResearchResult, Side, now_utc
from vibe_trading.planner import build_plan, estimate_fee


def rebuild(bench, plan, **kwargs):
    result = ResearchResult.model_validate(bench.store.get('research_result', plan.run_id))
    return build_plan(kwargs.get('result', result), kwargs.get('context', plan.context),
                      now_utc(), kwargs.get('policy', Policy()))


def test_sell_buy_fee_cash_plan(planned):
    bench, plan = planned
    assert not plan.blockers
    assert [o.side for o in plan.orders] == [Side.SELL, Side.SELL, Side.BUY, Side.BUY]
    assert [o.quantity for o in plan.orders] == [1500, 500, 800, 2000]
    assert plan.projected_cash == Decimal('35963.61')
    assert plan.cash_floor == Decimal('20000')
    assert plan.projected_cash + sum(o.estimated_fee for o in plan.orders) == Decimal('36000')
    assert estimate_fee(Decimal(0), Side.BUY, plan.context.fees) == 0


@pytest.mark.parametrize('field,code', [('orders_complete', 'INCOMPLETE_ORDERS'),
                                     ('consistent', 'INCONSISTENT_ACCOUNT')])
def test_incomplete_account_fails_closed(planned, field, code):
    bench, plan = planned
    context = plan.context.model_copy(update={'account': plan.context.account.model_copy(update={field: False})})
    assert code in {b.code for b in rebuild(bench, plan, context=context).blockers}


@pytest.mark.parametrize('case,code', [
    ('stale_account', 'STALE_ACCOUNT'), ('collection_skew', 'SNAPSHOT_SKEW'),
    ('closed_market', 'MARKET_CLOSED'), ('stale_quote', 'STALE_QUOTE'),
    ('expired_fee', 'EXPIRED_FEES'), ('expired_rule', 'EXPIRED_RULE'),
    ('no_quote', 'MISSING_MARKET_INPUT'), ('suspended', 'NOT_TRADABLE'),
    ('cannot_sell', 'SELL_NOT_ALLOWED'), ('cannot_buy', 'BUY_NOT_ALLOWED'),
    ('tplus1', 'INSUFFICIENT_SELLABLE')])
def test_safety_blockers(planned, case, code):
    bench, plan = planned
    c, a = plan.context, plan.context.account
    if case == 'stale_account':
        c = c.model_copy(update={'account': a.model_copy(update={'as_of': a.as_of-timedelta(minutes=1), 'collection_started_at': a.collection_started_at-timedelta(minutes=1)})})
    if case == 'collection_skew':
        c = c.model_copy(update={'account': a.model_copy(update={'collection_started_at': a.collection_started_at-timedelta(minutes=1)})})
    if case == 'closed_market': c = c.model_copy(update={'market_open': False})
    if case == 'stale_quote': c = c.model_copy(update={'quotes': tuple(q.model_copy(update={'as_of': q.as_of-timedelta(minutes=1)}) for q in c.quotes)})
    if case == 'expired_fee': c = c.model_copy(update={'fees': c.fees.model_copy(update={'valid_until': c.trading_date-timedelta(days=1)})})
    if case == 'expired_rule': c = c.model_copy(update={'rules': tuple(r.model_copy(update={'valid_until': c.trading_date-timedelta(days=1)}) for r in c.rules)})
    if case == 'no_quote': c = c.model_copy(update={'quotes': ()})
    if case == 'suspended': c = c.model_copy(update={'quotes': tuple(q.model_copy(update={'tradable': False}) for q in c.quotes)})
    if case == 'cannot_sell': c = c.model_copy(update={'quotes': tuple(q.model_copy(update={'can_sell': False}) for q in c.quotes)})
    if case == 'cannot_buy': c = c.model_copy(update={'quotes': tuple(q.model_copy(update={'can_buy': False}) for q in c.quotes)})
    if case == 'tplus1': c = c.model_copy(update={'account': a.model_copy(update={'positions': tuple(p.model_copy(update={'sellable_quantity': 0}) for p in a.positions)})})
    assert code in {b.code for b in rebuild(bench, plan, context=c).blockers}


def test_missing_holding_never_silently_sold(planned):
    bench, plan = planned
    result = ResearchResult.model_validate(bench.store.get('research_result', plan.run_id))
    result = result.model_copy(update={'targets': tuple(t for t in result.targets if t.symbol != '000001.SZ')})
    new = rebuild(bench, plan, result=result)
    assert 'UNREVIEWED_HOLDING' in {b.code for b in new.blockers}
    assert not any(o.symbol == '000001.SZ' for o in new.orders)


def test_watch_does_not_authorize_buy(planned):
    bench, plan = planned
    result = ResearchResult.model_validate(bench.store.get('research_result', plan.run_id))
    result = result.model_copy(update={'targets': tuple(t.model_copy(update={'verdict': 'WATCH'}) for t in result.targets)})
    assert 'VERDICT_NOT_BUYABLE' in {b.code for b in rebuild(bench, plan, result=result).blockers}


def test_cash_reserve_blocks_without_secret_resize(planned):
    bench, plan = planned
    new = rebuild(bench, plan, policy=Policy(cash_reserve='0.50'))
    assert 'CASH_RESERVE' in {b.code for b in new.blockers}
    assert new.orders == plan.orders


def test_stale_research_not_freshened_by_replan(planned):
    bench, plan = planned
    result = ResearchResult.model_validate(bench.store.get('research_result', plan.run_id))
    result = result.model_copy(update={'as_of': now_utc()-timedelta(hours=2)})
    assert 'STALE_RESEARCH' in {b.code for b in rebuild(bench, plan, result=result).blockers}
