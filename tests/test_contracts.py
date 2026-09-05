from datetime import datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from vibe_trading.models import (
    BrokerOrder, OrderStatus, Policy, Position, ResearchResult, Target, account_state_hash,
    canonical, digest, now_utc,
)
from vibe_trading.news import review_context, select_news
from vibe_trading.reconciliation import account_blockers, normalize_status


@pytest.mark.parametrize('value', [1.2, True, 'NaN', 'Infinity', '-1'])
def test_money_rejects_unsafe_values(value):
    with pytest.raises(ValidationError):
        Position(symbol='600001.SH', quantity=100, sellable_quantity=100,
                 frozen_quantity=0, market_value=value)


@pytest.mark.parametrize('value', [True, 1.5, '100', -1])
def test_quantities_are_strict_integers(value):
    with pytest.raises(ValidationError):
        Position(symbol='600001.SH', quantity=value, sellable_quantity=0,
                 frozen_quantity=0, market_value='0')


def test_position_freeze_invariant():
    with pytest.raises(ValidationError):
        Position(symbol='600001.SH', quantity=100, sellable_quantity=100,
                 frozen_quantity=1, market_value='10')


@pytest.mark.parametrize('changes', [{'pending_order_count': 0}, {'total_assets': '1'},
                                    {'orders_complete': 'true'}, {'consistent': 1}])
def test_account_contract_fails_closed(bench, changes):
    account = bench.provider.context([]).account
    with pytest.raises(ValidationError):
        type(account).model_validate(account.model_dump() | changes)


def test_canonical_decimal_hash_and_time():
    assert digest({'b': Decimal('1.00'), 'a': 'x'}) == digest({'a': 'x', 'b': Decimal('1')})
    with pytest.raises(ValueError): canonical({'bad': 1.2})
    with pytest.raises(ValueError): canonical({'bad': datetime(2026, 1, 1)})


def test_account_hash_ignores_timestamps_not_resources(bench):
    account = bench.provider.context([]).account
    newer = account.model_copy(update={'as_of': now_utc() + timedelta(seconds=1)})
    assert account_state_hash(account) == account_state_hash(newer)
    changed = account.model_copy(update={'available_cash': account.available_cash - 1})
    assert account_state_hash(account) != account_state_hash(changed)


@pytest.mark.parametrize('raw,expected', [
    ('\u5f85\u6210\u4ea4', OrderStatus.PENDING), ('\u672a\u6210\u4ea4', OrderStatus.PENDING),
    ('\u90e8\u5206\u6210\u4ea4', OrderStatus.PARTIALLY_FILLED),
    ('CANCEL_PENDING', OrderStatus.CANCEL_PENDING), ('cancelled', OrderStatus.CANCELLED),
    ('unmapped-status', OrderStatus.UNKNOWN)])
def test_normalization_is_conservative(raw, expected):
    assert normalize_status(raw) == expected


@pytest.mark.parametrize('status,filled', [(OrderStatus.FILLED, 99),
    (OrderStatus.PARTIALLY_FILLED, 0), (OrderStatus.PARTIALLY_FILLED, 100), (OrderStatus.PENDING, 101)])
def test_order_quantity_consistency(status, filled):
    with pytest.raises(ValidationError):
        BrokerOrder(order_id='o', symbol='600001.SH', side='BUY', quantity=100,
                    filled_quantity=filled, limit_price='10', status=status, updated_at=now_utc())


def test_partial_unknown_orders_block_planning(bench):
    account = bench.provider.context([]).account
    for status, qty, code in [(OrderStatus.PARTIALLY_FILLED, 10, 'EXTERNAL_OPEN_ORDER'),
                             (OrderStatus.UNKNOWN, 0, 'UNKNOWN_ORDER_STATUS')]:
        order = BrokerOrder(order_id='o', symbol='600001.SH', side='BUY', quantity=100,
                            filled_quantity=qty, limit_price='10', status=status, updated_at=now_utc())
        changed = account.model_copy(update={'orders': (order,)})
        assert code in {i.code for i in account_blockers(changed, now_utc(), Policy())}


def test_news_cutoff_duplicates_irrelevance_budget(bench):
    news, now = bench.provider.news(), now_utc()
    base = news[0]
    duplicate = base.model_copy(update={'news_id': 'duplicate'})
    future = base.model_copy(update={'news_id': 'future', 'published_at': now + timedelta(hours=1)})
    irrelevant = base.model_copy(update={'news_id': 'other', 'symbols': ('600999.SH',)})
    stale = base.model_copy(update={'news_id': 'old', 'published_at': now - timedelta(days=10)})
    selected = select_news((base, duplicate, future, irrelevant, stale), {'600001.SH'}, now)
    assert len(selected) == 1
    assert selected[0].news_id in (base.news_id, 'duplicate')
    assert select_news(news, set(), now, character_budget=1) == ()


def test_news_untrusted_and_existing_holdings_in_scope(bench):
    context = review_context(bench.provider.news(), {'000001.SZ'}, now_utc())
    assert any('000001.SZ' in n.symbols for n in context['news'])
    assert 'never instructions' in context['instruction']
    assert context['news_hash'] == digest(context['news'])


def test_invented_evidence_reference_rejected(planned):
    bench, plan = planned
    result = bench.store.get('research_result', plan.run_id)
    result['targets'][0]['evidence_ids'] = ['invented']
    with pytest.raises(ValidationError): ResearchResult.model_validate(result)


@pytest.mark.parametrize('name', ['', '   '])
def test_target_name_must_be_nonblank(name):
    with pytest.raises(ValidationError):
        Target(symbol='600001.SH', name=name, weight='0', reason='fixture', verdict='WATCH')


def test_target_name_is_required():
    with pytest.raises(ValidationError):
        Target(symbol='600001.SH', weight='0', reason='fixture', verdict='WATCH')
