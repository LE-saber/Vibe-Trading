import json
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from decimal import Decimal

import pytest

from vibe_trading.execution import GateError
from vibe_trading.models import BrokerOrder, OrderStatus, Plan, ProposedOrder, canonical, digest, now_utc


def start(bench, plan):
    approval = bench.execution.approve(plan.plan_id, digest(plan), 'local_owner')
    return bench.execution.start(approval.approval_id)


def drive(bench, batch, n=20):
    for _ in range(n): bench.execution.tick(batch)
    return bench.execution.snapshot(batch)


def test_no_orders_without_approval(planned):
    bench, plan = planned
    assert bench.store.list('demo_order') == []
    with pytest.raises(GateError): bench.execution.start('missing')
    assert bench.store.list('demo_order') == []


def test_wrong_hash_and_expired_approval(planned):
    bench, plan = planned
    with pytest.raises(GateError): bench.execution.approve(plan.plan_id, '0'*64, 'owner')
    with pytest.raises(GateError): bench.execution.approve(plan.plan_id, digest(plan), 'owner', plan.expires_at+timedelta(seconds=1))


def test_complete_batch_reconciles_releases_account(planned):
    bench, plan = planned
    batch = start(bench, plan)
    status = drive(bench, batch)
    assert status['state'] == 'COMPLETED', status
    assert len(bench.store.list('demo_order')) == 4
    assert bench.provider.context([]).account.available_cash == plan.projected_cash
    assert not bench.execution.active()
    assert all(i['receipt']['status'] == 'FILLED' for i in status['intents'])


def test_partial_sell_does_not_fund_new_buy(planned):
    bench, plan = planned
    batch = start(bench, plan)
    bench.execution.tick(batch); bench.execution.tick(batch)
    orders = bench.store.list('demo_order')
    assert len(orders) == 1
    assert orders[0]['order']['status'] == 'PARTIALLY_FILLED'
    assert orders[0]['order']['side'] == 'SELL'


def test_double_click_concurrent_start_idempotent(planned):
    bench, plan = planned
    with ThreadPoolExecutor(max_workers=5) as pool:
        ids = list(pool.map(lambda _: start(bench, plan), range(5)))
    assert len(set(ids)) == 1
    with ThreadPoolExecutor(max_workers=5) as pool:
        list(pool.map(lambda _: bench.execution.tick(ids[0]), range(5)))
    records = bench.store.list('demo_order')
    assert len({r['order']['client_order_id'] for r in records}) == len(records)


def test_another_batch_cannot_own_account(planned):
    bench, plan = planned
    batch = start(bench, plan)
    new = bench.execution.plan(bench.replan(plan.run_id))
    with pytest.raises(GateError): start(bench, new)
    assert bench.execution.active() == [batch]


def test_account_drift_invalidates_old_plan(planned):
    bench, plan = planned
    state = bench.store.get('demo', 'account'); state['cash'] = '55001'
    bench.store.put('demo', 'account', state)
    with pytest.raises(GateError): start(bench, plan)


def test_lost_response_after_acceptance_never_retries(planned, monkeypatch):
    bench, plan = planned
    original, calls = bench.provider.submit, []
    def ambiguous(order, key):
        calls.append(key); original(order, key)
        raise TimeoutError('FAKE_PRIVATE_TOKEN_MUST_NOT_LEAK')
    monkeypatch.setattr(bench.provider, 'submit', ambiguous)
    batch = start(bench, plan); drive(bench, batch)
    assert len(calls) == 1
    assert bench.execution.snapshot(batch)['state'] == 'MANUAL_RECONCILIATION'
    assert batch in bench.execution.active()
    bench.store.recover(); drive(bench, batch)
    assert len(calls) == 1
    assert 'FAKE_PRIVATE_TOKEN_MUST_NOT_LEAK' not in str(bench.execution.snapshot(batch))


def test_unknown_can_only_resolve_with_exact_authoritative_evidence(planned, monkeypatch):
    bench, plan = planned
    original = bench.provider.submit
    accepted = []
    def ambiguous(order, key):
        receipt = original(order, key)
        accepted.append(receipt)
        raise TimeoutError('lost response')
    monkeypatch.setattr(bench.provider, 'submit', ambiguous)
    batch = start(bench, plan); drive(bench, batch)
    intent = bench.execution.snapshot(batch)['intents'][0]
    with pytest.raises(GateError):
        bench.execution.resolve_unknown(batch, intent['id'], 'not-an-order', 'operator log', 'owner')
    authoritative = bench.provider.get_order
    monkeypatch.setattr(
        bench.provider, 'get_order',
        lambda order_id: authoritative(order_id).model_copy(update={'client_order_id': 'another-intent'}),
    )
    with pytest.raises(GateError, match='authoritatively bind'):
        bench.execution.resolve_unknown(
            batch, intent['id'], accepted[0].order_id, 'unrelated identical order', 'owner')
    monkeypatch.setattr(bench.provider, 'get_order', authoritative)
    resolution = bench.execution.resolve_unknown(
        batch, intent['id'], accepted[0].order_id, 'local authoritative order read', 'owner')
    assert resolution.original_intent_hash == digest(ProposedOrder.model_validate(intent['body']))
    assert resolution.record_hash == digest(resolution.model_dump(exclude={'record_hash'}))
    resolved = bench.execution.snapshot(batch)['intents'][0]
    assert resolved['state'] == 'SUBMITTED'
    assert resolved['broker_order_id'] == accepted[0].order_id
    with pytest.raises(GateError):
        bench.execution.resolve_unknown(
            batch, intent['id'], accepted[0].order_id, 'duplicate attempt', 'owner')


def test_unknown_resolution_rejects_live_sending_without_read(planned, monkeypatch):
    bench, plan = planned
    batch = start(bench, plan)
    intent = bench.execution.snapshot(batch)['intents'][0]
    with bench.store.transaction() as conn:
        conn.execute("UPDATE intents SET state='SENDING' WHERE id=?", (intent['id'],))
    reads = []
    monkeypatch.setattr(bench.provider, 'get_order', lambda order_id: reads.append(order_id))

    with pytest.raises(GateError) as caught:
        bench.execution.resolve_unknown(batch, intent['id'], 'broker-order', 'local exact read', 'owner')

    assert caught.value.code == 'NOT_UNKNOWN'
    assert reads == []


def test_unknown_resolution_does_not_win_race_with_sending(planned, monkeypatch):
    bench, plan = planned
    original_submit = bench.provider.submit
    accepted = []

    def ambiguous(order, key):
        receipt = original_submit(order, key)
        accepted.append(receipt)
        raise TimeoutError('lost response')

    monkeypatch.setattr(bench.provider, 'submit', ambiguous)
    batch = start(bench, plan)
    bench.execution.tick(batch)
    intent = bench.execution.snapshot(batch)['intents'][0]
    original_read = bench.provider.get_order

    def race(order_id):
        receipt = original_read(order_id)
        with bench.store.transaction() as conn:
            conn.execute("UPDATE intents SET state='SENDING' WHERE id=?", (intent['id'],))
        return receipt

    monkeypatch.setattr(bench.provider, 'get_order', race)
    with pytest.raises(GateError) as caught:
        bench.execution.resolve_unknown(
            batch, intent['id'], accepted[0].order_id, 'local exact read', 'owner')

    assert caught.value.code == 'RESOLUTION_RACE'
    assert bench.execution.snapshot(batch)['intents'][0]['state'] == 'SENDING'
    assert bench.store.list('operator_resolution') == []


def test_restart_recovery_keeps_unknown_resolution_available(planned, monkeypatch):
    bench, plan = planned
    batch = start(bench, plan)
    intent = bench.execution.snapshot(batch)['intents'][0]
    order = ProposedOrder.model_validate(intent['body'])
    receipt = BrokerOrder(
        order_id='restart-order', symbol=order.symbol, side=order.side,
        quantity=order.quantity, filled_quantity=0, limit_price=order.limit_price,
        status=OrderStatus.PENDING, updated_at=now_utc(), client_order_id=intent['id'],
    )
    with bench.store.transaction() as conn:
        conn.execute("UPDATE intents SET state='SENDING' WHERE id=?", (intent['id'],))
    bench.store.recover()
    monkeypatch.setattr(bench.provider, 'get_order', lambda order_id: receipt)

    assert bench.execution.snapshot(batch)['intents'][0]['state'] == 'UNKNOWN'
    resolution = bench.execution.resolve_unknown(
        batch, intent['id'], receipt.order_id, 'restart authoritative read', 'owner')

    assert resolution.broker_order_id == receipt.order_id
    assert bench.execution.snapshot(batch)['intents'][0]['state'] == 'SUBMITTED'


def test_unknown_recovery_rejects_intent_not_bound_to_approved_plan(planned, monkeypatch):
    bench, plan = planned
    original = bench.provider.submit
    accepted = []
    def ambiguous(order, key):
        receipt = original(order, key); accepted.append(receipt)
        raise TimeoutError('lost response')
    monkeypatch.setattr(bench.provider, 'submit', ambiguous)
    batch = start(bench, plan); drive(bench, batch)
    intent = bench.execution.snapshot(batch)['intents'][0]
    altered = ProposedOrder.model_validate(intent['body']).model_copy(update={'reason': 'tampered'})
    with bench.store.transaction() as conn:
        conn.execute('UPDATE intents SET body=? WHERE id=?', (canonical(altered), intent['id']))
    with pytest.raises(GateError) as caught:
        bench.execution.resolve_unknown(
            batch, intent['id'], accepted[0].order_id, 'local exact read', 'owner')
    assert caught.value.code == 'INTENT_CHANGED'
    assert bench.execution.snapshot(batch)['intents'][0]['state'] == 'UNKNOWN'


def test_unknown_recovery_rejects_self_consistent_plan_substitution(planned, monkeypatch):
    bench, plan = planned
    original = bench.provider.submit
    accepted = []
    def ambiguous(order, key):
        receipt = original(order, key); accepted.append(receipt)
        raise TimeoutError('lost response')
    monkeypatch.setattr(bench.provider, 'submit', ambiguous)
    batch = start(bench, plan); drive(bench, batch)
    intent = bench.execution.snapshot(batch)['intents'][0]

    document = bench.store.get('plan', plan.plan_id)
    substituted = plan.model_copy(update={'warnings': plan.warnings + ('substituted plan',)})
    document['plan'] = substituted.model_dump(mode='python')
    document['hash'] = digest(substituted)
    bench.store.put('plan', plan.plan_id, document)
    assert bench.execution.plan(plan.plan_id) == substituted

    reads = []
    authoritative = bench.provider.get_order
    def counted_read(order_id):
        reads.append(order_id)
        return authoritative(order_id)
    monkeypatch.setattr(bench.provider, 'get_order', counted_read)
    with pytest.raises(GateError) as caught:
        bench.execution.resolve_unknown(
            batch, intent['id'], accepted[0].order_id, 'local exact read', 'owner')
    assert caught.value.code == 'PLAN_CHANGED'
    assert reads == []
    assert bench.store.list('operator_resolution') == []
    unresolved = bench.execution.snapshot(batch)['intents'][0]
    assert unresolved['state'] == 'UNKNOWN'
    assert unresolved['broker_order_id'] is None


def test_crash_before_network_send_never_replays(planned):
    bench, plan = planned
    batch = start(bench, plan)
    with bench.store.transaction() as conn:
        conn.execute("UPDATE intents SET state='SENDING' WHERE batch_id=? AND ordinal=0", (batch,))
    bench.store.recover(); drive(bench, batch)
    assert bench.store.list('demo_order') == []
    assert bench.execution.snapshot(batch)['state'] == 'MANUAL_RECONCILIATION'


def test_receipt_mismatch_is_ambiguous(planned, monkeypatch):
    bench, plan = planned
    original = bench.provider.submit
    def bad(order, key): return original(order, key).model_copy(update={'symbol': '600999.SH'})
    monkeypatch.setattr(bench.provider, 'submit', bad)
    batch = start(bench, plan); drive(bench, batch)
    assert bench.execution.snapshot(batch)['state'] == 'MANUAL_RECONCILIATION'
    assert len(bench.store.list('demo_order')) == 1


@pytest.mark.parametrize('case', ['price', 'fee'])
def test_changed_market_gate_prevents_dispatch(planned, monkeypatch, case):
    bench, plan = planned
    batch = start(bench, plan); original = bench.provider.context
    def changed(symbols):
        c = original(symbols)
        if case == 'price':
            return c.model_copy(update={'quotes': tuple(q.model_copy(update={'last': q.last*Decimal('1.02')}) for q in c.quotes)})
        return c.model_copy(update={'fees': c.fees.model_copy(update={'version': 'new'})})
    monkeypatch.setattr(bench.provider, 'context', changed)
    bench.execution.tick(batch)
    assert bench.execution.snapshot(batch)['state'] == 'PAUSED'
    assert bench.store.list('demo_order') == []


def test_stop_does_not_cancel_open_order(planned):
    bench, plan = planned
    batch = start(bench, plan); bench.execution.tick(batch); bench.execution.stop(batch)
    assert bench.store.list('demo_order')[0]['order']['status'] == 'PENDING'
    status = drive(bench, batch)
    assert status['state'] == 'STOPPED'
    assert len(bench.store.list('demo_order')) == 1
    assert bench.store.list('demo_order')[0]['order']['status'] == 'FILLED'


def test_explicit_cancel_partial_order_requires_replan(planned):
    bench, plan = planned
    batch = start(bench, plan); bench.execution.tick(batch)
    intent = bench.execution.snapshot(batch)['intents'][0]
    bench.execution.request_cancel(batch, intent['id'])
    bench.execution.tick(batch); status = drive(bench, batch)
    assert status['state'] == 'STOPPED'
    assert status['intents'][0]['receipt']['status'] == 'CANCELLED'
    assert status['intents'][0]['receipt']['filled_quantity'] > 0
    assert len(bench.store.list('demo_order')) == 1


def test_expiry_stops_unsent_but_reconciles_existing(planned):
    bench, plan = planned
    batch = start(bench, plan); bench.execution.tick(batch)
    late = plan.expires_at+timedelta(seconds=1)
    bench.execution.tick(batch, late)
    assert bench.execution.snapshot(batch)['state'] == 'EXPIRED_WITH_OPEN_ORDERS'
    bench.execution.tick(batch, late)
    assert bench.execution.snapshot(batch)['state'] == 'EXPIRED'
    assert len(bench.store.list('demo_order')) == 1


def test_integrity_tamper_blocks_approval(planned):
    bench, plan = planned
    doc = bench.store.get('plan', plan.plan_id); doc['plan']['orders'][0]['quantity'] = 1
    bench.store.put('plan', plan.plan_id, doc)
    with pytest.raises(GateError): bench.execution.approve(plan.plan_id, digest(plan), 'owner')


def test_capabilities_fail_closed(planned):
    bench, plan = planned
    bench.execution.enabled = False
    with pytest.raises(GateError): start(bench, plan)


def test_fill_regression_rejected(planned):
    bench, plan = planned
    batch = start(bench, plan); bench.execution.tick(batch)
    order = bench.provider.get_order(bench.execution.snapshot(batch)['intents'][0]['broker_order_id'])
    earlier = order.model_copy(update={'filled_quantity': 0, 'status': OrderStatus.PENDING})
    with pytest.raises(GateError): bench.execution._match(earlier, plan.orders[0], order)


def test_missing_fill_fees_blocks_next_order(planned, monkeypatch):
    bench, plan = planned
    batch = start(bench, plan); original = bench.provider.get_order
    def missing(order_id): return original(order_id).model_copy(update={'fee_paid': None})
    monkeypatch.setattr(bench.provider, 'get_order', missing)
    for _ in range(3): bench.execution.tick(batch)
    assert 'MISSING_FILL_ACCOUNTING' in bench.execution.snapshot(batch)['reason']
    assert len(bench.store.list('demo_order')) == 1


def test_read_timeout_never_resubmits(planned, monkeypatch):
    bench, plan = planned
    batch = start(bench, plan); bench.execution.tick(batch)
    def fail(order_id): raise TimeoutError('private broker details')
    monkeypatch.setattr(bench.provider, 'get_order', fail)
    drive(bench, batch, 5)
    assert len(bench.store.list('demo_order')) == 1
    assert bench.execution.snapshot(batch)['state'] == 'PAUSED_PROVIDER'
    assert batch in bench.execution.active()


def test_restart_known_order_reconciles_without_dispatch(planned):
    bench, plan = planned
    batch = start(bench, plan); bench.execution.tick(batch)
    bench.store.recover(); status = drive(bench, batch)
    assert status['state'] == 'STOPPED'
    assert len(bench.store.list('demo_order')) == 1
    assert status['intents'][0]['receipt']['status'] == 'FILLED'


def test_cancel_cannot_target_arbitrary_order(planned):
    bench, plan = planned
    batch = start(bench, plan)
    with pytest.raises(GateError): bench.execution.request_cancel(batch, 'another-order')


def test_limit_violating_fill_rejected(planned):
    bench, plan = planned
    batch = start(bench, plan); bench.execution.tick(batch)
    order = bench.provider.get_order(bench.execution.snapshot(batch)['intents'][0]['broker_order_id'])
    with pytest.raises(GateError):
        bench.execution._match(order.model_copy(update={'average_fill_price': Decimal('1')}), plan.orders[0])


def test_cancel_transport_uncertainty_does_not_repeat(planned, monkeypatch):
    bench, plan = planned
    batch = start(bench, plan); bench.execution.tick(batch); calls = []
    def uncertain(order_id): calls.append(order_id); raise TimeoutError('cancel unknown')
    monkeypatch.setattr(bench.provider, 'cancel', uncertain)
    bench.execution.request_cancel(batch, bench.execution.snapshot(batch)['intents'][0]['id'])
    drive(bench, batch)
    assert len(calls) == 1
    assert len(bench.store.list('demo_order')) == 1


def test_existing_start_revalidates_persisted_plan_before_return(planned):
    bench, plan = planned
    approval = bench.execution.approve(plan.plan_id, digest(plan), 'local_owner')
    batch = bench.execution.start(approval.approval_id)
    document = bench.store.get('plan', plan.plan_id)
    document['plan']['warnings'].append('tampered existing batch')
    document['hash'] = digest(document['plan'])
    bench.store.put('plan', plan.plan_id, document)

    with pytest.raises(GateError) as caught:
        bench.execution.start(approval.approval_id)

    assert caught.value.code == 'PLAN_CHANGED'
    assert bench.execution.snapshot(batch)['state'] == 'QUEUED'


def test_cancel_revalidates_persisted_plan_before_mutation(planned):
    bench, plan = planned
    batch = start(bench, plan)
    bench.execution.tick(batch)
    intent = bench.execution.snapshot(batch)['intents'][0]
    document = bench.store.get('plan', plan.plan_id)
    document['plan']['warnings'].append('tampered cancel plan')
    document['hash'] = digest(document['plan'])
    bench.store.put('plan', plan.plan_id, document)

    with pytest.raises(GateError) as caught:
        bench.execution.request_cancel(batch, intent['id'])

    assert caught.value.code == 'PLAN_CHANGED'
    current = bench.execution.snapshot(batch)['intents'][0]
    assert current['state'] == 'SUBMITTED'
    assert current['cancel_state'] == ''


def test_changed_plan_even_with_rehashed_document_blocks(planned):
    bench, plan = planned
    batch = start(bench, plan)
    doc = bench.store.get('plan', plan.plan_id); doc['plan']['orders'][0]['reason'] = 'changed'
    doc['hash'] = digest(Plan.model_validate(doc['plan']))
    bench.store.put('plan', plan.plan_id, doc)
    bench.execution.tick(batch)
    assert bench.execution.snapshot(batch)['state'] == 'PAUSED'
    assert bench.store.list('demo_order') == []


def test_outbox_payload_must_match_approved_order(planned):
    bench, plan = planned
    batch = start(bench, plan)
    changed = plan.orders[0].model_copy(update={'quantity': 100})
    with bench.store.transaction() as conn:
        conn.execute('UPDATE intents SET body=? WHERE batch_id=? AND ordinal=0', (canonical(changed), batch))
    bench.execution.tick(batch)
    assert bench.execution.snapshot(batch)['state'] == 'PAUSED'
    assert bench.store.list('demo_order') == []


def test_stale_owned_order_in_snapshot_blocks_next_dispatch(planned, monkeypatch):
    bench, plan = planned
    batch = start(bench, plan); bench.execution.tick(batch)
    stale = bench.provider.context([]).account.orders[0]
    bench.execution.tick(batch)
    original = bench.provider.context
    def inconsistent(symbols):
        c = original(symbols)
        return c.model_copy(update={'account': c.account.model_copy(update={'orders': (stale,)})})
    monkeypatch.setattr(bench.provider, 'context', inconsistent)
    bench.execution.tick(batch)
    assert 'ORDER_STATE_NOT_RECONCILED' in bench.execution.snapshot(batch)['reason']
    assert len(bench.store.list('demo_order')) == 1


def test_replan_survives_reload_and_invalidates_unstarted_approval(planned):
    bench, plan = planned
    approval = bench.execution.approve(plan.plan_id, digest(plan), 'local_owner')
    new_id = bench.replan(plan.run_id)
    assert bench.store.get('run', plan.run_id)['plan_id'] == new_id
    assert bench.execution.plan(plan.plan_id) == plan
    with pytest.raises(GateError) as caught:
        bench.execution.start(approval.approval_id)
    assert caught.value.code == 'PLAN_SUPERSEDED'
    with pytest.raises(GateError):
        bench.execution.approve(plan.plan_id, digest(plan), 'local_owner')
    assert bench.store.list('demo_order') == []


def test_legacy_approved_batch_reconciles_without_upgrading_plan_hash(planned):
    bench, plan = planned
    legacy_document = bench.store.get('plan', plan.plan_id)
    legacy_document['plan']['context']['account'].pop('collected_at')
    legacy_document['hash'] = digest(legacy_document['plan'])
    bench.store.put('plan', plan.plan_id, legacy_document)
    legacy_document = bench.store.get('plan', plan.plan_id)

    loaded = bench.execution.plan(plan.plan_id)
    assert loaded.context.account.collected_at == loaded.context.account.as_of
    assert legacy_document['hash'] != digest(loaded)
    assert bench.store.get('plan', plan.plan_id) == legacy_document

    approval = bench.execution.approve(plan.plan_id, legacy_document['hash'], 'legacy_owner')
    with bench.store.connect() as conn:
        approval_body = conn.execute(
            'SELECT body FROM approvals WHERE id=?', (approval.approval_id,)
        ).fetchone()['body']
    assert json.loads(approval_body)['plan_hash'] == legacy_document['hash']

    batch = bench.execution.start(approval.approval_id)
    bench.execution.tick(batch)
    bench.execution.tick(batch)
    bench.execution.tick(batch)
    assert len(bench.store.list('demo_order')) == 2
    assert bench.execution.snapshot(batch)['intents'][0]['receipt']['status'] == 'FILLED'
    assert bench.store.get('plan', plan.plan_id) == legacy_document
    with bench.store.connect() as conn:
        assert conn.execute(
            'SELECT body FROM approvals WHERE id=?', (approval.approval_id,)
        ).fetchone()['body'] == approval_body

    tampered = bench.store.get('plan', plan.plan_id)
    tampered['plan']['warnings'].append('tampered')
    tampered['hash'] = digest(tampered['plan'])
    bench.store.put('plan', plan.plan_id, tampered)
    bench.execution.tick(batch)
    status = bench.execution.snapshot(batch)
    assert status['state'] == 'PAUSED'
    assert 'PLAN_CHANGED' in status['reason']
    assert len(bench.store.list('demo_order')) == 2


def test_legacy_existing_approval_must_keep_the_raw_plan_hash(planned):
    bench, plan = planned
    legacy_document = bench.store.get('plan', plan.plan_id)
    legacy_document['plan']['context']['account'].pop('collected_at')
    legacy_document['hash'] = digest(legacy_document['plan'])
    bench.store.put('plan', plan.plan_id, legacy_document)

    raw_hash = legacy_document['hash']
    approval = bench.execution.approve(plan.plan_id, raw_hash, 'legacy_owner')
    with bench.store.connect() as conn:
        approval_body = json.loads(
            conn.execute(
                'SELECT body FROM approvals WHERE id=?', (approval.approval_id,)
            ).fetchone()['body']
        )
    approval_body['plan_hash'] = digest(bench.execution.plan(plan.plan_id))
    with bench.store.transaction() as conn:
        conn.execute(
            'UPDATE approvals SET body=? WHERE id=?',
            (canonical(approval_body), approval.approval_id),
        )

    with pytest.raises(GateError, match='Approved plan failed integrity validation'):
        bench.execution.approve(plan.plan_id, raw_hash, 'legacy_owner')
    assert bench.store.get('plan', plan.plan_id) == legacy_document


def test_legacy_plan_cancel_uses_the_unmodified_approved_payload(planned):
    bench, plan = planned
    legacy_document = bench.store.get('plan', plan.plan_id)
    legacy_document['plan']['context']['account'].pop('collected_at')
    legacy_document['hash'] = digest(legacy_document['plan'])
    bench.store.put('plan', plan.plan_id, legacy_document)
    raw_hash = legacy_document['hash']
    approval = bench.execution.approve(plan.plan_id, raw_hash, 'legacy_owner')
    batch = bench.execution.start(approval.approval_id)

    bench.execution.tick(batch)
    intent = bench.execution.snapshot(batch)['intents'][0]
    bench.execution.request_cancel(batch, intent['id'])
    status = drive(bench, batch)

    assert status['state'] == 'STOPPED'
    assert status['intents'][0]['receipt']['status'] == 'CANCELLED'
    assert bench.store.get('plan', plan.plan_id) == legacy_document
    with bench.store.connect() as conn:
        assert json.loads(
            conn.execute(
                'SELECT body FROM approvals WHERE id=?', (approval.approval_id,)
            ).fetchone()['body']
        )['plan_hash'] == raw_hash


def test_legacy_plan_unknown_recovery_keeps_the_raw_hash_binding(planned, monkeypatch):
    bench, plan = planned
    legacy_document = bench.store.get('plan', plan.plan_id)
    legacy_document['plan']['context']['account'].pop('collected_at')
    legacy_document['hash'] = digest(legacy_document['plan'])
    bench.store.put('plan', plan.plan_id, legacy_document)
    raw_hash = legacy_document['hash']
    approval = bench.execution.approve(plan.plan_id, raw_hash, 'legacy_owner')

    original = bench.provider.submit
    accepted = []

    def ambiguous(order, key):
        receipt = original(order, key)
        accepted.append(receipt)
        raise TimeoutError('lost response')

    monkeypatch.setattr(bench.provider, 'submit', ambiguous)
    batch = bench.execution.start(approval.approval_id)
    bench.execution.tick(batch)
    intent = bench.execution.snapshot(batch)['intents'][0]
    resolution = bench.execution.resolve_unknown(
        batch, intent['id'], accepted[0].order_id, 'local exact read', 'legacy_owner'
    )

    assert resolution.broker_order_id == accepted[0].order_id
    assert bench.execution.snapshot(batch)['intents'][0]['state'] == 'SUBMITTED'
    assert bench.store.get('plan', plan.plan_id) == legacy_document
    with bench.store.connect() as conn:
        assert json.loads(
            conn.execute(
                'SELECT body FROM approvals WHERE id=?', (approval.approval_id,)
            ).fetchone()['body']
        )['plan_hash'] == raw_hash
