"""One-order target-position arithmetic; no account, quote or clock inference."""

from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR

from competition_core.pipeline.instruments import QuantityRules, floor_buy_quantity


def _decimal(value, name, *, positive=False):
    if isinstance(value, bool) or not isinstance(value, (Decimal, str, int, float)):
        raise ValueError(f"{name} must be numeric")
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if not result.is_finite() or result < 0 or result > Decimal("1e15") or (positive and result == 0):
        raise ValueError(f"{name} outside supported bounds")
    return result


def plan_quantity(*, current_quantity, sellable_quantity, target_value,
                  reference_price, rules: QuantityRules, slippage_buffer):
    """Preserve locked/odd-lot inventory; report inability to reach the budget.

    Buys round down within the incremental budget at a buffered limit price.
    Sells never exceed the desired reduction, explicit sellability or the one-
    order maximum. A residual position is visible, never silently discarded.
    Fees are deliberately handled separately by the portfolio cash allocator.
    """
    if (type(current_quantity) is not int or type(sellable_quantity) is not int
            or not 0 <= sellable_quantity <= current_quantity <= 10**12):
        raise ValueError("explicit current/sellable quantities are required")
    if not isinstance(rules, QuantityRules):
        raise ValueError("validated quantity rules are required")
    budget = _decimal(target_value, "target_value")
    price = _decimal(reference_price, "reference_price", positive=True)
    slip = _decimal(slippage_buffer, "slippage_buffer")
    if slip > Decimal("0.1") or price % rules.tick_size:
        raise ValueError("invalid slippage or off-tick reference price")
    current_value = current_quantity * price
    reasons, delta, order_price = [], 0, None
    if budget > current_value:
        order_price = ((price * (1 + slip) / rules.tick_size).to_integral_value(
            rounding=ROUND_CEILING) * rules.tick_size)
        delta = floor_buy_quantity(budget - current_value, order_price, rules)
        affordable = int((budget - current_value) // order_price)
        uncapped = affordable - affordable % rules.buy_step
        if uncapped > rules.limit_order_max:
            reasons.append("single_order_maximum")
        if not delta:
            reasons.append("below_minimum_buy_budget")
    elif budget < current_value:
        desired = current_quantity - int((budget / price).to_integral_value(rounding=ROUND_FLOOR))
        maximum = min(desired, sellable_quantity, rules.limit_order_max)
        if desired > sellable_quantity:
            reasons.append("sellable_quantity_constraint")
        if desired > rules.limit_order_max:
            reasons.append("single_order_maximum")
        if maximum == current_quantity == sellable_quantity:
            reduction = maximum
        else:
            reduction = maximum - maximum % rules.sell_step
            if reduction < rules.min_sell:
                reduction = 0
        delta = -reduction
        if reduction < desired:
            reasons.append("residual_position_above_target_budget")
        order_price = ((price * (1 - slip) / rules.tick_size).to_integral_value(
            rounding=ROUND_FLOOR) * rules.tick_size)
        if order_price <= 0:
            delta = 0
            reasons.append("nonpositive_buffered_sell_price")
    side = "BUY" if delta > 0 else "SELL" if delta < 0 else "HOLD"
    if delta and not rules.is_valid_quantity(side, abs(delta), current_quantity, sellable_quantity):
        raise ValueError("quantity calculation violates instrument rules")
    if not delta:
        order_price = None
    money = lambda value: format(value.quantize(Decimal("0.01")), "f")
    return {"current_quantity": current_quantity, "sellable_quantity": sellable_quantity,
            "target_quantity": current_quantity + delta, "delta_quantity": delta, "side": side,
            "reference_price": money(price), "order_price": money(order_price) if order_price is not None else None,
            "target_value": money(budget), "target_market_value": money((current_quantity + delta) * price),
            "gross_buy_cash": money(delta * order_price) if delta > 0 else "0.00",
            "gross_sell_proceeds": money(-delta * order_price) if delta < 0 else "0.00",
            "reasons": reasons}
