"""Read-only execution-input diagnostics, explicitly separate from readiness."""

from datetime import datetime
import math

from contracts.symbol import CanonicalSymbol
from competition_core.pipeline.account import AccountSnapshot, assess_account
from competition_core.pipeline.config import PipelineConfig
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.portfolio import FeeSchedule
from competition_core.pipeline.realtime_gate import evaluate_realtime_quotes
from competition_core.pipeline.time_context import ResearchContext, SHANGHAI


def audit_execution_inputs(*, account: AccountSnapshot, fees: FeeSchedule | None,
                           context: ResearchContext, config: PipelineConfig,
                           observed_at: datetime, test_mode: bool = False,
                           quotes: list[dict] | None = None,
                           symbols: list[str] | None = None) -> dict:
    """Complete diagnostics even for unusable data; never create a portfolio.

    Test mode downgrades ONLY account age to a warning. It does not change the
    clock, source account, production config or strict account/quote assessment.
    """
    if not isinstance(account, AccountSnapshot) or type(test_mode) is not bool:
        raise ValueError("valid account and explicit boolean test_mode are required")
    if fees is not None and not isinstance(fees, FeeSchedule):
        raise ValueError("fee schedule must use the existing validated contract")
    strict = assess_account(account, context=context, config=config, observed_at=observed_at)
    warnings = []
    account_issues = list(strict["reasons"])
    if test_mode and "STALE_ACCOUNT_STATE" in account_issues:
        account_issues.remove("STALE_ACCOUNT_STATE")
        warnings.append("STALE_ACCOUNT_STATE")
    issues = list(account_issues)
    if fees is None:
        issues.append("MISSING_COMPETITION_FEE_SCHEDULE")
    else:
        if fees.as_of > observed_at:
            issues.append("FUTURE_FEE_SCHEDULE")
        if not fees.as_of.astimezone(SHANGHAI).date() <= context.research_date <= fees.valid_through:
            issues.append("FEE_SCHEDULE_DATE_MISMATCH")

    quote_audit = None
    if quotes is not None or symbols is not None:
        if (not isinstance(symbols, list) or not symbols
                or any(not isinstance(symbol, str) for symbol in symbols)
                or len(set(symbols)) != len(symbols)):
            raise ValueError("nonempty unique requested symbols are required")
        for symbol in symbols:
            CanonicalSymbol.from_core(symbol)
        supplied = [] if quotes is None else quotes
        if not isinstance(supplied, list):
            raise ValueError("quotes must be a list")
        seen = set()
        bad_ohlc = []
        for row in supplied:
            if not isinstance(row, dict):
                raise ValueError("quote rows must be mappings")
            symbol = row.get("symbol")
            CanonicalSymbol.from_core(symbol)
            if symbol in seen:
                raise ValueError("duplicate quote identity")
            seen.add(symbol)
            values = [row.get(key) for key in ("price", "open", "high", "low")]
            # This independent check must also work when prev_close is absent.
            if all(type(v) in (int, float) and math.isfinite(v) and v > 0 for v in values):
                price, opening, high, low = values
                if high < max(price, opening, low) or low > min(price, opening, high):
                    bad_ohlc.append(symbol)
        wanted = set(symbols)
        gate, gate_error = None, None
        try:
            gate = evaluate_realtime_quotes(symbols=symbols,
                quotes=[row for row in supplied if row["symbol"] in wanted],
                context=context, config=config, observed_at=observed_at,
                snapshot_id="offline-input-audit-" + account.snapshot_hash)
        except PipelineError as exc:
            # Historical diagnostics retain the live gate's date rejection.
            # Never substitute a historical observation clock or a passing gate.
            if exc.code != "NOT_READY" or observed_at.astimezone(SHANGHAI).date() == context.research_date:
                raise
            gate_error = {"code": exc.code}
        missing = sorted(wanted - seen)
        quote_audit = dict(requested_count=len(symbols), supplied_count=len(supplied),
            covered_symbols=sorted(wanted & seen), missing_symbols=missing,
            outside_requested_symbols=sorted(seen - wanted),
            ohlc_inconsistent_symbols=sorted(bad_ohlc), strict_gate=gate,
            strict_gate_error=gate_error, strict_gate_relaxed=False)
        if missing:
            issues.append("REQUESTED_QUOTES_INCOMPLETE")
        if bad_ohlc:
            issues.append("OHLC_PRICE_INCONSISTENT")
        if gate is None or len(gate["output_symbols"]) != len(symbols):
            issues.append("QUOTE_EXECUTION_CHECKS_NOT_PASSED")
    else:
        issues.append("MISSING_QUOTES")

    return dict(kind="execution_input_diagnostic", mode="TEST_ONLY" if test_mode else "STRICT_DIAGNOSTIC",
        validation_completed=True, observed_at=observed_at.isoformat(), context=context.to_dict(),
        config_hash=config.config_hash, account_schema_valid=True, strict_account=strict,
        account_test_issues=account_issues, account_test_warnings=warnings,
        issues=sorted(set(issues)), fee_schedule=fees.to_dict() if fees else None,
        quote_audit=quote_audit, execution_ready=False, production_accepted=False,
        orders_submitted=0, portfolio_generated=False)
