import json

import pytest

from contracts.symbol import CanonicalSymbol, SymbolContractError


def test_core_symbol_has_stable_contract_fields():
    symbol = CanonicalSymbol.from_core("000977.SZ")

    assert symbol.symbol == "000977.SZ"
    assert symbol.code == "000977"
    assert symbol.exchange == "SZSE"
    assert symbol.market == "CN_A"
    assert symbol.board == "MAIN"
    assert symbol.currency == "CNY"
    assert symbol.lot_size == 100


def test_board_is_derived_for_supported_a_share_boards():
    assert CanonicalSymbol.from_core("600000.SH").board == "MAIN"
    assert CanonicalSymbol.from_core("688001.SH").board == "STAR"
    assert CanonicalSymbol.from_core("300750.SZ").board == "CHINEXT"
    assert CanonicalSymbol.from_core("302132.SZ").board == "CHINEXT"


@pytest.mark.parametrize(
    ("provider", "value", "expected"),
    [
        ("tushare", "000977.SZ", "000977.SZ"),
        ("tushare", "600000.SH", "600000.SH"),
        ("qlib", "SZ000977", "000977.SZ"),
        ("qlib", "SH600000", "600000.SH"),
        ("vnpy", "000977.SZSE", "000977.SZ"),
        ("vnpy", "600000.SSE", "600000.SH"),
    ],
)
def test_provider_to_core_round_trip(provider, value, expected):
    canonical = CanonicalSymbol.from_provider(value, provider)

    assert canonical.symbol == expected
    assert CanonicalSymbol.from_provider(
        canonical.to_provider(provider), provider
    ) == canonical


@pytest.mark.parametrize(
    ("provider", "value"),
    [
        ("tushare", "000977.SZ"),
        ("qlib", "SZ000977"),
        ("vnpy", "000977.SZSE"),
    ],
)
def test_core_to_provider_round_trip(provider, value):
    canonical = CanonicalSymbol.from_provider(value, provider)

    assert CanonicalSymbol.from_provider(
        canonical.to_provider(provider), provider
    ) == canonical


@pytest.mark.parametrize(
    ("value", "provider", "exchange"),
    [
        ("000977.SH", "tushare", None),
        ("SZ600000", "qlib", None),
        ("SH000977", "qlib", None),
        ("000977.SZSE", "vnpy", "SSE"),
        ("600000.SSE", "vnpy", "SZSE"),
        ("000977.SZ", "tushare", "SSE"),
    ],
)
def test_exchange_suffix_conflicts_fail_closed(value, provider, exchange):
    with pytest.raises(SymbolContractError):
        CanonicalSymbol.from_provider(value, provider, exchange=exchange)


@pytest.mark.parametrize(
    ("provider", "value"),
    [
        ("tushare", "830001.BJ"),
        ("tushare", "510300.SH"),
        ("tushare", "159915.SZ"),
        ("tushare", "000000.SZ"),
        ("tushare", "000977"),
        ("tushare", "000977.sz"),
        ("tushare", "SZ000977"),
        ("qlib", "000977.SZ"),
        ("qlib", "BJ830001"),
        ("vnpy", "000977.SZ"),
        ("vnpy", "000977.SH"),
        ("vnpy", "000977.SZEX"),
        ("tushare", None),
        ("tushare", 977),
        ("other", "000977.SZ"),
    ],
)
def test_unsupported_or_unrecognized_inputs_are_rejected(provider, value):
    with pytest.raises((SymbolContractError, ValueError)):
        CanonicalSymbol.from_provider(value, provider)


def test_symbol_is_immutable_and_json_serializable():
    symbol = CanonicalSymbol.from_core("000977.SZ")

    with pytest.raises(AttributeError):
        symbol.code = "600000"

    payload = symbol.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert CanonicalSymbol.from_dict(json.loads(symbol.to_json())) == symbol


def test_direct_construction_rejects_conflicting_fields():
    with pytest.raises(SymbolContractError):
        CanonicalSymbol(
            symbol="000977.SZ",
            code="000977",
            exchange="SSE",
            market="CN_A",
            board="MAIN",
            currency="CNY",
            lot_size=100,
        )
