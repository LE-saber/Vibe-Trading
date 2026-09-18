from competition_core.config import load_config
from competition_core.universe.ranking import screen_competition_universe
from tests.fakes import LargeCompetitionClient


def test_frozen_legacy_preopen_counts_and_amount_top20() -> None:
    result = screen_competition_universe(
        client=LargeCompetitionClient(),
        config=load_config(),
        as_of="2026-09-01",
        phase="preopen",
        sort_by="amount",
        limit=20,
    )

    assert result["market_data_reference_date"] == "2026-08-31"
    assert result["counts"]["candidates"] == 5216
    assert result["counts"]["eligible"] == 5007
    assert result["returned"] == 20
    assert [row["symbol"] for row in result["rows"]] == [
        f"{number:06d}.SH" for number in range(603009, 603000, -1)
    ] + [
        f"{number:06d}.SH" for number in range(601999, 601988, -1)
    ]


def test_frozen_legacy_momentum_path_is_deterministic() -> None:
    first = screen_competition_universe(
        client=LargeCompetitionClient(), config=load_config(), as_of="2026-09-01",
        phase="preopen", sort_by="momentum_5d", limit=20,
    )
    second = screen_competition_universe(
        client=LargeCompetitionClient(), config=load_config(), as_of="2026-09-01",
        phase="preopen", sort_by="momentum_5d", limit=20,
    )

    assert first == second
    assert all(row["momentum_5d"] is not None for row in first["rows"])
