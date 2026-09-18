from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from adapters.tushare.client import (
    TushareConfigurationError,
    create_tushare_client,
    configured_tushare_client,
)


class FakeClient:
    def __init__(self) -> None:
        self._DataApi__http_url = "https://api.tushare.pro"


def _fake_tushare(monkeypatch: pytest.MonkeyPatch) -> tuple[FakeClient, list[str]]:
    client = FakeClient()
    tokens: list[str] = []

    def pro_api(token: str) -> FakeClient:
        tokens.append(token)
        return client

    monkeypatch.setitem(sys.modules, "tushare", SimpleNamespace(pro_api=pro_api))
    return client, tokens


def test_official_url_is_untouched_without_override(monkeypatch: pytest.MonkeyPatch) -> None:
    client, tokens = _fake_tushare(monkeypatch)

    result = create_tushare_client(environ={"TUSHARE_TOKEN": "token"})

    assert result is client
    assert tokens == ["token"]
    assert client._DataApi__http_url == "https://api.tushare.pro"


def test_compatible_url_is_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    client, _ = _fake_tushare(monkeypatch)

    result = create_tushare_client(
        environ={
            "TUSHARE_TOKEN": "token",
            "TUSHARE_BASE_URL": "https://t.xiaodefa.top/",
        }
    )

    assert result is client
    assert client._DataApi__http_url == "https://t.xiaodefa.top/"


def test_missing_token_fails_closed() -> None:
    with pytest.raises(TushareConfigurationError, match="TUSHARE_TOKEN"):
        create_tushare_client(environ={})


def test_explicit_file_fallback_does_not_override_injected_credential(monkeypatch, tmp_path):
    _, tokens = _fake_tushare(monkeypatch)
    configured_tushare_client(token_file=tmp_path / "does-not-exist", environ={"TUSHARE_TOKEN": "injected"})
    assert tokens == ["injected"]


def test_file_bom_and_compatible_base(monkeypatch, tmp_path):
    client, tokens = _fake_tushare(monkeypatch)
    path = tmp_path / "synthetic-token.txt"
    path.write_text("synthetic-test-only\n", encoding="utf-8-sig")
    configured_tushare_client(token_file=path, base_url="https://t.xiaodefa.top/", environ={})
    assert tokens == ["synthetic-test-only"] and client._DataApi__http_url == "https://t.xiaodefa.top/"


@pytest.mark.parametrize("content", ["", "one two", "x" * 4097])
def test_invalid_token_file_is_never_echoed(monkeypatch, tmp_path, content):
    _, tokens = _fake_tushare(monkeypatch)
    path = tmp_path / "synthetic-token.txt"
    path.write_text(content, encoding="utf-8")
    with pytest.raises(TushareConfigurationError, match="exactly one"):
        configured_tushare_client(token_file=path, environ={})
    assert not tokens
