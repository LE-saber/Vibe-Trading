"""Credential handling uses synthetic temporary files; no real key is read."""

import importlib.util
import json
import os
from pathlib import Path

import pytest
import tushare


spec = importlib.util.spec_from_file_location(
    "prepare_canonical_token_file", Path(__file__).resolve().parents[2] / "scripts/prepare-canonical.py"
)
prepare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prepare)


def arguments(*extra):
    return ["--date", "2026-09-02", "--as-of", "2026-09-02T14:50:00+08:00", *map(str, extra)]


@pytest.mark.parametrize("bom", [False, True])
def test_file_and_url_override_only_the_current_factory_call(tmp_path, monkeypatch, bom):
    token = tmp_path / "key.txt"
    token.write_text("synthetic-file-token\n", encoding="utf-8-sig" if bom else "utf-8")
    env = tmp_path / "example.env"
    env.write_text("TUSHARE_TOKEN=synthetic-dotenv\nTUSHARE_BASE_URL=https://dotenv.invalid/\n")
    monkeypatch.setenv("TUSHARE_TOKEN", "synthetic-process")
    monkeypatch.setenv("TUSHARE_BASE_URL", "https://process.invalid/")
    clients = []

    def fake_pro_api(value):
        client = type("FakeClient", (), {})()
        client.token_value = value
        client._DataApi__http_url = "http://api.tushare.pro"
        clients.append(client)
        return client

    def execute(_args, **kwargs):
        client = kwargs["client_factory"]()
        assert client.token_value == "synthetic-file-token"
        assert client._DataApi__http_url == "https://compatible.invalid/"
        return {"ok": True}

    monkeypatch.setattr(tushare, "pro_api", fake_pro_api)
    monkeypatch.setattr(prepare, "_execute", execute)
    args = prepare._parser().parse_args(arguments("--env-file", env, "--token-file", token,
                                                 "--base-url", "https://compatible.invalid/"))
    assert prepare.run(args) == {"ok": True}
    assert len(clients) == 1
    assert os.environ["TUSHARE_TOKEN"] == "synthetic-process"
    assert os.environ["TUSHARE_BASE_URL"] == "https://process.invalid/"


def test_explicit_file_token_is_redacted_on_failure_and_environment_restored(tmp_path, monkeypatch, capsys):
    token = tmp_path / "key.txt"
    secret = "synthetic-do-not-log-file-key"
    token.write_text(secret)
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.delenv("TUSHARE_BASE_URL", raising=False)

    def fail(_args, **kwargs):
        assert os.environ["TUSHARE_TOKEN"] == secret
        raise prepare.PipelineError("SOURCE_UNAVAILABLE", f"upstream repeated {secret}",
                                    nested={secret: [f"prefix-{secret}"]})

    monkeypatch.setattr(prepare, "_execute", fail)
    assert prepare.main(arguments("--token-file", token, "--base-url", "https://compatible.invalid/")) == 2
    output = capsys.readouterr().out
    assert secret not in output
    assert json.loads(output)["error"]["code"] == "SOURCE_UNAVAILABLE"
    assert "TUSHARE_TOKEN" not in os.environ and "TUSHARE_BASE_URL" not in os.environ


@pytest.mark.parametrize("body", [None, b"", b"first-token\nsecond-token", b"token with spaces", b"\xff"])
def test_invalid_or_unreadable_file_fails_without_exposing_body(tmp_path, monkeypatch, capsys, body):
    token = tmp_path / "key.txt"
    if body is not None:
        token.write_bytes(body)
    called = []
    monkeypatch.setattr(prepare, "_execute", lambda *args, **kwargs: called.append(True))
    assert prepare.main(arguments("--token-file", token)) == 2
    report = capsys.readouterr().out
    assert json.loads(report)["error"]["code"] == "NOT_CONFIGURED"
    assert "first-token" not in report and "token with spaces" not in report
    assert called == []


def test_no_flags_do_not_add_a_third_party_default(monkeypatch):
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.delenv("TUSHARE_BASE_URL", raising=False)
    assert prepare._credential_env(prepare._parser().parse_args(arguments())) == {}
