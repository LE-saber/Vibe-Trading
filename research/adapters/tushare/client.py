"""Construct an isolated Tushare Pro client for Competition Core."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping


class TushareConfigurationError(RuntimeError):
    """Raised when the Core Tushare adapter has no usable credential."""


def create_tushare_client(
    token: str | None = None,
    base_url: str | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> Any:
    """Create a Pro client, preserving the official URL unless explicitly overridden."""

    environment = os.environ if environ is None else environ
    resolved_token = (environment.get("TUSHARE_TOKEN", "") if token is None else token).strip()
    if not resolved_token:
        raise TushareConfigurationError("TUSHARE_TOKEN is not configured")

    import tushare as ts

    client = ts.pro_api(resolved_token)
    resolved_base_url = (
        environment.get("TUSHARE_BASE_URL", "") if base_url is None else base_url
    ).strip()
    if resolved_base_url:
        client._DataApi__http_url = resolved_base_url
    return client


def configured_tushare_client(*, token_file: Path | None = None, base_url: str | None = None,
                             environ: Mapping[str, str] | None = None) -> Any:
    """Process credentials win; an explicitly supplied file is a bounded fallback."""
    environment = os.environ if environ is None else environ
    token = environment.get("TUSHARE_TOKEN", "").strip()
    if not token and token_file is not None:
        try:
            with Path(token_file).open("r", encoding="utf-8-sig") as handle:
                token = handle.read(4097).strip()
        except (OSError, UnicodeError) as exc:
            raise TushareConfigurationError("cannot read the configured Tushare token file") from exc
        if len(token) > 4096 or not token or any(c.isspace() for c in token):
            raise TushareConfigurationError("token file must contain exactly one credential")
    return create_tushare_client(token=token or None, base_url=base_url, environ=environment)


__all__ = ["TushareConfigurationError", "create_tushare_client", "configured_tushare_client"]
