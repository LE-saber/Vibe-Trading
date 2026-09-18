"""Strict, bounded and point-in-time Ninef news collection for S5 review."""

from __future__ import annotations

from datetime import datetime
import hashlib
import re
from typing import Any, Callable, Mapping

from adapters.ninef.account import NinefAccountAdapterError, _clean_token, _request_json
from competition_core.pipeline.artifacts import content_hash
from competition_core.pipeline.time_context import SHANGHAI


Requester = Callable[[str, str, Mapping[str, str] | None], dict[str, Any]]
_SYMBOL = re.compile(r"(?:[036]\d{5}\.(?:SH|SZ))")
_MAX_PAGES = 20


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise NinefAccountAdapterError(f"Ninef news {field} is invalid")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise NinefAccountAdapterError(f"Ninef news {field} is invalid") from exc
    # Current Ninef read responses use local Shanghai wall time without an
    # offset; aware values remain accepted for saved fixtures and old captures.
    if parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=SHANGHAI)
    return parsed.astimezone(SHANGHAI)


def _symbols(item: Mapping[str, Any]) -> list[str]:
    found: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, child in value.items():
                if key.lower() in {"symbol", "ts_code", "code"}:
                    text = str(child or "").strip().upper()
                    if _SYMBOL.fullmatch(text):
                        found.add(text)
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    for key in ("relatedStocks", "recommendedStocks", "portfolioMatches", "leaders", "sourceStock"):
        visit(item.get(key))
    return sorted(found)


def _pages(token: str, read: Requester) -> list[Mapping[str, Any]]:
    cursor = None
    seen: set[str] = set()
    rows: list[Mapping[str, Any]] = []
    for _ in range(_MAX_PAGES):
        params = {"cursor": cursor} if cursor is not None else None
        payload = read(token, "hot_topics", params)
        data = payload.get("data")
        items = data.get("items") if isinstance(data, Mapping) else None
        if not isinstance(items, list) or any(not isinstance(item, Mapping) for item in items):
            raise NinefAccountAdapterError("Ninef news response is invalid")
        rows.extend(items)
        next_cursor = data.get("nextCursor")
        has_more = data.get("hasMore") is True or next_cursor not in (None, "")
        if not has_more:
            return rows
        if not isinstance(next_cursor, str) or not next_cursor or next_cursor in seen:
            raise NinefAccountAdapterError("Ninef news pagination cursor is invalid")
        seen.add(next_cursor)
        cursor = next_cursor
    raise NinefAccountAdapterError("Ninef news pagination exceeds the page limit")


def fetch_ninef_news(
    token: str,
    *,
    cutoff: datetime,
    requester: Requester | None = None,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Return one content-addressed snapshot; it performs GET requests only."""
    credential = _clean_token(token)
    if not isinstance(cutoff, datetime) or cutoff.utcoffset() is None:
        raise NinefAccountAdapterError("Ninef news cutoff must be timezone-aware")
    cutoff = cutoff.astimezone(SHANGHAI)
    now = (clock or (lambda: datetime.now(SHANGHAI)))()
    if not isinstance(now, datetime) or now.utcoffset() is None:
        raise NinefAccountAdapterError("Ninef news observation clock must be timezone-aware")
    read = requester or (lambda supplied, name, params=None: _request_json(supplied, name, params))
    raw_rows = _pages(credential, read)
    items: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    malformed = 0
    for row in raw_rows:
        raw_id = row.get("id")
        title = row.get("title")
        body = next((row.get(key).strip() for key in ("content", "detail", "recommendationReason")
                     if isinstance(row.get(key), str) and row.get(key).strip()), "")
        source = next((row.get(key).strip() for key in ("dataSource", "source")
                       if isinstance(row.get(key), str) and row.get(key).strip()), "")
        if (raw_id is None or isinstance(raw_id, bool) or not str(raw_id).strip()
                or not isinstance(title, str) or not title.strip() or not body or not source):
            malformed += 1
            continue
        raw_id = str(raw_id).strip()
        if raw_id in seen_ids:
            raise NinefAccountAdapterError("Ninef news contains duplicate identifiers")
        seen_ids.add(raw_id)
        try:
            published = _timestamp(row.get("createdAt"), "published timestamp")
        except NinefAccountAdapterError:
            malformed += 1
            continue
        if published > cutoff:
            continue
        news_ref = "ninef-news-" + hashlib.sha256(raw_id.encode("utf-8")).hexdigest()[:24]
        truncated = len(title.strip()) > 400 or len(body) > 12000 or len(source) > 120
        items.append({
            "news_ref": news_ref,
            "title": title.strip()[:400],
            "body": body[:12000],
            "symbols": _symbols(row),
            "published_at": published.isoformat(),
            "source": source[:120],
            "source_ref": f"ninef:data/hot-topics item:{news_ref}"
                          + (";content-truncated-to-contract" if truncated else ""),
        })
    if raw_rows and malformed == len(raw_rows):
        raise NinefAccountAdapterError("Ninef news response contains no trustworthy items")
    items.sort(key=lambda item: (item["published_at"], item["news_ref"]), reverse=True)
    value = {
        "schema_version": "ninef.news.v1",
        "status": "AVAILABLE" if items else "EMPTY_VERIFIED",
        "cutoff": cutoff.isoformat(),
        "fetched_at": now.astimezone(SHANGHAI).isoformat(),
        "items": items,
        "source": "ninef.hot_topics.readonly",
        "raw_item_count": len(raw_rows),
        "malformed_item_count": malformed,
    }
    return value | {"snapshot_hash": content_hash(value)}


__all__ = ["fetch_ninef_news"]
