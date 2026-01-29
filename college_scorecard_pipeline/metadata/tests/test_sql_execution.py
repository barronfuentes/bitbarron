from __future__ import annotations

import json

from urllib.error import HTTPError, URLError

import pytest

from column_sync import sql_execution


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def test_request_json_with_retries_recovers(monkeypatch) -> None:
    calls: list[str] = []

    def fake_urlopen(_req, timeout=0):
        calls.append("call")
        if len(calls) == 1:
            raise HTTPError("https://example", 503, "unavailable", None, None)
        return _FakeResponse({"ok": True})

    monkeypatch.setattr(sql_execution.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(sql_execution.time, "sleep", lambda _s: None)

    payload = sql_execution._request_json_with_retries(
        sql_execution.request.Request("https://example"),
        timeout=1,
        max_retries=2,
        backoff_seconds=0.0,
        error_message="Boom",
    )

    assert payload == {"ok": True}
    assert len(calls) == 2


def test_request_json_with_retries_exhausts(monkeypatch) -> None:
    def fake_urlopen(_req, timeout=0):
        raise URLError("network down")

    monkeypatch.setattr(sql_execution.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(sql_execution.time, "sleep", lambda _s: None)

    with pytest.raises(ValueError, match="Boom"):
        sql_execution._request_json_with_retries(
            sql_execution.request.Request("https://example"),
            timeout=1,
            max_retries=1,
            backoff_seconds=0.0,
            error_message="Boom",
        )
