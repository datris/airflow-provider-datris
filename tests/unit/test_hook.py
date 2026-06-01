"""Unit tests for DatrisHook (HTTP mocked with `responses`)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest import mock

import pytest
import responses

from datris_provider.hooks.datris import DatrisHook

try:
    from airflow.exceptions import AirflowException
except Exception:  # pragma: no cover
    AirflowException = Exception  # type: ignore


def _conn(host="https://datris.test", password=None, extra=None):
    return SimpleNamespace(
        host=host, password=password, extra_dejson=extra or {}
    )


def _hook(**conn_kwargs):
    hook = DatrisHook("datris_default")
    hook.get_connection = mock.Mock(return_value=_conn(**conn_kwargs))  # type: ignore
    return hook


@responses.activate
def test_run_tap_sends_body_and_returns_json():
    responses.post(
        "https://datris.test/api/v1/tap/run",
        json={"status": "started", "publisherToken": "pub-1"},
        status=200,
    )
    hook = _hook(password="key-123")
    out = hook.run_tap("my_tap", mode="run", params={"since": "2026-05-01"})

    assert out["publisherToken"] == "pub-1"
    sent = responses.calls[0].request
    assert sent.headers["x-api-key"] == "key-123"
    assert b'"since": "2026-05-01"' in sent.body or b'"since":"2026-05-01"' in sent.body


@responses.activate
def test_api_key_omitted_when_no_password():
    responses.post("https://datris.test/api/v1/tap/run", json={}, status=200)
    hook = _hook(password=None)
    hook.run_tap("my_tap")
    assert "x-api-key" not in responses.calls[0].request.headers


@responses.activate
def test_error_status_raises():
    responses.post(
        "https://datris.test/api/v1/tap/run", json={"error": "nope"}, status=401
    )
    hook = _hook(password="bad")
    with pytest.raises(AirflowException):
        hook.run_tap("my_tap")


@responses.activate
def test_pipeline_status_passes_rollup_param():
    responses.get(
        "https://datris.test/api/v1/pipeline/status",
        json={"rollup": {"allDone": True, "status": "success", "jobs": []}},
        status=200,
    )
    hook = _hook()
    out = hook.get_pipeline_status("pub-1", rollup=True)
    assert out["rollup"]["allDone"] is True
    assert "withrollup=true" in responses.calls[0].request.url
    assert "publishertoken=pub-1" in responses.calls[0].request.url


@responses.activate
def test_get_tap_returns_config():
    responses.get(
        "https://datris.test/api/v1/tap",
        json={"name": "my_tap", "cronExpression": "0 0 2 * * ?"},
        status=200,
    )
    hook = _hook()
    out = hook.get_tap("my_tap")
    assert out["cronExpression"] == "0 0 2 * * ?"
    assert "name=my_tap" in responses.calls[0].request.url


@responses.activate
def test_kill_job_posts_token():
    responses.post(
        "https://datris.test/api/v1/job/kill", json={"cancelled": True}, status=200
    )
    hook = _hook()
    hook.kill_job("pipe-9")
    assert b"pipe-9" in responses.calls[0].request.body


def test_missing_host_raises():
    hook = DatrisHook("datris_default")
    hook.get_connection = mock.Mock(return_value=_conn(host=None))  # type: ignore
    with pytest.raises(AirflowException):
        hook.run_tap("my_tap")
