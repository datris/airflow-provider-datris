"""Unit tests for DatrisRunTapOperator (hook mocked)."""

from __future__ import annotations

from unittest import mock

import pytest

from datris_provider.operators.run_tap import DatrisRunTapOperator

try:
    from airflow.exceptions import AirflowException
except Exception:  # pragma: no cover
    AirflowException = Exception  # type: ignore


def _operator(**kwargs):
    op = DatrisRunTapOperator(
        task_id="t", tap_name="my_tap", poll_interval=0, **kwargs
    )
    op._hook = mock.Mock()
    return op


def test_happy_path_polls_to_success():
    op = _operator()
    op.hook.run_tap.return_value = {"status": "started", "publisherToken": "pub-1"}
    op.hook.get_pipeline_status.side_effect = [
        {"rollup": {"allDone": False, "status": "processing",
                    "jobs": [{"pipelineToken": "pipe-1", "status": "processing"}]},
         "events": [{"message": "fetching"}]},
        {"rollup": {"allDone": True, "status": "success", "totalTime": 1234,
                    "jobs": [{"pipelineToken": "pipe-1", "status": "success",
                              "recordCount": 42}]},
         "events": [{"message": "fetching"}, {"message": "done"}]},
    ]

    result = op.execute(context={})

    assert result["publisher_token"] == "pub-1"
    assert result["pipeline_tokens"] == ["pipe-1"]
    assert result["row_count"] == 42
    assert result["duration_ms"] == 1234
    assert result["status"] == "success"


def test_debounced_is_success_without_polling():
    op = _operator()
    op.hook.run_tap.return_value = {
        "status": "skipped",
        "persistedReason": "debounced",
    }
    result = op.execute(context={})
    assert result["status"] == "skipped"
    op.hook.get_pipeline_status.assert_not_called()


def test_no_publisher_token_returns_without_waiting():
    op = _operator()
    op.hook.run_tap.return_value = {
        "status": "success",
        "persistedReason": "no_target_pipeline",
    }
    result = op.execute(context={})
    assert result["publisher_token"] is None
    op.hook.get_pipeline_status.assert_not_called()


def test_error_rollup_raises():
    op = _operator()
    op.hook.run_tap.return_value = {"status": "started", "publisherToken": "pub-1"}
    op.hook.get_pipeline_status.return_value = {
        "rollup": {"allDone": True, "status": "error",
                   "jobs": [{"pipelineToken": "pipe-1", "status": "error"}]},
        "events": [],
    }
    with pytest.raises(AirflowException):
        op.execute(context={})


def test_no_wait_skips_polling():
    op = _operator(wait_for_completion=False)
    op.hook.run_tap.return_value = {"status": "started", "publisherToken": "pub-1"}
    result = op.execute(context={})
    assert result["publisher_token"] == "pub-1"
    op.hook.get_pipeline_status.assert_not_called()


def test_params_not_in_template_fields_uses_tap_params():
    # Regression: `params` is a reserved BaseOperator attribute and must NOT be
    # a templated field (it breaks DAG serialization). The operator exposes
    # `tap_params` instead and forwards it to the REST `params` field.
    assert "params" not in DatrisRunTapOperator.template_fields
    assert "tap_params" in DatrisRunTapOperator.template_fields

    op = _operator(tap_params={"since": "2026-05-01"})
    op.hook.run_tap.return_value = {"status": "started", "publisherToken": "pub-1"}
    op.hook.get_pipeline_status.return_value = {
        "rollup": {"allDone": True, "status": "success", "jobs": []},
        "events": [],
    }
    op.execute(context={})
    op.hook.run_tap.assert_called_once_with(
        "my_tap", mode="run", params={"since": "2026-05-01"}
    )


def test_on_kill_cancels_tracked_jobs():
    op = _operator()
    op._pipeline_tokens = ["pipe-1", "pipe-2"]
    op.on_kill()
    assert op.hook.kill_job.call_count == 2
    op.hook.kill_job.assert_any_call("pipe-1")
    op.hook.kill_job.assert_any_call("pipe-2")
