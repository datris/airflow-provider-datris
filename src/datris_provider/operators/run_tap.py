"""Operator that runs a Datris tap and waits for the pipeline to finish."""

from __future__ import annotations

import time
from typing import Any, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from datris_provider.hooks.datris import DatrisHook

# Rollup statuses that mean "no more work is coming".
_TERMINAL_JOB_STATES = {"success", "warning", "error", "timed_out", "cancelled"}


class DatrisRunTapOperator(BaseOperator):
    """Trigger a Datris tap, then poll its pipeline to a terminal state.

    :param tap_name: name of the saved Datris tap to run.
    :param datris_conn_id: Airflow connection id (type ``datris``).
    :param wait_for_completion: poll the pipeline until it finishes (default).
    :param poll_interval: seconds between status polls.
    :param tap_params: per-run parameters forwarded to the tap script as env vars.
        Templated, so ``{"since": "{{ ds }}"}`` works for date-windowed backfills.
        (Named ``tap_params`` rather than ``params`` because ``params`` is a
        reserved BaseOperator attribute and cannot be a templated field.)
    """

    template_fields: Sequence[str] = ("tap_name", "tap_params")
    ui_color = "#6366f1"

    def __init__(
        self,
        *,
        tap_name: str,
        datris_conn_id: str = DatrisHook.default_conn_name,
        wait_for_completion: bool = True,
        poll_interval: int = 15,
        tap_params: dict | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.tap_name = tap_name
        self.datris_conn_id = datris_conn_id
        self.wait_for_completion = wait_for_completion
        self.poll_interval = poll_interval
        self.tap_params = tap_params
        self._hook: DatrisHook | None = None
        self._pipeline_tokens: list[str] = []

    @property
    def hook(self) -> DatrisHook:
        if self._hook is None:
            self._hook = DatrisHook(self.datris_conn_id)
        return self._hook

    # ── execution ─────────────────────────────────────────────────
    def execute(self, context: Any) -> dict:
        self.log.info("Triggering Datris tap '%s'", self.tap_name)
        resp = self.hook.run_tap(self.tap_name, mode="run", params=self.tap_params)

        # Server-side debounce: a retry that lands inside the window is reported
        # as skipped. The in-flight run is the real one — succeed with a warning
        # rather than failing and masking it.
        if resp.get("status") == "skipped" and resp.get("persistedReason") == "debounced":
            self.log.warning(
                "Datris debounced this run of '%s' (a run is already in flight). "
                "Treating as success.",
                self.tap_name,
            )
            return resp

        self._stream_logs(resp.get("logs"))

        publisher_token = resp.get("publisherToken")
        if not publisher_token:
            # No pipeline was fed (no target pipeline, zero records, or test mode).
            self.log.info(
                "Tap '%s' did not feed a pipeline (reason: %s). Nothing to wait for.",
                self.tap_name,
                resp.get("persistedReason", "unknown"),
            )
            return self._result(resp, publisher_token=None, rollup=None)

        self.log.info("Datris publisher token: %s", publisher_token)
        if not self.wait_for_completion:
            return self._result(resp, publisher_token=publisher_token, rollup=None)

        rollup = self._poll_to_terminal(publisher_token)
        status = (rollup or {}).get("status")
        if status in ("error", "timed_out", "cancelled"):
            raise AirflowException(
                f"Datris tap '{self.tap_name}' finished with status '{status}'. "
                f"See the Datris Pipelines tab (publisher token {publisher_token})."
            )
        self.log.info("Datris tap '%s' completed with status '%s'", self.tap_name, status)
        return self._result(resp, publisher_token=publisher_token, rollup=rollup)

    def _poll_to_terminal(self, publisher_token: str) -> dict:
        logged_events = 0
        while True:
            time.sleep(self.poll_interval)
            status = self.hook.get_pipeline_status(publisher_token, rollup=True)
            rollup = status.get("rollup", {}) or {}

            events = status.get("events", []) or []
            logged_events = self._stream_events(events, logged_events)

            jobs = rollup.get("jobs", []) or []
            self._pipeline_tokens = [
                j["pipelineToken"] for j in jobs if j.get("pipelineToken")
            ]

            if rollup.get("allDone"):
                return rollup
            if jobs and all(
                j.get("status") in _TERMINAL_JOB_STATES for j in jobs
            ):
                return rollup

    # ── helpers ───────────────────────────────────────────────────
    def _stream_logs(self, logs: Any) -> None:
        if not logs:
            return
        for line in logs if isinstance(logs, list) else [logs]:
            self.log.info("[datris] %s", line if isinstance(line, str) else line.get("message", line))

    def _stream_events(self, events: list, already_logged: int) -> int:
        for event in events[already_logged:]:
            msg = event.get("message") if isinstance(event, dict) else event
            if msg:
                self.log.info("[datris] %s", msg)
        return len(events)

    def _result(self, resp: dict, publisher_token: str | None, rollup: dict | None) -> dict:
        jobs = (rollup or {}).get("jobs", []) or []
        row_count = sum(int(j.get("recordCount", 0) or 0) for j in jobs) or resp.get("recordCount")
        # Returned dict is auto-pushed to XCom as ``return_value`` (do_xcom_push),
        # so downstream tasks can read publisher_token / pipeline_tokens / metrics.
        return {
            "publisher_token": publisher_token,
            "pipeline_tokens": self._pipeline_tokens
            or [j["pipelineToken"] for j in jobs if j.get("pipelineToken")],
            "row_count": row_count,
            "duration_ms": (rollup or {}).get("totalTime") or resp.get("durationMs"),
            "status": (rollup or {}).get("status") or resp.get("status"),
        }

    def on_kill(self) -> None:
        """Cancel the Datris job(s) when the Airflow task is killed/times out."""
        for token in self._pipeline_tokens:
            try:
                self.log.info("Cancelling Datris job %s", token)
                self.hook.kill_job(token)
            except Exception as exc:  # noqa: BLE001 — best-effort cleanup
                self.log.warning("Failed to cancel Datris job %s: %s", token, exc)
