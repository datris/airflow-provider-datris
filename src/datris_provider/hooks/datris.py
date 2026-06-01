"""Hook for talking to a Datris server over its REST API."""

from __future__ import annotations

from typing import Any

import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class DatrisHook(BaseHook):
    """Thin REST client for a Datris server.

    Connection (type ``datris``):
      * **host** — Datris base URL, e.g. ``https://datris.example.com``.
      * **password** — API key sent as ``x-api-key``. Optional: only required
        when the Datris install enforces API keys. Leave blank for a local
        single-tenant install with ``useApiKeys=false``.
      * **extra** — optional JSON: ``{"verify": true, "timeout": 30}``.
    """

    conn_name_attr = "datris_conn_id"
    default_conn_name = "datris_default"
    conn_type = "datris"
    hook_name = "Datris"

    def __init__(self, datris_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.datris_conn_id = datris_conn_id
        self._base_url: str | None = None
        self._headers: dict[str, str] = {}
        self._verify: bool = True
        self._timeout: int = 30

    # ── connection ────────────────────────────────────────────────
    def _resolve(self) -> None:
        if self._base_url is not None:
            return
        conn = self.get_connection(self.datris_conn_id)
        if not conn.host:
            raise AirflowException(
                f"Datris connection '{self.datris_conn_id}' has no host set."
            )
        self._base_url = conn.host.rstrip("/")
        extra = conn.extra_dejson or {}
        self._verify = extra.get("verify", True)
        self._timeout = int(extra.get("timeout", 30))
        headers = {"Content-Type": "application/json"}
        # Send x-api-key only when a key is configured; omit otherwise so a
        # default single-tenant install (useApiKeys=false) works key-free.
        if conn.password:
            headers["x-api-key"] = conn.password
        self._headers = headers

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        self._resolve()
        url = f"{self._base_url}/api/v1/{path.lstrip('/')}"
        try:
            resp = requests.request(
                method,
                url,
                headers=self._headers,
                verify=self._verify,
                timeout=self._timeout,
                **kwargs,
            )
        except requests.RequestException as exc:
            raise AirflowException(f"Datris request to {url} failed: {exc}") from exc
        if resp.status_code >= 400:
            raise AirflowException(
                f"Datris {method} {path} returned {resp.status_code}: {resp.text[:1000]}"
            )
        if not resp.content:
            return None
        try:
            return resp.json()
        except ValueError:
            return resp.text

    # ── endpoints ─────────────────────────────────────────────────
    def get_tap(self, name: str) -> dict:
        """Return a tap's saved configuration (includes ``cronExpression``)."""
        return self._request("GET", "tap", params={"name": name}) or {}

    def run_tap(
        self, name: str, mode: str = "run", params: dict | None = None
    ) -> dict:
        """Trigger a tap. ``params`` becomes per-run env vars for the script."""
        body: dict[str, Any] = {"name": name, "mode": mode}
        if params:
            body["params"] = params
        return self._request("POST", "tap/run", json=body) or {}

    def get_pipeline_status(
        self, publisher_token: str, rollup: bool = True
    ) -> dict:
        """Status for every ingestion job sharing ``publisher_token``.

        With ``rollup=True`` returns ``{"rollup": {...}, "events": [...]}``.
        """
        params = {"publishertoken": publisher_token}
        if rollup:
            params["withrollup"] = "true"
        return self._request("GET", "pipeline/status", params=params) or {}

    def get_tap_logs(self, name: str) -> list[dict]:
        """Return the tap's recent run-log entries (last 50)."""
        return self._request("GET", "tap/logs", params={"name": name}) or []

    def kill_job(self, pipeline_token: str) -> dict:
        """Cancel a running ingestion job by its pipeline token."""
        return self._request(
            "POST", "job/kill", json={"pipelineToken": pipeline_token}
        ) or {}
