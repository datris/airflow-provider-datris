# airflow-provider-datris

An [Apache Airflow](https://airflow.apache.org/) provider for
[Datris](https://datris.ai). Airflow orchestrates; Datris executes. This package
ships an operator that triggers a Datris tap, waits for the resulting pipeline to
reach a terminal state, streams Datris logs into the Airflow task log, and pushes
run tokens and metrics back as XComs.

No execution moves into Airflow — it only triggers and observes.

## Requirements

- Apache Airflow 3.x
- A reachable Datris server (minimum version: the current Datris release at the
  time this provider was published)

## Install

```bash
pip install airflow-provider-datris
```

## Configure a connection

Add a connection of type **Datris**:

| Field    | Value                                                    |
| -------- | -------------------------------------------------------- |
| Host     | Datris base URL, e.g. `https://datris.example.com`       |
| Password | API key (`x-api-key`) — only if the install requires one |
| Extra    | `{"verify": true, "timeout": 30}` (optional)             |

The API key is **optional**: leave it blank for a local single-tenant install
with `useApiKeys=false`. For a hosted/multi-tenant install, use a key that holds
the `tap:run` capability for the taps you trigger. A key scoped
`tap:run:owner=self` can only run taps it created.

## Usage

```python
from datris_provider.operators import DatrisRunTapOperator

ingest = DatrisRunTapOperator(
    task_id="ingest_customers",
    tap_name="customers_pg_to_snowflake",
    datris_conn_id="datris_default",
    wait_for_completion=True,
    poll_interval=15,
    tap_params={"since": "{{ ds }}"},  # optional per-run params (env vars in the tap)
)
```

### Behavior

- Triggers the tap via `POST /tap/run` (`mode=run`).
- Polls `GET /pipeline/status?publishertoken=…&withrollup=true` to a terminal
  rollup, streaming Datris log events into the task log.
- Pushes `publisher_token`, `pipeline_tokens`, `row_count`, and `duration_ms`
  as XComs (the operator's return value).
- A debounced trigger (`status=skipped, persistedReason=debounced`) is treated
  as success-with-warning.
- On Airflow task kill/timeout, calls `POST /job/kill` for the in-flight jobs.

### One scheduler at a time

A tap is scheduled by **either** Datris **or** Airflow, never both. The operator
enforces this from the tap's cron:

- Tap **has a cron** → Datris already schedules it → the operator **refuses to
  trigger** and fails the task.
- Tap **has no cron** (manual-only) → Airflow drives it → the operator triggers
  it normally.

To run a tap from Airflow, leave its cron empty in Datris.

## Development

```bash
pip install -e ".[dev]"
pytest
```

## License

Apache-2.0. The Datris platform is also Apache-2.0 licensed as of its current release; earlier platform releases shipped under AGPL-3.0.
