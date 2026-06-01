"""Apache Airflow provider for Datris."""

__version__ = "0.1.2"


def get_provider_info() -> dict:
    """Airflow provider manifest (referenced by the entry point)."""
    return {
        "package-name": "airflow-provider-datris",
        "name": "Datris",
        "description": "Trigger and monitor Datris taps from Apache Airflow.",
        "connection-types": [
            {
                "connection-type": "datris",
                "hook-class-name": "datris_provider.hooks.datris.DatrisHook",
            }
        ],
        "versions": [__version__],
    }
