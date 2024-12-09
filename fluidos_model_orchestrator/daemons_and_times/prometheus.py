from typing import Any

from prometheus_api_client import PrometheusConnect


def connect(endpoint: dict[str, str]) -> PrometheusConnect:
    return PrometheusConnect(
        endpoint["endpoint"],
        disable_ssl=True
    )

def query(connection: PrometheusConnect) -> Any:
    pass
