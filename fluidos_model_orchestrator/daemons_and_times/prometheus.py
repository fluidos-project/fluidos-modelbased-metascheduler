from dataclasses import dataclass
from datetime import time
from typing import Any

from prometheus_api_client import PrometheusConnect  # type: ignore


def connect(endpoint: dict[str, str]) -> PrometheusConnect:
    return PrometheusConnect(
        endpoint["endpoint"],
        disable_ssl=True
    )


@dataclass(frozen=True, kw_only=True, eq=True)
class MetricResult:
    source: Any
    timestamp: time
    metric_name: str
    metric_value: Any


def _build_query(id: str) -> str:
    return ""


def _to_timestamp(time_data: str) -> time:
    return time()


def _to_my_metric(metric_data: Any) -> MetricResult:
    return MetricResult(
        source="",
        timestamp=_to_timestamp(""),
        metric_name="",
        metric_value="",
    )


def _correct_origin(metric_data: Any, id: str) -> bool:
    return False


def query(connection: PrometheusConnect, id: str) -> list[MetricResult]:
    metrics: list[Any] = connection.custom_query(
        query=_build_query(id)
    )

    return [
        _to_my_metric(metric) for metric in metrics if _correct_origin(metric, id)
    ]
