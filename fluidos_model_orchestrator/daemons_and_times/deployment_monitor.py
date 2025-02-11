import asyncio
import logging
from typing import Any

import kopf  # type: ignore

from fluidos_model_orchestrator.common import Intent
from fluidos_model_orchestrator.configuration import CONFIGURATION
from fluidos_model_orchestrator.daemons_and_times.prometheus import MetricResult
from fluidos_model_orchestrator.model import _extract_intents


logger = logging.getLogger(__name__)


def extract_intents(spec: dict[str, Any]) -> list[Intent]:
    return _extract_intents(spec.get("metadata", {}).get("annotations", {}))


def compute_criticality(intents: list[Intent]) -> int:
    return 60


def get_data_from_prometheus(endpoint: dict[str, str], name: str, metrics: list[str]) -> dict[str, list[MetricResult]]:
    raise NotImplementedError()


def intent_still_valid(intents: list[Intent], metrics_data: dict[str, list[MetricResult]]) -> bool:
    raise NotImplementedError()


def convert_to_metrics(intents: list[Intent]) -> list[str]:
    raise NotImplementedError()


@kopf.daemon("fluidosdeployments")  # type: ignore
async def monitor_application_if_required(stopped, name: str, spec: dict[str, Any], status: dict[str, Any], logger: logging.Logger) -> None:
    intents: list[Intent] = extract_intents(spec)
    metrics: list[str] = convert_to_metrics(intents)

    waiting_time = compute_criticality(intents)

    while not stopped:
        await asyncio.sleep(waiting_time)
        data = get_data_from_prometheus(CONFIGURATION.prometheus_endpoint, name, metrics)
        if not intent_still_valid(intents, data):
            # fail accordingly
            pass
