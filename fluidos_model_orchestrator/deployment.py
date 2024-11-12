import asyncio
import json
import logging
import time
from typing import Any

import kopf  # type: ignore
from kubernetes.utils import create_from_dict  # type: ignore
from kubernetes.utils import FailToCreateError  # type: ignore

from .common import ExternalResourceProvider
from .common import Intent
from .common import ModelPredictResponse
from .common import ResourceProvider
from .configuration import CONFIGURATION


logger = logging.getLogger()


def apply_external_resource(spec: dict[str, Any], resource_and_intent: tuple[ExternalResourceProvider, Intent]) -> bool:
    # retrieve credentials as secret named f"credentials-{contact_name}"
    # stored in namespaced called f"{contact_name}"
    # to be decoded

    match spec["kind"]:
        case "Pod":
            for container in spec["spec"]["containers"]:
                (resource, _) = resource_and_intent
                resource.enrich(container, spec["metadata"]["name"])
            return True
        case "Deployment":
            raise ValueError(f"Unsupported type: {spec['kind']}")
        case _:
            raise ValueError(f"Unsupported type: {spec['kind']}")

    return False


async def deploy(
        spec: dict[str, Any],
        provider: ResourceProvider,
        expanding_resources: list[tuple[ExternalResourceProvider, Intent]],
        response: ModelPredictResponse,
        namespace: str) -> bool:
    spec_dict = {
        k: v for k, v in spec.items()
    }

    enrich(spec_dict, provider)

    for expanding_resource in expanding_resources:
        apply_external_resource(spec_dict, expanding_resource)

    delay_time = response.delay * 60 * 60
    logger.info(f"Waiting to deploy {delay_time=}")
    await asyncio.sleep(delay_time)

    k8s_client = CONFIGURATION.k8s_client

    kopf.adopt(spec_dict)

    try:
        time.sleep(5)
        reference = create_from_dict(k8s_client=k8s_client, data=spec_dict, namespace=namespace)

        return reference is not None

    except FailToCreateError as e:
        logger.error("Unable to create resource")
        logger.error(f"Missed resource: {json.dumps(spec_dict)}")
        logger.error(e)

    return False


def enrich(spec: dict[str, Any], provider: ResourceProvider) -> bool:
    logger.info("Enriching workload manifest with resource provider information")

    labels = provider.get_label()

    if not len(labels):
        logger.info("No label provided, deploying randomly")
        return True

    nodeSelector: dict[str, str] = _get_node_selector(spec)

    nodeSelector.update(labels)

    return True


def _get_node_selector(spec: dict[str, Any]) -> dict[str, str]:
    if spec["kind"] == "Deployment":
        logger.debug("Workload manifest of type Deployment")
        spec = spec.get("spec", {}).get("template", {})
    elif spec["kind"] == "Pod":
        logger.debug("Workload manifest of type Pod")
    else:
        logger.warning(f"Unsupported manifest type: {spec['kind']}")
        raise ValueError(f"Unsupported manifest kind: {spec['kind']}")

    if "nodeSelector" not in spec["spec"]:
        spec["spec"]["nodeSelector"] = {}

    return spec["spec"]["nodeSelector"]
