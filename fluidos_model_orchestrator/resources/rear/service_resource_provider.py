import json
import logging
from base64 import b64decode
from typing import Any

from kubernetes.client import CoreV1Api  # type: ignore
from kubernetes.client.api_client import ApiClient  # type: ignore
from kubernetes.client.exceptions import ApiException  # type: ignore

from fluidos_model_orchestrator.common import ExternalResourceProvider


logger = logging.getLogger(__name__)


class REARServiceResourceProvider(ExternalResourceProvider):
    def __init__(self, endpoints: str, username: str, password: str) -> None:
        # for the demo only!!
        self.endpoints = endpoints
        self.username = username
        self.password = password

    def enrich(self, container: dict[str, Any], name: str) -> None:
        if "env" not in container:
            container["env"] = []

        env: list[dict[str, str]] = container["env"]

        env.append({"name": "FLUIDOS_MQTT_ENDPOINTS", "value": self.endpoints})
        env.append({"name": "FLUIDOS_MQTT_USERNAME", "value": self.username})
        env.append({"name": "FLUIDOS_MQTT_PASSWORD", "value": self.password})


def build_REARServiceResourceProvider(api_client: ApiClient | None, allocation: dict[str, Any]) -> REARServiceResourceProvider:
    if api_client is None:
        raise ValueError("api_client is None")

    client = CoreV1Api(api_client=api_client)

    logger.info(f"Building REARServiceResourceProvider from \n----\n{json.dumps(allocation)}")

    secret_name = allocation["status"]["resourceRef"]["name"]
    namespace = allocation["status"]["resourceRef"]["namespace"]

    try:
        secret = client.read_namespaced_secret(secret_name, namespace)

        if secret.data is None:
            raise ValueError("Unexpected None value in secret data")

        return REARServiceResourceProvider(
            endpoints=b64decode(secret.data["endpoints"]).decode(),
            username=b64decode(secret.data["username"]).decode(),
            password=b64decode(secret.data["password"]).decode(),
        )

    except ApiException as e:
        logger.error("Unable to retrieve sercret")
        raise e
