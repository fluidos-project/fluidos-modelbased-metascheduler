from typing import Any

from fluidos_model_orchestrator.common import ExternalResourceProvider
from fluidos_model_orchestrator.common import Intent
from fluidos_model_orchestrator.common import KnownIntent
from fluidos_model_orchestrator.configuration import CONFIGURATION
from fluidos_model_orchestrator.resources.mspl import request_application


class MSPLIntentWrapper(ExternalResourceProvider):
    def __init__(self, intent: Intent):
        if intent.name is not KnownIntent.mspl:
            raise ValueError("Expected MSPL intent")
        self.intent = intent

    def enrich(self, container: dict[str, Any], name: str) -> None:
        response = request_application(
            policy=self.intent.value,
            endpoint=CONFIGURATION.MSPL_ENDPOINT,
            request_name=name
        )

        if response == "ALL GOOD":
            return None
        else:
            raise ValueError("Something went wrong")
