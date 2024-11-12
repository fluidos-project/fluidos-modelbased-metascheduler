from fluidos_model_orchestrator.common import Flavor
from fluidos_model_orchestrator.common import ResourceProvider
from fluidos_model_orchestrator.configuration import CONFIGURATION


class LocalResourceProvider(ResourceProvider):
    def __init__(self, id: str, flavor: Flavor) -> None:
        super().__init__(id, flavor)

    def get_label(self) -> dict[str, str]:
        return {
            CONFIGURATION.local_node_key: "true"
        }

    def acquire(self, namespace: str) -> bool:
        return True
