from fluidos_model_orchestrator.common import Flavor
from fluidos_model_orchestrator.common import ResourceProvider


class LocalResourceProvider(ResourceProvider):
    def __init__(self, id: str, flavor: Flavor) -> None:
        super().__init__(id, flavor)

    def get_label(self) -> str:
        return ""

    def acquire(self) -> bool:
        return True
