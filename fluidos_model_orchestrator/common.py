from __future__ import annotations

import logging
from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from enum import unique
from typing import Any


logger = logging.getLogger(__name__)


class ResourceProvider(ABC):
    def __init__(self, id: str) -> None:
        self.id = id

    def acquire(self) -> bool:
        return True

    @abstractmethod
    def get_label(self) -> str:
        raise NotImplementedError("Abstract method")


@dataclass(kw_only=True)
class Resource:
    id: str
    cpu: str | None = None
    memory: str | None = None
    architecture: str | None = None
    gpu: str | None = None
    ephemeral_storage: str | None = None
    persistent_storage: str | None = None
    region: str | None = None

    def can_run_on(self, flavor: Flavor) -> bool:
        logger.debug(f"Testing {self=} against {flavor=}")
        if not _cpu_compatible(self.cpu, flavor.cpu):
            return False

        if not _memory_compatible(self.memory, flavor.memory):
            return False

        if self.architecture is not None and self.architecture != flavor.architecture:
            return False

        if self.gpu is not None and int(self.gpu) > int(flavor.gpu):
            return False

        # TODO: add checks for storage

        return True


def _memory_compatible(memory_a_spec: str | None, memory_b_spec: str | None) -> bool:
    if memory_a_spec is None:
        return False
    if memory_b_spec is None:
        return False

    memory_a: int = _memory_to_int(memory_a_spec)
    memory_b: int = _memory_to_int(memory_b_spec)

    return memory_b >= memory_a


def _memory_to_int(spec: str) -> int:
    unit = spec[-2:]
    magnitude = int(spec[:-2])

    if unit == "Ki":
        return magnitude
    if unit == "Mi":
        return 1024 * magnitude
    if unit == "Gi":
        return 1024 * 1024 * magnitude

    raise ValueError(f"Not known {unit=}")


def _cpu_compatible(cpu_a_spec: str | None, cpu_b_spec: str | None) -> bool:
    if cpu_a_spec is None:
        return False
    if cpu_b_spec is None:
        return False
    cpu_a: int = _cpu_to_int(cpu_a_spec)
    cpu_b: int = _cpu_to_int(cpu_b_spec)

    return cpu_b >= cpu_a


def _cpu_to_int(spec: str) -> int:
    if spec[-1] == 'n':
        return int(spec[:-1])
    elif spec[-1] == "m":
        return int(spec[:-1]) * 1000
    else:
        return int(spec) * (1000 * 1000)


@dataclass
class Flavor:
    id: str
    cpu: str
    architecture: str
    gpu: str
    memory: str


@dataclass
class ContainerImageEmbedding:
    image: str
    embedding: str | None = None


@dataclass(kw_only=True)
class ModelPredictRequest:
    id: str
    namespace: str
    pod_request: Any
    container_image_embeddings: list[ContainerImageEmbedding]
    intents: list[Intent] = field(default_factory=list)


@dataclass
class ModelPredictResponse:
    id: str
    resource_profile: Resource

    def to_resource(self) -> Resource:
        return self.resource_profile


class ModelInterface(ABC):
    @abstractmethod
    def predict(self, data: ModelPredictRequest, architecture: str = "amd64") -> ModelPredictResponse:
        raise NotImplementedError("Not implemented: abstract method")


@unique
class KnownIntent(Enum):
    # k8s resources
    cpu = "cpu", False
    memory = "memory", False

    # high order requests
    latency = "latency", False
    location = "location", False
    throughput = "throughput", False
    compliance = "compliance", False
    energy = "energy", False
    battery = "battery", False

    # service
    service = "service", True

    def __new__(cls, *args: str, **kwds: str) -> KnownIntent:
        obj = object.__new__(cls)
        obj._value_ = args[0]
        return obj

    def __init__(self, _: str, external: bool):
        self._external = external

    def __repr__(self) -> str:
        return super().__repr__()

    def to_intent_key(self) -> str:
        return f"fluidos-intent-{self.name}"

    def has_external_requirement(self) -> bool:
        return self._external

    @staticmethod
    def is_supported(intent_name: str) -> bool:
        if intent_name.startswith("fluidos-intent-"):
            intent_name = "-".join(intent_name.split("-")[2:])

        return any(
            known_intent.name == intent_name for known_intent in KnownIntent
        )

    @staticmethod
    def get_intent(intent_name: str) -> KnownIntent:
        # defensive programming
        if not KnownIntent.is_supported(intent_name):
            raise ValueError(f"Unsupported intent: {intent_name=}")

        name = "-".join(intent_name.split("-")[2:]).casefold()
        return next(known_intent for known_intent in KnownIntent if known_intent.name == name)


@dataclass
class Intent:
    name: KnownIntent
    value: str

    def has_external_requirement(self) -> bool:
        return self.name.has_external_requirement()


def find_best_validation(providers: list[ResourceProvider], intents: list[Intent]) -> ResourceProvider | None:
    return providers[0]  # for now


def validate_on_intent(resources: list[ResourceProvider], intent: Intent) -> ResourceProvider:
    return resources[0]  # for now


class ResourceFinder(ABC):
    def find_best_match(self, resource: Resource | Intent, namespace: str) -> list[ResourceProvider]:
        raise NotImplementedError()
