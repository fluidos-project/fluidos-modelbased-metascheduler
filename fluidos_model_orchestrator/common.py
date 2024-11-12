from __future__ import annotations

import json
import logging
from abc import ABC
from abc import abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from enum import unique
from typing import Any
from typing import cast

from .flavor import build_flavor  # noqa
from .flavor import Flavor
from .flavor import FlavorCharacteristics  # noqa
from .flavor import FlavorK8SliceData
from .flavor import FlavorMetadata  # noqa
from .flavor import FlavorSpec  # noqa
from .flavor import FlavorType
from .flavor import FlavorTypeData  # noqa
from .flavor import GPUData


logger = logging.getLogger(__name__)


class ResourceProvider(ABC):
    def __init__(self, id: str, flavor: Flavor) -> None:
        self.id = id
        self.flavor = flavor

    @abstractmethod
    def acquire(self, namespace: str) -> bool:
        raise NotImplementedError("Abstract method")

    @abstractmethod
    def get_label(self) -> dict[str, str]:
        raise NotImplementedError("Abstract method")

    def __str__(self) -> str:
        return f"{self.id=}: {self.flavor=}"


class ExternalResourceProvider(ABC):
    def enrich(self, container: dict[str, Any], name: str) -> None:
        raise NotImplementedError("Abstract method")


@dataclass(kw_only=True)
class Resource:
    id: str
    cpu: str | None = None
    memory: str | None = None
    architecture: str | None = None
    gpu: str | None = None
    storage: str | None = None
    region: str | None = None
    pods: str | int | None = None

    def can_run_on(self, flavor: Flavor) -> bool:
        if flavor.spec.flavor_type.type_identifier is not FlavorType.K8SLICE:
            return False

        type_data = cast(FlavorK8SliceData, flavor.spec.flavor_type.type_data)

        if self.cpu is not None and not _cpu_compatible(self.cpu, type_data.characteristics.cpu):
            return False

        if self.memory is not None and not _memory_compatible(self.memory, type_data.characteristics.memory):
            return False

        if self.architecture is not None and self.architecture != type_data.characteristics.architecture:
            return False

        if self.gpu is not None:
            our_gpu = _convert_to_gpudata(self.gpu)
            their_gpu = _convert_to_gpudata(type_data.characteristics.gpu)

            return our_gpu.can_run_on(their_gpu)

        if self.pods is not None:
            return int(self.pods) == int(type_data.characteristics.pods if type_data.characteristics.pods is not None else 0)

        # TODO: add checks for storage

        return True


def _memory_compatible(req_spec: str | None, offer_spec: str | None) -> bool:
    if req_spec is None:
        return False
    if offer_spec is None:
        return False

    memory_a: int = memory_to_int(req_spec)
    memory_b: int = memory_to_int(offer_spec)

    return memory_b >= memory_a


def memory_to_int(spec: str) -> int:
    unit = spec[-2:]
    magnitude = int(spec[:-2])

    if unit == "Ki":
        return magnitude
    if unit == "Mi":
        return 1024 * magnitude
    if unit == "Gi":
        return 1024 * 1024 * magnitude

    raise ValueError(f"Not known {unit=}")


def _cpu_compatible(req_spec: str | None, offer_spec: str | None) -> bool:
    if req_spec is None:
        return False
    if offer_spec is None:
        return False
    cpu_a: int = cpu_to_int(req_spec)
    cpu_b: int = cpu_to_int(offer_spec)

    return cpu_b >= cpu_a


def cpu_to_int(spec: str) -> int:
    if spec[-1] == 'n':
        return int(spec[:-1])
    elif spec[-1] == "m":
        return int(spec[:-1]) * 1000
    else:
        return int(spec) * (1000 * 1000)


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
    delay: int = 0  # time in hours

    def to_resource(self) -> Resource:
        return self.resource_profile


class OrchestratorInterface(ABC):
    def load(self) -> Any:
        raise NotImplementedError("Not implemented: abstract method")

    @abstractmethod
    def predict(self, data: ModelPredictRequest, architecture: str = "arm64") -> ModelPredictResponse | None:
        raise NotImplementedError("Not implemented: abstract method")

    def rank_resource(self, providers: list[ResourceProvider], prediction: ModelPredictResponse, request: ModelPredictRequest) -> list[ResourceProvider]:
        return providers


_always_true: Callable[[ResourceProvider, str], bool] = lambda provider, value: True


def _validate_regulations(provider: ResourceProvider, value: str) -> bool:
    """
    Assumes values of the form:
    GDPR
    DORA
    or such
    """
    value = value.casefold()
    if provider.flavor.spec.flavor_type.type_identifier is FlavorType.K8SLICE:
        for _, field_value in cast(FlavorK8SliceData, provider.flavor.spec.flavor_type.type_data).properties.items():
            if str(field_value).casefold() == value:
                return True

    return False


def _check_cpu(provider: ResourceProvider, value: str) -> bool:
    if provider.flavor.spec.flavor_type.type_identifier is FlavorType.K8SLICE:
        return _cpu_compatible(value, cast(FlavorK8SliceData, provider.flavor.spec.flavor_type.type_data).characteristics.cpu)
    return False


def _check_memory(provider: ResourceProvider, value: str) -> bool:
    if provider.flavor.spec.flavor_type.type_identifier is FlavorType.K8SLICE:
        return _memory_compatible(value, cast(FlavorK8SliceData, provider.flavor.spec.flavor_type.type_data).characteristics.memory)
    return False


def _convert_to_gpudata(gpu: GPUData | str | int | dict[str, Any]) -> GPUData:
    if type(gpu) is GPUData:
        return gpu
    if type(gpu) is dict:
        # loads of faith required here:
        return GPUData(
            cores=gpu.get("core", "0"),
            memory=gpu.get("memory", "0"),
            model=gpu.get("model", "")
        )

    if type(gpu) is str:
        gpu = int(gpu)
    if type(gpu) is int:
        return GPUData(cores=gpu)

    raise ValueError(f"Unable to convert value '{gpu}' of type {type(gpu)}")


def _check_gpu(provider: ResourceProvider, value: str) -> bool:
    if provider.flavor.spec.flavor_type.type_identifier is FlavorType.K8SLICE:
        data = json.loads(value)

        gpu: GPUData = _convert_to_gpudata(cast(FlavorK8SliceData, provider.flavor.spec.flavor_type.type_data).characteristics.gpu)

        if type(data) is dict:
            return gpu.can_run_on(_convert_to_gpudata(data))
        elif type(data) is int:
            return int(gpu.cores) >= data

    return False


def validate_location(provider: ResourceProvider, value: str) -> bool:
    value = value.casefold()

    location = provider.flavor.spec.location

    return any(
        value == str(_val).casefold() for _val in location.values()
    )


def _validate_bandwidth_against_point(provider: ResourceProvider, value: str) -> bool:
    if provider.flavor.spec.flavor_type.type_identifier is not FlavorType.K8SLICE:
        return False

    # assumes value is of the form "<operator> value <point>"
    [operator, quantity, point] = value.split(" ")

    type_data = cast(FlavorK8SliceData, provider.flavor.spec.flavor_type.type_data)

    bandwidth_properties = type_data.properties.get("bandwidth", {}).get(point, None)

    if bandwidth_properties is not None:
        # assume that bandwidth_property is in ms
        bandwidth = int(bandwidth_properties[:-2])
        required_bandwidth = int(quantity[:-2])   # assumes quantity = "\d+ms"

        match operator:
            case ">":
                return bandwidth > required_bandwidth
            case ">=":
                return bandwidth >= required_bandwidth
            case "<":
                return bandwidth < required_bandwidth
            case "<=":
                return bandwidth <= required_bandwidth
            case "=":
                return bandwidth == required_bandwidth
            case _:
                raise ValueError(f"Unknown operator {operator=}")
    return False


def _validate_architecture(provider: ResourceProvider, value: str) -> bool:
    if provider.flavor.spec.flavor_type.type_identifier is FlavorType.K8SLICE:
        return value == cast(FlavorK8SliceData, provider.flavor.spec.flavor_type.type_data).characteristics.architecture
    return False


def _validate_tee_available(provider: ResourceProvider, value: str) -> bool:
    if provider.flavor.spec.flavor_type.type_identifier is FlavorType.K8SLICE:
        properties = cast(FlavorK8SliceData, provider.flavor.spec.flavor_type.type_data).properties
        return value.capitalize() == str(properties.get("TEE", "False")).capitalize()
    return False


@unique
class KnownIntent(Enum):
    # k8s resources
    cpu = "cpu", False, _check_cpu
    memory = "memory", False, _check_memory
    gpu = "gpu", False, _check_gpu
    architecture = "architecture", False, _validate_architecture

    # high order requests
    latency = "latency", False, _always_true
    location = "location", False, validate_location
    throughput = "throughput", False, _always_true
    compliance = "compliance", False, _validate_regulations
    energy = "energy", False, _always_true
    battery = "battery", False, _always_true

    # carbon aware requests
    max_delay = "max-delay", False, _always_true
    carbon_aware = "carbon-aware", False, _always_true

    # TER
    bandwidth_against = "bandwidth-against", False, _validate_bandwidth_against_point
    tee_available = "tee-available", False, _validate_tee_available

    # service
    service = "service", True, _always_true

    #mspl
    mspl = "mspl", False, _always_true

    def __new__(cls, *args: str, **kwds: str) -> KnownIntent:
        obj = object.__new__(cls)
        obj._value_ = args[0]
        return obj

    def __init__(self, label: str, external: bool, validator: Callable[[ResourceProvider, str], bool]):
        self.label = label
        self._external = external
        self._validator = validator

    def to_intent_key(self) -> str:
        return f"fluidos-intent-{self.label}"

    def is_external_requirement(self) -> bool:
        return self._external

    def validates(self, provider: ResourceProvider, value: str) -> bool:
        return self._validator(provider, value)

    @staticmethod
    def is_supported(intent_name: str) -> bool:
        if intent_name.startswith("fluidos-intent-"):
            intent_name = "-".join(intent_name.split("-")[2:])

        return any(
            known_intent.label == intent_name for known_intent in KnownIntent
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

    def is_external_requirement(self) -> bool:
        return self.name.is_external_requirement()

    def validates(self, provider: ResourceProvider) -> bool:
        return self.name.validates(provider, self.value)


class ResourceFinder(ABC):
    def find_best_match(self, resource: Resource, namespace: str) -> list[ResourceProvider]:
        raise NotImplementedError()

    def find_service(self, id: str, service: Intent, namespace: str) -> list[ExternalResourceProvider]:
        raise NotImplementedError()

    def retrieve_all_flavors(self, namespace: str) -> list[Flavor]:
        raise NotImplementedError()

    def update_local_flavor(self, flavor: Flavor, data: Any, namespace: str) -> None:
        raise NotImplementedError()
