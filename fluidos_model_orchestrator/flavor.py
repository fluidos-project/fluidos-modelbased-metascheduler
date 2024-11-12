from __future__ import annotations

import logging
from dataclasses import dataclass
from dataclasses import field
from enum import auto
from enum import Enum
from enum import unique
from typing import Any


logger = logging.getLogger(__name__)


@dataclass
class GPUData:
    cores: int | str = 0
    memory: int | str = 0
    model: str = ""

    def can_run_on(self, other: GPUData) -> bool:
        return int(other.cores) >= int(self.cores) and int(other.memory) >= int(self.memory)


@dataclass(kw_only=True)
class FlavorCharacteristics:
    cpu: str
    architecture: str
    memory: str
    gpu: GPUData | str | int = "0"
    pods: str | None = None
    storage: str | None = None


@dataclass(kw_only=True)
class FlavorK8SliceData:
    characteristics: FlavorCharacteristics
    policies: dict[str, Any] = field(default_factory=dict)
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass(kw_only=True)
class FlavorServiceData:
    category: str
    configurationTemplate: dict[str, Any]
    description: str
    hostingPolicies: list[str]
    name: str
    tags: list[str]


@unique
class FlavorType(Enum):
    K8SLICE = auto()
    VM = auto()
    SERVICE = auto()
    SENSOR = auto()

    @staticmethod
    def factory(type_name: str) -> FlavorType:
        if type_name == "K8Slice":
            return FlavorType.K8SLICE
        if type_name == "Service":
            return FlavorType.SERVICE

        raise ValueError(f"Not supported {type_name=}")


@dataclass(kw_only=True)
class FlavorTypeData:
    type_identifier: FlavorType
    type_data: FlavorK8SliceData | FlavorServiceData


@dataclass(kw_only=True)
class FlavorSpec:
    availability: bool
    flavor_type: FlavorTypeData
    network_property_type: str
    owner: dict[str, Any]
    providerID: str
    price: dict[str, Any] = field(default_factory=dict)
    location: dict[str, Any] = field(default_factory=dict)


@dataclass(kw_only=True)
class FlavorMetadata:
    name: str
    owner_references: dict[str, Any]


@dataclass(kw_only=True)
class Flavor:
    metadata: FlavorMetadata
    spec: FlavorSpec


def build_flavor(flavor: dict[str, Any]) -> Flavor:
    if "kind" in flavor:
        if flavor["kind"] != "Flavor":
            raise ValueError(f"Unable to process kind {flavor['kind']}")
    else:
        logger.info("Building flavor from spec, not object")

    return Flavor(
        metadata=_build_metadata(flavor["metadata"]),
        spec=_build_spec(flavor["spec"]),
    )


def _build_metadata(metadata: dict[str, Any]) -> FlavorMetadata:
    return FlavorMetadata(
        name=metadata["name"],
        owner_references=metadata.get("ownerReferences", {}),
    )


def _build_spec(spec: dict[str, Any]) -> FlavorSpec:
    return FlavorSpec(
        availability=spec["availability"],
        flavor_type=_build_flavor_type(spec["flavorType"]),
        location=spec["location"],
        network_property_type=spec["networkPropertyType"],
        owner=spec["owner"],
        price=spec["price"],
        providerID=spec["providerID"],
    )


def _build_flavor_type(flavor_type_data: dict[str, Any]) -> FlavorTypeData:
    flavor_type = FlavorType.factory(flavor_type_data["typeIdentifier"])
    return FlavorTypeData(
        type_identifier=flavor_type,
        type_data=_build_flavor_type_data(flavor_type, flavor_type_data["typeData"])
    )


def _build_flavor_type_data(flavor_type: FlavorType, data: dict[str, Any]) -> FlavorK8SliceData | FlavorServiceData:
    if flavor_type is FlavorType.K8SLICE:
        return FlavorK8SliceData(
            characteristics=FlavorCharacteristics(
                cpu=data["characteristics"]["cpu"],
                architecture=data["characteristics"]["architecture"],
                memory=data["characteristics"]["memory"],
                gpu=data["characteristics"]["gpu"],
                pods=data["characteristics"]["pods"],
                storage=data["characteristics"]["storage"],
            ),
            policies=data.get("policies", {}),
            properties=data.get("properties", {})
        )
    elif flavor_type is FlavorType.SERVICE:
        return FlavorServiceData(
            category=data["category"],
            configurationTemplate=data["configurationTemplate"],
            description=data["description"],
            hostingPolicies=data["hostingPolicies"],
            name=data["name"],
            tags=data["tags"],
        )

    raise ValueError(f"Unsupported flavor type: {flavor_type}")
