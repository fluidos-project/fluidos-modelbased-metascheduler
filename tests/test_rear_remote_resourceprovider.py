from typing import Any

import kubernetes  # type: ignore
import pkg_resources  # type: ignore
import pytest  # type: ignore
from pytest_kubernetes.providers.base import AClusterManager  # type: ignore

from fluidos_model_orchestrator.common import Flavor
from fluidos_model_orchestrator.common import FlavorCharacteristics
from fluidos_model_orchestrator.common import FlavorK8SliceData
from fluidos_model_orchestrator.common import FlavorMetadata
from fluidos_model_orchestrator.common import FlavorSpec
from fluidos_model_orchestrator.common import FlavorType
from fluidos_model_orchestrator.common import FlavorTypeData
from fluidos_model_orchestrator.configuration import _build_k8s_client
from fluidos_model_orchestrator.resources.rear.remote_resource_provider import RemoteResourceProvider


def test_basic_creation(k8s: AClusterManager) -> None:
    k8s.create()

    myconfig = kubernetes.client.Configuration()  # type: ignore
    kubernetes.config.kube_config.load_kube_config(client_configuration=myconfig, config_file=str(k8s.kubeconfig))

    k8s_client = kubernetes.client.CustomObjectsApi(_build_k8s_client(myconfig))

    owner: dict[str, Any] = {
        "domain": "fluidos.eu",
        "ip": "172.18.0.2:30000",
        "nodeID": "l6936ty08l",
    }

    seller: dict[str, Any] = {
        "domain": "fluidos.eu",
        "ip": "172.18.0.3:30001",
        "nodeID": "sellerID",
    }

    provider = RemoteResourceProvider(
        "id",
        Flavor(
            metadata=FlavorMetadata(name="flavor_id", owner_references=owner),
            spec=FlavorSpec(
                availability=True,
                flavor_type=FlavorTypeData(
                    type_identifier=FlavorType.K8SLICE,
                    type_data=FlavorK8SliceData(
                        characteristics=FlavorCharacteristics(cpu="1", architecture="amd", gpu="0", memory="1000"),
                        policies={},
                        properties={}
                    )
                ),
                location={},
                network_property_type="",
                owner=owner,
                providerID="foo",
                price={}),
        ),
        "peeringcandidate-fluidos.eu-k8s-fluidos-c3978e7c",
        "reservation-test-sample",
        k8s_client,
        seller
    )

    assert provider


@pytest.mark.skip()
def test_resource_buying(k8s: AClusterManager) -> None:
    k8s.create()

    myconfig = kubernetes.client.Configuration()  # type: ignore
    kubernetes.config.kube_config.load_kube_config(client_configuration=myconfig, config_file=str(k8s.kubeconfig))

    k8s_client = kubernetes.client.CustomObjectsApi(_build_k8s_client(myconfig))

    k8s.apply(pkg_resources.resource_filename(__name__, "node/crds/tests/node/crds/reservation.fluidos.eu_reservations.yaml"))

    # create reservation
    k8s.apply(pkg_resources.resource_filename(__name__, "node/crds/tests/node/examples/example-reservation-test.yaml"))

    owner: dict[str, Any] = {
        "domain": "fluidos.eu",
        "ip": "172.18.0.2:30000",
        "nodeID": "l6936ty08l",
    }

    seller: dict[str, Any] = {
        "domain": "fluidos.eu",
        "ip": "172.18.0.3:30001",
        "nodeID": "sellerID",
    }

    provider = RemoteResourceProvider(
        "id",
        Flavor(
            metadata=FlavorMetadata(name="flavor_id", owner_references=owner),
            spec=FlavorSpec(
                availability=True,
                flavor_type=FlavorTypeData(
                    type_identifier=FlavorType.K8SLICE,
                    type_data=FlavorK8SliceData(
                        characteristics=FlavorCharacteristics(cpu="1", architecture="amd", gpu="0", memory="1000"),
                        policies={},
                        properties={}
                    )
                ),
                location={},
                network_property_type="",
                owner=owner,
                providerID="foo",
                price={}),
        ),
        "peeringcandidate-fluidos.eu-k8s-fluidos-c3978e7c",
        "reservation-test-sample",
        k8s_client,
        seller
    )

    assert provider

    contract_information = provider._buy()

    assert contract_information
