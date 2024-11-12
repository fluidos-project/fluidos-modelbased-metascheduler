from __future__ import annotations

import logging
import time
from typing import Any

from kubernetes import client  # type: ignore
from kubernetes.client.exceptions import ApiException  # type: ignore

from fluidos_model_orchestrator.common import Flavor
from fluidos_model_orchestrator.common import ResourceProvider
from fluidos_model_orchestrator.configuration import CONFIGURATION

logger = logging.getLogger(__name__)


class RemoteResourceProvider(ResourceProvider):
    def __init__(self, id: str, flavor: Flavor, peering_candidate: str, reservation: str, api_client: client.CustomObjectsApi, seller: dict[str, Any]) -> None:
        super().__init__(id, flavor)
        self.peering_candidate = peering_candidate
        self.reservation = reservation
        self.api_client = api_client
        self.seller = seller
        self.contract: str | None = None

    def acquire(self, namespace: str) -> bool:
        logger.info("Creating connection to remote node")
        contract = self._buy()
        if contract is None:
            return False
        self.contract = contract
        return self._establish_peering(contract, namespace)

    def get_label(self) -> dict[str, str]:
        if self.contract is None:
            logger.error("Remote resource not bougth, cannot return valid label")
            raise RuntimeError("RemoteResourceProvider not connected to active resource")

        return {
            CONFIGURATION.remote_node_key: self._get_remote_cluster_id()
        }

    def _get_remote_cluster_id(self) -> str:
        if self.contract is None:
            raise RuntimeError("Unable to retrieve contract with no contract id specified")

        try:
            resource = self.api_client.get_namespaced_custom_object(
                group="reservation.fluidos.eu",
                version="v1alpha1",
                namespace=CONFIGURATION.namespace,
                plural="contracts",
                name=self.contract
            )

            if resource is None:
                raise RuntimeError(f"Unable to retrieve {self.contract=}")

            return resource["spec"]["peeringTargetCredentials"]["clusterID"]
        except ApiException as e:
            logger.error(f"Unable to reserve and buy {self.peering_candidate}")
            logger.debug(f"Reason: {e=}")
            raise RuntimeError(e)

    def _buy(self) -> str | None:
        logger.info(f"Establishing buying of {self.peering_candidate}")

        try:
            logger.info(f"Reserving peering candidate {self.peering_candidate}")
            body = self._create_reservation(self.id, self.peering_candidate, CONFIGURATION.namespace, self.seller)

            response: dict[str, Any] = self.api_client.create_namespaced_custom_object(
                group="reservation.fluidos.eu",
                version="v1alpha1",
                namespace=CONFIGURATION.namespace,
                plural="reservations",
                body=body,
                async_req=False
            )  # type: ignore

        except ApiException as e:
            logger.error(f"Unable to reserve and buy {self.peering_candidate}")
            logger.debug(f"Reason: {e=}")
            return None

        for _ in range(CONFIGURATION.n_try):
            if "name" in response.get("status", {}).get("contract", {}):
                logger.info("Contract available")
                break
            else:
                logger.info("Contract name not available")

            time.sleep(0.2)
            try:
                response = self.api_client.get_namespaced_custom_object(
                    group="reservation.fluidos.eu",
                    version="v1alpha1",
                    namespace=CONFIGURATION.namespace,
                    plural="reservations",
                    name=body["metadata"]["name"],
                    async_req=False
                )  # type: ignore
            except ApiException as e:
                logger.error(f"Unable to reserve and buy {self.peering_candidate}")
                logger.error(f"Reason: {e=}")
        else:
            logger.info("Contract not available")
            return None

        contract_name = response["status"]["contract"]["name"]

        return contract_name

    def _establish_peering(self, contract_name: str, namespace: str) -> bool:
        logger.info(f"Establishing peering for {self.peering_candidate}")

        allocation_name = f"{self.id}-allocation"

        body: dict[str, Any] = {
            "apiVersion": "nodecore.fluidos.eu/v1alpha1",
            "kind": "Allocation",
            "metadata": {
                "name": allocation_name,
            },
            "spec": {
                # Get it from the solver
                "intentID": self.id,
                # Retrieve information from the reservation and the contract
                "contract": {
                    "name": contract_name,
                    "namespace": CONFIGURATION.namespace
                }
            }
        }

        try:
            allocation = self.api_client.create_namespaced_custom_object(
                group="nodecore.fluidos.eu",
                version="v1alpha1",
                namespace=CONFIGURATION.namespace,
                plural="allocations",
                body=body,
                async_req=False)  # type: ignore

            if allocation is not None:
                logger.info("Allocation created")
                # logger.info(f"{json.dumps(allocation)}")

                return self._create_namespace_offload_resource(namespace)
        except ApiException as e:
            logger.error(f"Error establishing peering for {self.peering_candidate}")
            logger.error(f"{e=}")

        return False

    def _create_namespace_offload_resource(self, namespace: str) -> bool:
        try:
            for _ in range(CONFIGURATION.n_try):
                res = self.api_client.create_namespaced_custom_object(
                    group="offloading.liqo.io",
                    version="v1alpha1",
                    namespace=namespace,
                    plural="namespaceoffloadings",
                    body={
                        "apiVersion": "offloading.liqo.io/v1alpha1",
                        "kind": "NamespaceOffloading",
                        "metadata": {
                            "name": "offloading"
                        },
                        "spec": {
                            "kind": "NamespaceOffloading",
                            "clusterSelector": {
                                "nodeSelectorTerms": []
                            },
                            "namespaceMappingStrategy": "DefaultName",
                            "podOffloadingStrategy": "LocalAndRemote",
                        }
                    },
                    async_req=False)  # type: ignore

                if res is not None:
                    logger.info(f"NamespaceOffload created for {namespace}")

                    return True
        except ApiException as e:
            logger.error(f"Error offloading namespace {namespace}")
            logger.error(f"{e=}")
            logger.error(f"{e.reason=}")

        return False

    def _create_reservation(self, solver_name: str, candidate: str, namespace: str, seller: dict[str, Any]) -> dict[str, Any]:
        logger.info(f'Creating reservation for {candidate}')

        return {
            "apiVersion": "reservation.fluidos.eu/v1alpha1",
            "kind": "Reservation",
            "metadata": {
                "name": f'{candidate}-reservation'
            },
            "spec": {
                "solverID": solver_name,
                "buyer": CONFIGURATION.identity,
                # Retrieve from PeeringCandidate Flavor Owner field
                "seller": seller,
                # Set it to reserve
                "reserve": True,
                # Set it to purchase after reservation is completed and you have a transaction
                "purchase": True,
                # Retrieve from PeeringCandidate chosen to reserve
                "peeringCandidate": {
                    "name": candidate,
                    "namespace": namespace
                }
            }
        }
