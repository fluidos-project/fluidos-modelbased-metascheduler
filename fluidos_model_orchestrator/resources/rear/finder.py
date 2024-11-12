from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any

from kubernetes import client  # type: ignore
from kubernetes.client.exceptions import ApiException  # type: ignore

from fluidos_model_orchestrator.common import build_flavor
from fluidos_model_orchestrator.common import ExternalResourceProvider
from fluidos_model_orchestrator.common import Flavor
from fluidos_model_orchestrator.common import FlavorType
from fluidos_model_orchestrator.common import Intent
from fluidos_model_orchestrator.common import Resource
from fluidos_model_orchestrator.common import ResourceFinder
from fluidos_model_orchestrator.common import ResourceProvider
from fluidos_model_orchestrator.configuration import CONFIGURATION
from fluidos_model_orchestrator.configuration import Configuration
from fluidos_model_orchestrator.resources.rear.local_resource_provider import LocalResourceProvider
from fluidos_model_orchestrator.resources.rear.remote_resource_provider import RemoteResourceProvider
from fluidos_model_orchestrator.resources.rear.service_resource_provider import build_REARServiceResourceProvider

logger = logging.getLogger(__name__)


class REARResourceFinder(ResourceFinder):
    def __init__(self, configuration: Configuration = CONFIGURATION) -> None:
        self.configuration = configuration
        self.api_client: client.CustomObjectsApi = client.CustomObjectsApi(api_client=self.configuration.k8s_client)
        self.identity: dict[str, str] = configuration.identity

    def find_best_match(self, resource: Resource, namespace: str) -> list[ResourceProvider]:
        logger.info("Retrieving resource best match with REAR")

        local: list[ResourceProvider] = self._find_local(resource, CONFIGURATION.namespace)

        if len(local):
            logger.info(f"Found local resource {local=}")
        else:
            logger.info("No local resource compatible")

        remote = self._find_remote(resource, CONFIGURATION.namespace)

        logger.info(f"Found remote resource {remote=}")

        return local + remote

    def _resource_to_service_sorver_request(self, service: Intent, intent_id: str) -> tuple[dict[str, Any], str]:
        if intent_id is None:
            intent_id = str(uuid.uuid4())

        solver_request = {
            "apiVersion": "nodecore.fluidos.eu/v1alpha1",
            "kind": "Solver",
            "metadata": {
                "name": f"service-{service.name.name}-{intent_id}-solver"
            },
            "spec": {
                "intentID": intent_id,
                "findCandidate": True,
                "reserveAndBuy": True,
                "establishPeering": True,
                "selector": {
                    "flavorType": "Service",
                    "filters": {
                        "categoryFilter": {
                            "name": "Match",
                            "data": {
                                "value": service.value
                            }
                        }
                    }
                }
            }
        }

        return (solver_request, intent_id)

    def find_service(self, id: str, service: Intent, namespace: str) -> list[ExternalResourceProvider]:
        logger.info("Retrieving service with REAR")

        body, _ = self._resource_to_service_sorver_request(service, id)

        solver_name = self._initiate_search(body, namespace)

        # NOTE: FOR SERVICE SOLVER DOES NOT SOLVE
        # Check status of Allocation with .status.status == "Active" and
        # find right allocation using .spec.contract.name == "<contract name>"
        # contract name is retrieved from Reservation where .spec.solverID == "solver-name", there one finds .status.contract.name

        for _ in range(CONFIGURATION.n_try):
            time.sleep(CONFIGURATION.SOLVER_SLEEPING_TIME)

            reservations: None | dict[str, Any] = self.api_client.list_namespaced_custom_object(
                group="reservation.fluidos.eu",
                version="v1alpha1",
                plural="reservations",
                namespace=CONFIGURATION.namespace,
                async_req=False  # type: ignore
            )

            if reservations is None:
                continue

            valid_reservations = [reservation for reservation in reservations.get("items", []) if reservation["spec"]["solverID"] == solver_name and reservation.get("status", {}).get("contract", {}).get("name", None) is not None]

            if len(valid_reservations) == 0:
                # either no reservation or not with valid status
                continue
            if len(valid_reservations) == 1:
                break
        else:
            # assume no service found
            logger.info("No valid service found (no valid reservation)")
            return []

        contract_name = valid_reservations[0]["status"]["contract"]["name"]

        # Check status of Allocation with .status.status == "Active" and
        # find right allocation using .spec.contract.name == "<contract name>"
        for _ in range(CONFIGURATION.n_try):
            time.sleep(CONFIGURATION.SOLVER_SLEEPING_TIME * 1.5)
            logger.info(f"Searching valid allocation for {contract_name}")

            allocations: None | dict[str, Any] = self.api_client.list_namespaced_custom_object(
                group="nodecore.fluidos.eu",
                version="v1alpha1",
                plural="allocations",
                namespace=CONFIGURATION.namespace,
                async_req=False  # type: ignore
            )

            if allocations is None:
                continue

            for allocation in allocations.get("items", []):
                if allocation.get("spec", {}).get("contract", {}).get("name", None) == contract_name:
                    logger.info("Allocation found")
                    if allocation.get("status", {}).get("status", "") == "Active":
                        logger.info("Allocation is active!")
                        if len(allocation["status"]["resourceRef"]):
                            logger.info("resourceRef available")
                            return [
                                build_REARServiceResourceProvider(self.configuration.k8s_client, allocation)
                            ]
                    else:
                        logger.info(f"Allocation is not active \n---\n {json.dumps(allocation)}\n---\n")
        else:
            # assume no allocation found in time
            logger.info("No valid service found (no active allocation for contract)")
            return []

    def retrieve_all_flavors(self, namespace: str) -> list[Flavor]:

        logger.info("Retrieving all flavours")

        locally_available_flavours = self._get_locally_available_flavors(namespace)
        logger.debug(f"Retrieved {len(locally_available_flavours)} local flavors")

        remotely_available_flavours = self._get_remotely_available_flavors(namespace)
        logger.debug(f"Retrieved {len(remotely_available_flavours)} remote flavors")

        return locally_available_flavours + remotely_available_flavours

    def update_local_flavor(self, flavor: Flavor, data: Any, namespace: str) -> None:
        logger.info(f"Updating {flavor=} with {data=}")

        response = self.api_client.patch_namespaced_custom_object(
            group="nodecore.fluidos.eu",
            version="v1alpha1",
            namespace=namespace,
            plural="flavors",
            name=flavor.metadata.name,
            body={
                "spec": {
                    "flavorType": {
                        "typeData": {
                            "properties": data
                        }
                    }
                }
            }
        )

        if response is None:
            raise ValueError()

    def _initiate_search(self, body: dict[str, Any], namespace: str) -> str:
        logger.info("Initiating remote search")
        logger.debug(f"Solver body: {body}")

        try:
            response = self.api_client.get_namespaced_custom_object(
                group="nodecore.fluidos.eu",
                version="v1alpha1",
                namespace=namespace,
                plural="solvers",
                name=body["metadata"]["name"],
                async_req=False)  # type: ignore
        except ApiException as e:
            logger.debug(f"Error retrieving {body['metadata']['name']}: {e=}")
            response = None

        if response is None or response["kind"] != "Solver":
            logger.debug("Solver not existing, creating")
            response = self.api_client.create_namespaced_custom_object(
                group="nodecore.fluidos.eu",
                version="v1alpha1",
                namespace=namespace,
                plural="solvers",
                body=body,
                async_req=False
            )  # type: ignore
        else:
            logger.debug("Solver already existing")

        return response["metadata"]["name"]

    def _check_solver_status(self, solver_name: str, namespace: str) -> dict[str, Any] | None:
        logger.info(f"Retrieving solver/{solver_name} status")

        try:
            remote_flavour_status = self.api_client.get_namespaced_custom_object(
                group="nodecore.fluidos.eu",
                version="v1alpha1",
                namespace=namespace,
                plural="solvers",
                name=solver_name,
                async_req=False
            )  # type: ignore
        except ApiException as e:
            logger.error("Unable to retrieve solver status")
            logger.debug(f"Reason: {e=}")
            return None

        logger.debug(f"Received {json.dumps(remote_flavour_status)}")

        return remote_flavour_status

    def _find_remote(self, resource: Resource, namespace: str) -> list[ResourceProvider]:
        logger.info(f"Retrieving remote flavours in {namespace=}")

        body, _ = self._resource_to_solver_request(resource, resource.id)

        solver_name = self._initiate_search(body, namespace)

        for _ in range(CONFIGURATION.n_try):
            time.sleep(CONFIGURATION.SOLVER_SLEEPING_TIME)
            remote_flavour_status = self._check_solver_status(solver_name, namespace)

            if remote_flavour_status is None or "status" not in remote_flavour_status:
                return []

            phase: str = remote_flavour_status["status"]["solverPhase"]["phase"]

            if phase == "Solved":
                break

            if phase == "Failed" or phase == "Timed Out":
                logger.info("Unable to find matching flavour")
                return []

            if phase == "Running" or phase == "Pending":
                logger.debug("Still processing, wait...")
        else:
            logger.error("Solver did not finish withing the allocated time")
            return []

        # resource found and reserved, now we need to return the best matching
        peering_candidates = self._retrieve_peering_candidates(solver_name, namespace)
        if peering_candidates is None:
            logger.error("Error retrieving peering candidates from Discovery")
            return []

        if len(peering_candidates) == 0:
            logger.info("No valid peering candidates found")
            return []

        logger.debug(f"{peering_candidates=}")

        matching_resources: list[ResourceProvider] = self._reserve_all(solver_name, peering_candidates, namespace)

        logger.debug(f"{matching_resources=}")

        return matching_resources

    def _retrieve_peering_candidates(self, solver_name: str, namespace: str) -> list[dict[str, Any]] | None:
        logger.info(f"Retrieving discovery for {solver_name} in {namespace}")

        try:
            discovery = self.api_client.get_namespaced_custom_object(
                group="advertisement.fluidos.eu",
                version="v1alpha1",
                namespace=namespace,
                plural="discoveries",
                name=f"discovery-{solver_name}",
                async_req=False
            )  # type: ignore

            return discovery.get("status", {}).get("peeringCandidateList", {}).get("items", None)

        except ApiException as e:
            logger.error("Unable to retrieve solver status")
            logger.debug(f"Reason: {e=}")
            return None

    def _reserve_all(self, solver_name: str, peering_candidates: list[dict[str, Any]], namespace: str) -> list[ResourceProvider]:
        logger.info("Reserving all peering candidates, just in case")
        return [
            resource for resource in
            [
                self._reserve_peering_candidate(solver_name, candidate, namespace) for candidate in peering_candidates
                if candidate is not None and candidate["spec"]["available"] is True
            ]
        ]

    def _reserve_peering_candidate(self, solver_name: str, candidate: dict[str, Any], namespace: str) -> RemoteResourceProvider:
        logger.info(f"Reserving peering candidate {candidate['metadata']['name']} but not for real")

        return RemoteResourceProvider(
            id=solver_name,
            flavor=build_flavor(candidate["spec"]["flavor"]),
            peering_candidate=candidate["metadata"]["name"],
            reservation="",  # response["metadata"]["name"],
            api_client=self.api_client,
            seller=candidate["spec"]["flavor"]["spec"]["owner"]
        )

    def _resource_to_solver_request(self, resource: Resource, intent_id: str | None = None) -> tuple[dict[str, Any], str]:
        if intent_id is None:
            intent_id = str(uuid.uuid4())

        solver_request = {
            "apiVersion": "nodecore.fluidos.eu/v1alpha1",
            "kind": "Solver",
            "metadata": {
                "name": f"{intent_id}-solver"
            },
            "spec": {
                "intentID": intent_id,
                "findCandidate": True,
                "reserveAndBuy": False,
                "enstablishPeering": False,
                "selector": self._build_flavour_selector(resource)
            }
        }

        return (solver_request, intent_id)

    def _build_flavour_selector(self, resource: Resource) -> dict[str, Any]:
        selector: dict[str, Any] = {
            # The flavorType is the type of the Flavor (FLUIDOS node) that the solver should find
            "flavorType": "K8Slice",
        }

        # The filters are used to filter the Flavors (FLUIDOS nodes) that the solver should consider
        selector_filters: dict[str, Any] = {}

        if resource.architecture is not None:
            selector_filters["architectureFilter"] = {
                "name": "Match",
                "data": {
                    "value": resource.architecture
                }
            }

        if resource.cpu is not None:
            selector_filters["cpuFilter"] = {
                "name": "Range",
                "data": {
                    "min": resource.cpu
                }
            }
        if resource.memory is not None:
            selector_filters["memoryFilter"] = {
                "name": "Range",
                "data": {
                    "min": resource.memory
                }
            }
        if resource.pods is not None:
            selector_filters["modsFilter"] = {
                "name": "Match",
                "data": {
                    "value": resource.pods
                }
            }

        if len(selector_filters):
            selector["filters"] = selector_filters

        return selector

    def _build_range_selector(self, resource: Resource) -> dict[str, str]:
        selector: dict[str, str] = {
            "minCpu": resource.cpu or "0n",
            "minMemory": resource.memory or "1Ki"
        }

        if resource.gpu is not None:
            selector["minGpu"] = resource.gpu

        return selector

    def _find_local(self, resource: Resource, namespace: str) -> list[ResourceProvider]:
        logger.info(f"Retrieving locally available flavours for {resource=}")

        fitting_resources: list[ResourceProvider] = []

        local_flavours = self._get_locally_available_flavors(namespace)

        for flavor in local_flavours:
            name = flavor.metadata.name

            logger.info(f"Processing flavor {name=}")

            if flavor.spec.flavor_type.type_identifier is not FlavorType.K8SLICE:
                logger.info(f"Skipping, wrong flavour type {flavor.spec.flavor_type}")
                continue

            if resource.can_run_on(flavor):
                logger.info(f"Local flavour {name=} is compatible")
                fitting_resources.append(
                    LocalResourceProvider(
                        flavor.metadata.name,
                        flavor
                    ))
            else:
                logger.info("Not able to run the request")

        return fitting_resources

    def _get_locally_available_flavors(self, namespace: str) -> list[Flavor]:
        flavor_list: dict[str, Any]

        try:
            flavor_list = self.api_client.list_namespaced_custom_object(
                group="nodecore.fluidos.eu",
                version="v1alpha1",
                plural="flavors",
                namespace=namespace)

        except ApiException:
            logger.warning("Failed to retrieve local flavors, is node available?")
            flavor_list = {}

        return [build_flavor(flavor) for flavor in flavor_list.get("items", [])]

    def _get_remotely_available_flavors(self, namespace: str) -> list[Flavor]:
        return []  # TODO: waiting for REAR 2
