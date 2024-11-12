from typing import Any

import yaml

from fluidos_model_orchestrator.common import build_flavor


def test_flavor_construction() -> None:
    example = """apiVersion: nodecore.fluidos.eu/v1alpha1
kind: Flavor
metadata:
  name: fluidos.eu-k8slice-2f1640fd4dfb151b4d81ad9590dcb6cc
  ownerReferences:
  - apiVersion: nodecore.fluidos.eu/v1alpha1
    kind: Node
    name: fluidos-consumer-1-worker
    uid: bcbc5b65-3de6-434a-aef9-a329317a189d
  resourceVersion: "1956"
  uid: 1dbd9361-a36b-4ee6-adff-958b5e0647bf
spec:
  availability: true
  flavorType:
    typeData:
      characteristics:
        architecture: amd64
        cpu: 1947481697n
        gpu:
          cores: "0"
          memory: "0"
          model: ""
        memory: 3735836Ki
        pods: "110"
        storage: "0"
      policies:
        partitionability:
          cpuMin: "0"
          cpuStep: "1"
          gpuMin: "0"
          gpuStep: "0"
          memoryMin: "0"
          memoryStep: 100Mi
          podsMin: "0"
          podsStep: "0"
      properties: {}
    typeIdentifier: K8Slice
  location:
    additionalNotes: None
    city: Turin
    country: Italy
    latitude: "10"
    longitude: "58"
  networkPropertyType: networkProperty
  owner:
    domain: fluidos.eu
    ip: 172.18.0.7:30000
    nodeID: ekvjnuvsel
  price:
    amount: ""
    currency: ""
    period: ""
  providerID: ekvjnuvsel
"""

    data: dict[str, Any] = yaml.safe_load(example)

    flavor = build_flavor(data)

    assert flavor is not None


def test_service_flavor_construction() -> None:
    example = """apiVersion: nodecore.fluidos.eu/v1alpha1
kind: Flavor
metadata:
  creationTimestamp: "2024-10-10T17:49:40Z"
  generation: 1
  name: provider.fluidos.eu-service-df0f
  namespace: fluidos
  ownerReferences:
  - apiVersion: nodecore.fluidos.eu/v1alpha1
    kind: ServiceBlueprint
    name: mq-rabbitmq
    uid: b1c9bf29-935b-4f61-af30-392f7d3c50b9
  resourceVersion: "4966"
  uid: e52f9585-a8e6-457e-8741-e50f759e34bb
spec:
  availability: true
  flavorType:
    typeData:
      category: message-queue
      configurationTemplate:
        $schema: http://json-schema.org/draft-07/schema#
        properties:
          password:
            type: string
          username:
            type: string
        required:
        - username
        - password
        type: object
      description: A message queue service blueprint
      hostingPolicies:
      - Consumer
      name: mq
      tags:
      - message-queue
      - rabbitmq
      - mqtt
    typeIdentifier: Service
  location:
    additionalNotes: None
    city: Turin
    country: Italy
    latitude: "10"
    longitude: "58"
  networkPropertyType: networkProperty
  owner:
    domain: provider.fluidos.eu
    ip: 10.89.0.54:30001
    nodeID: nx5w2u96xt
  price:
    amount: ""
    currency: ""
    period: ""
  providerID: nx5w2u96xt"""
    data: dict[str, Any] = yaml.safe_load(example)

    flavor = build_flavor(data)

    assert flavor is not None
