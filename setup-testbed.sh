#!/usr/bin/env bash

set -xao pipefail


# basic setup
helm repo add fluidos https://fluidos-project.github.io/node/ --force-update
consumer_node_port=30000
provider_node_port=30001

kind create cluster --name provider --config $PWD/cluster-config.yaml --kubeconfig $PWD/provider-config.yaml
provider_controlplane_ip=$(podman inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' provider-control-plane)
kind create cluster --name consumer --config $PWD/cluster-config.yaml --kubeconfig $PWD/consumer-config.yaml
consumer_controlplane_ip=$(podman inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' consumer-control-plane)

# setup provider

liqoctl install kind --kubeconfig $PWD/provider-config.yaml
helm upgrade --install --devel -n fluidos --create-namespace node fluidos/node \
  --set "provider=kind" \
  --set "networkManager.configMaps.nodeIdentity.ip=$provider_controlplane_ip:$provider_node_port" \
  --set "networkManager.configMaps.nodeIdentity.domain=provider.fluidos.eu" \
  --set "networkManager.configMaps.nodeIdentity.nodeId=provider" \
  --wait \
  --kubeconfig $PWD/provider-config.yaml

# setup consumer
liqoctl install kind --kubeconfig $PWD/consumer-config.yaml

helm upgrade --install --devel -n fluidos --create-namespace node fluidos/node \
  --set "provider=kind" \
  --set "networkManager.configMaps.nodeIdentity.ip=$consumer_controlplane_ip:$consumer_node_port" \
  --set "networkManager.configMaps.providers.local=$provider_controlplane_ip:$provider_node_port" \
  --set "networkManager.configMaps.nodeIdentity.domain=consumer.fluidos.eu" \
  --set "networkManager.configMaps.nodeIdentity.nodeId=consumer" \
  --wait \
  --kubeconfig $PWD/consumer-config.yaml

kubectl apply -f $PWD/example-mbmo-config-map.yaml --kubeconfig $PWD/consumer-config.yaml

# kubectl get flavor -n fluidos --kubeconfig $PWD/consumer-config.yaml --no-headers | xargs kubectl  patch --namespace fluidos --kubeconfig $PWD/consumer-config.yaml
