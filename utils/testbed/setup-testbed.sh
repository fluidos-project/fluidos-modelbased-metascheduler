#!/usr/bin/env bash

set -xeuo pipefail


function _get_command {
  if command -v docker 2>/dev/null >&2 ; then
    echo "docker"
  else
    echo "podman"
  fi
}



# basic setup
helm repo add fluidos https://fluidos-project.github.io/node/ --force-update

CONSUMER_NODE_PORT=30000
PROVIDER_NODE_PORT=30001

COMMAND=$(_get_command)


# setup provider DE
kind create cluster --name provider-germany --config $PWD/provider-cluster-config.yaml --kubeconfig $PWD/provider-DE-config.yaml

DE_PROVIDER_CONTROLPLANE_IP=$($COMMAND inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' provider-germany-control-plane)

liqoctl install kind --kubeconfig $PWD/provider-DE-config.yaml

helm upgrade --install --devel -n fluidos --create-namespace node fluidos/node \
  --set "provider=kind" \
  --set "networkManager.configMaps.nodeIdentity.ip=$DE_PROVIDER_CONTROLPLANE_IP:$PROVIDER_NODE_PORT" \
  --set "networkManager.configMaps.nodeIdentity.domain=provider.fluidos.eu" \
  --set "networkManager.configMaps.nodeIdentity.nodeId=provider_DE" \
  --set "rearController.service.gateway.nodePort.port=$PROVIDER_NODE_PORT" \
  --wait \
  --kubeconfig $PWD/provider-DE-config.yaml



kubectl get flavor -n fluidos --no-headers --kubeconfig $PWD/provider-DE-config.yaml | cut -f1 -d\  | xargs -I% kubectl patch flavor/%  --patch-file ./flavors-location-germany.yaml --type merge -n fluidos --kubeconfig $PWD/provider-DE-config.yaml

# setup provider Italy
kind create cluster --name provider-italy --config $PWD/provider-cluster-config.yaml --kubeconfig $PWD/provider-IT-config.yaml

IT_PROVIDER_CONTROLPLANE_IP=$($COMMAND inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' provider-italy-control-plane)

liqoctl install kind --kubeconfig $PWD/provider-IT-config.yaml

helm upgrade --install --devel -n fluidos --create-namespace node fluidos/node \
  --set "provider=kind" \
  --set "networkManager.configMaps.nodeIdentity.ip=$IT_PROVIDER_CONTROLPLANE_IP:$PROVIDER_NODE_PORT" \
  --set "networkManager.configMaps.nodeIdentity.domain=provider.fluidos.eu" \
  --set "networkManager.configMaps.nodeIdentity.nodeId=provider_IT" \
  --set "rearController.service.gateway.nodePort.port=$PROVIDER_NODE_PORT" \
  --wait \
  --kubeconfig $PWD/provider-IT-config.yaml

# setup consumer
kind create cluster --name consumer --config $PWD/consumer-cluster-config.yaml --kubeconfig $PWD/consumer-config.yaml

CONSUMER_CONTROLPLANE_IP=$($COMMAND inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' consumer-control-plane)

liqoctl install kind --kubeconfig $PWD/consumer-config.yaml

helm upgrade --install --devel -n fluidos --create-namespace node fluidos/node \
  --set "provider=kind" \
  --set "networkManager.configMaps.nodeIdentity.ip=$CONSUMER_CONTROLPLANE_IP:$CONSUMER_NODE_PORT" \
  --set "networkManager.configMaps.providers.local='$IT_PROVIDER_CONTROLPLANE_IP:$PROVIDER_NODE_PORT\,$DE_PROVIDER_CONTROLPLANE_IP:$PROVIDER_NODE_PORT'" \
  --set "networkManager.configMaps.nodeIdentity.domain=consumer.fluidos.eu" \
  --set "networkManager.configMaps.nodeIdentity.nodeId=consumer" \
  --set "rearController.service.gateway.nodePort.port=$CONSUMER_NODE_PORT" \
  --wait \
  --kubeconfig $PWD/consumer-config.yaml

# add config map required to run MBMO
kubectl apply -f $PWD/example-mbmo-config-map.yaml --kubeconfig $PWD/consumer-config.yaml -n fluidos

# add CRDs required
kubectl apply --kubeconfig $PWD/consumer-config.yaml -f $PWD/../../deployment/fluidos-meta-orchestrator/crds


# pretend the consumer cluster is in Dublin, Ireland
kubectl get flavor -n fluidos --no-headers --kubeconfig $PWD/consumer-config.yaml | cut -f1 -d\  | xargs -I% kubectl patch flavor/%  --patch-file $PWD/flavors-location-ireland.yaml --type merge -n fluidos --kubeconfig $PWD/consumer-config.yaml
