#!/usr/bin/env bash

set -xeuo pipefail

./setup-testbed.sh

# create service flavor in the DE provider
kubectl apply -f $PWD/rabbitmq-service.yaml --kubeconfig $PWD/provider-DE-config.yaml
