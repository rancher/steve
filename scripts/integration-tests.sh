#!/bin/sh

CLUSTER_NAME=steve-integration-test

if [ -z "$KUBECONFIG" ] || [ -z "$(kubectl config get-contexts --no-headers)" ]; then
  if ! docker version >/dev/null 2>&1; then
    echo "docker not running"
    exit 1
  fi

  KUBECONFIG=$(mktemp)
  export KUBECONFIG
  if ! k3d kubeconfig get "$CLUSTER_NAME" >"$KUBECONFIG"; then
    k3d cluster create "$CLUSTER_NAME"
  fi
fi

RUN_INTEGRATION_TESTS=true go test -v $TEST_ARGS ./tests/integration/...
