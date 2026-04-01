#!/bin/sh
set -e

./scripts/install-envtest.sh

ENVTEST_VERSION=$(grep '^ENVTEST_VERSION=' scripts/install-envtest.sh | cut -d= -f2)
SEMVER=${ENVTEST_VERSION#v}

export KUBEBUILDER_ASSETS=$(go tool -modfile gotools/setup-envtest/go.mod setup-envtest use -p path -i "${SEMVER}")
go test ./...
