#!/bin/bash

set -e

STEVE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RANCHER_DIR="/tmp/rancher-build/rancher"

if [ -d "$RANCHER_DIR" ]; then
    echo "Rancher repository already exists at $RANCHER_DIR"
    echo "Skipping clone"
else
    echo "Cloning rancher repository to $RANCHER_DIR..."
    mkdir -p "$(dirname "$RANCHER_DIR")"
    git clone https://github.com/rancher/rancher.git "$RANCHER_DIR"
fi

cd "$RANCHER_DIR"

echo "Adding replace directive for steve module..."
go mod edit -replace github.com/rancher/steve="$STEVE_DIR"

echo "Running go mod tidy..."
go mod tidy

echo "Running make quick..."
STEVE_PARENT_DIR=$(dirname "$STEVE_DIR")
if [ -n "$BUILD_SAFE_DIRS" ]; then
    export BUILD_SAFE_DIRS="${BUILD_SAFE_DIRS}:${STEVE_PARENT_DIR}"
else
    export BUILD_SAFE_DIRS="${STEVE_PARENT_DIR}"
fi
echo "Setting BUILD_SAFE_DIRS=${BUILD_SAFE_DIRS} to allow local replace directive"
make quick

echo ""
echo "Detecting built images..."
RANCHER_IMAGE=$(docker images --format "{{.Repository}}:{{.Tag}}" --filter "reference=rancher/rancher" 2>/dev/null | head -n1)
RANCHER_AGENT_IMAGE=$(docker images --format "{{.Repository}}:{{.Tag}}" --filter "reference=rancher/rancher-agent" 2>/dev/null | head -n1)

if [ -z "$RANCHER_IMAGE" ]; then
    echo "Warning: Could not detect rancher image, using default rancher/rancher:dev"
    RANCHER_IMAGE="rancher/rancher:dev"
fi

if [ -z "$RANCHER_AGENT_IMAGE" ]; then
    echo "Warning: Could not detect rancher-agent image, using default rancher/rancher-agent:dev"
    RANCHER_AGENT_IMAGE="rancher/rancher-agent:dev"
fi

echo ""
echo "Build complete! Docker images created:"
echo "  Rancher server: ${RANCHER_IMAGE}"
echo "  Rancher agent: ${RANCHER_AGENT_IMAGE}"

