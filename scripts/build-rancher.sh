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
echo "Loading images from buildx cache to local Docker daemon..."

# copied from make quick
COMMIT=$(git rev-parse --short HEAD)
TAG="${TAG:-$(grep -m1 ' TAG:' .github/workflows/pull-request.yml | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e "s/\${{ github.sha }}/$COMMIT/g" | cut -d' ' -f2)}"
OS="${OS:-linux}"
ARCH="${ARCH:-amd64}"
REPO="${REPO:-rancher}" 

echo "Using values: REPO=${REPO}, TAG=${TAG}, OS=${OS}, ARCH=${ARCH}"

# Load rancher server image
echo "Loading rancher server image from buildx cache..."
docker buildx build --load \
    --tag "${REPO}/rancher:${TAG}" \
    --platform "${OS}/${ARCH}" \
    --target server \
    --file ./package/Dockerfile . || echo "Warning: Could not load rancher server image"

# Load rancher agent image
echo "Loading rancher agent image from buildx cache..."
docker buildx build --load \
    --tag "${REPO}/rancher-agent:${TAG}" \
    --platform "${OS}/${ARCH}" \
    --target agent \
    --file ./package/agent/Dockerfile ./package/agent || echo "Warning: Could not load rancher agent image"

echo ""
echo "Detecting built images..."
echo "All images:"
docker images
echo "--------------------------------"
echo "Rancher images:"
docker images | grep rancher || echo "No rancher images found"
echo "--------------------------------"

RANCHER_IMAGE=$(docker images --format "{{.Repository}}:{{.Tag}}" --filter "reference=rancher/rancher" 2>/dev/null | head -n1)
echo "RANCHER_IMAGE: ${RANCHER_IMAGE}"
RANCHER_AGENT_IMAGE=$(docker images --format "{{.Repository}}:{{.Tag}}" --filter "reference=rancher/rancher-agent" 2>/dev/null | head -n1)
echo "RANCHER_AGENT_IMAGE: ${RANCHER_AGENT_IMAGE}"

if [ -z "$RANCHER_IMAGE" ]; then
    echo "Warning: Could not detect rancher image"
    echo "Trying alternative detection method..."
    RANCHER_IMAGE=$(docker images rancher/rancher --format "{{.Repository}}:{{.Tag}}" 2>/dev/null | head -n1)
    if [ -z "$RANCHER_IMAGE" ]; then
        echo "Warning: Still could not detect rancher image, using default rancher/rancher:dev"
        RANCHER_IMAGE="rancher/rancher:dev"
    fi
fi

if [ -z "$RANCHER_AGENT_IMAGE" ]; then
    echo "Warning: Could not detect rancher-agent image"
    echo "Trying alternative detection method..."
    RANCHER_AGENT_IMAGE=$(docker images rancher/rancher-agent --format "{{.Repository}}:{{.Tag}}" 2>/dev/null | head -n1)
    if [ -z "$RANCHER_AGENT_IMAGE" ]; then
        echo "Warning: Still could not detect rancher-agent image, using default rancher/rancher-agent:dev"
        RANCHER_AGENT_IMAGE="rancher/rancher-agent:dev"
    fi
fi

echo ""
echo "Build complete! Docker images created:"
echo "  Rancher server: ${RANCHER_IMAGE}"
echo "  Rancher agent: ${RANCHER_AGENT_IMAGE}"

# Output for GitHub Actions
if [ -n "${GITHUB_OUTPUT:-}" ]; then
    echo "rancher-image=${RANCHER_IMAGE}" >> "$GITHUB_OUTPUT"
    echo "rancher-agent-image=${RANCHER_AGENT_IMAGE}" >> "$GITHUB_OUTPUT"
else
    # Fallback for older GitHub Actions or local runs
    echo "::set-output name=rancher-image::${RANCHER_IMAGE}"
    echo "::set-output name=rancher-agent-image::${RANCHER_AGENT_IMAGE}"
fi

