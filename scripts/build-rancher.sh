#!/bin/bash

set -e

# Variables
STEVE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RANCHER_DIR="/tmp/rancher-build/rancher"
STEVE_PARENT_DIR=$(dirname "$STEVE_DIR")

# Build variables (can be overridden by environment)
OS="${OS:-linux}"
ARCH="${ARCH:-amd64}"
REPO="${REPO:-rancher}"
BUILD_SAFE_DIRS="${STEVE_PARENT_DIR}"
export BUILD_SAFE_DIRS

# Global variables for build
COMMIT=""
TAG=""
BUILD_ARGS=()
RANCHER_IMAGE=""
RANCHER_AGENT_IMAGE=""

# because macos doesn't have realpath apparently
abs_path() {
  echo "$(cd "$(dirname "$1")" && pwd -P)/$(basename "$1")"
}

setup_rancher_dir() {
    if [ -d "$RANCHER_DIR" ]; then
        echo "Rancher repository already exists at $RANCHER_DIR"
        echo "Skipping clone"
    else
        echo "Cloning rancher repository to $RANCHER_DIR..."
        mkdir -p "$(dirname "$RANCHER_DIR")"
        git clone https://github.com/rancher/rancher.git "$RANCHER_DIR"
    fi
    
    cd "$RANCHER_DIR"
    
    # Build variables that depend on RANCHER_DIR
    COMMIT=$(git rev-parse --short HEAD)
    TAG="${TAG:-$(grep -m1 ' TAG:' .github/workflows/pull-request.yml | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e "s/\${{ github.sha }}/$COMMIT/g" | cut -d' ' -f2)}"
}

setup_go_mod() {
    echo "Adding replace directive for steve module..."
    go mod edit -replace github.com/rancher/steve="$STEVE_DIR"
    echo "Running go mod tidy..."
    go mod tidy
}


setup_build_args() {
    BUILD_ARGS=()
    BUILD_ARGS+=("--build-arg=VERSION=${TAG}")
    BUILD_ARGS+=("--build-arg=ARCH=${ARCH}")
    BUILD_ARGS+=("--build-arg=IMAGE_REPO=${REPO}")
    BUILD_ARGS+=("--build-arg=COMMIT=${COMMIT}")
    BUILD_ARGS+=("--build-arg=RANCHER_TAG=${TAG}")
    BUILD_ARGS+=("--build-arg=RANCHER_REPO=${REPO}")
}

add_context() {
  if ! replace=$(grep "$1 =>" go.mod); then
    return 0
  fi
  if [ -n "$(echo "$replace" | cut -d= -f2 | cut -d' ' -f3)" ]; then
    return 0
  fi

  godep=$(echo "$replace" | cut -d= -f2 | cut -d' ' -f2)
  path=$(abs_path "$godep")
  
  BUILD_ARGS+=("--build-context=$2=$path")
  BUILD_ARGS+=("--build-arg=$3=$2")
  BUILD_ARGS+=("--build-arg=$3_PATH=$path")
}

setup_build_context() {
    add_context "github.com/rancher/steve" "steve-context" "GODEP_STEVE"
    BUILD_ARGS+=("--build-arg=BUILD_WORKDIR=$PWD")
}

build_server_image() {
    echo "Building rancher server image..."
    docker buildx build \
      "${BUILD_ARGS[@]}" \
      --load \
      --tag "${REPO}"/rancher:"${TAG}" \
      --platform="${OS}/${ARCH}" \
      --target server \
      --file ./package/Dockerfile .
}

build_agent_image() {
    echo "Building rancher agent image..."
    docker buildx build \
      "${BUILD_ARGS[@]}" \
      --load \
      --tag "${REPO}"/rancher-agent:"${TAG}" \
      --platform="${OS}/${ARCH}" \
      --target agent \
      --file ./package/Dockerfile .
}

detect_images() {
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
}

output_results() {
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
}

main() {
    echo "Building Rancher images..."
    echo "Setting BUILD_SAFE_DIRS=${BUILD_SAFE_DIRS} to allow local replace directive"
    
    setup_rancher_dir
    setup_go_mod
    setup_build_args
    setup_build_context
    build_server_image
    build_agent_image
    detect_images
    output_results
}

main "$@"
