#!/bin/bash

set -e

show_disk_space() {
    echo ""
    echo "=== Disk Space Usage ==="
    echo "Root filesystem:"
    df -h /
    echo ""
    echo "/tmp filesystem:"
    df -h /tmp 2>/dev/null || df -h | grep -E "(tmp|Filesystem)"
    echo ""
    if command -v docker &> /dev/null; then
        echo "Docker disk usage:"
        docker system df 2>/dev/null || echo "Docker not available or not running"
    fi
    echo "========================"
    echo ""
}

echo "Initial disk space:"
show_disk_space

STEVE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RANCHER_DIR="/tmp/rancher-build/rancher"

if [ -d "$RANCHER_DIR" ]; then
    echo "Rancher repository already exists at $RANCHER_DIR"
    echo "Skipping clone"
    if [ -d "$RANCHER_DIR/.git" ]; then
        echo "Repository size: $(du -sh "$RANCHER_DIR" 2>/dev/null | cut -f1)"
    fi
else
    echo "Disk space before cloning:"
    show_disk_space
    echo "Cloning rancher repository to $RANCHER_DIR..."
    mkdir -p "$(dirname "$RANCHER_DIR")"
    git clone https://github.com/rancher/rancher.git "$RANCHER_DIR"
    echo "Disk space after cloning:"
    show_disk_space
fi

cd "$RANCHER_DIR"

echo "Adding replace directive for steve module..."
go mod edit -replace github.com/rancher/steve="$STEVE_DIR"

echo "Disk space before go mod tidy:"
show_disk_space

echo "Running go mod tidy..."
go mod tidy

echo "Disk space after go mod tidy:"
show_disk_space

echo "Running make quick..."
STEVE_PARENT_DIR=$(dirname "$STEVE_DIR")
if [ -n "$BUILD_SAFE_DIRS" ]; then
    export BUILD_SAFE_DIRS="${BUILD_SAFE_DIRS}:${STEVE_PARENT_DIR}"
else
    export BUILD_SAFE_DIRS="${STEVE_PARENT_DIR}"
fi
echo "Setting BUILD_SAFE_DIRS=${BUILD_SAFE_DIRS} to allow local replace directive"

echo "Disk space before make quick:"
show_disk_space

make quick

echo "Disk space after make quick:"
show_disk_space

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

