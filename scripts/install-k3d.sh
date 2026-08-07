#!/bin/bash
set -e

# renovate: datasource=github-release-attachments depName=k3d-io/k3d
K3D_VERSION=v5.9.0
# renovate: datasource=github-release-attachments depName=k3d-io/k3d digestVersion=v5.9.0
K3D_SUM_linux_amd64=06d8f25bc3a971c4eb29e0ff08429b180402db0f4dec838c9eac427e296800a0
# renovate: datasource=github-release-attachments depName=k3d-io/k3d digestVersion=v5.9.0
K3D_SUM_linux_arm64=03cde5cf23e6e8e67de5a039ecf26e5b85aca82fba3e5d13dadf904cd218a250
# renovate: datasource=github-release-attachments depName=k3d-io/k3d digestVersion=v5.9.0
K3D_SUM_darwin_amd64=b4aabc37534f95b9c764e7823f2df923f50d57600837aa60a06266cce47db732
# renovate: datasource=github-release-attachments depName=k3d-io/k3d digestVersion=v5.9.0
K3D_SUM_darwin_arm64=fe106541d5d0a3f18debcd4d432a16f8c0ce3e6ddc06f8fbb6f696a122313e00

DEST_DIR="./bin"
mkdir -p "$DEST_DIR"

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/')

eval "K3D_SUM=\$K3D_SUM_${OS}_${ARCH}"

if [ -z "$K3D_SUM" ]; then
    echo "Unsupported platform: ${OS}-${ARCH}"
    exit 1
fi

BINARY_NAME="k3d-${OS}-${ARCH}"
URL="https://github.com/k3d-io/k3d/releases/download/${K3D_VERSION}/${BINARY_NAME}"

echo "Downloading k3d ${K3D_VERSION}..."
curl -sfL "$URL" -o "$DEST_DIR/k3d"

echo "Verifying checksum..."
echo "${K3D_SUM}  $DEST_DIR/k3d" | sha256sum --check

chmod +x "$DEST_DIR/k3d"
echo "k3d installed to $DEST_DIR/k3d"
