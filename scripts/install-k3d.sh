#!/bin/bash
set -e

# renovate: datasource=github-release-attachments depName=k3d-io/k3d
K3D_VERSION=v5.8.3
# renovate: datasource=github-release-attachments depName=k3d-io/k3d digestVersion=v5.8.3
K3D_SUM_linux_amd64=dbaa79a76ace7f4ca230a1ff41dc7d8a5036a8ad0309e9c54f9bf3836dbe853e
# renovate: datasource=github-release-attachments depName=k3d-io/k3d digestVersion=v5.8.3
K3D_SUM_linux_arm64=0b8110f2229631af7402fb828259330985918b08fefd38b7f1b788a1c8687216
# renovate: datasource=github-release-attachments depName=k3d-io/k3d digestVersion=v5.8.3
K3D_SUM_darwin_amd64=fd0f8e9e8ea4d8bc3674572ca6ed0833b639bf57c43c708616d937377324cfea
# renovate: datasource=github-release-attachments depName=k3d-io/k3d digestVersion=v5.8.3
K3D_SUM_darwin_arm64=8da468daa7dc7cf7cdd4735f90a9bb05179fa27858250f62e3d8cdf5b5ca0698

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
