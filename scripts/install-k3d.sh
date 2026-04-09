#!/bin/bash
set -e

# renovate: datasource=github-release-attachments depName=k3d-io/k3d
K3D_VERSION=v5.8.2
# renovate: datasource=github-release-attachments depName=k3d-io/k3d digestVersion=v5.8.2
K3D_SUM_linux_amd64=7e92e883f2457aa8702f9f504a772fadec3ef3f9d678f929b2b8e05f3910a30c
# renovate: datasource=github-release-attachments depName=k3d-io/k3d digestVersion=v5.8.2
K3D_SUM_linux_arm64=8134a7047afb3ed7aa32b7a967bc299dbe3abe640bab27fc9d2c9328b2361bbf
# renovate: datasource=github-release-attachments depName=k3d-io/k3d digestVersion=v5.8.2
K3D_SUM_darwin_amd64=51fcb8208408bca3d476679a1a673fe47820cf134b650ef4583d710316896a1f
# renovate: datasource=github-release-attachments depName=k3d-io/k3d digestVersion=v5.8.2
K3D_SUM_darwin_arm64=72d488935c962ff8c01708e288f426e78e05df34b14062e8fdd4e9f6661cd378

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
