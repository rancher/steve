#!/bin/sh
set -e

# The envtest version and SHAs can be found here: https://raw.githubusercontent.com/kubernetes-sigs/controller-tools/HEAD/envtest-releases.yaml

ENVTEST_VERSION=v1.37.0
ENVTEST_SUM_linux_amd64=1d1c453633b72c161a5d5a886cde7ac850be1a2ac796a9e1d4ffacacc64868295bdd2d57aa66cd0c158f5ce510f5dfe3fbc61ac21bd3dcfb875bd70658aa663a
ENVTEST_SUM_linux_arm64=ae6a670502988200b0131c943758cfd3d3a58cf4e6247ef7f0e6a6467f1cd9a333c802e86101f0446b1b92a0e327387224e1e44fe1a4693da0929c6e529cfe9a
ENVTEST_SUM_darwin_amd64=19a2a5376a8aa57a7b25ec5198834db29bf2ba0d6ff572d7f45ba683b13fb14c3ebec8069e92db66c39f1c7a9cff1bc2be879e2aacdc9fa8b8a7e861959bdf7b
ENVTEST_SUM_darwin_arm64=fb38cfacdd71b5e97a4d4cceac861af5f55069cf783f0e49cf181bfc32eb3e557c2091a534dc5d38f1b92c5ba142bc1979f215a5385b3630ea6a661be6fa161b

CLIENT_GO_MINOR=$(go mod graph | grep ' k8s.io/client-go@' | head -n1 | cut -d@ -f2 | cut -d '.' -f 2)
ENVTEST_MINOR=$(echo "$ENVTEST_VERSION" | cut -d '.' -f 2)

if [ "$CLIENT_GO_MINOR" != "$ENVTEST_MINOR" ]; then
    echo "k8s.io/client-go minor version ($CLIENT_GO_MINOR) does not match envtest minor version ($ENVTEST_MINOR)" >&2
    exit 1
fi

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/')

eval "ENVTEST_SUM=\$ENVTEST_SUM_${OS}_${ARCH}"

if [ -z "$ENVTEST_SUM" ]; then
    echo "Unsupported platform: ${OS}-${ARCH}" >&2
    exit 1
fi

TARBALL="envtest-${ENVTEST_VERSION}-${OS}-${ARCH}.tar.gz"
URL="https://github.com/kubernetes-sigs/controller-tools/releases/download/envtest-${ENVTEST_VERSION}/${TARBALL}"
DEST="/tmp/${TARBALL}"

SEMVER=${ENVTEST_VERSION#v}

if ! go tool -modfile gotools/setup-envtest/go.mod setup-envtest list -i | grep -q "v${SEMVER}"; then
    curl -sL -o "$DEST" "$URL"

    if command -v sha512sum >/dev/null 2>&1; then
        ACTUAL_SUM=$(sha512sum "$DEST" | awk '{print $1}')
    elif command -v shasum >/dev/null 2>&1; then
        ACTUAL_SUM=$(shasum -a 512 "$DEST" | awk '{print $1}')
    else
        echo "No SHA-512 checksum tool found (need sha512sum or shasum)" >&2
        exit 1
    fi

    if [ "$ACTUAL_SUM" != "$ENVTEST_SUM" ]; then
        echo "Checksum verification failed for ${DEST}" >&2
        exit 1
    fi

    cat "$DEST" | go tool -modfile gotools/setup-envtest/go.mod setup-envtest sideload "${SEMVER}" > /dev/null
fi
