#!/bin/sh
set -e

# The envtest version and SHAs can be found here: https://raw.githubusercontent.com/kubernetes-sigs/controller-tools/HEAD/envtest-releases.yaml

ENVTEST_VERSION=v1.36.0
ENVTEST_SUM_linux_amd64=302d6a4c3b7d79b8a6168cd9fd5f18e718048211d25012d969d320e06b39672d17338bcd008c29adf98b41048a6f7d80c90ab0165b4b861497ef74c7bc6c1531
ENVTEST_SUM_linux_arm64=16b416776b6f5e6a13be33b7e9f248cbffb71b40bc286436c93b83239b965d07b902ce0da75b721aea9356a764ca896154bc420618bc7fb9d1dc4a6b2fa49405
ENVTEST_SUM_darwin_amd64=2f73d7c2c1408c3334f978c2c7f8a64bc691277d1b482930ae516b425882464f8970b0e47010520cdd29f1e756adfab2721a53857a976a462c16edcef9604644
ENVTEST_SUM_darwin_arm64=4b3542f707ffaa4bc0d5a07d25290323ff0ad1efd90560571705daf958db772fd8e78b5d6ae878df1840d8d39c1250ebfaf8518bfd02b10757ae445ec0cafe9e

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
