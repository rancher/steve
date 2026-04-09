#!/bin/sh
set -e

# The envtest version and SHAs can be found here: https://raw.githubusercontent.com/kubernetes-sigs/controller-tools/HEAD/envtest-releases.yaml

ENVTEST_VERSION=v1.35.0
ENVTEST_SUM_linux_amd64=130369c16f076e724d089189afaede960316f5f5dea6cf57be7a4fc6f09c77342893192509790e4056e116e232dff832ed863f5bd55dcb55d38f3ab834828a11
ENVTEST_SUM_linux_arm64=e53e2b88398f5b9503e3f074d82a2dcb090c708b34940848607ce658138a5d4a25962e042ab683ccc026a8a6c90c0be7f658e42dde0887369d73c3b68e2fc86c
ENVTEST_SUM_darwin_amd64=fccc583ba6d322c88a8c56f7876090a7ad63460046a4bcae414093b23ce68a75f172ca7484b7c7475b707657eca5108a8ad3fd85e1b4f70a6c99ca2f22dbd6b2
ENVTEST_SUM_darwin_arm64=bb5d0bb3975956331b0aa0c039955b4c4dc6c5c288e5af369364c7d2fbeac11a025227dabb127569080b259dd29b697cc77fa77abd27ae26721ddb23e8ee0613

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

    echo "${ENVTEST_SUM}  ${DEST}" | sha512sum --check > /dev/null

    cat "$DEST" | go tool -modfile gotools/setup-envtest/go.mod setup-envtest sideload "${SEMVER}" > /dev/null
fi
