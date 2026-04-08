#!/bin/sh
set -e

# The envtest version and SHAs can be found here: https://raw.githubusercontent.com/kubernetes-sigs/controller-tools/HEAD/envtest-releases.yaml

ENVTEST_VERSION=v1.32.0
ENVTEST_SUM_linux_amd64=3a9584af30d041c42893d8f7a860aa434976d4aee479cf2e9a50a9e5677dcc83d3012a2146a6feb5b2e95a7b3c6f657ae9c591745981262da8b06e4b61dcdf17
ENVTEST_SUM_linux_arm64=ce6e44c5a3e99b595138cde396c1b179095db477be8ccd9f05cbd135bc15c391000d6e146907922872f0199b9c13a7e8ab6b5402f8cae3522514f0ef65914000
ENVTEST_SUM_darwin_amd64=a7824ff8ae9c5062bcbde243a7a3a1c76e02de0c92e2b332daf1954405aeded856023f277d74197862d0d5909e9e1dca451b5f2e84796e451ad6011ec98f8433
ENVTEST_SUM_darwin_arm64=728c66ef9c2503dd1eda1a398f57a04829ab7033e1007b5e23d6cc865e8ac5a753e4986847c8f5b76b3497ae4e3e468120e25ec5dfe3a3b1901d3ff20b97a7f9

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
