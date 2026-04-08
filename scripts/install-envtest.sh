#!/bin/sh
set -e

# The envtest version and SHAs can be found here: https://raw.githubusercontent.com/kubernetes-sigs/controller-tools/HEAD/envtest-releases.yaml

ENVTEST_VERSION=v1.34.1
ENVTEST_SUM_linux_amd64=c5e7c237ae18a8c65d0df1214b864ecd19aa9ff4f1383dbd477c202546d778c0f74efe15750ec38f57d8de26ba17cd62f404446cdd3c975399a5d4589de35cdd
ENVTEST_SUM_linux_arm64=bf13d2456183e66d084e28e1c9eed6a408fb77a06d8d1fd0461a7d9bc0c90b63d29ea67870b28f80d1f992acb026007d6f43987753c859c81d6a78bc1ca2c903
ENVTEST_SUM_darwin_amd64=d9e0464dc1708c36ab41147b5e364c8b8e201eafc14c299dcbe58ebeaa82c651b8e834647167a951ded8c6d08c616aadbae1673a54743f9c8fa15a278c78c0c0
ENVTEST_SUM_darwin_arm64=43d7c10b74100d20044df7c73be03e26f8243a9cb7c1d3035e581ee7bb0c7ffcaf759b9e182186b5bb2f47d1608835414ca443181a02d9ecb6c9cf04b43e3809

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
