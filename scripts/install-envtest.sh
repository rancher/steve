#!/bin/sh
set -e

# The envtest version and SHAs can be found here: https://raw.githubusercontent.com/kubernetes-sigs/controller-tools/HEAD/envtest-releases.yaml

ENVTEST_VERSION=v1.33.0
ENVTEST_SUM_linux_amd64=2cb7f5468ed7cea1492f971b715bcc27069e824cf7d5927b7f127f1e8c75cf086eea050543cdb5f79faee0a2bf775f160adf27443aa7ee845d962d04e9d43ac9
ENVTEST_SUM_linux_arm64=366ba32b2154c16e0ce952ed69731feefed187c88030f76a14bda5921a498d0aa25528629fc41c225cf78c91fd4a424a472e38efec91e8fcbd254fac0e150a54
ENVTEST_SUM_darwin_amd64=71a387a4cac32b17e22046df594090b6503fc074be8caae78bc80ef83949e292a1290eefe725acd41ce4bec730f7442006f2a9eb19d8e2d4d9df2feb67da04ba
ENVTEST_SUM_darwin_arm64=623dc02432905e58738c5611b8b0808437762755003dbaee266ded27dfe9a37d5a75ed6578ac4fbb261c7cd19c0199a7428de77b40b2f881912a4468c4d98d61

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
