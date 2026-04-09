#!/bin/sh
set -e

# The envtest version and SHAs can be found here: https://raw.githubusercontent.com/kubernetes-sigs/controller-tools/HEAD/envtest-releases.yaml

ENVTEST_VERSION=v1.31.0
ENVTEST_SUM_linux_amd64=5d96ae284610863ce5974e030aecd2eaad693f3210103ca778107aa0ea00f6f1d0a7b1b34aa74d7257cb0d7f713c2da365beba89b1d60823ce56c7b84b935423
ENVTEST_SUM_linux_arm64=72f5c8fd615c9db62eeb66e30edfda0f3879bffa3577c5776ec83363c018d7f51c174ac5ea807414072a21f8151a7bdf9826f414543f690686550e49db202ca0
ENVTEST_SUM_darwin_amd64=d681838609a1b0856e731888e0db0a1191003e1021801b5969b7d7084130076b30b2d99e53e460f1c5202b3308354a2118bd4a330d06d97797ef009dd56e256e
ENVTEST_SUM_darwin_arm64=e42c27e1ee90d13d56189e665d4c79b7a34f637581fc7e20b028a3c16b22b85060760eb91ca79901bc1c22dcd0d66ef41a0f760c2f1ae65265f0576e4109d87d

CLIENT_GO_MINOR=$(go list -m all | grep 'k8s.io/client-go' | head -n1 | cut -d ' ' -f 2 | cut -d '.' -f 2)
ENVTEST_MINOR=$(echo "$ENVTEST_VERSION" | cut -d '.' -f 2)

if [ "$CLIENT_GO_MINOR" != "$ENVTEST_MINOR" ]; then
    echo "k8s.io/client-go minor version ($CLIENT_GO_MINOR) does not match envtest minor version ($ENVTEST_MINOR)" >&2
    exit 1
fi

if ! command -v setup-envtest >/dev/null; then
    echo "Installing setup-envtest..."
    go install sigs.k8s.io/controller-runtime/tools/setup-envtest@f9589b9f2b9d
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

if ! setup-envtest list -i | grep -q "v${SEMVER}"; then
    curl -sL -o "$DEST" "$URL"

    echo "${ENVTEST_SUM}  ${DEST}" | sha512sum --check > /dev/null

    cat "$DEST" | setup-envtest sideload "${SEMVER}" > /dev/null
fi
