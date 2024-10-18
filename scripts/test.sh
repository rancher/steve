#!/bin/bash

if ! command -v setup-envtest; then
	echo "setup-envtest is required for tests, but was not installed"
	echo "see the 'Running Tests' section of the readme for install instructions"
	exit 127
fi

minor=$(go mod graph | grep ' k8s.io/client-go@' | head -n1 | cut -d@ -f2 | cut -d '.' -f 2)
version="1.$minor.x"

export KUBEBUILDER_ASSETS=$(setup-envtest use -p path "$version")
go test ./...
