#!/bin/bash


shopt -s extglob

FULLTAG=$(git describe --tags --long)
VERSION=$(echo $FULLTAG | cut -d- -f1)
NUM=$(echo $FULLTAG | cut -d- -f2)
COMMIT=$(echo $FULLTAG | cut -d- -f3)
if [[ -n "${NUM:-}" ]] ; then
    VERSION=$VERSION-$NUM
fi
if [[ -z "${COMMIT:-}" ]] then
   COMMIT=$(git rev-parse --short HEAD)
fi

CGO_ENABLED=0 go build -ldflags "-extldflags -static -s -X github.com/rancher/steve/pkg/version.Version=$VERSION  -X github.com/rancher/steve/pkg/version.GitCommit=$COMMIT" -o ./bin/steve
