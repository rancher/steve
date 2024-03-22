#!/bin/bash

set -e
go generate ./...
golangci-lint run
go mod tidy
go mod verify
unclean=$(git status --porcelain --untracked-files=no)
if [ -n "$unclean" ]; then
  echo "Encountered dirty repo!"
  echo "$unclean"
  exit 1
fi
