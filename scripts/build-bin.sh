#!/bin/bash

CGO_ENABLED=0 go build -ldflags "-extldflags -static -s" -o ./bin/steve