#!/usr/bin/env bash
set -euo pipefail

mkdir -p build
GOCACHE="$PWD/build/go-cache" \
    go build -buildvcs=false -trimpath -ldflags='-s -w' -o build/go-lunemec .
