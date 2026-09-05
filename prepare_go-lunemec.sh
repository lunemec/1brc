#!/usr/bin/env bash
set -euo pipefail

mkdir -p build
go build -trimpath -ldflags='-s -w' -o build/go-lunemec .
