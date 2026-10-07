#!/usr/bin/env bash
set -euo pipefail

threads=$(getconf _NPROCESSORS_ONLN)
[[ "$threads" =~ ^[1-9][0-9]*$ ]] || {
    echo "could not determine the logical CPU count" >&2
    exit 1
}

mkdir -p build
cc -std=c11 -O3 -march=native -pthread \
    -DMAX_THREAD="$threads" \
    -o build/c-matt-re \
    third_party/c-matt-re/main.c
