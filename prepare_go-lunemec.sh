#!/usr/bin/env bash
set -euo pipefail

mkdir -p build
go_experiments="$(go env GOEXPERIMENT)"
case ",$go_experiments," in
    *,simd,*|*,nosimd,*) ;;
    *)
        simd_experiments="${go_experiments:+$go_experiments,}simd"
        if GOEXPERIMENT="$simd_experiments" go env GOVERSION >/dev/null 2>&1; then
            go_experiments="$simd_experiments"
        fi
        ;;
esac

GOEXPERIMENT="$go_experiments" GOCACHE="${GOCACHE:-$PWD/build/go-cache}" \
    go build -buildvcs=false -trimpath -ldflags='-s -w' -o build/go-lunemec .
