#!/usr/bin/env bash
set -euo pipefail

command -v native-image >/dev/null || {
    echo "native-image is required for java-thomaswue-native" >&2
    exit 1
}

./prepare_java-thomaswue-jvm.sh

native-image \
    -O3 \
    -H:TuneInlinerExploration=1 \
    -march=native \
    --enable-preview \
    "--initialize-at-build-time=dev.morling.onebrc.CalculateAverage_thomaswue\$Scanner" \
    --gc=epsilon \
    -H:-GenLoopSafepoints \
    -cp build/java-thomaswue/classes \
    -o build/java-thomaswue/native \
    dev.morling.onebrc.CalculateAverage_thomaswue
