#!/usr/bin/env bash
set -euo pipefail

if ! command -v native-image >/dev/null; then
    sdkman_dir=${SDKMAN_DIR:-"$HOME/.sdkman"}
    graal_bin="$sdkman_dir/candidates/java/21.0.2-graal/bin"
    if [[ -x "$graal_bin/native-image" ]]; then
        export PATH="$graal_bin:$PATH"
    else
        echo "GraalVM 21.0.2 native-image is required for java-thomaswue-native" >&2
        exit 1
    fi
fi

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
