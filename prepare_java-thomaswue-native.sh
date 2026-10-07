#!/usr/bin/env bash
set -euo pipefail

native=build/java-thomaswue/native
source=third_party/java-thomaswue/CalculateAverage_thomaswue.java
fingerprint_file="$native.sha256"

sdkman_dir=${SDKMAN_DIR:-"$HOME/.sdkman"}
native_image="$sdkman_dir/candidates/java/21.0.2-graal/bin/native-image"
if [[ ! -x "$native_image" ]]; then
    native_image=$(command -v native-image || true)
fi
[[ -x "$native_image" ]] || {
    echo "Oracle GraalVM 21.0.2+13.1 native-image is required for java-thomaswue-native" >&2
    exit 1
}

native_image_version=$("$native_image" --version)
grep -Fq 'Oracle GraalVM 21.0.2+13.1' <<< "$native_image_version" || {
    echo "unsupported native-image; expected Oracle GraalVM 21.0.2+13.1" >&2
    echo "$native_image_version" >&2
    exit 1
}

fingerprint=$(
    {
        for input in "$source" "$0" prepare_java-thomaswue-jvm.sh; do
            shasum -a 256 "$input" | awk '{print $1}'
        done
        printf '%s\n' "$native_image_version"
    } | shasum -a 256 | awk '{print $1}'
)
if [[ -x "$native" && -f "$fingerprint_file" && $(<"$fingerprint_file") == "$fingerprint" ]]; then
    exit 0
fi

./prepare_java-thomaswue-jvm.sh

native_tmp="$native.tmp.$$"
fingerprint_tmp="$fingerprint_file.tmp.$$"
native_image_tmp="$PWD/build/java-thomaswue/tmp"
mkdir -p "$native_image_tmp"
cleanup() {
    rm -f "$native_tmp" "$fingerprint_tmp"
}
trap cleanup EXIT

TMPDIR="$native_image_tmp" "$native_image" \
    "-J-Djava.io.tmpdir=$native_image_tmp" \
    -O3 \
    -H:TuneInlinerExploration=1 \
    -march=native \
    --enable-preview \
    "--initialize-at-build-time=dev.morling.onebrc.CalculateAverage_thomaswue\$Scanner" \
    --gc=epsilon \
    -H:-GenLoopSafepoints \
    -cp build/java-thomaswue/classes \
    -o "$native_tmp" \
    dev.morling.onebrc.CalculateAverage_thomaswue

mv "$native_tmp" "$native"
printf '%s\n' "$fingerprint" > "$fingerprint_tmp"
mv "$fingerprint_tmp" "$fingerprint_file"
trap - EXIT
