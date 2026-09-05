#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

rows=${1:-1000000000}
output=${2:-measurements_1B.txt}

[[ "$rows" =~ ^[1-9][0-9]*$ ]] || {
    echo "row count must be a positive integer" >&2
    exit 1
}
[[ "$output" != measurements.txt ]] || {
    echo "output must not be measurements.txt" >&2
    exit 1
}
[[ ! -e "$output" && ! -L "$output" ]] || {
    echo "output already exists: $output" >&2
    exit 1
}
[[ ! -e measurements.txt && ! -L measurements.txt ]] || {
    echo "measurements.txt already exists; move it aside first" >&2
    exit 1
}

classes=build/java-generator/classes
mkdir -p "$classes"
javac --release 21 -d "$classes" third_party/java-generator/CreateMeasurements.java

cleanup() {
    rm -f measurements.txt
}
trap cleanup EXIT

java -cp "$classes" dev.morling.onebrc.CreateMeasurements "$rows"
mv measurements.txt "$output"
trap - EXIT

echo "generated: $output"
