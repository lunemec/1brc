#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

variant=standard
if [[ ${1:-} == 10k ]]; then
    variant=10k
    shift
fi
rows=${1:-1000000000}

if [[ "$variant" == 10k ]]; then
    source=third_party/java-generator/CreateMeasurements3.java
    generated=measurements3.txt
    output=${2:-measurements_10K_1B.txt}
else
    source=third_party/java-generator/CreateMeasurements.java
    generated=measurements.txt
    output=${2:-measurements_1B.txt}
fi

[[ "$rows" =~ ^[1-9][0-9]*$ ]] || {
    echo "row count must be a positive integer" >&2
    exit 1
}
[[ "$output" != "$generated" ]] || {
    echo "output must not be $generated" >&2
    exit 1
}
[[ ! -e "$output" && ! -L "$output" ]] || {
    echo "output already exists: $output" >&2
    exit 1
}
[[ ! -e "$generated" && ! -L "$generated" ]] || {
    echo "$generated already exists; move it aside first" >&2
    exit 1
}

classes=build/java-generator/classes
mkdir -p "$classes"
javac --release 21 -d "$classes" "$source"

cleanup() {
    rm -f "$generated"
}
trap cleanup EXIT

if [[ "$variant" == 10k ]]; then
    java -cp "$classes" dev.morling.onebrc.CreateMeasurements3 "$rows"
else
    java -cp "$classes" dev.morling.onebrc.CreateMeasurements "$rows"
fi
mv "$generated" "$output"
trap - EXIT

echo "generated: $output"
