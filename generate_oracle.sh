#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

[[ $# -ge 1 && $# -le 2 ]] || {
    echo "Usage: ./generate_oracle.sh <dataset.txt> [output.out]" >&2
    exit 1
}

dataset=$1
output=${2:-${dataset%.txt}.out}

[[ -f "$dataset" ]] || {
    echo "dataset not found: $dataset" >&2
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

classes=build/java-baseline/classes
mkdir -p "$classes"
javac --release 21 -d "$classes" third_party/java-generator/CalculateAverage_baseline.java

cleanup() {
    rm -f measurements.txt "$output.tmp"
}
trap cleanup EXIT

ln -s "$dataset" measurements.txt
java -cp "$classes" dev.morling.onebrc.CalculateAverage_baseline > "$output.tmp"
mv "$output.tmp" "$output"
rm measurements.txt
trap - EXIT

echo "generated oracle: $output"
