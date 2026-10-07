#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "$0")" && pwd -P)
cd "$repo_dir"

replace=0
if [[ ${1:-} == --replace ]]; then
    replace=1
    shift
fi

[[ $# -ge 1 && $# -le 2 ]] || {
    echo "Usage: ./generate_oracle.sh [--replace] <dataset.txt> [output.out]" >&2
    exit 1
}

dataset=$1
output=${2:-${dataset%.txt}.out}
checksum="${dataset%.txt}.sha256"

[[ -f "$dataset" ]] || {
    echo "dataset not found: $dataset" >&2
    exit 1
}
[[ "$dataset" != "$output" ]] || {
    echo "dataset and output must be different files" >&2
    exit 1
}
if [[ "$replace" -eq 0 ]]; then
    [[ ! -e "$output" && ! -L "$output" ]] || {
        echo "output already exists: $output (use --replace to refresh it)" >&2
        exit 1
    }
    [[ ! -e "$checksum" && ! -L "$checksum" ]] || {
        echo "checksum already exists: $checksum (use --replace to refresh it)" >&2
        exit 1
    }
fi

./prepare_java-baseline.sh

dataset_dir=$(cd "$(dirname "$dataset")" && pwd -P)
dataset_absolute="$dataset_dir/$(basename "$dataset")"
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/1brc-oracle.XXXXXX")
output_tmp="$output.tmp.$$"
checksum_tmp="$checksum.tmp.$$"
cleanup() {
    rm -rf "$tmp_dir"
    rm -f "$output_tmp" "$checksum_tmp"
}
trap cleanup EXIT

ln -s "$repo_dir/build" "$tmp_dir/build"
ln -s "$repo_dir/calculate_average_java-baseline.sh" "$tmp_dir/calculate_average_java-baseline.sh"
ln -s "$dataset_absolute" "$tmp_dir/measurements.txt"

(cd "$tmp_dir" && ./calculate_average_java-baseline.sh) > "$output_tmp"
dataset_hash=$(shasum -a 256 "$dataset" | awk '{print $1}')
output_hash=$(shasum -a 256 "$output_tmp" | awk '{print $1}')
printf '%s  %s\n%s  %s\n' \
    "$dataset_hash" "$dataset" \
    "$output_hash" "$output" > "$checksum_tmp"

mv "$output_tmp" "$output"
mv "$checksum_tmp" "$checksum"
trap - EXIT
rm -rf "$tmp_dir"

echo "generated oracle: $output"
echo "generated checksums: $checksum"
