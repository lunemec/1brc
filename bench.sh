#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

usage() {
    cat <<'EOF'
Usage:
  ./bench.sh validate <implementation>...
  ./bench.sh compare <dataset.txt> <implementation>...
EOF
    exit 1
}

[[ $# -ge 2 ]] || usage

action=$1
shift

if [[ -e measurements.txt || -L measurements.txt ]]; then
    echo "measurements.txt already exists; move it aside before running the harness" >&2
    exit 1
fi

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/1brc-bench.XXXXXX")
cleanup() {
    rm -f measurements.txt
    rm -rf "$tmp_dir"
}
trap cleanup EXIT

prepare() {
    local implementation=$1
    local runner="./calculate_average_${implementation}.sh"
    local builder="./prepare_${implementation}.sh"

    [[ -x "$runner" ]] || {
        echo "missing executable runner: $runner" >&2
        exit 1
    }

    if [[ -x "$builder" ]]; then
        "$builder"
    fi
}

link_input() {
    rm -f measurements.txt
    ln -s "$1" measurements.txt
}

check_output() {
    local implementation=$1
    local input=$2
    local expected=$3
    local actual="$tmp_dir/${implementation}.out"

    link_input "$input"
    "./calculate_average_${implementation}.sh" > "$actual"
    if ! diff -u "$expected" "$actual"; then
        echo "incorrect output: $implementation on $input" >&2
        exit 1
    fi
}

validate() {
    local implementation=$1
    local samples=(test/resources/samples/*.txt)

    [[ -e "${samples[0]}" ]] || {
        echo "no sample inputs found under test/resources/samples" >&2
        exit 1
    }

    for sample in "${samples[@]}"; do
        echo "validating $implementation: $sample"
        check_output "$implementation" "$sample" "${sample%.txt}.out"
    done
}

case "$action" in
    validate)
        for implementation in "$@"; do
            prepare "$implementation"
            validate "$implementation"
        done
        ;;
    compare)
        [[ $# -ge 2 ]] || usage
        dataset=$1
        shift
        expected="${dataset%.txt}.out"
        runs=${RUNS:-10}

        [[ -f "$dataset" ]] || {
            echo "dataset not found: $dataset" >&2
            exit 1
        }
        [[ -f "$expected" ]] || {
            echo "trusted output not found: $expected" >&2
            exit 1
        }
        [[ "$runs" =~ ^[1-9][0-9]*$ ]] || {
            echo "RUNS must be a positive integer" >&2
            exit 1
        }
        command -v hyperfine >/dev/null || {
            echo "hyperfine is required" >&2
            exit 1
        }

        for implementation in "$@"; do
            prepare "$implementation"
            validate "$implementation"
            echo "validating $implementation: $dataset"
            check_output "$implementation" "$dataset" "$expected"
        done

        mkdir -p results
        result="results/$(date +%Y%m%d-%H%M%S).json"
        hyperfine_args=(--warmup 0 --runs "$runs" --export-json "$result")
        for implementation in "$@"; do
            hyperfine_args+=(--command-name "$implementation" "./calculate_average_${implementation}.sh > /dev/null")
        done

        link_input "$dataset"
        hyperfine "${hyperfine_args[@]}"
        echo "raw results: $result"
        ;;
    *)
        usage
        ;;
esac
