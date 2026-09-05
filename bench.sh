#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

usage() {
    cat <<'EOF'
Usage:
  ./bench.sh validate <implementation>...
  ./bench.sh stress <implementation>...
  ./bench.sh verify <dataset.txt> <implementation>...
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
        return 1
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
    local runner=("./calculate_average_${implementation}.sh")
    if command -v timeout >/dev/null; then
        runner=(timeout -s KILL 300 "${runner[@]}")
    elif command -v gtimeout >/dev/null; then
        runner=(gtimeout -s KILL 300 "${runner[@]}")
    fi

    if "${runner[@]}" > "$actual"; then
        :
    else
        local exit_code=$?
        echo "command failed: $implementation on $input (exit $exit_code)" >&2
        return 1
    fi
    if ! cmp -s "$expected" "$actual"; then
        cmp "$expected" "$actual" || true
        echo "incorrect output: $implementation on $input" >&2
        return 1
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

repeat_sample() {
    local source=$1
    local destination=$2
    local copies=$3
    local prefix=${4:-}

    awk -v copies="$copies" -v prefix="$prefix" '
        { lines[NR] = prefix $0 }
        END {
            for (copy = 0; copy < copies; copy++)
                for (line = 1; line <= NR; line++)
                    print lines[line]
        }
    ' "$source" > "$destination.tmp"
    mv "$destination.tmp" "$destination"
    awk -v prefix="$prefix" '{
        sub(/^\{/, "{" prefix)
        gsub(/, /, ", " prefix)
        print
    }' "${source%.txt}.out" > "${destination%.txt}.out"

    [[ $(wc -l < "$destination") -eq $(($(wc -l < "$source") * copies)) ]] || {
        echo "incorrect generated row count: $destination" >&2
        return 1
    }
}

generate_stress_inputs() {
    local prefix=ABCDEFGHIJKLM
    local source=test/resources/samples/measurements-10000-unique-keys.txt
    local destination=build/stress/measurements-10000-long-utf8.txt

    mkdir -p build/stress

    for ((i = 0; i < 40; i++)); do
        prefix="é$prefix"
    done
    repeat_sample "$source" "$destination" 40 "$prefix"
    LC_ALL=C awk -F';' '
        !seen[$1]++ { unique++; if (length($1) > longest) longest = length($1) }
        END { exit !(unique == 10000 && longest == 100) }
    ' "$destination" || {
        echo "generated corpus must contain 10,000 stations up to 100 bytes" >&2
        return 1
    }

    repeat_sample \
        test/resources/samples/measurements-complex-utf8.txt \
        build/stress/measurements-long-utf8.txt \
        15000
    repeat_sample \
        test/resources/samples/measurements-boundaries.txt \
        build/stress/measurements-sum-overflow.txt \
        2150000
}

verify_dataset() {
    local dataset=$1
    shift
    local expected="${dataset%.txt}.out"
    local failed=0

    [[ -f "$dataset" ]] || {
        echo "dataset not found: $dataset" >&2
        return 1
    }
    [[ -f "$expected" ]] || {
        echo "trusted output not found: $expected" >&2
        return 1
    }

    for implementation in "$@"; do
        if ! prepare "$implementation"; then
            echo "prepare failed: $implementation" >&2
            failed=1
            continue
        fi
        echo "validating $implementation: $dataset"
        if ! check_output "$implementation" "$dataset" "$expected"; then
            failed=1
        fi
    done
    return "$failed"
}

case "$action" in
    validate)
        for implementation in "$@"; do
            prepare "$implementation"
            validate "$implementation"
        done
        ;;
    stress)
        generate_stress_inputs
        failed=0
        datasets=(build/stress/*.txt)
        for implementation in "$@"; do
            if ! prepare "$implementation"; then
                echo "prepare failed: $implementation" >&2
                failed=1
                continue
            fi
            for dataset in "${datasets[@]}"; do
                echo "validating $implementation: $dataset"
                if ! check_output "$implementation" "$dataset" "${dataset%.txt}.out"; then
                    failed=1
                fi
            done
        done
        if [[ "$failed" -ne 0 ]]; then
            echo "stress validation failed" >&2
            exit 1
        fi
        echo "all stress validations passed"
        ;;
    verify)
        [[ $# -ge 2 ]] || usage
        dataset=$1
        shift
        verify_dataset "$dataset" "$@"
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
