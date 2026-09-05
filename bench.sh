#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "$0")" && pwd -P)
cd "$repo_dir"

usage() {
    cat <<'EOF'
Usage:
  ./bench.sh validate <implementation>...
  ./bench.sh stress <implementation>...
  ./bench.sh verify <dataset.txt> <implementation>...
  ./bench.sh validate-full <implementation>...
  ./bench.sh compare <dataset.txt> <implementation>...
EOF
    exit 1
}

[[ $# -ge 2 ]] || usage

action=$1
shift

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/1brc-bench.XXXXXX")
run_dir="$tmp_dir/run"
mkdir -p "$run_dir/src/test/resources" build
ln -s "$repo_dir/build" "$run_dir/build"
ln -s "$repo_dir/test/resources/samples" "$run_dir/src/test/resources/samples"
ln -s "$repo_dir/third_party/gunnarmorling-test/test.sh" "$run_dir/test.sh"
ln -s "$repo_dir/third_party/gunnarmorling-test/tocsv.sh" "$run_dir/tocsv.sh"
trap 'rm -rf "$tmp_dir"' EXIT

absolute_path() {
    local path=$1
    local directory
    directory=$(cd "$(dirname "$path")" && pwd -P)
    printf '%s/%s\n' "$directory" "$(basename "$path")"
}

stage_adapter() {
    local implementation=$1
    local runner="$repo_dir/calculate_average_${implementation}.sh"

    [[ "$implementation" =~ ^[a-zA-Z0-9._-]+$ ]] || {
        echo "invalid implementation name: $implementation" >&2
        return 1
    }
    [[ -x "$runner" ]] || {
        echo "missing executable runner: $runner" >&2
        return 1
    }
    ln -sfn "$runner" "$run_dir/calculate_average_${implementation}.sh"
}

prepare() {
    local implementation=$1
    local builder="./prepare_${implementation}.sh"

    stage_adapter "$implementation"
    if [[ -x "$builder" ]]; then
        "$builder"
    fi
}

link_input() {
    local input
    input=$(absolute_path "$1")
    rm -f "$run_dir/measurements.txt"
    ln -s "$input" "$run_dir/measurements.txt"
}

run_implementation() {
    local implementation=$1
    local runner=("./calculate_average_${implementation}.sh")

    if command -v timeout >/dev/null; then
        runner=(timeout -s KILL 300 "${runner[@]}")
    elif command -v gtimeout >/dev/null; then
        runner=(gtimeout -s KILL 300 "${runner[@]}")
    fi
    (cd "$run_dir" && "${runner[@]}")
}

check_output() {
    local implementation=$1
    local input=$2
    local expected=$3
    local actual="$tmp_dir/${implementation}.out"
    local exit_code

    link_input "$input"
    if run_implementation "$implementation" > "$actual"; then
        :
    else
        exit_code=$?
        echo "command failed: $implementation on $input (exit $exit_code)" >&2
        return 1
    fi
    if ! cmp -s "$expected" "$actual"; then
        cmp "$expected" "$actual" || true
        echo "incorrect output: $implementation on $input" >&2
        return 1
    fi
}

validate_upstream() {
    local implementation=$1
    local test_command=(./test.sh "$implementation")

    echo "running pinned upstream tests: $implementation"
    if command -v timeout >/dev/null; then
        test_command=(timeout -s KILL 300 "${test_command[@]}")
    elif command -v gtimeout >/dev/null; then
        test_command=(gtimeout -s KILL 300 "${test_command[@]}")
    fi
    (cd "$run_dir" && "${test_command[@]}")
}

validate() {
    local implementation=$1
    local samples=(test/resources/samples/*.txt)

    [[ -e "${samples[0]}" ]] || {
        echo "no sample inputs found under test/resources/samples" >&2
        return 1
    }

    validate_upstream "$implementation"
    for sample in "${samples[@]}"; do
        echo "validating exact output: $implementation: $sample"
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

    [[ $(wc -l < "$destination") -eq $(($(wc -l < "$source") * copies)) ]] || {
        echo "incorrect generated row count: $destination" >&2
        return 1
    }
}

generate_expected() {
    local input=$1
    local expected=$2
    local generated

    generated="$tmp_dir/$(basename "$expected")"

    link_input "$input"
    run_implementation java-baseline > "$generated"
    mv "$generated" "$expected"
}

generate_stress_inputs() {
    local prefix=ABCDEFGHIJKLM
    local source=test/resources/samples/measurements-10000-unique-keys.txt
    local destination=build/stress/measurements-10000-long-utf8.txt
    local datasets

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

    prepare java-baseline
    datasets=(
        build/stress/measurements-10000-long-utf8.txt
        build/stress/measurements-long-utf8.txt
        build/stress/measurements-sum-overflow.txt
    )
    for dataset in "${datasets[@]}"; do
        echo "generating trusted output: $dataset"
        generate_expected "$dataset" "${dataset%.txt}.out"
    done
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

verify_checksum_pair() {
    local dataset=$1
    local expected="${dataset%.txt}.out"
    local checksum="${dataset%.txt}.sha256"
    local expected_absolute
    local found_dataset=0
    local found_expected=0
    local path

    [[ -f "$checksum" ]] || {
        echo "checksum file not found: $checksum" >&2
        return 1
    }
    while read -r _ path; do
        [[ "$path" == "$dataset" ]] && found_dataset=1
        [[ "$path" == "$expected" ]] && found_expected=1
    done < "$checksum"
    [[ "$found_dataset" -eq 1 && "$found_expected" -eq 1 ]] || {
        echo "$checksum must contain hashes for $dataset and $expected" >&2
        return 1
    }
    shasum -a 256 -c "$checksum"
}

checksum_hash_for() {
    local checksum=$1
    local wanted=$2
    local hash path

    while read -r hash path; do
        if [[ "$path" == "$wanted" ]]; then
            echo "$hash"
            return 0
        fi
    done < "$checksum"
    return 1
}

artifact_path() {
    case "$1" in
        go-lunemec) echo build/go-lunemec ;;
        java-baseline) echo build/java-baseline/classes ;;
        java-thomaswue-jvm) echo build/java-thomaswue/classes ;;
        java-thomaswue-native) echo build/java-thomaswue/native ;;
        c-matt-re) echo build/c-matt-re ;;
        rust-mtopolnik) echo build/rust-mtopolnik/release/rust-1brc ;;
        *) return 1 ;;
    esac
}

hash_artifact() {
    local path=$1
    if [[ -d "$path" ]]; then
        find "$path" -type f -exec shasum -a 256 {} \; | LC_ALL=C sort | shasum -a 256 | awk '{print $1}'
    else
        shasum -a 256 "$path" | awk '{print $1}'
    fi
}

join_by_comma() {
    local IFS=,
    echo "$*"
}

write_metadata() {
    local metadata=$1
    local dataset=$2
    local expected=$3
    local runs=$4
    local warmups=$5
    local order=$6
    shift 6
    local checksum="${dataset%.txt}.sha256"
    local implementations=("$@")
    local reverse_implementations=()
    local native_image_path
    local forward_runs=0
    local reverse_runs=0
    local i

    expected_absolute=$(absolute_path "$expected")

    for ((i = ${#implementations[@]} - 1; i >= 0; i--)); do
        reverse_implementations+=("${implementations[i]}")
    done
    case "$order" in
        forward) forward_runs=$runs ;;
        reverse) reverse_runs=$runs ;;
        balanced)
            forward_runs=$(((runs + 1) / 2))
            reverse_runs=$((runs / 2))
            ;;
    esac

    {
        echo "schema=1brc-local-benchmark-v1"
        echo "created_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "measurement=wall-clock"
        echo "dataset=$dataset"
        echo "dataset_sha256=$(checksum_hash_for "$checksum" "$dataset")"
        echo "oracle=$expected"
        echo "oracle_sha256=$(checksum_hash_for "$checksum" "$expected")"
        echo "runs_per_implementation=$runs"
        echo "warmups_per_pass=$warmups"
        echo "order=$order"
        echo "forward_runs=$forward_runs"
        echo "reverse_runs=$reverse_runs"
        echo "forward_order=$(join_by_comma "${implementations[@]}")"
        echo "reverse_order=$(join_by_comma "${reverse_implementations[@]}")"
        echo "timed_output=$tmp_dir/timed-output.out"
        echo "conclude=cmp -s $expected_absolute $tmp_dir/timed-output.out"
        echo "implementations=$(join_by_comma "${implementations[@]}")"
        echo "git_commit=$(git rev-parse HEAD)"
        if [[ -n $(git status --porcelain --untracked-files=all) ]]; then
            echo "git_dirty=true"
        else
            echo "git_dirty=false"
        fi
        echo "git_diff_sha256=$(git diff --binary HEAD | shasum -a 256 | awk '{print $1}')"
        echo "harness_sha256=$(shasum -a 256 bench.sh | awk '{print $1}')"
        echo
        echo "[git_status]"
        git status --short --untracked-files=all
        echo
        echo "[system]"
        uname -a
        command -v sw_vers >/dev/null && sw_vers
        if command -v system_profiler >/dev/null; then
            system_profiler SPHardwareDataType 2>/dev/null |
                awk '/Model Name:|Model Identifier:|Chip:|Total Number of Cores:|Memory:/' || true
        fi
        command -v sysctl >/dev/null && sysctl -n hw.model hw.ncpu hw.memsize 2>/dev/null || true
        command -v pmset >/dev/null && pmset -g batt || true
        uptime
        echo
        echo "[toolchains]"
        hyperfine --version
        go version 2>&1 || true
        java -version 2>&1 || true
        javac -version 2>&1 || true
        cc --version 2>&1 | head -n 1 || true
        rustc --version 2>&1 || true
        cargo --version 2>&1 || true
        native_image_path=$(command -v native-image || true)
        if [[ -z "$native_image_path" && -x "${SDKMAN_DIR:-$HOME/.sdkman}/candidates/java/21.0.2-graal/bin/native-image" ]]; then
            native_image_path="${SDKMAN_DIR:-$HOME/.sdkman}/candidates/java/21.0.2-graal/bin/native-image"
        fi
        [[ -z "$native_image_path" ]] || "$native_image_path" --version 2>&1
        echo
        echo "[adapters]"
        for implementation in "${implementations[@]}"; do
            local artifact
            artifact=$(artifact_path "$implementation")
            echo "$implementation command=./calculate_average_${implementation}.sh > $tmp_dir/timed-output.out"
            echo "$implementation artifact=$artifact sha256=$(hash_artifact "$artifact")"
            shasum -a 256 "calculate_average_${implementation}.sh"
            if [[ -f "prepare_${implementation}.sh" ]]; then
                shasum -a 256 "prepare_${implementation}.sh"
            fi
        done
    } > "$metadata"
}

benchmark_pass() {
    local pass=$1
    local pass_runs=$2
    local result=$3
    local expected=$4
    shift 4
    local output="$tmp_dir/timed-output.out"
    local expected_quoted output_quoted conclude
    local args=(
        --warmup "$warmups"
        --runs "$pass_runs"
        --export-json "$result"
    )

    printf -v expected_quoted '%q' "$expected"
    printf -v output_quoted '%q' "$output"
    conclude="cmp -s $expected_quoted $output_quoted"
    args+=(--conclude "$conclude")
    for implementation in "$@"; do
        args+=(--command-name "$implementation" "./calculate_average_${implementation}.sh > $output_quoted")
    done

    echo "benchmark pass $pass ($pass_runs runs): $(join_by_comma "$@")"
    (cd "$run_dir" && hyperfine "${args[@]}")
}

write_summary() {
    local summary=$1
    shift
    jq -rs '
        [ .[] | .results[] | .command as $command | .times[] |
          { command: $command, time: . } ]
        | sort_by(.command)
        | group_by(.command)
        | map(
            (map(.time) | sort) as $times
            | ($times | length) as $n
            | ($times | add / $n) as $mean
            | (if $n % 2 == 1 then
                   $times[($n / 2 | floor)]
               else
                   ($times[$n / 2 - 1] + $times[$n / 2]) / 2
               end) as $median
            | (if $n > 2 then ($times[1:-1] | add / ($n - 2)) else $mean end) as $trimmed
            | (if $n > 1 then
                   ([$times[] | . - $mean | . * .] | add / ($n - 1) | sqrt)
               else 0 end) as $stddev
            | {
                implementation: .[0].command,
                runs: $n,
                trimmed_mean_seconds: $trimmed,
                median_seconds: $median,
                mean_seconds: $mean,
                stddev_seconds: $stddev,
                cv_percent: (if $mean == 0 then 0 else $stddev / $mean * 100 end)
              }
          )
        | (["implementation", "runs", "trimmed_mean_seconds", "median_seconds", "mean_seconds", "stddev_seconds", "cv_percent", "quality"] | @tsv),
          (.[] | [
              .implementation,
              .runs,
              .trimmed_mean_seconds,
              .median_seconds,
              .mean_seconds,
              .stddev_seconds,
              .cv_percent,
              (if .cv_percent > 3 then "noisy" else "ok" end)
          ] | @tsv)
    ' "$@" > "$summary"
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
        datasets=(
            build/stress/measurements-10000-long-utf8.txt
            build/stress/measurements-long-utf8.txt
            build/stress/measurements-sum-overflow.txt
        )
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
    validate-full)
        failed=0
        for implementation in "$@"; do
            prepare "$implementation"
            validate "$implementation"
        done
        for dataset in measurements_1B.txt measurements_10K_1B.txt; do
            if ! verify_checksum_pair "$dataset"; then
                failed=1
                continue
            fi
            for implementation in "$@"; do
                echo "validating $implementation: $dataset"
                if ! check_output "$implementation" "$dataset" "${dataset%.txt}.out"; then
                    failed=1
                fi
            done
        done
        if [[ "$failed" -ne 0 ]]; then
            echo "full-corpus validation failed" >&2
            exit 1
        fi
        echo "all full-corpus validations passed"
        ;;
    compare)
        [[ $# -ge 2 ]] || usage
        dataset=$1
        shift
        expected="${dataset%.txt}.out"
        runs=${RUNS:-10}
        warmups=${WARMUPS:-0}
        order=${ORDER:-balanced}

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
        [[ "$warmups" =~ ^[0-9]+$ ]] || {
            echo "WARMUPS must be a non-negative integer" >&2
            exit 1
        }
        [[ "$order" == balanced || "$order" == forward || "$order" == reverse ]] || {
            echo "ORDER must be balanced, forward, or reverse" >&2
            exit 1
        }
        command -v hyperfine >/dev/null || {
            echo "hyperfine is required" >&2
            exit 1
        }
        command -v jq >/dev/null || {
            echo "jq is required" >&2
            exit 1
        }
        verify_checksum_pair "$dataset"

        implementations=("$@")
        reverse_implementations=()
        for ((i = ${#implementations[@]} - 1; i >= 0; i--)); do
            reverse_implementations+=("${implementations[i]}")
        done

        for implementation in "${implementations[@]}"; do
            prepare "$implementation"
            validate "$implementation"
            echo "validating $implementation: $dataset"
            check_output "$implementation" "$dataset" "$expected"
        done

        timestamp=$(date -u +%Y%m%d-%H%M%S)
        dataset_name=$(basename "${dataset%.txt}")
        prefix="$tmp_dir/${timestamp}-${dataset_name}"
        final_prefix="$repo_dir/results/${timestamp}-${dataset_name}"
        metadata="$prefix.meta.txt"
        summary="$prefix.summary.tsv"
        expected_absolute=$(absolute_path "$expected")
        result_files=()
        write_metadata "$metadata" "$dataset" "$expected" "$runs" "$warmups" "$order" "${implementations[@]}"
        link_input "$dataset"

        case "$order" in
            forward)
                result="$prefix-forward.json"
                benchmark_pass forward "$runs" "$result" "$expected_absolute" "${implementations[@]}"
                result_files+=("$result")
                ;;
            reverse)
                result="$prefix-reverse.json"
                benchmark_pass reverse "$runs" "$result" "$expected_absolute" "${reverse_implementations[@]}"
                result_files+=("$result")
                ;;
            balanced)
                forward_runs=$(((runs + 1) / 2))
                reverse_runs=$((runs / 2))
                result="$prefix-forward.json"
                benchmark_pass forward "$forward_runs" "$result" "$expected_absolute" "${implementations[@]}"
                result_files+=("$result")
                if [[ "$reverse_runs" -gt 0 ]]; then
                    result="$prefix-reverse.json"
                    benchmark_pass reverse "$reverse_runs" "$result" "$expected_absolute" "${reverse_implementations[@]}"
                    result_files+=("$result")
                fi
                ;;
        esac

        write_summary "$summary" "${result_files[@]}"
        mkdir -p "$repo_dir/results"
        final_result_files=()
        for result in "${result_files[@]}"; do
            final_result="$final_prefix${result#"$prefix"}"
            mv "$result" "$final_result"
            final_result_files+=("$final_result")
        done
        final_summary="$final_prefix.summary.tsv"
        final_metadata="$final_prefix.meta.txt"
        mv "$summary" "$final_summary"
        mv "$metadata" "$final_metadata"
        summary="$final_summary"
        metadata="$final_metadata"
        result_files=("${final_result_files[@]}")
        echo "summary: $summary"
        if command -v column >/dev/null; then
            column -t -s $'\t' "$summary"
        else
            cat "$summary"
        fi
        echo "metadata: $metadata"
        echo "raw results: $(join_by_comma "${result_files[@]}")"
        ;;
    *)
        usage
        ;;
esac
