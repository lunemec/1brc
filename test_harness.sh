#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "$0")" && pwd -P)
cd "$repo_dir"

sentinel=measurements.txt
broken=calculate_average_harness-broken.sh
[[ ! -e "$sentinel" && ! -L "$sentinel" ]] || {
    echo "$sentinel already exists; refusing to overwrite it" >&2
    exit 1
}
[[ ! -e "$broken" && ! -L "$broken" ]] || {
    echo "$broken already exists; refusing to overwrite it" >&2
    exit 1
}

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/1brc-harness-test.XXXXXX")
printf '%s\n' harness-isolation-sentinel > "$tmp_dir/sentinel"
printf '%s\n' '#!/usr/bin/env bash' 'printf "{}\n"' > "$tmp_dir/broken"
cp "$tmp_dir/sentinel" "$sentinel"
cp "$tmp_dir/broken" "$broken"
chmod +x "$broken"

cleanup() {
    if [[ ! -e "$sentinel" && ! -L "$sentinel" ]] || cmp -s "$tmp_dir/sentinel" "$sentinel"; then
        rm -f "$sentinel"
    else
        echo "$sentinel changed during the test; leaving it in place" >&2
    fi
    if [[ ! -e "$broken" && ! -L "$broken" ]] || cmp -s "$tmp_dir/broken" "$broken"; then
        rm -f "$broken"
    else
        echo "$broken changed during the test; leaving it in place" >&2
    fi
    rm -rf "$tmp_dir"
}
trap cleanup EXIT

if ./bench.sh validate harness-broken >/dev/null 2>&1; then
    echo "broken adapter unexpectedly passed validation" >&2
    exit 1
fi
cmp -s "$tmp_dir/sentinel" "$sentinel"

./bench.sh validate java-baseline >/dev/null
cmp -s "$tmp_dir/sentinel" "$sentinel"

cp test/resources/samples/measurements-20.txt "$tmp_dir/oracle.txt"
./generate_oracle.sh "$tmp_dir/oracle.txt" >/dev/null
shasum -a 256 -c "$tmp_dir/oracle.sha256" >/dev/null
cmp -s "$tmp_dir/sentinel" "$sentinel"

mkdir -p "$tmp_dir/bin" "$tmp_dir/results"
printf '%s\n' \
    '#!/usr/bin/env bash' \
    'printf "CPU usage: 0%% user, 0%% sys, 100%% idle\n"' \
    > "$tmp_dir/bin/top"
# These single-quoted lines are the source of the generated test double.
# shellcheck disable=SC2016
printf '%s\n' \
    '#!/usr/bin/env bash' \
    'set -euo pipefail' \
    'if [[ ${1:-} == --version ]]; then echo "hyperfine test double"; exit 0; fi' \
    'export_json=' \
    'command_name=' \
    'conclude=' \
    'benchmark=' \
    'while (($#)); do' \
    '    case "$1" in' \
    '        --warmup|--runs) shift 2 ;;' \
    '        --export-json) export_json=$2; shift 2 ;;' \
    '        --command-name) command_name=$2; shift 2 ;;' \
    '        --conclude) conclude=$2; shift 2 ;;' \
    '        *) benchmark=$1; shift ;;' \
    '    esac' \
    'done' \
    'bash -c "$benchmark"' \
    'bash -c "$conclude"' \
    'call_index=$(wc -l < "$HYPERFINE_CALLS")' \
    'IFS=, read -r -a times <<< "${HYPERFINE_TIMES:-1.0,1.0,1.0,1.0}"' \
    'time=${times[$call_index]:-1.0}' \
    'printf "%s\n" "$command_name" >> "$HYPERFINE_CALLS"' \
    'printf "{\"results\":[{\"command\":\"%s\",\"times\":[%s]}]}\n" "$command_name" "$time" > "$export_json"' \
    'echo "mock benchmark: $command_name"' \
    > "$tmp_dir/bin/hyperfine"
chmod +x "$tmp_dir/bin/top" "$tmp_dir/bin/hyperfine"

null_input="$tmp_dir/null.txt"
null_output="$tmp_dir/null.out"
calls="$tmp_dir/hyperfine.calls"
expected_calls="$tmp_dir/hyperfine.expected"
cp test/resources/samples/measurements-20.txt "$null_input"
cp test/resources/samples/measurements-20.out "$null_output"
shasum -a 256 "$null_input" "$null_output" > "$tmp_dir/null.sha256"
: > "$calls"

PATH="$tmp_dir/bin:$PATH" \
HYPERFINE_CALLS="$calls" \
HYPERFINE_TIMES=1.0,1.0,1.0,1.0 \
PRECONDITION_SECONDS=0 \
RUNS=1 \
WARMUPS=0 \
RESULTS_DIR="$tmp_dir/results" \
    ./bench.sh null-control "$null_input" java-baseline >/dev/null

printf '%s\n' null-a null-b null-b null-a > "$expected_calls"
cmp -s "$expected_calls" "$calls"
jq -e '
    [.blocks[].label] == ["null-a", "null-b", "null-b", "null-a"] and
    .gate.pass == true and .gate.threshold_percent == 1
' "$tmp_dir/results/"*.analysis.json >/dev/null
cmp -s "$tmp_dir/sentinel" "$sentinel"

mkdir "$tmp_dir/results-fail"
: > "$calls"
if PATH="$tmp_dir/bin:$PATH" \
    HYPERFINE_CALLS="$calls" \
    HYPERFINE_TIMES=1.0,1.0,1.2,1.2 \
    PRECONDITION_SECONDS=0 \
    RUNS=1 \
    WARMUPS=0 \
    RESULTS_DIR="$tmp_dir/results-fail" \
    ./bench.sh null-control "$null_input" java-baseline >/dev/null 2>&1; then
    echo "drifting null control unexpectedly passed" >&2
    exit 1
else
    exit_code=$?
    [[ "$exit_code" -eq 2 ]] || {
        echo "drifting null control exited $exit_code instead of 2" >&2
        exit 1
    }
fi
jq -e '.gate.pass == false' "$tmp_dir/results-fail/"*.analysis.json >/dev/null

mkdir "$tmp_dir/results-relaxed"
: > "$calls"
PATH="$tmp_dir/bin:$PATH" \
HYPERFINE_CALLS="$calls" \
HYPERFINE_TIMES=1.0,1.0,1.015,1.015 \
PRECONDITION_SECONDS=0 \
RUNS=1 \
WARMUPS=0 \
DRIFT_THRESHOLD_PERCENT=2.0 \
RESULTS_DIR="$tmp_dir/results-relaxed" \
    ./bench.sh null-control "$null_input" java-baseline >/dev/null
jq -e '
    .gate.threshold_percent == 2 and .gate.pass == true and
    .gate.max_abs_drift_percent > 1 and .gate.max_abs_drift_percent < 2
' "$tmp_dir/results-relaxed/"*.analysis.json >/dev/null

for invalid_threshold in 0 -1 invalid 1%; do
    if DRIFT_THRESHOLD_PERCENT="$invalid_threshold" \
        ./bench.sh null-control "$null_input" java-baseline > "$tmp_dir/invalid-threshold.log" 2>&1; then
        echo "invalid threshold unexpectedly accepted: $invalid_threshold" >&2
        exit 1
    else
        exit_code=$?
        [[ "$exit_code" -eq 1 ]] || {
            echo "invalid threshold exited $exit_code instead of 1" >&2
            exit 1
        }
    fi
    [[ $(<"$tmp_dir/invalid-threshold.log") == *'DRIFT_THRESHOLD_PERCENT must be a positive decimal number'* ]]
done

cleanup
trap - EXIT
echo "harness isolation passed"
