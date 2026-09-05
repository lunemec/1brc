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

cleanup
trap - EXIT
echo "harness isolation passed"
