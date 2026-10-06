# 1BRC cross-language benchmark

This repository compares selected implementations of the One Billion Row
Challenge on the same machine and input file, then provides a tight loop for
tuning the Go implementation in `main.go`.

The benchmark protocol is adapted from
[gunnarmorling/1brc](https://github.com/gunnarmorling/1brc): every
implementation has an optional build script and a required run script.

## Requirements

- Bash
- Python 3.10 or newer for full-run host quietness monitoring
- `jq`
- Go 1.23 or newer
- [hyperfine](https://github.com/sharkdp/hyperfine)
- OpenJDK 21 for the Java JVM reference
- A C11 compiler with AArch64 CRC32C or x86-64 SSE4.2 support for the C reference
- Rust and Cargo
- GraalVM 21.0.2 `native-image` only for the optional Java native reference

## Adapter convention

For an implementation named `<id>`:

- `prepare_<id>.sh` optionally builds it.
- `calculate_average_<id>.sh` runs it against `measurements.txt` and writes
  only the canonical result to stdout.

The wrapper owns the temporary `measurements.txt` symlink, so third-party
implementations can retain the filename expected by the original challenge.
All runners execute in a disposable workspace; validation never removes or
replaces a repository-root `measurements.txt`.

Available implementations:

- `go-lunemec`
- `java-baseline` (correctness and oracle generation only)
- `java-thomaswue-jvm`
- `java-thomaswue-native` (requires GraalVM Native Image)
- `c-matt-re`
- `rust-mtopolnik`

Reference source revisions and local correctness changes are documented in
[`third_party/README.md`](third_party/README.md).

## Validate correctness

```sh
./bench.sh validate \
    java-baseline \
    go-lunemec \
    java-thomaswue-jvm \
    c-matt-re \
    rust-mtopolnik
```

Every selected implementation first runs the pinned upstream `test.sh` and
`tocsv.sh`, then must match every expected sample output byte-for-byte. The
strict pass includes local rounding and supplementary-Unicode ordering cases
that are not in the original suite.

`./test_harness.sh` checks that passing and deliberately broken adapters, plus
oracle generation, leave a repository-root `measurements.txt` untouched.

Run the generated stress suite separately. It covers 10,000 mostly 96–100 byte
UTF-8 station names, long files with diverse UTF-8 names, and aggregates large
enough to overflow a 32-bit sum:

```sh
./bench.sh stress \
    go-lunemec \
    java-thomaswue-jvm \
    java-thomaswue-native \
    c-matt-re \
    rust-mtopolnik
```

This is a correctness gate only; it does not invoke Hyperfine.

The inputs are generated under `build/stress/`; the slow Java baseline creates
their trusted outputs. To validate any other corpus without timing it, place
its oracle beside it with the same base name and run:

```sh
./bench.sh verify measurements_10K_1B.txt go-lunemec rust-mtopolnik
```

The original README has one distinct bonus corpus: the 10K Key Set, containing
one billion rows across 10,000 station names. Generate it and its independent
baseline output once:

```sh
./generate_measurements.sh 10k
./generate_oracle.sh measurements_10K_1B.txt
```

Then validate both original one-billion-row corpora without running Hyperfine:

```sh
./bench.sh validate-full \
    go-lunemec \
    java-thomaswue-jvm \
    java-thomaswue-native \
    c-matt-re \
    rust-mtopolnik
```

The README's 32-core bonus is a hardware configuration, not another corpus, so
it has the same expected output as the canonical input. `CreateMeasurements2`
and `CreateMeasurementsFast` are alternative ways to create the canonical
corpus rather than separate validation cases.

The pinned upstream generators are intentionally unseeded. A newly generated
corpus will have different bytes and must receive its own oracle and checksum
manifest; the committed manifests identify the retained local corpora.

## Establish or refresh a full baseline

Generate the original one-billion-row corpus as `measurements_1B.txt`, then
create its trusted output once as `measurements_1B.out` using an independent
reference implementation. Oracle generation also writes a checksum manifest
for the dataset and output. Existing files are never replaced implicitly; use
`--replace` when intentionally refreshing both files.

```sh
./generate_measurements.sh
./generate_oracle.sh measurements_1B.txt

# Only when deliberately refreshing an existing oracle:
./generate_oracle.sh --replace measurements_1B.txt

./bench.sh null-control measurements_1B.txt go-lunemec

./bench.sh compare measurements_1B.txt \
    go-lunemec \
    java-thomaswue-jvm \
    c-matt-re \
    rust-mtopolnik
```

Run the null control once after the machine is quiet and before comparing real
implementations. It preconditions the selected implementation for 60 seconds,
then benchmarks the same unchanged artifact under two labels in isolated
`A-B-B-A` blocks without cooldowns. Each block defaults to two warmups and five
measured runs. The wrapper verifies exact output, checks that the artifact did
not change, records a system and thermal snapshot before every block, and exits
with status 2 when label or order drift exceeds `DRIFT_THRESHOLD_PERCENT`
(default 1%). This is the maximum absolute difference in mean runtime across
pooled labels, the first and second halves, and each label's two blocks. It
measures repeatability, independently of CPU load. Keep 1% for small-change
optimization; 2% is a reasonable declared tolerance for broad comparisons.
A larger tolerance changes the gate, not the observed noise or precision.
Override the duration, sample count, or tolerance deliberately:

```sh
PRECONDITION_SECONDS=90 RUNS=5 WARMUPS=2 \
    ./bench.sh null-control measurements_1B.txt go-lunemec

DRIFT_THRESHOLD_PERCENT=2 \
    ./bench.sh null-control measurements_10K_1B.txt go-lunemec
```

Null-control JSON, logs, block statistics, drift analysis, health snapshots,
and metadata are written under `results/`.

Full timing runs (inputs of at least 1 GiB) now require a host quietness check.
`benchmark_quiet.py` samples ten seconds before launching the harness and monitors
background work every two seconds throughout the window, without adding pauses
between timing blocks. It excludes benchmark descendants, records temperatures,
and stops the owned benchmark if unrelated CPU work, swapping, I/O or memory
contention exceeds its limits. Defaults permit at most 0.5 busy CPU cores in total
and 0.25 cores per background process; one core means one logical CPU fully busy.
Paging above 64 KiB/s rejects a window. With Linux RAM-only zram swap, page-ins
are recorded and assessed through CPU/I/O/memory pressure; swap-outs still reject
above that rate. Other swap devices retain the combined paging limit.
Linux also checks kernel CPU, pressure and available memory. macOS checks process
CPU, VM counters and reported thermal limits. An incomplete Linux process view
fails closed, so run full comparisons on the host. Every attempted window keeps
its `.quiet.json` report under `results/`. A passed quietness check still requires
the declared null and order-drift checks before accepting a performance result.
Preparatory cache reads are recorded but exempt from the I/O and paging limits;
those limits apply once the harness marks the cache ready for timing. Background
CPU remains checked throughout preparation and timing; preparation-only activity
is retained as a flag, including each block's excluded warm-ups. Violations during
measured runs stop the run. The initial idle
preflight still requires all quietness limits to pass.
Experiment runners can declare `--timed-process-names` to apply the I/O gate
only to intervals where the same owned benchmark process remained active.
This excludes report/file bookkeeping between runs from I/O rejection; every
interval is still recorded. CPU, memory and paging gates retain their timing-phase
coverage. Other runners keep the whole-phase I/O check.
Linux also accounts for exited benchmark children when checking residual host
CPU, adopts orphaned benchmark descendants and reaps them before returning.
macOS relies on observed processes and second-resolution birth identities;
short activity between samples remains a limitation of that process view.

Before timing, the wrapper verifies the dataset and oracle checksums, builds
each implementation, runs both validation suites, and compares its full output
with `measurements_1B.out`. Hyperfine then performs ten timed runs by default.
Each timed output is compared with the oracle before the next run.

The default balanced order splits those ten runs between a forward adapter pass
and a reverse adapter pass. `WARMUPS` applies to each pass. Use `ORDER=forward`
or `ORDER=reverse` only when deliberately collecting a single-order result.

Override the run and Hyperfine warm-up counts when needed:

```sh
RUNS=20 WARMUPS=2 ./bench.sh compare measurements_1B.txt go-lunemec
```

Each comparison writes raw Hyperfine JSON, a tab-separated statistical summary,
and metadata under `results/`. Set `RESULTS_DIR` to place final artifacts outside
the repository. The summary includes the upstream-style mean
after dropping the fastest and slowest observations, median, standard
deviation, coefficient of variation, and a warning above 3% variation. Metadata
records the corpus and oracle hashes, Git state, command order, system and power
state, toolchains, and adapter artifact hashes. Only wall-clock measurements are
reported because the Thomas JVM launcher creates a worker process.

Reference results are valid only for the recorded hardware, OS, toolchains,
source revisions, and protocol. Rerun them after any of those change and once
more before publishing a final cross-language comparison.

Health snapshots before each comparison pass and null-control block record
system activity. On Linux they include memory, CPU/I/O/memory pressure, CPU
governor, temperatures, and a one-second process sample. Run the harness outside
a PID-isolating sandbox when the process sample must include desktop and service
activity.

The Thomas Würthinger leaderboard entry is a GraalVM native image, not a normal
JVM run. Keep `java-thomaswue-jvm` and `java-thomaswue-native` as separate
results. Once `native-image` is installed, validate and include the latter like
any other adapter:

```sh
./bench.sh validate java-thomaswue-native
```

## Go tuning loop

Use the full executable benchmark as the deciding metric:

```sh
./bench.sh validate go-lunemec
./bench.sh compare measurements_1B.txt go-lunemec
```

Use Go microbenchmarks and profiles only to explain an observed result. Change
one thing at a time and retain the Hyperfine JSON for each meaningful version.
Accepted, rejected, and queued changes are tracked in
[`EXPERIMENTS.md`](EXPERIMENTS.md).

## Challenge contract

Input lines have the form `<station>;<temperature>`. The program emits stations
in alphabetical order as `{station=min/mean/max, ...}`, rounded to one decimal
place. The full challenge input contains 1,000,000,000 rows.
