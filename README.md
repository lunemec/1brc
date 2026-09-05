# 1BRC cross-language benchmark

This repository compares selected implementations of the One Billion Row
Challenge on the same machine and input file, then provides a tight loop for
tuning the Go implementation in `main.go`.

The benchmark protocol is adapted from
[gunnarmorling/1brc](https://github.com/gunnarmorling/1brc): every
implementation has an optional build script and a required run script.

## Requirements

- Bash
- Go 1.23 or newer
- [hyperfine](https://github.com/sharkdp/hyperfine)
- OpenJDK 21 for the Java JVM reference
- An Apple Silicon C11 compiler for the pinned AArch64 C reference
- Rust and Cargo
- GraalVM 21.0.2 `native-image` only for the optional Java native reference

## Adapter convention

For an implementation named `<id>`:

- `prepare_<id>.sh` optionally builds it.
- `calculate_average_<id>.sh` runs it against `measurements.txt` and writes
  only the canonical result to stdout.

The wrapper owns the temporary `measurements.txt` symlink, so third-party
implementations can retain the filename expected by the original challenge.

Available implementations:

- `go-lunemec`
- `java-thomaswue-jvm`
- `java-thomaswue-native` (requires GraalVM Native Image)
- `c-matt-re`
- `rust-mtopolnik`

Reference source revisions and the two correctness fixes needed for the C and
Rust fixtures are documented in [`third_party/README.md`](third_party/README.md).

## Validate correctness

```sh
./bench.sh validate \
    go-lunemec \
    java-thomaswue-jvm \
    c-matt-re \
    rust-mtopolnik
```

Every selected implementation must match every expected sample output exactly.

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

The inputs and trusted outputs are generated under `build/stress/`. To validate
any other corpus without timing it, place its oracle beside it with the same
base name and run:

```sh
./bench.sh verify measurements_10K_1B.txt go-lunemec rust-mtopolnik
```

## Establish or refresh a full baseline

Generate the original one-billion-row corpus as `measurements_1B.txt`, then
create its trusted output once as `measurements_1B.out` using an independent
reference implementation.

```sh
./generate_measurements.sh
shasum -a 256 -c measurements_1B.sha256

./bench.sh compare measurements_1B.txt \
    go-lunemec \
    java-thomaswue-jvm \
    c-matt-re \
    rust-mtopolnik
```

Before timing, the wrapper builds each implementation, validates the sample
suite, and compares its full output with `measurements_1B.out`. The correctness
runs also warm the filesystem cache. Hyperfine then performs ten timed runs by
default and saves its raw JSON under `results/`.

Override the run count when needed:

```sh
RUNS=20 ./bench.sh compare measurements_1B.txt go-lunemec
```

Reference results are valid only for the recorded hardware, OS, toolchains,
source revisions, and protocol. Rerun them after any of those change and once
more before publishing a final cross-language comparison.

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

## Challenge contract

Input lines have the form `<station>;<temperature>`. The program emits stations
in alphabetical order as `{station=min/mean/max, ...}`, rounded to one decimal
place. The full challenge input contains 1,000,000,000 rows.
