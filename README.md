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
- The toolchains required by whichever reference implementations are selected

## Adapter convention

For an implementation named `<id>`:

- `prepare_<id>.sh` optionally builds it.
- `calculate_average_<id>.sh` runs it against `measurements.txt` and writes
  only the canonical result to stdout.

The wrapper owns the temporary `measurements.txt` symlink, so third-party
implementations can retain the filename expected by the original challenge.

The repository's Go implementation is named `go-lunemec`.

## Validate correctness

```sh
./bench.sh validate go-lunemec
```

Every selected implementation must match every expected sample output exactly.

## Establish or refresh a full baseline

Generate the original one-billion-row corpus as `measurements_1B.txt`, then
create its trusted output once as `measurements_1B.out` using an independent
reference implementation.

```sh
./bench.sh compare measurements_1B.txt go-lunemec <other-id>...
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
