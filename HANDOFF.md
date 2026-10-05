# Handoff: 1BRC cross-language benchmarking and Go tuning

As of 2026-10-05. This document is the entry point for the next agent session.
It intentionally links to the detailed experiment records rather than copying
all raw results into Git.

## Objective

Run the selected Go, Java, GraalVM native, C, and Rust 1BRC implementations
locally against identical corpora, validate every output against an independent
Java oracle, and improve the Go implementation until it approaches the faster
implementations on this Apple M5 MacBook Pro.

## Resume here

- Work on branch `codex/benchmark-handoff`.
- The retained Go baseline is commit `84e642c` (`perf(parser): scan separators
  eight bytes at a time`).
- Read [README.md](README.md) for harness commands and [EXPERIMENTS.md](EXPERIMENTS.md)
  for the measurement rules, profile interpretation, experiment decisions, and
  prioritized backlog.
- Read [third_party/README.md](third_party/README.md) before changing pinned
  third-party sources or adapters.
- Local benchmark evidence lives under `results/`; it is intentionally ignored
  by Git and therefore is available only on this machine unless copied
  elsewhere.

## What is implemented

The repository has adapters and build scripts for:

- `go-lunemec`
- `java-baseline` (oracle generation and correctness only)
- `java-thomaswue-jvm`
- `java-thomaswue-native`
- `c-matt-re`
- `rust-mtopolnik`

`bench.sh` isolates the repository-root `measurements.txt`, runs the pinned
upstream test shell scripts, validates local edge cases and generated stress
corpora, verifies checksum-backed corpora/oracles, and checks timed output after
each Hyperfine run. The canonical and 10K-station one-billion-row corpora and
their oracle outputs are retained locally.

This branch adds a steady-state null-control action:

```sh
./bench.sh null-control measurements_1B.txt go-lunemec
```

It preconditions one immutable artifact for 60 seconds, runs isolated
`A-B-B-A` blocks with no deliberate cooldown, captures system/thermal state
before each block, verifies artifact hashes and exact output, and exits `2` if
label or order drift exceeds 1%. `PRECONDITION_SECONDS`, `RUNS`, and `WARMUPS`
are the only protocol controls. The real 1B null control has not been rerun
since this action was added.

## Measurements worth retaining

The best clean cross-language canonical reference is
`results/20260905-190636-measurements_1B.summary.tsv` with metadata beside it.
It used ten measurements per implementation, two warmups per pass, balanced
forward/reverse order, exact oracle checks, and commit `6bfc2ec`:

| Implementation | Trimmed mean | CV |
| --- | ---: | ---: |
| GraalVM native | 0.913 s | 1.18% |
| Java Thomas JVM | 1.247 s | 2.11% |
| C matt-re | 2.002 s | 1.57% |
| Rust mtopolnik | 2.024 s | 2.06% |
| Go | 3.471 s | 2.49% |

These numbers predate the retained Go SWAR change, so they are a historical
cross-language baseline, not a direct measurement of current `84e642c`.

The accepted eight-byte SWAR semicolon scan is documented in
`results/experiments/swar-semicolon-20260906/README.md`. It improved the
canonical corpus by 9.64% and the 10K-station corpus by 7.12%; this is the code
kept in `84e642c`.

The first immutable-binary null control is in
`results/experiments/null-control-20260918/README.md`. Pooled label bias was
only 0.16%, but the second half was 1.69% slower and A2 was 3.53% slower than
A1. That session failed the 1% order-drift gate. It used 30-second cooldowns;
the new harness replaces that protocol with one preconditioning phase and no
deliberate cooldown.

Do not compare absolute timings across result directories or sessions. Use the
same-session ratios and the metadata stored beside each result.

## What was tried

The complete table and decision rationale are in [EXPERIMENTS.md](EXPERIMENTS.md).
The important state is:

- Kept: eight-byte SWAR semicolon scanning (9.64% canonical, 7.12% 10K).
- Rejected: one-slot chunk buffering (15.65% slower), power-of-two chained
  table (0.36% canonical gain), eight-byte word hash (0.70% slower), odd-tail
  hashing, semicolon-first stdlib parsing, and fused scan/hash (only 0.92%
  whole-program gain with order bias).
- Revisit: the fixed 32K inline open-addressed table was 2.52% faster on the
  old canonical baseline, but it was never measured on 10K stations or against
  the retained SWAR baseline. Its old source delta is preserved at
  `results/experiments/open-addressing-20260906/candidate.diff`.
- Not yet measured: whole-program PGO on the retained baseline.
- Not preserved: newer open-addressing and branchless-temperature prototypes
  were left uncommitted in `/private/tmp` worktrees. Those directories have
  since disappeared, `git worktree list` marks them prunable, and both
  `codex/experiment-*` branch tips still point at `84e642c`. Do not assume
  those branches contain the prototypes. Reconstruct the open-addressing work
  from the retained old diff; reimplement the branchless parser from the notes
  and the pinned fast implementations.

## Profile conclusions

The retained post-SWAR profiles are:

- `results/experiments/fused-swar-hash-20260906/profile-standard.cpu.pprof`
- `results/experiments/fused-swar-hash-20260906/profile-10k.cpu.pprof`

They show that parsing and station-table lookup dominate. On 10K stations,
`simpleMap.pos` plus `simpleMap.get` accounts for about 66% of sampled CPU and
`runtime.memequal` for about 23%. The canonical parser remains expensive, but
the program is not primarily I/O-, GC-, heap-retention-, or goroutine-limited.
Most allocation volume is transient 6 MiB chunk buffers; live heap falls back
below 114 KiB after a run. Goroutine counts and run queues were stable.

The faster Java, C, and Rust implementations all use flat inline
open-addressed tables and cheap early station-name rejection. They also use
branch-light temperature parsing. This is why the flat table is the strongest
next source experiment and the one-word temperature parser is second.

## Machine-noise lessons

Benchmark only on AC power after Docker, Syncthing, Time Machine, and macOS
background work are idle. `fseventsd` previously consumed a full core for an
extended period, and Syncthing plus recreated Docker/Postgres containers
invalidated earlier passes. A quiet preflight alone was insufficient: the old
null control still drifted during the run. Treat a failing null control as a
hard stop for small changes and inspect its health snapshots rather than
rerunning until it passes.

## Next session

1. Confirm the corpora and ignored `results/` directory are still present.
2. Check that the machine is quiet, then run the new canonical null control.
   If it exits `2`, stop performance comparisons and diagnose the recorded
   drift; do not retry until significant.
3. If it passes, recreate the 32K inline open-addressed candidate in a fresh
   worktree from `84e642c`. Run Go tests, vet, pinned upstream tests, stress
   validation, and exact validation on both 1B corpora before timing.
4. Benchmark baseline and candidate in isolated `A-B-B-A` blocks on canonical
   and 10K corpora. Keep only a repeatable gain meeting the thresholds in
   [EXPERIMENTS.md](EXPERIMENTS.md).
5. Then prototype the one-word branchless temperature parser. Use PGO as a
   quick zero-source-change screen, not as a substitute for full executable
   measurements.
6. Update [EXPERIMENTS.md](EXPERIMENTS.md) and retain raw artifacts for every
   accepted or rejected result.

Suggested skills for the next agent: `cc-skills-golang:golang-how-to`,
`cc-skills-golang:golang-benchmark`, `cc-skills-golang:golang-performance`, and
`review-fix-loop` for any candidate that survives correctness checks.

## Verification already completed for this branch

- `bash -n bench.sh`
- `bash -n test_harness.sh`
- `shellcheck bench.sh test_harness.sh`
- `./test_harness.sh` (stable and deliberately drifting null-control cases)
- `go test ./...` (17 tests)

The full 1B null control and candidate benchmarks remain intentionally pending
because they require a quiet machine.
