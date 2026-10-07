# Handoff: 1BRC cross-language benchmarking and Go tuning

As of 2026-10-06. This document is the entry point for the next agent session.
It intentionally links to the detailed experiment records rather than copying
all raw results into Git.

## Objective

Run the selected Go, Java, GraalVM native, C, and Rust 1BRC implementations
locally against identical corpora, validate every output against an independent
Java oracle, and improve the Go implementation until it approaches the faster
implementations. The earlier sessions used an Apple M5 MacBook Pro; current
Linux research uses a Ryzen 7 5800X desktop. Compare ratios within a session.

## Resume here

- Work on branch `codex/benchmark-handoff`.
- Always verify live host quietness before and during full timing. The guard in
  `benchmark_quiet.py` now enforces this for full `bench.sh` timing runs and the
  table hot-path experiment. Run outside the PID-isolating sandbox; inspect the
  retained `.quiet.json` alongside fresh null controls and confirmation order drift.
- Production Go uses the accepted robust 32K table and hot/cold split:
  standard 2.587→2.444 s (5.54% faster), extended
  5.745→3.310 s (42.39% faster), with passing fresh controls and monitored timing.
  See [the acceptance record](EXPERIMENTS.md#accepted-robust-table-and-hotcold-split-2026-10-06).
  The retained pre-change Go baseline is commit `84e642c` (`perf(parser): scan
  separators eight bytes at a time`).
- Parser-word reuse is now applied on top of committed baseline `f8a90a5`.
  Standard confirmation is 2.444→2.312 s (5.42% less time); extended is
  3.268→3.173 s (2.90%) and independently repeats at 3.269→3.141 s (3.93%).
  All fresh null, order-drift and real-host quietness checks pass. The hash,
  40-byte entries, decoder, reader and sixteen workers are unchanged.
  `stationPos` had only an obsolete benchmark caller and is removed; its
  benchmark now exercises the production fingerprint. New inline explanations
  and concrete data examples are scoped to `main.go` as requested.
  See [the parser-word acceptance record](EXPERIMENTS.md#accepted-parser-word-reuse-2026-10-06)
  and local `results/research/20261006/parser-word/`.
- Second-word reuse and exact 9..15-byte matching are now tested and rejected
  on this Linux host. Reuse confirms -0.89% standard time but +1.06% extended;
  exact matching confirms -2.87% standard, but its extended screen regresses
  +2.72% and stops before confirmation. Both windows pass host/null/order checks.
  `51e20a1` was retained as the baseline for the decoder experiment; sources/
  tests, the 48-byte footprint
  control, full oracles/counts and all allocation/wait profiles are retained in
  `results/research/20261006/second-word/`. See
  [the rejection record](EXPERIMENTS.md#second-word-reuse-and-exact-matching-rejected-2026-10-06).
  The bounded temperature decoder is now accepted below. Further exact-word work
  should isolate a small inlined 9..15-byte hash path and a cold tail array;
  neither alternative is yet implemented or measured.
- Production now adds the accepted bounded temperature-word decoder to
  first-word reuse: standard 2.292→2.210 s (3.56% less runtime), extended
  3.131→3.072 s (1.89%) independently repeated at 3.143→3.022 s (3.83%).
  Both extended windows are reported; all three fresh null/order/host controls
  pass. Eight-byte loads are bounded and short final rows keep the scalar
  fallback. The 40-byte table/hash, reader/channels, sixteen workers and output
  remain fixed. Exhaustive temperatures, tails, ARM64 build, all small/full
  oracles and independent row counts pass. See
  [the decoder acceptance](EXPERIMENTS.md#accepted-bounded-temperature-decoder-2026-10-06)
  and local `results/research/20261006/temperature-word/`.
  This version is committed as `ef7418a`.
- Exact scalar 64-byte mask batching is now tested and rejected. Six-permutation
  screens are +4.97% standard and +3.15% extended versus `ef7418a`; both pass
  fresh null/host checks. Extended screen baseline half drift is -2.25%, so
  neither screen is presented as a precision confirmation. Mask reuse beats
  the per-row restart control but not production. All tests, ARM64 build,
  small/full oracles and independent billion-row counts pass. Allocations stay
  unchanged; scalar mask/consumer work increases instructions and caller stack.
  Sources, patches and full allocation/wait evidence are retained in
  `results/research/20261006/delimiter-batch/`. See
  [the rejection record](EXPERIMENTS.md#scalar-delimiter-batching-rejected-2026-10-06).
  Preserve this table/decoder/reader and bounded tails. `parseNumber` still
  serves the short final row of each chunk; remove it only with an equivalent
  bounded tail decoder, as a separate cleanup.
- Go SIMD mask generation is now validated but not promoted. With matching
  flags, standard A/B/B/A confirmation is 2.196326→2.177410 s (-0.861%),
  extended 3.022108→3.020680 s (-0.047%). Both completed windows pass fresh
  null, order and host checks, but neither reaches >=1% standard or >=3%
  extended; retain `ef7418a`. Four variants include a plain build-flag control,
  matched production, scalar batching and AVX2. The native kernel inlines and
  dispatches once per worker, with the original parser for unsupported builds/
  CPUs. Unit/vet/race/ARM64, protected-page/bounds, disabled CPU/build, all
  small/full oracles and independent billion-row counts pass. Sources/patches,
  all allocation/wait evidence and timing/decision records live in
  `results/research/20261006/simd-mask/`. A report-variable bug stopped the first
  standard window; preserve it and the old harness. Completed standard window 2
  uses the corrected harness and fresh controls. See
  [the SIMD record](EXPERIMENTS.md#go-simd-mask-validated-not-promoted-2026-10-06).
  Those producer/allocation traces motivated the accepted buffer experiment below.
- Bounded buffer reuse is applied and committed on top of `ef7418a`.
  The resulting commit is `57591e7` (`perf: reuse bounded chunk buffers`).
  Standard confirmation is 2.204872→1.926155 s (12.64% less runtime), extended
  3.049592→2.398277 s (21.36%). Both fresh null/order/real-host controls pass.
  A lazy producer-owned pool allocates at most seventeen 6 MiB buffers here;
  workers return them after their final row update. Existing cloned keys,
  parser/table/decoder, sequential ranges, work-channel buffering and sixteen
  workers are fixed. New comments/examples remain scoped to `main.go`.
  Allocation volume falls 13.822→0.132 GB standard and 17.098→0.137 GB extended
  (over 99%); GC cycles 130/152→3, post-GC heap stays around 0.6 MB. Ownership,
  EOF/stale bytes, multiworker/race, ARM64, all small/full oracles and independent
  billion-row counts/pool bounds pass. The normal adapter is rebuilt/validated
  and repository executable AST matches the measured candidate. See
  [the pool acceptance](EXPERIMENTS.md#accepted-bounded-buffer-reuse-2026-10-06)
  and local `results/research/20261006/buffer-pool/`.
  CPU-class metrics are cached until GC on this runtime; with only three startup
  collections, pre-GC idle ratios are stale. The retained analysis refreshes from
  the existing post-forced-GC snapshot and preserves raw values/old analyzer;
  endpoint includes cleanup/reporting. Refreshed pool GC capacity is below 0.1%.
  Keep the measured pool changes intact; subsequent retries and next steps are
  recorded below.
- Scalar/SIMD mask retries now use the committed pool. Independent subagents
  implemented/audited both and reviewed producer/worker profiles. Scalar remains
  slower (+7.37% standard/+6.48% extended against matched production). SIMD
  passes the numerical rule in two independent direct normal-production windows
  per corpus: standard -1.19%/-1.34%, extended -1.65%/-1.05%, all quiet/null/order
  checks passing. Matched-flag gains of -2.24%/-2.95% overstate the practical
  normal-build gain. Two interrupted windows are excluded in full and retained.
  Initially retained as research on complexity grounds; the user subsequently
  requested promotion and commitment of this exact validated SIMD version.
  Production now includes the AVX2 path with the current pooled scalar fallback.
  The adapter enables SIMD when the compiler supports it, preserves existing
  experiment flags and honors explicit `nosimd`. Sources/patches, fallback/bounds/
  ownership checks, all small/full oracles and independent counts, profiles,
  six valid timing windows and decisions are in local ignored
  `results/research/20261006/pooled-mask-retry/`. See
  [the pooled retry and next experiment](EXPERIMENTS.md#pooled-mask-retries-and-simd-promotion-2026-10-06).
  Next test static parallel reusable ReadAt: sixteen newline-aligned worker
  ranges/6 MiB buffers with the current portable parser/table/owned names.
  Prove consumed-byte coverage and raw station counts/sums, not just rounded
  output or total rows. Include setup/cleanup and profile kernel/user CPU,
  memory/GC, reads, scheduler/channel waits separately. If rejected, prioritize
  staged two/three cursors with a one-lane control; mmap remains separate.
- The user explicitly added the remaining C/C++/Java mechanisms as test and
  benchmark items: mapped input, phased cursors, SIMD station equality and
  full-name hashing, hardware CRC hashing, inline key/stat layouts, fixed
  eight/sixteen workers and the four-byte temperature decoder. Read the
  [reference-derived matrix](EXPERIMENTS.md#reference-derived-test-and-benchmark-items-2026-10-06)
  for isolated controls, correctness checks, profiles and later combination
  benchmarks. Static parallel ReadAt stays next. Preserve exact short-key
  identity when changing hashes; avoid prefix-only hashes, length-free equality
  and unbounded vector loads. Full timing always requires the unchanged live
  quiet guard, fresh nulls and both full-corpus checks.
- Read [README.md](README.md) for harness commands and [EXPERIMENTS.md](EXPERIMENTS.md)
  for the measurement rules, profile interpretation, experiment decisions, and
  prioritized backlog.
- The user added compact prefix routing and a compressed radix trie to the
  [experiment list](EXPERIMENTS.md#prefix-directory-and-compressed-radix-trie-2026-10-06).
  Test prefix routing after exact-word lookup, and the arena-backed trie at
  lower priority. Keep exact verification/full-hash fallback for shared prefixes.
  Station-key distribution evidence is in `results/research/20261006/prefix-analysis.json`.
- The 2026-10-05 Linux cross-language reference is recorded in
  [EXPERIMENTS.md](EXPERIMENTS.md#linux-cross-language-reference-2026-10-05).
  Both full corpora passed correctness checks. Canonical null drift was 0.485%
  (pass); extended drift was 1.093% (failed the then-declared 1% gate and fits
  the subsequently selected 2% broad-comparison limit). Its local inputs live
  under `build/corpora/`. Later external-race controls are recorded separately.
- Follow the measured
  [Linux optimization research plan](EXPERIMENTS.md#linux-optimization-research-and-plan-2026-10-05)
  for this desktop: the original robust prototype gained 40.46% on 10K but lost
  3.28% on standard. The accepted hot/cold split now resolves that regression.
  The parser gained
  4.92% on standard, while baseline PGO and eight workers did not win across
  corpora. Fresh allocation, heap, goroutine, channel, mutex, syscall, and
  scheduling evidence is retained under `results/research/20261005/`.
- The [native Java source/build review](EXPERIMENTS.md#native-java-review-and-implications-2026-10-05)
  refines that plan: exact two-word name equality covers 97.58% of standard
  station keys, then staged two/three scan cursors precede rewriting I/O.
  The first cached-word prototype is correct but fails paired Linux timing
  as recorded above; Java's exact shortcut remains a source idea, not a Go win.
  Java uses a sparse pointer table; the Go prototype already has competitive
  probe counts. Native compilation includes ML-inferred profiles, Epsilon GC,
  host CPU targeting, and fewer loop safepoints. Their individual speed
  contributions have not been measured.
- The [external implementation research](EXPERIMENTS.md#external-implementation-comparison-2026-10-05)
  tests the user-supplied LE, Danny, HappyCerberus and Ragnar references. LE's
  separately corrected SIMD implementation is the strongest new reference.
  Original and corrected sources are retained separately, with exact full
  oracles and independent billion-row counts. Ragnar skips rows and is excluded.
  Test multi-row delimiter-mask reuse early, alongside exact short-key/table
  work and staged cursors; memory ownership and I/O remain separate experiments.
  The research runner now reaps descendants before launching the next variant
  and reports result return plus complete-process time. Extended null drift
  narrowly fails 2%, so those timings are exploratory. Copy ignored
  `results/research/20261005/external/` and the recorded build artifacts when
  transferring this research to another machine.
- Go 1.27 SIMD's first AVX2 mask experiment now misses production gain criteria,
  as recorded above. Keep its kernel and matched scalar/production controls for
  later research; default flags and production source remain unchanged. A
  separate GOAMD64=v3 performance screen, portable SIMD and ARM64 NEON are
  still unmeasured; scalar fallbacks already cross-build on ARM64.
  The robust table's hot/cold split was implemented as an isolated experiment
  with hash, capacity, entry layout, parsing and I/O fixed. See
  [the implementation record](EXPERIMENTS.md#robust-table-hotcold-split-implementation-2026-10-05)
  and `results/research/20261005/table-hotpath/README.md` for correctness, row
  counts, compiler evidence, allocation/scheduling profiles and the timing decision.
  All exact oracles and independent 1B counts pass. Standard window9 and extended
  window3 now satisfy the declared 2% controls and confirmation-order limits,
  and the candidate is promoted. Earlier provisional/failed/interrupted windows
  remain retained; the provisional 7.5% result is not acceptance evidence.
  First parser-word reuse is accepted. The subsequent second-word/48-byte
  exact-identity experiment is rejected; retain the forty-byte production table.
- Read [third_party/README.md](third_party/README.md) before changing pinned
  third-party sources or adapters.
- Local `results/go.mod` excludes mutually exclusive archived Go prototypes
  from recursive application tests. Copy it with the ignored research artifacts.
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
label or order drift exceeds `DRIFT_THRESHOLD_PERCENT` (default 1%).
`PRECONDITION_SECONDS`, `RUNS`, `WARMUPS`, and `DRIFT_THRESHOLD_PERCENT` are the
protocol controls. Use 2% for broad comparisons when declared in advance;
retain 1% for small-change experiments. The Mac 1B null control has not been
rerun since this action was added; the Linux controls are summarized above.

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
- Linux screen: baseline whole-program PGO did not win across both corpora;
  revisit with candidate-specific profiles after hot-path changes.
- Not preserved: newer open-addressing and branchless-temperature prototypes
  were left uncommitted in `/private/tmp` worktrees. Those directories have
  since disappeared, `git worktree list` marks them prunable, and both
  `codex/experiment-*` branch tips still point at `84e642c`. Do not assume
  those branches contain the Mac prototypes. The later Linux reconstruction
  is retained under `results/research/20261005/station-table/` and `parser/`;
  start from those measured artifacts when available. The old diff and notes
  remain historical evidence if the Linux copies are unavailable.

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

The faster implementations use open addressing and cheap station-name checks;
C and Rust keep entries inline, while Thomas Java uses a pointer table with
two cached name words in each result object. They also use branch-light
temperature parsing. Current Linux measurements and the native review above
take precedence over the earlier Mac experiment ordering below.

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
2. Follow the current Linux ordered experiment plan in [EXPERIMENTS.md](EXPERIMENTS.md).
   Retain the accepted robust table, first-word reuse, bounded decoder and buffer pool.
   Exact two-word lookup, scalar batching and the first Go SIMD mask experiment
   fail their production timing criteria. Bounded buffer reuse is accepted and
   committed as `57591e7`. The user requested promotion of the validated pooled
   SIMD path despite its modest gains; scalar batching still regresses. Next
   probe static parallel reusable ReadAt before the larger cursor refactor;
   follow the detailed coverage/control plan in the latest experiment record.
   The reference-derived matrix explicitly tracks later SIMD equality/hash,
   CRC, layout, worker-count, decoder and combination benchmarks.
   Existing prototype patches and builds
   are retained under `results/research/20261005/` and `20261006/`; do not reconstruct from
   stale Mac branch names if these artifacts are available.
3. Run Go tests/vet, pinned tests, stress/adversarial cases, independent full
   oracles, and row-count/range-coverage checks before timing a new scanner.
4. Declare the drift threshold, check machine state and run a fresh null
   control for each corpus. Exit `2` blocks precision/adoption work; retain
   failure rather than retrying until a pass. Broad comparisons may use 2%.
5. Compare immutable baseline/candidate binaries in isolated `A-B-B-A` blocks
   with exact output checks and complete-process isolation. Keep only a
   repeatable gain satisfying both corpora's acceptance rules. Profile CPU,
   allocations/heap, GC and goroutine/channel/scheduler waits separately.
6. Update [EXPERIMENTS.md](EXPERIMENTS.md) and retain raw artifacts for every
   accepted or rejected result. Transfer ignored evidence explicitly.

Suggested skills for the next agent: `cc-skills-golang:golang-how-to`,
`cc-skills-golang:golang-benchmark`, `cc-skills-golang:golang-performance`, and
`review-fix-loop` for any candidate that survives correctness checks.

## Verification already completed for this branch

- `bash -n bench.sh`
- `bash -n test_harness.sh`
- `shellcheck bench.sh test_harness.sh`
- `./test_harness.sh` (stable and deliberately drifting null-control cases)
- `go test -pgo=off ./...`, `go vet -pgo=off ./...`, `go test -race -pgo=off ./...`
- Linux ARM64 cross-build and normal-adapter 28 small / both full 1B output checks

The original Mac full null rerun remains pending. Linux full-corpus references,
null controls and prototype comparisons are complete as recorded above. The
accepted robust table, first-word reuse, bounded decoder and buffer reuse pass
the Linux acceptance protocol; current production includes all four changes.
