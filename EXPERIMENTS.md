# Go optimization experiment log

Last reviewed: 2026-10-05

Current baseline: `84e642c` (`perf(parser): scan separators eight bytes at a time`)

The full executable on the retained one-billion-row corpora is the deciding
metric. Microbenchmarks, compiler output, and profiles explain a result; they
do not accept or reject a change by themselves.

## Measurement protocol

Before the next code experiment, run `./bench.sh null-control` on the canonical
corpus. It preconditions the same immutable artifact for 60 seconds, then runs
two labels in isolated `A-B-B-A` blocks without cooldowns. Each block gets two
warmups and five measured runs, with a health snapshot before it. If the 1%
label/order drift gate fails, do not pursue sub-1% changes in that session.

For real experiments:

- Build immutable baseline and candidate binaries and record their hashes.
- Run isolated single-command `A-B-B-A` blocks, with two warmups and five
  measured runs per block (ten measured runs per variant).
- Compare every execution byte-for-byte with the checksum-backed Java oracle.
- Report every block and adjacent-pair ratios; pooled CV alone can hide block
  and first-command bias.
- A repeatable gain of at least 3% can be decided in one quiet window. Repeat a
  1-3% result in a second quiet window.
- Keep a repeatable gain of at least 1% on the canonical corpus, or at least 3%
  on the 10K corpus, provided neither corpus regresses by more than 1%.
  Additional complexity requires a larger win.

## Results to date

Each percentage is from that experiment's same-session A/B; absolute timings
and percentages must not be compared across rows or sessions.

| Experiment | Result | Decision |
| --- | --- | --- |
| One-slot chunk channel buffer | 15.65% slower canonical | Rejected |
| Power-of-two 16,384-slot chained table | 0.36% faster canonical; unbalanced 10K pass was 3.61% faster | Rejected; weak 10K evidence |
| Eight-byte word hash | 0.70% slower canonical | Rejected |
| Hash the odd trailing byte | 7.21% slower canonical, 6.19% faster 10K; also lost inlining | Rejected |
| Fixed 32K inline open-addressed table | 2.52% faster canonical in clean isolated blocks | Rejected by the old 3% gate; retest against current SWAR baseline |
| Semicolon-first stdlib parser | 2.80% slower parser microbenchmark | Rejected at the old microbenchmark gate; no whole-program timing |
| Eight-byte SWAR semicolon scan | 9.64% faster canonical, 7.12% faster 10K | Kept in `84e642c` |
| Fused SWAR scan and hash | 8.26% faster microbenchmark, 0.92% faster canonical with order bias | Rejected |

Detailed local evidence is under `results/experiments/`. Those generated
artifacts are intentionally ignored by Git.

## Post-SWAR profile review

The retained CPU profiles are
`results/experiments/fused-swar-hash-20260906/profile-standard.cpu.pprof` and
`results/experiments/fused-swar-hash-20260906/profile-10k.cpu.pprof`. Despite
the directory name, their baseline profiles are the accepted SWAR-only
implementation at the current commit.

| CPU area | Canonical | 10K stations |
| --- | ---: | ---: |
| `parseLine` cumulative | 48.15% | 20.19% |
| `simpleMap.pos` + `simpleMap.get` | 31.01% | 65.87% |
| `runtime.memequal` | 5.19% | 22.86% |
| `parseNumber` | 6.31% | 2.06% |
| `updateStats` | 3.61% | 1.79% |
| raw file syscall | 6.93% | 3.44% |

The current bottleneck is CPU in parsing and station-table lookup, especially
collision/equality work on 10K stations. It is not primarily I/O, GC, heap
retention, or goroutine scheduling.

- One full benchmark iteration allocated 13.8 GB on canonical and 17.1 GB on
  10K, almost entirely transient 6 MiB chunk buffers. Mid-run live heap was
  131.6 MB and 146.4 MB; post-run live heap fell below 114 KiB. Peak RSS was
  343.2 MiB and 324.6 MiB. There is no leak.
- GC ran 114 and 126 times but accounted for about 0% and 1% of sampled CPU.
- Runs held 15 goroutines with 7-9 workers active, negligible run queues, and
  no goroutine growth. Channel and `pread` block times are cumulative rather
  than wall time. The channel-buffer experiment already showed that buffering
  this handoff is counterproductive.
- PGO compiler inspection did not inline `parseLine`, `parseNumber`, or
  `chunkReader`; their costs remained above the inliner budget. Hash lookup and
  statistics updates were already inlined.

## What the faster implementations do

- Java Thomas Würthinger: memory maps the input, claims 2 MiB segments
  atomically, interleaves three scanners, uses SWAR delimiter detection and a
  branchless one-word temperature parser, and aggregates into a 131,072-slot
  open-addressed table with word-prefix equality checks.
- C matt-re: uses fixed 8 MiB read buffers, fuses hardware CRC hashing with
  delimiter scanning, parses temperatures with few branches, and stores names
  and statistics inline in a 32,768-slot open-addressed table.
- Rust mtopolnik: memory maps static per-core ranges, uses a first-word hash and
  SWAR temperature parsing, and stores entries inline in a 32,768-slot
  open-addressed table.

The shared, portable idea worth copying first is the flat inline table. The
current ARM64 Go SWAR scan compiles to scalar `ldr`, `rbit`, and `clz`, not
NEON, so SIMD work is a separate and less certain experiment.

## Next experiments

1. Run the immutable-binary null control described above.
2. Run a quick whole-program PGO A/B as a zero-source-change screen. Expect a
   small result because PGO did not inline the remaining parser calls.
3. Reapply the 32K inline open-addressed table to the current SWAR baseline and
   benchmark both corpora. This is the strongest next code experiment: the old
   candidate was consistently 2.52% faster and removed 28 lines, but was never
   measured on 10K or against SWAR.
4. Try the one-word branchless temperature parser used by the faster Java, C,
   and Rust implementations.
5. If the flat table remains, test cached full-hash and station-name prefix
   rejection to reduce 10K equality work.
6. Treat chunk-buffer reuse only as a later memory/RSS experiment; it is not a
   current CPU priority.

Two reviewed candidates were left uncommitted in temporary worktrees based on
`84e642c`: a 32K inline open-addressed table and a one-word branchless
temperature parser. Those `/private/tmp` directories have since disappeared,
their worktree registrations are prunable, and both `codex/experiment-*`
branches still point at `84e642c`; the reviewed changes are not on those branch
tips. Reconstruct the flat-table experiment from
`results/experiments/open-addressing-20260906/candidate.diff`. The newer
branchless parser must be reimplemented from the pinned fast implementations
and the profile notes above.

Do not retry the old odd-tail hash, word hash, one-slot channel buffer, standard
map, semicolon-first parser, or fused scan/hash forms without new profile
evidence or a materially different implementation.
