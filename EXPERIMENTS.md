# Go optimization experiment log

Last reviewed: 2026-10-06

Current production: accepted robust table and hot/cold split (2026-10-06). Retained pre-change baseline: `84e642c` (`perf(parser): scan
separators eight bytes at a time`).

The full executable on the retained one-billion-row corpora is the deciding
metric. Microbenchmarks, compiler output, and profiles explain a result; they
do not accept or reject a change by themselves.

## Measurement protocol

Always check the live host before a full benchmark and monitor it throughout
the window. A sandbox-only process list cannot establish desktop quietness.
`benchmark_quiet.py` now guards full `bench.sh` timing runs and the table hot-path
experiment, retaining both failed preflights and interrupted windows. See the
[harness instructions](README.md) for limits and platform coverage. Quietness
and null/order drift are separate requirements; neither substitutes for the other.

On 2026-10-06 the paging guard was refined after a quiet-host attempt stopped
on a paging burst with low CPU/I/O/memory pressure and about 25 GiB available.
The live host uses zram without disk backing. Record swap-in and swap-out
separately: verified RAM-only page-ins are assessed through the existing CPU and
pressure limits; swap-out above 64 KiB/s still rejects. Disk-backed/unknown swap
retains the combined paging limit. The 2% null gate is unchanged, and the earlier
interrupted attempt is retained. This is a measurement-harness change, not a
system swap/configuration change. See
[the kernel zram documentation](https://docs.kernel.org/admin-guide/blockdev/zram.html).
Preparation-only activity is recorded separately from timing-window rejection:
the initial idle preflight and every timed interval retain the declared limits.
This avoids discarding a warm-up before a control can test repeatability.

Before the next code experiment, run `./bench.sh null-control` on the canonical
corpus. It preconditions the same immutable artifact for 60 seconds, then runs
two labels in isolated `A-B-B-A` blocks without cooldowns. Each block gets two
warmups and five measured runs, with a health snapshot before it. If the 1%
label/order drift gate fails, do not pursue sub-1% changes in that session.
For broad cross-language comparisons, declare `DRIFT_THRESHOLD_PERCENT=2`;
the 1% default remains the protocol for small-change optimization. Relaxing
the gate does not reduce observed noise or improve timing precision.

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

The table hot-path experiment uses the user-selected **2%** fresh null gate on
each corpus. Keep its failed windows; do not repeatedly rerun a failed control
until a passing result appears. A new attempt requires an independent quiet
window or an identified change in host conditions or measurement protocol.

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
  open-addressed pointer table. Result objects cache two name words; masked
  words including the semicolon give exact equality for names up to 15 bytes.
- C matt-re: uses fixed 8 MiB read buffers, fuses hardware CRC hashing with
  delimiter scanning, parses temperatures with few branches, and stores names
  and statistics inline in a 32,768-slot open-addressed table.
- Rust mtopolnik: memory maps static per-core ranges, uses a first-word hash and
  SWAR temperature parsing, and stores entries inline in a 32,768-slot
  open-addressed table.

The shared, portable ideas are open addressing and cheap exact name checks;
C and Rust additionally keep entries inline. The
current ARM64 Go SWAR scan compiles to scalar `ldr`, `rbit`, and `clz`, not
NEON, so SIMD work is a separate and less certain experiment.

## Next experiments

For current Linux work, follow the measured
[Linux optimization research plan](#linux-optimization-research-and-plan-2026-10-05)
below. This list records the earlier Mac backlog.

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

## Linux cross-language reference (2026-10-05)

Ran the handoff baseline on an AMD Ryzen 7 5800X (8 cores, 16 threads,
32 GiB RAM), using independently generated canonical and 10K-station corpora
with one billion rows each. This is a separate hardware reference from the Mac
experiments above. Sources were `c8908972` plus the recorded Linux portability
and health-snapshot patch; Go source was unchanged. The C adapter now uses the
equivalent x86-64 SSE4.2 CRC32C intrinsics and declares POSIX file-size APIs.

Both comparisons used ten measured runs per implementation, split into five
forward and five reverse runs, with two warmups per implementation in each
pass. This balances command order across two passes; it does not rotate the
implementation after each individual run. Each timed output matched the
independent Java oracle. Pinned upstream tests, exact local samples, generated
stress inputs, harness isolation tests, and shell syntax checks passed.
ShellCheck was unavailable on this Linux host.

| Implementation | Canonical trimmed mean | CV | 10K trimmed mean | CV |
| --- | ---: | ---: | ---: | ---: |
| GraalVM native | 0.768 s | 1.57% | 2.119 s | 0.60% |
| Rust mtopolnik | 1.338 s | 2.10% | 2.062 s | 6.71% |
| C matt-re | 1.816 s | 1.82% | 3.345 s | 3.57% |
| Java Thomas JVM | 1.972 s | 5.29% | 3.290 s | 7.53% |
| Go | 2.634 s | 0.80% | 6.148 s | 4.31% |

The initial host preflight was approximately 99% idle, without swap usage or
measured I/O wait, and used the `performance` CPU governor. The canonical Go
null control passed with maximum drift **0.485%**. Canonical Go forward/reverse
block means differed by -0.08%; the other implementations differed by roughly
1–2%. JVM variability exceeded the harness's 3% CV threshold.

The 10K comparison showed memory reclaim, swap use, and four implementations
above 3% CV. After timing finished, `POSIX_FADV_DONTNEED` released 9.07 GB of
inactive canonical-corpus cache while retaining the complete extended corpus
in memory. A separate 10K Go null control then failed with maximum drift
**1.093%**, despite approximately 10 GiB free and zero recent I/O/memory
pressure in its snapshots. The remaining drift's cause was not isolated.
Timing stopped at that gate; no repeat comparison was run. Treat the 10K
numbers as approximate, retain the large performance gaps, and avoid deciding
the close Rust/native or C/JVM ordering from this session. Small Go changes on
10K require another controlled session that passes the null gate.

Raw JSON, logs, checksums, toolchains, artifact hashes, health snapshots,
forward/reverse drift analysis, the measured source patch, and an immutable Go
baseline binary are retained locally under
`results/linux-comparison/20261005-141846/`. That ignored directory must be
copied separately when transferring the evidence to another machine.

At the user's request, adopted a **2%** tolerance for this broad comparison
after reviewing the completed measurements. Both observed null-control drifts
meet that limit. This is a retrospective interpretation recorded separately in
`relaxed-gate.json`; the original 1% analyses and all raw timings are preserved.
The extended CV warnings and uncertainty in close rankings still apply. No
additional timing run is required to evaluate the same observations at 2%.

## Linux optimization research and plan (2026-10-05)

Three subagents investigated station tables, temperature parsing, and the
reference architectures/compiler options. Fresh Linux diagnostics cover CPU,
allocated bytes/objects, live heap/RSS, GC, goroutines, channel blocking, mutexes,
syscalls, scheduling, and execution traces. Production Go source is unchanged;
all prototypes and raw evidence are retained in `results/research/20261005/`.

CPU-heavy builds, tests, profiling, and timing were serialized with one shared
lock. Full-program screens used immutable binaries, exact oracle checks after
every execution, two warmups, and three measurements per block. Table/parser
screens used A-B-B-A; the PGO/worker screen used forward/reverse order. Screens
identify promising experiments; they are not acceptance runs. Inactive corpus
cache was released before switching inputs, then the active input was warmed.

| Experiment | Standard result | 10K result | Decision |
| --- | --- | --- | --- |
| Robust hash + short-name fingerprint + 32K inline table | 2.626 → 2.712 s; 3.28% slower | 6.000 → 3.572 s; 40.46% faster | Strong extended opportunity; fix standard regression before adoption |
| Bounded one-word temperature decoder | 2.601 → 2.473 s; 4.92% faster | 5.906 → 5.967 s; noisy, no established gain | Test in combination after table work |
| Merged warm-profile PGO | 2.617 → 2.661 s; 1.68% slower | 6.054 → 5.975 s; 1.31% faster | Keep PGO off on the current baseline |
| Actual 8 workers, GOMAXPROCS still 16 | 2.617 → 3.535 s; 35.09% slower | 6.054 → 7.101 s; 17.29% slower | Keep 16 workers on the current baseline |

Each row uses its own same-window baseline. Do not multiply the gains or
compare small absolute differences across these screens. The earlier language
reference implies required runtime reductions of approximately 25% for JVM,
31% for C, 49% for Rust, and 71% for native Java on standard; extended parity
requires roughly 46%, 46%, 66%, and 66%. These are planning targets, not forecasts.

### Why the table experiment changed

The current pair hash ignores odd tails. On the actual extended keys it gives
only 7,560 distinct hashes for 10,000 names; all 54 one-byte names collide.
Blindly using that hash in a flat 32K table made representative lookup/update
micros 2.43 times slower. A key-distribution simulation found 26.37 successful
probes per key, compared with 1.21 for the pinned Rust prefix hash. These are
distribution measurements, independently corroborated by the lookup micros.

78.15% of extended names fit within eight bytes. A complete short-name
fingerprint plus exact length gives exact equality for those names. Longer
names still need full hashing and exact equality: the legal 10K long-UTF8
stress input gives every name an identical first eight bytes. Prefix-only
hashing would degenerate even though it remained correct.

The robust candidate halves extended lookup/update micro time and improves
the full extended program by 40.46%. Its standard regression is consistent
with new hot helpers exceeding Go's normal inlining budget: `find` costs 167,
`stationFingerprint` 193, versus budget 80. This is a concrete next target;
candidate-specific profiles and compiler output must verify any remedy.

The candidate copies names at first insertion. Its 40-byte entries use
1.25 MiB per worker, or 20 MiB for sixteen workers. Extended initialization
drops from 1.740 MB/15,431 allocations to 1.440 MB/10,001 allocations per worker;
standard table allocation rises from 350 KB to 1.315 MB. Owned keys remove
input-buffer lifetime dependence, enabling a safe buffer-recycling experiment.
Whole-program memory improvement for this candidate has not yet been measured.

### Memory and concurrency findings

The following are instrumented baseline runs, separate from performance timing.
The sampling period was 200 ms. Sampling/tracing adds overhead; heap samples
and goroutine counts include the profiling/test harness.

| Observation | Standard | 10K |
| --- | ---: | ---: |
| Total allocated bytes per run | 13.807 GB | 17.104 GB |
| Allocated objects | 18,878 | 302,164 |
| Sampled peak live heap | 416 MiB | 521 MiB |
| Sampled peak RSS | 491 MiB | 540 MiB |
| Live heap after forced GC | 0.60 MiB | 0.65 MiB |
| GC collections / total pause | 69 / 20.2 ms | 75 / 24.3 ms |
| Estimated runtime capacity spent in GC | 0.83% | 1.29% |
| Estimated idle runtime capacity | 3.61% | 1.01% |
| Sampled goroutines, minimum–maximum | 22–22 | 22–22 |
| Average workers in trace Running state | 14.12 | 14.78 |
| Worker chunk-receive waits, cumulative | 3.35 s | 3.47 s |
| Producer chunk-send waits | 0.243 s | 1.484 s |
| Producer ReadAt syscall intervals | 1.257 s | 1.991 s |

Allocation profiles attribute approximately 99.9% of allocated bytes to fresh
6 MiB chunk buffers. Heap returns below 1 MiB after collection, and goroutine
counts remain stable. GC is a small share of current runtime capacity; buffer
reuse still merits measurement for zeroing, memory traffic, and ownership.

Waits overlap across goroutines. Much of the raw blocking profile is the main
goroutine waiting for completed worker maps, the closer's WaitGroup, and trace/
sampling machinery. Workers are in Running state for roughly 88%/92% of their
combined trace capacity, and all sixteen workers waiting while the producer
is in a syscall totals only about one millisecond on either corpus. The warm
baseline is not broadly starved for chunks. Mutex stacks mainly identify the
runtime scheduler rather than a shared station-table lock. Application workers
have no intentional sleeps; runtime/trace timer sleeps are instrumentation.
Running is a Go scheduler state, not a direct hardware utilization counter.

Serial reads still consume substantial elapsed producer time and can become
the limiting stage after lookup speeds up. Reprofile the faster candidate;
the current lack of starvation does not settle the architecture question.

Fresh warm CPU profiles put lookup plus hashing at 48.8% of standard CPU and
74.1% of extended CPU. Numeric decode is 7.0%/2.7%. Earlier cold-transition
CPU profiles inflated standard syscall cost, so warm profiles are the planning
and PGO inputs. CPU samples, runtime capacity estimates, and trace elapsed
intervals are different quantities and must not be added together.

### Native Java review and implications (2026-10-05)

The pinned Thomas source and native build explain mechanisms worth testing,
not measured contributions to its runtime. Standard native/JVM times were
0.768/1.972 s; extended times were 2.119/3.290 s. Both adapters use the same
Java algorithm, so three scanners and mmap alone cannot explain the native
versus JVM difference. Fresh JVM processes also start fresh worker JVMs;
harness warmups warm the input/system, not JIT code retained across executions.

**The strongest new source finding is exact two-word name equality.** The
scanner loads both first words unconditionally, finds the semicolon in either,
and uses mask tables to retain the name plus its delimiter. For names of at
most 15 bytes those two words are the complete identity, rather than a prefix
or probabilistic hash. The semicolon distinguishes different lengths. A hit
at the home slot returns without general string equality; collisions and longer
names use exact comparisons against the mapped bytes.

| Name length coverage, station keys | Standard (413) | Extended (10,000) |
| --- | ---: | ---: |
| At most 7 bytes, delimiter fits one word | 52.06% | 76.71% |
| At most 8 bytes, existing Go candidate's shortcut | 65.86% | 78.15% |
| At most 15 bytes, Java's two-word shortcut | 97.58% | 83.70% |
| At most 16 bytes, requires another byte for Java's delimiter | 99.03% | 84.27% |

These are key fractions, not measured row frequencies. Extending exact identity
from eight to fifteen bytes covers another 31.72 percentage points of standard
keys versus 5.55 points of extended keys. This directly targets the corpus where
the robust table regressed. Java deliberately avoids a branch between names
fitting one versus two words; compare that code shape with conditional loads,
including tiny/chunk-tail fallbacks. Keep full long-name hashing and equality.

Java's table is a 131,072-slot array of references to separately allocated
Result objects, not inline name/stat entries. At 10,000 stations its occupancy
is at most 7.63%, compared with 30.52% for the Go candidate's 32K table. Its
XOR/mix hash and stride-31 probing should be assessed together with capacity;
neither sparse tables nor that hash guarantee performance on legal shared-prefix
inputs. Compare layouts/capacities independently rather than copying a 131K
table of larger inline Go entries. Result objects retain mapped name addresses;
strings are created during final merge, not for every row. Scanner objects are
created per segment; their physical allocation/elimination has not been profiled.

Simulating the exact Java hash and stride on oracle keys gives mean successful
probes of 1.000 on standard and 1.276 on extended; the robust Go candidate gives
1.005/1.209. Probe count is already competitive. Java's XOR/mix hash in a 32K
stride-one table instead gives 25.77 probes on extended, versus 2.282 with
stride 31. These are uniform-key simulations, not timed lookup costs. Copying
the equality shortcut is better supported than copying the hash in isolation.

**Three cursors expose independent work.** Each 2 MiB claimed segment is split
at newlines. Java stages first-word loads across all three cursors, then second
words/masks, lookups, numeric decodes, and sequential statistic updates. This
can overlap dependencies and memory latency within a thread; simply invoking
three complete parse calls consecutively is a different experiment. The source
establishes the schedule; its speed contribution needs an ablation. Current Go
traces show busy workers, so this should precede the I/O rewrite.

**Mapping avoids the Go baseline's fresh input-buffer allocation and copy.**
Workers read the mapping directly and claim segments through an AtomicLong;
there is no producer sending copied chunks. There are no per-row result-object
allocations after station initialization. This addresses our 13.8/17.1 GB
allocation volume, but GC accounts for only about 1% of current Go runtime
capacity, so turning off GC alone is not a plausible explanation for the gap.
Memory comparisons must distinguish native anonymous heap from file-backed mmap
RSS/page cache; total RSS is not comparable to Go's live heap.

**The native build has materially different compiler/runtime settings.** Its
actual build log records Oracle GraalVM 21.0.2+13.1, optimization level 3,
target machine `native`, **ML-inferred PGO**, and Epsilon GC. The script also
sets inliner exploration, initializes Scanner at image build time, and disables
loop safepoint generation, not all safepoints. ML-inferred profiles are not workload-trained PGO;
no training file is supplied. Go screens used GOAMD64=v1 and PGO off. Epsilon
does not collect heap garbage; it still allocates and needs sufficient heap.
There is no demonstrated safe, equivalent Go flag bundle. Do not assign the
2.57x/1.55x native/JVM ratios to startup, inlining, CPU target, GC, or safepoints
without individual controlled experiments. Build GC/RSS statistics in the
native build log describe compilation, not benchmark runtime.

The absolute JVM/native differences are similar: about 1.204 s standard and
1.171 s extended. That is consistent with substantial fixed startup/JIT work,
but noisy JVM measurements do not establish attribution. Add a matched size
sweep or warm in-process diagnostic if quantifying it. The JVM is also Oracle
GraalVM and already detects host CPU features; GOAMD64=v1 still permits runtime
dispatch in some Go library paths, so build targets are not direct equivalents.

The Java parent returns when worker stdout closes, without waiting for worker
termination. This applies to both adapters and can exclude mmap/process cleanup
from the reported latency. Keep that pinned reference, but add diagnostic modes
that separately timestamp complete output and worker reaping. Running `--worker`
directly includes worker exit and removes one launch, so that difference alone
does not isolate cleanup. For compiler attribution, use isolated builds with one
flag changed, paired exact-output timing on both corpora, and code-generation or
allocation evidence. A persistent-JVM/direct-worker diagnostic measures a
different execution contract and must be labelled accordingly.

### Ordered implementation experiments

1. **Recover standard performance with exact one-/two-word lookup.** Separate
   cold insert and long-name handling from the hot home-slot hit, reuse the
   scanner's loaded words/masks, and verify bounds checks/inlining. Start with
   one isolated change: split the existing robust table's uncommon insertion,
   collision and long-name paths out of its common hit path. Keep its hash,
   32K capacity, 40-byte entry layout, parser, reader and sixteen workers fixed.
   Inspect compiler inlining and bounds checks, then compare with both the
   original table prototype and retained baseline on both full corpora. The
   goal is to remove the measured 3.28% standard regression while preserving
   the 40.46% extended improvement; neither outcome is assumed. Only then compare
   eight-byte fingerprint plus length with masked name-plus-semicolon identity
   up to fifteen bytes; test unconditional second-word loads with one bounded
   tail fallback separately from name-length branching. Preserve full long-name
   hashing and exact equality. Compare compact pointer-free entries with cold
   owned-name offsets against the existing 40-byte entry; assess entry size and
   probe distribution before increasing capacity. Sparse/adaptive table size
   and probe stride are separate candidates, not an automatic Java-size copy.
   Add a separate **compact prefix directory** experiment after exact-word
   lookup: reuse parser-loaded words to route to dense station IDs/statistics,
   keep exact name/length verification, and switch crowded prefix groups to
   full hashing. Test an arena-backed compressed radix trie separately at
   lower priority; avoid a Go map or allocation for every node. Preserve the
   arbitrary-name and shared-prefix input contract. See the prefix analysis below.
2. **Integrate the bounded one-word decoder into that hot path.** Keep the
   validated numeric implementation and fallback while avoiding repeated scans
   or word packing. Both full corpora must pass correctness and timing; gains
   are not additive. Check generated code on amd64 and ARM64. Train candidate-
   specific PGO only after hot paths are stable; baseline PGO did not win across
   corpora. An independent GOAMD64=v3 screen on this supported CPU tests the
   build-target difference without introducing SIMD or claiming parity with
   GraalVM's `-march=native`.
3. **Batch delimiter masks across several rows.** The separately corrected
   LE reference is faster than native Java on the standard corpus, including
   complete process cleanup; its 64-byte scan reuses semicolon bits across
   several rows. Move this experiment ahead of the I/O rewrite. Test portable
   SWAR mask reuse first, using an exact per-byte mask rather than assuming a
   zero-detection mask identifies every byte independently. Widening a scanner
   that still restarts for each
   row is a different experiment. Keep parser/table/I/O fixed and retain
   full-name hashing. The external whole-program win does not isolate SIMD's
   contribution or predict the Go gain.
4. **Explore Go's new SIMD support after the scalar batching control.** The
   installed Go 1.27.1 provides experimental `simd` and `simd/archsimd`, enabled
   with `GOEXPERIMENT=simd`; preserve this machine's existing experiment with
   `GOEXPERIMENT=nodwarf5,simd`. Build a matched scalar control with the same
   flags. First replace only the 64-byte delimiter-mask generator with two
   AVX2 32-byte loads, equality comparisons and `Mask8x32.ToBits`; retain the
   same mask-consuming loop, table, decoder, I/O and worker count. Dispatch
   once per worker using guarded CPU-feature paths and keep bounded scalar
   tails. Inspect generated instructions, bounds checks and register spills;
   compare full-program timings, instructions/cycles, allocations and waits.
   Explore portable `simd` and ARM64 NEON separately: their current masks lack
   the same direct scalar `ToBits` reduction, so measure reduction cost and
   retain SWAR fallback. SIMD key equality/hash work follows only if profiles
   justify it. See [Go's architecture-specific SIMD design](https://go.dev/blog/archsimd)
   and the retained API audit in `external/lehuyduc/go-transfer-plan.md`.
5. **Interleave two/three cursors within the current chunk topology.** Split
   chunks at newlines, stage word loads/masks, lookups, numeric decodes, and
   sequential updates across cursors, then drain remaining rows through the
   scalar loop. Test one, two, and three lanes with the same parser/table/I/O.
   Updates must preserve earlier changes when lanes resolve to the same station;
   do not load all statistics and write back stale copies. Verify boundary and
   same-station cases, and instrument total consumed rows. Matching rounded
   aggregates alone can conceal dropped rows: the reviewed Ragnar scanner loses
   440 standard rows while still matching its billion-row output oracle.
   This is moved ahead of I/O because the native source and
   current worker traces support an instruction/dependency experiment first.
6. **Recycle bounded chunk buffers after keys are owned.** Retain current
   producer/channel topology first, return each buffer only after all lanes
   finish, and measure allocated bytes, peak/live heap, GC, wall time, and
   channel overhead. This isolates allocation savings from an I/O rewrite.
   Borrowed unsafe strings must never point into recycled storage.
7. **Compare parallel reusable ReadAt ranges and mmap.** Keep the surviving
   hot loop unchanged, align ranges to newlines, and check every row is consumed
   exactly once. Include setup and cleanup in Go timing. Test atomic 2 MiB claims
   separately from static ranges. For mmap, names may borrow mapped bytes if the
   mapping remains alive through merge/output; owned copies are mandatory for
   recycled buffers, not for every architecture. The old mmap comment is not a
   matched experiment against current code. Reprofile producer/wait/GC behavior
   after the lookup/lane changes before attributing any I/O gain.
8. **Reprofile and then choose compiler/architecture work.** Revisit worker
   count and PGO after dependency/cache behavior changes. Compare scalar,
   batched SWAR, and SIMD generated code and profiles before extending SIMD to
   hashing/equality. Go 1.27 experimental SIMD supports amd64 and ARM64, but
   bounded tails and a scalar fallback remain necessary. No Go SIMD or
   GOAMD64=v3 performance claim is made.

### Prefix directory and compressed radix trie (2026-10-06)

Added at the user's request. Inspecting the checksum-backed oracle station
sets, 4-byte prefixes uniquely distinguish 88.86% of standard keys and 91.84%
of extended keys; 8-byte prefixes distinguish 99.52% and 99.92%. These are key
fractions, not row frequencies or speed measurements. Two-byte prefixes are
less selective: 15.01%/4.96% of keys are alone in their prefix group.

The compact directory candidate should use cached words and small station IDs,
with statistics in dense arrays and owned names kept separately. Full-name
verification remains mandatory; adapt crowded groups to full hashing because
the legal 10K long-UTF8 stress input shares its first eight bytes across all keys.
Measure cache/instruction cost, allocation/GC behavior and both full corpora
against the surviving exact-word flat table. Do not bundle a new parser or I/O
change into this comparison.

A compressed radix trie is a separate lower-priority candidate. Use contiguous
arena storage and integer child indexes, compressed paths and bounded byte/word
comparisons. A conventional byte-at-a-time trie adds dependent accesses to a
table already averaging about 1.005/1.209 probes. Prefix sharing saves dictionary
storage but does not remove the work of reading each input name; current total
allocation is dominated by chunk buffers. Retain the existing output ordering.

Evidence: `results/research/20261006/prefix-analysis.json` and
[the Adaptive Radix Tree paper](https://db.in.tum.de/~leis/papers/ART.pdf).

Before acceptance, run the declared null protocol (2% for broad comparisons),
then five measured runs per A-B-B-A block with two warmups and exact outputs.
Retain every block, drift/CV, hashes, source patch, and memory/trace summaries.
Require a repeatable meaningful gain and verify the other corpus's existing
nonregression rule; repeat close effects in another controlled window. The
table alone currently fails that rule. Linux ARM64 cross-building the parser
passed; native Mac correctness/performance remains unmeasured.

Evidence: `station-table/proposal.txt`, `parser/README.md`, `pgo-workers/README.md`,
`hash-distribution.json`, `architecture-research.md`, and
`root/runtime-summary.json`, plus the focused source/build review in
`native-java/`, under `results/research/20261005/`. Full sources,
binaries, profiling helpers, raw JSON, profiles, traces, and replay scripts are
retained there. Copy this ignored directory separately for another machine.

Primary sources: [Go PGO guidance](https://go.dev/doc/pgo),
[Go 1.27 SIMD notes](https://go.dev/doc/go1.27#standard-library),
[Topolnik's optimization report](https://questdb.com/blog/billion-row-challenge-step-by-step/),
[Hoyt's Go report](https://benhoyt.com/writings/go-1brc/), and the pinned sources
listed in `third_party/README.md`. Native Thomas Java starts a child and returns
its parent on stdout EOF before child cleanup finishes; its reference includes
that result-latency advantage. Reaching that number is not solely a compiler
comparison, and cross-machine absolute timings are not evidence of a gain.

The focused build audit links [GraalVM build-output interpretation](https://www.graalvm.org/jdk21/reference-manual/native-image/overview/BuildOutput/)
for ML-inferred profiles and [GraalVM memory management](https://www.graalvm.org/jdk21/reference-manual/native-image/optimizations-and-performance/MemoryManagement/)
for Epsilon's allocation/collection contract. Exact installed 21.0.2 option help
and sources, preserved in `native-java/`, take precedence over maintained web
documentation for the pinned experimental flags.

## External implementation comparison (2026-10-05)

Three subagents built and audited the user-supplied LE SIMD, Ragnar Rust,
Danny C and HappyCerberus C++ repositories. Clean pinned checkouts, original
failures, separate correction patches, release binaries, provenance and raw
evidence are retained under `results/research/20261005/external/`. The entry
point is its `README.md`; the specific transfer matrix is
`lehuyduc/go-transfer-plan.md`. No new foreign implementation has been added
to the production adapters; production Go source is unchanged.

LE and Happy's originals match both generated full oracles but fail legal edge
cases; Danny's original fails both full corpora. Separately corrected LE,
Danny and Happy 09 pass all 24 common sample/stress/adversarial cases and both
full oracles. LE passes four additional rounding, prefix-equality, aligned-EOF
and empty-input cases. Corrected Happy 08 also passes exact-output validation,
but 09 is the representative timed here. Corrections are explicit and are
included in the timed artifacts, including LE's short-key length check.

Ragnar's original fails both full outputs. A narrow merge fix restores standard
output, but independent diagnostics count 440 missing standard rows and
148,080 missing extended rows. Extended values/order remain incorrect. Its
warm single-run internal times are excluded from the correctness-qualified
ranking; phased cursors and delimiter scanning remain useful source ideas.
Matching rounded output alone cannot validate scanner coverage.

Separate diagnostic builds of corrected LE, Danny and Happy 09 independently
count exactly 1,000,000,000 rows on each full corpus, preserve exact output,
and exit successfully through complete cleanup. Six results are in
`external/counts/{standard,10k}/summary.json`; timed artifacts were unchanged.

The completion-aware research race reports both **result return** and
**complete process**. Native Java, LE and Danny return results before their
worker finishes mapping/process cleanup. An initial process-group check missed
Java threads after the leader became a zombie; exact syscall tracing exposed
the error. The replay uses a process-local Linux child subreaper and blocking
waits until every descendant is reaped, before the next variant launches.
Initial measurements remain preserved and are superseded by these replay
numbers. The production `bench.sh` foreign-reference path still needs this
lifecycle isolation before another interleaved forked-reference comparison.

| Implementation | Standard result | Standard complete | 10K result | 10K complete |
| --- | ---: | ---: | ---: | ---: |
| LE SIMD, corrected | 0.649 s | 0.970 s | 1.306 s | 1.697 s |
| Native Java | 0.746 s | 1.071 s | 2.070 s | 2.462 s |
| Rust mtopolnik | 1.387 s | 1.387 s | 2.028 s | 2.029 s |
| Danny C, corrected | 1.412 s | 1.739 s | 4.228 s | 4.624 s |
| C matt-re | 1.835 s | 1.835 s | 3.171 s | 3.171 s |
| JVM Java | 1.932 s | 1.945 s | 3.262 s | 3.348 s |
| Happy 09, corrected | 1.941 s | 1.941 s | 60.935 s | 60.935 s |
| Go retained baseline | 2.609 s | 2.609 s | 5.822 s | 5.822 s |

These are trimmed means from ten rotated/reversed measured rounds and two
warmup rounds, with immutable artifacts and exact outputs after every execution.
Happy's slow extended case has six measurements in paired positions; others
have ten. Both corpora remain 100% resident. LE selects 16 workers on standard
and 8 on extended; Danny and Happy use 16. Worker count, compiler, layout,
parser and I/O differ, so this does not attribute a whole-program gain to SIMD.

The replay retains the original null blocks: standard maximum drift 0.949%
passes the declared broad 2% gate; extended **2.026% fails**, and all extended
numbers are exploratory. No control was rerun until it passed. Reused controls
make this a research screen rather than production acceptance. Standard JVM
CV is 3.54%; extended C/JVM CV is 3.96%/4.24%, so close rankings need another
controlled window. The broad LE advantage survives complete-process timing:
it uses 62.8% less standard time and 70.9% less extended time than Go, and
9.4%/31.1% less than native Java. Extended percentages share the same caveat.

Separate standard hardware-counter/memory diagnostics show about 300 user
instructions per row for Go, 72 for LE and 104 for native Java. Go and LE IPC
are similar (1.86/1.87), while native is 2.27. Reducing repeated work matters
alongside dependency overlap. All six events ran about 83% of enabled time
and were scaled; these are broad estimates, not fine attribution. User-only
events exclude kernel I/O/teardown. Sampled anonymous PSS is roughly 479 MiB
for Go, 133 MiB for LE and 15 MiB for native, including the diagnostic launcher;
file-backed mmap memory and sampled lower bounds need separate interpretation.
The earlier Go allocation, heap, goroutine/channel and scheduling profiles
remain the better evidence for allocation volume and precise wait attribution.

Extended counters reinforce the lookup case: Go is about 412 instructions per
row at 1.06 IPC, versus LE's 79 at 1.88. Happy executes roughly 5,990
instructions per row, consistent with its independently simulated probe chains.
Sampled anonymous PSS is 508/69/24 MiB for Go/LE/native. Every instrumented
full output and child status passed. Thread snapshots confirm LE's 16/8 worker
selection and show Happy with 16 runnable threads despite its slowdown;
parent output waits and runtime/join futexes do not establish application idle
fractions. Raw timestamps, wchan and schedstat are retained in
`external/diagnostics/`; nominal 100 ms sampling has additional scan overhead.

The revised ordered plan above moves **multi-row delimiter-mask reuse** early:
LE scans 64 bytes once and consumes multiple semicolon bits, rather than
rescanning from every row. Compare an exact SWAR mask generator against AVX2
generation under the same table/parser/I/O before combining phased cursors.
Go 1.27 `simd/archsimd` exposes AVX2 compare/movemask directly, enabled with
`GOEXPERIMENT=nodwarf5,simd` here and CPU dispatch once per worker. The portable
and ARM64 mask APIs do not currently offer the same scalar `ToBits` reduction;
retain SWAR and measure NEON reduction separately. No Go SIMD candidate has
yet been built or measured. Native Java's <=15-byte exact identity, inlinable
home-slot hits and independently drained cursors remain central.

Danny's sparse-index/dense-owned-record layout is worth an isolated experiment.
Happy's actual extended keys average 153.75 successful probes (p99 1,257,
maximum 2,153), explaining substantial lookup cost despite a large table;
do not transplant its multiply-by-seven hash. Preserve full-name hashing,
exact equality and bounded mapping tails. Own keys before buffer reuse,
profile allocations/waits after hot-loop changes, and then compare independent
ReadAt ranges versus mmap including cleanup. Revisit workers/PGO on the new
kernel instead of extending baseline conclusions automatically.

Happy's pinned source is MIT; LE, Danny and Ragnar's application trees have
no declared source license. Independently implement the algorithmic ideas.
Primary sources and provenance links are in the per-repository reports, with
[Ragnar's development article](https://curiouscoding.nl/posts/1brc/) for ILP
history and [Go's architecture-specific SIMD design](https://go.dev/blog/archsimd)
for API/dispatch context. Copy ignored sources, builds, reports and corpus
manifests explicitly when transferring this research.


## Robust table hot/cold split implementation (2026-10-05)

The isolated split is implemented under
`results/research/20261005/table-hotpath/`. Baseline, original robust table and
split release artifacts have matching Go1.27.1 / GOAMD64=v1 / nodwarf5 / PGOoff
flags. The reader computes the original fingerprint once; `homeHit` inlines,
while insertion/collision probing and long hashing remain cold functions. The
32K capacity,40-byte entry, owned keys, parser, reader and16workers are unchanged.
The compiler and assembly confirm that the common loop avoids the original
per-row lookup/hash calls. No decoder, two-word key or SIMD change is bundled.

All three pass tests, vet, race,28small/adversarial oracles and both full1B
oracles. Original and split cross-build for Linux ARM64. Additional differential
hash, forced probe wrap/merge and borrowed-name ownership tests pass. Separate
count artifacts independently consume exactly1,000,000,000rows on each full
corpus for all three versions.

Separate grouped hardware counters (both100%scheduled) show original→split
instructions/row291.354→273.126 standard and317.861→301.960 extended, with
IPC1.767→1.883 and1.525→1.567. These diagnostics explain generated-code changes;
they do not establish accepted speedups. Allocation totals remain13.822/17.099GB
standard/extended, almost all chunk buffers. Sampled RSS is below350MB in these
owned-table runs, with post-GC live heap about0.63MB. GC capacity is2.2–2.6%.
Worker traces average12.69–13.46Running out of16; total time with every worker
waiting while the producer is in a syscall is only0.3–1.2ms. Channel receive
waits overlap across workers; trace/testing sleeps and scheduler locks must not
be counted as application sleeps or a shared table lock. The faster loop exposes
some reader/channel idle capacity, without a measured reason to change buffering
or worker count in this experiment.

The initial standard null passes1.225%, but its promising−7.519% pooled
confirmation has−3.815% baseline order drift and remains provisional. The
initial extended null fails3.803%; no extended candidate timing followed.
Continuous real-host monitoring was added after these windows. Early guard
attempts are retained, including a cold-cache preparation false positive and a
one-page paging false positive; the corrected guard distinguishes warmup from
timing and records tiny page-ins while rejecting paging above64KiB/s. Fresh
monitored standard window5 passed preflight but stopped during its fourth null
block when Discord used0.273CPUcores, above the declared0.25per-process limit.
Its first three blocks were2.577/2.579/2.589s; no candidate timing followed.
Those earlier windows did not justify adoption. The user authorized a fresh
quiet window on 2026-10-06; the paired acceptance and promotion are recorded below.
See the local experiment README for all controls, interruptions and guard verification.

Evidence: `table-hotpath/README.md`, `metadata.json`, source snapshots/diffs,
compiler/assembly logs, `validation/report.json`, diagnostic count/perf reports,
`runtime-summary.json`, raw timing reports and their `.quiet.json` host monitors.


## Accepted robust table and hot/cold split (2026-10-06)

Promoted to `main.go`, adapted `main_test.go`, and new `hotpath_test.go` after
paired-corpus acceptance. The change replaces the production chained table with
32K inline 40-byte entries, complete short fingerprints, owned names, exact
long-name equality and an inlined home-slot path. Parser, numeric decoder, I/O,
worker count and output formatting remain the measured baseline implementations.
The hot/cold split itself preserves the original robust prototype's hash/layout.

| Corpus | Production baseline mean | Split mean | Runtime reduction | Fresh null max drift |
| --- | ---: | ---: | ---: | ---: |
| Standard 1B | 2.587160 s | 2.443863 s | 5.539% | 0.062% |
| 10K-station 1B | 5.744953 s | 3.309773 s | 42.388% | 0.962% |

Means pool the ten measured runs per variant from baseline/split/split/baseline
blocks, each with two excluded warm-ups and five measured runs. All outputs are
exact; release hashes match before/after; both corpora are 100% resident. Standard
baseline/split order drift is -0.061%/-0.322%; extended is +0.487%/-1.735%, within
the declared 2% limit. Adjacent-pair gains agree in sign. Both gains exceed 3%,
neither corpus regresses, and the adoption decision does not depend on a small
or borderline gain. Six permutation rounds separately preserve original-table
attribution; the earlier provisional 7.5% standard result is not used.

Accepted evidence: `table-hotpath/timing/standard-window-9/` and
`10k-window-3/`, their `.quiet.json` monitors, and
`results/research/20261006/table-hotpath-acceptance.json`. Both host preflights
passed, and no measured-run monitor violation occurred. Interrupted attempts
remain retained with their harness hashes; they were stopped before candidate
timing. The guard now distinguishes compressed RAM page-ins, excluded warm-ups,
and between-run file bookkeeping from measured-process intervals. CPU thresholds,
pressure thresholds and the 2% null gate remain declared; I/O gate coverage is
recorded explicitly rather than claimed for bookkeeping intervals.

The repository build passes tests, vet, race and Linux ARM64 cross-build, all
28 small/adversarial outputs, and both complete 1B oracles. Separate diagnostic
artifacts also independently count exactly 1B rows for all three versions on
both corpora. The local ignored `results/go.mod` module boundary keeps mutually
exclusive archived `.go` prototypes out of `go test ./...`; copy it with the
research archive. Source, tests, benchmark tooling and this summary form the
accepted baseline; raw corpus and measurement artifacts remain ignored.

Next lookup experiment: exact two-word identity and parser-word reuse on the
accepted table. Compact prefix routing and a compressed radix trie remain
separate backlog entries; numeric decoding, scalar batching, Go SIMD and I/O
experiments retain their separate comparisons.


## Accepted parser-word reuse (2026-10-06)

Applied on top of committed robust-table baseline `f8a90a5`. The parser peels
its first SWAR block and returns the already loaded first station word. Names
shorter than eight bytes are masked to exclude the delimiter, temperature and
following row; complete inputs shorter than eight bytes use the bounded word
fallback. Short lookup hashes that cached word, and long hashing continues from
it. The fingerprint mapping, 32K/40-byte table, owned keys, exact long equality,
numeric decoder, reader/channel topology and sixteen workers remain fixed.
This experiment does not add a second cached word or change key identity.

`stationPos` had no production caller: its only use was an obsolete benchmark.
The function is removed and the benchmark now measures `stationFingerprint`.
Inline comments in `main.go` explain the new parser contract, byte masking,
hashes, table entries, ownership and probing, with concrete input/output examples.

| Corpus/window | Baseline mean | Reuse mean | Runtime reduction | Fresh null max drift |
| --- | ---: | ---: | ---: | ---: |
| Standard 1B / 6 | 2.444292 s | 2.311884 s | 5.417% | 0.299% |
| 10K-station 1B / 2 | 3.267638 s | 3.172713 s | 2.905% | 0.259% |
| 10K-station 1B / 3, independent repeat | 3.269167 s | 3.140641 s | 3.931% | 0.777% |

Each window uses a fresh real-host preflight and continuous monitoring,
60-second preconditioning, baseline A/B/B/A null blocks, six alternating
variant-order screen rounds and baseline/reuse/reuse/baseline confirmation.
Each null/confirmation block has two excluded warmups and five measured runs;
means pool ten measurements per variant per window. The predeclared null and
confirmation-order bound is 2%; the CPU guard remains 0.5 background cores total
and 0.25 per process. Standard baseline/reuse order drift is -0.013%/-0.016%;
extended window 2 is -0.556%/+0.009%, and repeat is -0.146%/-0.667%.
Adjacent-pair gains agree in sign. The first extended effect fell below the
predeclared 3% replication threshold, so window 3 independently repeated it;
both complete windows are retained and reported. No accepted window has a
measured-run host violation. Outputs are exact, artifacts unchanged, and active
inputs remain 100% resident. Ratios are within-window comparisons.

Earlier windows remain excluded: standard preflight 1 failed I/O pressure;
windows 2–4 stopped for CPU noise; window 5 passed its null but Discord interrupted
the screen. Extended window 1 passed its null but editor/short-lived CPU work
interrupted the screen. After the user paused Chromium, the complete windows
above passed with unchanged limits. No interrupted screen is adoption evidence.

Separate grouped counters, both 100% scheduled, show instructions/row
273.409→254.401 standard and 302.044→284.906 extended, with IPC 1.912→1.976
and 1.564→1.639. `homeHit` remains inlined. The caller shrinks, while the parser
stack frame grows from 48 to 56 bytes; compiler/assembly and differential hash
tests are retained. Allocation volume remains 13.822/17.098 GB, about 99.8% chunk
buffers, with approximately 0.63/0.65 MB live heap after GC. Candidate runtime
GC/idle capacity is 2.91%/10.78% standard and 2.91%/15.86% extended in instrumented
diagnostics. Cumulative worker channel waits overlap; time with every worker
waiting while the producer is in a syscall is only 0.36/0.70 ms. Runtime mutex
and profiling-harness sleeps do not establish an application table lock or sleep.

Both variants pass unit tests, vet, race, Linux ARM64 cross-build, all 28 small/
adversarial exact outputs and both checksum-backed full 1B oracles. Separate
diagnostic artifacts independently count exactly 1B rows on both corpora for
each variant. New tests cover name lengths 1–100, UTF-8, NUL/length distinctions,
shared first words, temperature forms, following-row independence and truncated
tails. The documented repository source has the same executable Go AST as the
measured candidate; comments are added only to `main.go`.

Evidence: local ignored `results/research/20261006/parser-word/`, including
`acceptance.json`, source/binary hashes, compiler/assembly, `validation/`,
`diagnostics/`, `runtime-summary.json`, complete timing windows and host monitors.
Next isolate exact two-word identity, reusing the second scanner word while
preserving full-name fallback. Prefix routing/trie, bounded numeric decoding,
scalar mask batching, Go SIMD and reader/allocation work remain separate steps.


## Second-word reuse and exact matching rejected (2026-10-06)

Implemented and compared four immutable variants on committed `51e20a1`:
baseline (40-byte entries); second-word reuse with unchanged fingerprint and
full long equality (40 bytes); the same reuse with an unused field as a footprint
control (48 bytes); and reuse with a cached second name word and exact matching
through fifteen bytes (48 bytes). The second SWAR block is peeled and its load
reused; bounded short-row fallbacks and name-length masking exclude row suffixes.
All variants retain hash mapping/probes, 32K capacity, owned keys, numeric decoder,
reader/channels, sixteen workers and output. Production remains `51e20a1`.

Exact matching uses the existing full fingerprint, normalized second word and
length. For 9..15-byte names the hash steps are invertible for a fixed second
word, so the tuple identifies the complete name. Names sixteen bytes and longer
retain full string equality. Differential tests include every length through
100, UTF-8, unaligned starts, truncated rows, suffix independence, zero-padded
tail/length distinctions and ownership after input mutation. Deliberate legal
printable-name hash collisions verify rejection for fifteen-byte keys and the
full-string fallback for twenty-three-byte keys. All home-hit helpers inline.

| Comparison | Standard 1B | 10K-station 1B | Decision |
| --- | ---: | ---: | --- |
| Reuse vs committed baseline, ABBA | -0.893% runtime | +1.058% runtime | No qualifying gain; extended nonregression fails |
| Exact vs committed baseline | -2.869%, ABBA | +2.719%, balanced screen | Reject; no extended precision confirmation |
| Exact vs reuse | -0.973%, ABBA | +2.988%, balanced screen | Reject extended pair |
| Layout control vs reuse, balanced screen | -1.028% | +1.221% | Footprint/code-generation attribution only |
| Exact vs layout control, balanced screen | -0.740% | +1.746% | Exact-check gain is not the whole standard benefit |

Negative means less runtime. Both windows pass fresh null controls: maximum
drift 0.271% standard and 0.590% extended, against the declared 2% limit. Both
real-host preflights and continuous monitors pass with no measured violations.
Inputs remain fully resident, releases unchanged and every measured output exact.
Eight cyclic/reversed screen rounds put every variant in each position twice.
Qualifying pairs use ABBA with two excluded warmups and five measurements per
block; confirmation order drift stays below 0.62%. Extended exact pairs exceed
the predeclared 1% screen nonregression bound and are not precision-rerun.
Reuse's standard confirmation is below the 1% minimum gain and extended is
slower; exact's standard gain cannot compensate for its extended regression.
Small-effect replication is for qualifying adoption, not retrying rejected
candidates until they win. Neither prototype is promoted.

Independent diagnostic counts also establish actual row coverage: at most
8/15/16 bytes covers 65.857%/97.578%/99.031% of standard rows and
78.150%/83.700%/84.270% of extended rows. The extra exact shortcut applies to
31.72 percentage points of standard rows but only 5.55 points of extended.
This aligns with the different effects across corpora; it does not attribute
all runtime differences to equality or table stride.

Separate grouped user counters, both 100% scheduled, show baseline/reuse/layout/
exact at 254.308/253.603/256.753/247.073 instructions per row standard and
284.596/290.707/293.079/291.640 extended. Corresponding IPC is
1.974/2.022/2.078/2.041 and 1.635/1.683/1.700/1.643. Returning another word
adds masking, register and branch work on rows that do not benefit from the
shortcut. Parser stack grows 56→64 bytes; exact caller grows 216→224 bytes.
Home-hit inline cost is 40 for baseline/reuse/layout and 55 for exact.

Allocation volume is 13.822 GB standard and 17.098 GB extended for baseline/
reuse, versus 13.826/17.103 GB for layout/exact; almost all is chunk buffers.
The entry field adds about 4.2 MB across sixteen tables. Post-GC heap remains
about 0.63 MB; sampled RSS ranges 280–353 MB. Instrumented GC capacity spans
2.79–3.47%, idle capacity 9.84–15.31%. All-worker wait while the producer is
in a syscall stays below 1.22 ms. Overlapping channel waits, runtime mutexes
and instrumentation sleeps do not indicate a shared table lock or application
sleep; there is no measured reason to change reader/channel topology here.

All four pass tests, vet, race and Linux ARM64 cross-build, all 28 small/adversarial
oracles each, both paired-checksum full 1B oracles each and independent exact 1B
counts on both inputs. Evidence is retained under local ignored
`results/research/20261006/second-word/`: snapshots, release/build hashes,
compiler/assembly, row-length counts, CPU/allocation/heap/GC/block/mutex/goroutine/
scheduling profiles, complete timing/host records and `decision.json`.

Next test the previously validated bounded temperature decoder on retained
`51e20a1`, keeping table/hash/I/O fixed. Exact word lookup remains a later revisit:
isolate an inlinable 9..15-byte fingerprint path and assess a cold tail array
separately from entry stride/extra-return overhead. Neither alternative is
implemented or measured. Prefix routing/trie, scalar delimiter batching and Go
SIMD remain on the ordered backlog.


## Accepted bounded temperature decoder (2026-10-06)

Applied to retained first-word baseline `51e20a1` after isolated validation and
paired-corpus timing. Port the previously validated temperature-word arithmetic
onto the accepted parser/table. One eight-byte load finds the decimal position,
aligns digit nibbles, forms integer tenths with a multiply and restores the sign.
For `-12.6`, the decimal mask selects bit 28, sign is -1 and magnitude is 126.
The load occurs only with eight in-slice bytes after the semicolon; short final
rows retain `parseNumber`. The exact newline remains checked, and bytes from
the following row never enter the digit mask. The existing name/first-word
normalization block moves before numeric decoding so both returns share it.

Table capacity/layout/hash/probing, owned keys, first-word reuse, reader/channel
topology, sixteen workers, integer aggregation/rounding and output are fixed.
There is no second-word station cache, SIMD, target-flag change or buffer change.
New inline explanations and data examples are scoped to `main.go`.

| Corpus/window | Baseline mean | Decoder mean | Runtime reduction | Fresh null max drift |
| --- | ---: | ---: | ---: | ---: |
| Standard 1B / 1 | 2.291744 s | 2.210153 s | 3.560% | 0.548% |
| 10K-station 1B / 1 | 3.130962 s | 3.071864 s | 1.888% | 1.172% |
| 10K-station 1B / 2, independent repeat | 3.142725 s | 3.022448 s | 3.827% | 0.464% |

Each window uses live-host preflight/continuous monitoring, unchanged CPU/IO/
paging limits, sixty-second preconditioning, baseline A/B/B/A null blocks,
six balanced alternating screen rounds and baseline/decoder/decoder/baseline
confirmation. Null and confirmation blocks have two excluded warmups and five
measurements each; means pool ten measurements per variant. All null/order
checks pass the predeclared 2% bound and all adjacent pairs favor the decoder.
Standard baseline/decoder order drift is +0.545%/-0.973%; extended window 1 is
-0.219%/-0.437%, repeat -0.042%/-0.698%. No measured host violations occur,
artifacts are unchanged, active inputs fully resident and all outputs exact.
The first extended gain is below the 3% replication threshold and is therefore
independently repeated; both complete windows are retained rather than selecting
the larger effect. These are within-window comparisons, not cross-session gains.

Both variants pass unit tests, vet, race and Linux ARM64 cross-build. New
exhaustive tests independently verify all 1,999 values -999..999 tenths plus
the `-0.0` spelling, Unicode and 1..100-byte names, unaligned starts, zero..eight
following bytes, every truncated row prefix and following-row independence.
All 28 small/adversarial outputs and both paired-checksum full 1B oracles pass;
separate artifacts independently consume exactly 1B rows on both corpora.
The promoted repository source matches the measured candidate's executable
Go AST. Its normal adapter is rebuilt and passes the same small/full oracles.

Separate grouped user counters, both 100% scheduled, show instructions/row
254.357→236.523 standard and 285.162→266.826 extended (7.01%/6.43% reductions),
with IPC 1.960→2.033 and 1.675→1.734. Lookup remains inlined and the parser
stack stays at 56 bytes; the caller is unchanged. Numeric position and value
now derive from one word rather than repeated sign/width branches and byte loads.

Allocation volume remains 13.822/17.098 GB, dominated by chunk buffers, with
about 0.63 MB live heap after GC. Sampled candidate RSS is 324/267 MB. Instrumented
decoder GC/idle capacity is 3.33%/15.28% standard and 3.26%/18.04% extended.
Time with every worker waiting while the producer is in a syscall is only
0.26/0.60 ms. Cumulative channel waits overlap; profiling/testing sleeps and
runtime locks do not establish application sleeps or a shared station-table lock.
No reader/channel or allocation strategy is changed in this experiment.

Evidence: local ignored `results/research/20261006/temperature-word/` contains
source/release hashes, compiler/assembly, exhaustive tests, full validation/counts,
CPU/allocation/heap/GC/goroutine/block/mutex/scheduling profiles, all three raw
timing/host windows, `acceptance.json` and promoted build/oracle checks.
Next isolate scalar delimiter-mask batching across rows, preserving the surviving
decoder/table/reader and full-name hashing. Then compare a Go SIMD mask generator
against a flag-matched scalar control. Staged cursors, buffer reuse, I/O variants
and prefix/trie work remain separate experiments.
