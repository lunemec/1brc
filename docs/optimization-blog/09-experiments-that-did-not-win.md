# 9. Useful ideas that did not win

A change can look useful and still make a program slower. This chapter records
those experiments. Their results explain the current design and the conditions
that can justify another attempt.

A baseline is the version used for comparison. A window is one period of
measurements under the same controls. The table below comes from the committed
[experiment log](../../EXPERIMENTS.md). Each row uses its own baseline and window.

Early results have summary evidence only. Later records include controls and
correctness tests. Results from different rows do not establish a ranking of
implementations.

## Early experiments

A microbenchmark measures one small part of a program. Some early experiments
used only that measurement. The table identifies those results separately from
tests of the whole program.

| Idea | Observed result | Why it stayed out |
| --- | --- | --- |
| Add a one-slot work-channel buffer | Standard runtime +15.65% | Queuing was worse in that whole-program test |
| Use a power-of-two 16,384-slot chained table | Standard -0.36%, unbalanced 10K -3.61% | The standard effect was weak. The 10K order was not balanced. |
| Hash eight bytes at once | Standard +0.70% | Extra hashing/layout effects did not yield a net win |
| Include the old hash's odd trailing byte | Standard +7.21%, 10K -6.19% | A slower corpus and lost inlining outweighed fewer collisions. |
| Fixed 32K inline open-addressed table | Standard -2.52% | The gain fell below the old 3% threshold. Later table work used a new baseline. |
| Semicolon-first standard-library parser | Parser microbenchmark +2.80% | Work stopped at the old microbenchmark threshold. No whole-program timing followed. |
| Fuse SWAR scan and hash | Microbenchmark -8.26%, standard whole run -0.92% | Run order affected the result. The whole-program effect was small. |

Signs in these tables show runtime change. A negative value means less time.
A positive value means more time. The accepted chapters express their gains
as positive runtime reductions.

The old 3% threshold differs from the later measurement rules. The record keeps
the rule used for each decision. A later rule does not change an earlier result.

A hash converts name bytes to an integer. A collision occurs when names share
a table position. The old hash omitted one byte from odd-length names. The
trailing-byte experiment included that byte to distinguish more names.

The hash runs on every row. Less collision work must outweigh the added
instructions and any compiler effect on the frequent path. The compiler decides
whether to copy a function's body into its caller. This process is called
inlining. The standard and 10K inputs can give these costs different weights.

## Larger name caches were not automatically better

After first-word reuse, we tested reuse of the second word. A word holds eight
bytes in this program. That variant kept 40-byte entries.

A separate layout control added an unused field and used 48-byte entries.
The exact-matching variant also used 48-byte entries. It cached another word
to replace string comparison for 9–15-byte names. The retained results were:

| Variant | Standard | 10K | Decision |
| --- | ---: | ---: | --- |
| Second-word reuse, confirmed | -0.89% | +1.06% | Reject |
| Exact matching, standard confirmation / 10K screen | -2.87% | +2.72% | Reject before 10K confirmation |

A 48-byte entry is 20% larger than a 40-byte entry. At 32,768 slots, that adds
262,144 bytes per table. This size difference applies to the layout control and
exact-matching variant. It does not explain the plain reuse variant's result.

Larger storage and more work on frequent paths can offset fewer equality loads.
These results reject the measured implementations. They do not prove that exact
word equality always loses. A different layout needs its own control and full
comparison.

Source: [second-word record](../../EXPERIMENTS.md#second-word-reuse-and-exact-matching-rejected-2026-10-06).

## Batch masks need to save more work than they add

A delimiter separates fields in a row. A mask records delimiter positions
as bits. Batching processes several rows from one mask. Scalar code uses
ordinary integer operations instead of vector instructions.

Before the pool, scalar 64-byte batching passed correctness tests. It ran
4.97% / 3.15% slower in the standard / 10K screens. A screen is an initial
comparison before a final measurement.

Drift is a change in timing across a window. The 10K baseline also had
-2.25% drift between the window's halves. The record does not treat that screen
as a precise final comparison.

SIMD instructions process several values at once. The first native SIMD attempt
used a baseline with matching build flags. It measured 0.861% / 0.047% less
runtime on standard / 10K. Those gains did not meet the adoption thresholds. The decoder baseline
therefore remained production.

After buffer reuse, scalar batching still ran 7.37% / 6.48% slower than the
control with matching flags on standard / 10K. AVX2 passed two independent final comparisons
against normal production for each input file.
[Part 8](08-pooled-avx2-scanning.md) includes all four results and the decision
to adopt it.

Sources: [scalar batching](../../EXPERIMENTS.md#scalar-delimiter-batching-rejected-2026-10-06),
[first SIMD attempt](../../EXPERIMENTS.md#go-simd-mask-validated-not-promoted-2026-10-06),
[pooled retries](../../EXPERIMENTS.md#pooled-mask-retries-and-simd-promotion-2026-10-06).

## What can be concluded

A microbenchmark measures one small part of a program. Its gain does not
establish a faster executable. A correct answer also does not imply a faster
program. Each change adds costs as well as removing work.

If a relevant condition changes, test an idea again. Examples include producer
allocation, entry layout, compiler inlining, and mask-generation cost. Keep the
old result. Measure the new version against its actual production predecessor.

If a window fails its host-activity controls, exclude it from performance claims.
Repeated attempts must not select a favorable result from an unchanged noisy
test. The experiment log records the conditions for another attempt.

The series stops at pooled AVX2, `0e5dd35`. Later work includes parallel reads,
mapped input, launchers, prefix routing, and staged cursors. Those changes need
their own chapters and clearly defined timing endpoints. The other session's
uncommitted work is outside this version.

[← Previous](08-pooled-avx2-scanning.md) · [Back to the series](README.md)
