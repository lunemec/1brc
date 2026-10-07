# 9. Ideas that did not win

Each row uses its own baseline and measurement period.
Negative percentages mean less runtime, while positive percentages mean more.
The [experiment log](../../EXPERIMENTS.md) keeps the original decisions and evidence limits.

## Early experiments

| Idea | Observed result | Why it stayed out |
| --- | --- | --- |
| Add a one-slot work-channel buffer | Standard runtime +15.65% | Queuing was worse in that whole-program test |
| Use a power-of-two 16,384-slot chained table | Standard -0.36%, unbalanced 10K -3.61% | The standard effect was weak. The 10K order was not balanced. |
| Hash eight bytes at once | Standard +0.70% | Extra hashing/layout effects did not yield a net win |
| Include the old hash's odd trailing byte | Standard +7.21%, 10K -6.19% | A slower corpus and lost inlining outweighed fewer collisions. |
| Fixed 32K inline open-addressed table | Standard -2.52% | The gain fell below the old 3% threshold. Later table work used a new baseline. |
| Semicolon-first standard-library parser | Parser microbenchmark +2.80% | Work stopped at the old microbenchmark threshold. No whole-program timing followed. |
| Fuse SWAR scan and hash | Microbenchmark -8.26%, standard whole run -0.92% | Run order affected the result. The whole-program effect was small. |

A microbenchmark measures one small part of the program.
Its result cannot establish a faster executable.
Early results without original timing archives remain historical summaries.

## Second-word reuse

Plain second-word reuse keeps 40-byte entries.
A separate layout control and the exact-matching variant use 48-byte entries.
Their results differ:

| Variant | Standard | 10K | Decision |
| --- | ---: | ---: | --- |
| Second-word reuse, confirmed | -0.89% | +1.06% | Reject |
| Exact matching, standard confirmation / 10K screen | -2.87% | +2.72% | Reject before 10K confirmation |

The larger layout adds 262,144 bytes per 32,768-slot table.
That cost applies to the layout control and exact matching, not plain reuse.
The [record](../../EXPERIMENTS.md#second-word-reuse-and-exact-matching-rejected-2026-10-06) keeps the separate comparisons.

## Mask batching before and after the pool

Before buffer reuse, scalar batching screens run 4.97% / 3.15% slower on standard / 10K.
The 10K screen also has -2.25% half-period drift and is not a precision comparison.
The first SIMD comparison gains only 0.861% / 0.047%, below the adoption thresholds.

After the pool, scalar batching still loses by 7.37% / 6.48% against matched-flag production.
AVX2 passes two independent normal-build comparisons per input.
[Part 8](08-pooled-avx2-scanning.md) shows all four results.

Retest an idea when its costs change, such as allocation or entry layout.
Keep the old result and compare against the actual current baseline.
Do not select favorable partial runs from periods that fail the host controls.

[← Previous](08-pooled-avx2-scanning.md) · [Back to the series](README.md)
