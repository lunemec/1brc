# 2. Learning to measure a real improvement

A harness runs benchmarks and compares their results.
The benchmark harness starts at
[`0f6e86d`](https://github.com/lunemec/1brc/commit/0f6e86d).
An adapter runs one implementation through a common interface.
Later commits add adapters for other languages.
A pinned version stays fixed during an experiment.
The commits also pin generator versions and add an independent oracle.

An oracle supplies an independently calculated expected answer.
Other harness changes add stress tests and separate files for each experiment.
A warmup runs the program before timed measurements start.
These changes also let each experiment set the number of warmups.

A null control measures one executable under different labels.
[`c890897`](https://github.com/lunemec/1brc/commit/c890897)
adds the null control and documentation for future experiment work.
Later tools monitor the computer continuously and test for changes across block order.

These changes improve the performance evidence.
They do not make the row parser faster.
The full instructions remain in the [main README](../../README.md).

## Measure the same bytes and the same answer

A corpus is a file of input measurements.
A workload is the input and work under measurement.
A generator can create a billion rows each time and still produce a different file on each run.
The generators here do not use a fixed random seed.
As a result, a row count and filename cannot identify a workload.

A checksum is a value that identifies file contents.
The harness records a cryptographic checksum for each retained input.
A baseline is the implementation used for comparison.
The harness also records the expected output from an independent Java baseline.
A window is one uninterrupted series of benchmark runs.
The accepted Linux windows in this series use these corpora:

| Corpus | Rows | Stations | File bytes |
| --- | ---: | ---: | ---: |
| Standard | 1,000,000,000 | 413 | 13,795,469,979 |
| 10K | 1,000,000,000 | 10,000 | 17,067,837,357 |

The exact Linux checksums are in [the committed timing extract](data/timings.json).
These records do not establish that the earlier Mac sessions used the same files.
The second corpus has more distinct station names.
This changes the station table's behavior, so it is a separate workload.
Both corpora contain the same number of rows.

The harness compares every timed output byte-for-byte with its oracle.
Small samples cover rounding and Unicode.
Generated stress cases cover long names, collisions, and large sums.
Later diagnostics also count rows and compare the totals before rounding.
Rounded final means can hide some skipped or duplicated observations.

## Build before the clock starts

A candidate is the new implementation under comparison.
Compile the baseline and candidate once before measurement.
Record each executable's hash before and after the timed runs.
Use the same executables throughout the comparison.

Runtime is the elapsed time for one program run.
The clock covers process launch through process exit.
This includes setup, file reads, parsing, the merge, formatting, and normal cleanup.
The exact-output comparison occurs after the clock stops.

The timing extract preserves two original fields:

| Field | Meaning |
| --- | --- |
| `seconds` | The original harness's launch-through-exit observation used for acceptance |
| `complete_process_seconds` | The observer's later completion point, including its child/cleanup bookkeeping |

The charts use `seconds`, as the historical acceptance analyses did.
The charts preserve that measurement boundary.
They do not use the earlier time when output becomes ready.
They also do not mix measurements from launchers that return before child cleanup ends.

A profile records where a program spends work.
Keep profile and counter runs separate from normal runtime measurements.
A counter probe counts events such as executed instructions.
These measurements can explain a change's effect.
Their runtime does not replace the comparison of normal executables.

## The null control: compare an executable with itself

Suppose a desktop becomes warmer or starts indexing halfway through a test.
Even an unchanged executable can appear faster or slower under a different label.
The null control uses the same executable under labels A and B:

```text
60 seconds conditioning
     ↓
A block → B block → B block → A block
2 warmups + 5 measured runs in each block
```

Conditioning runs the program before the measurement window.
Drift is a change in measurements over time.
The labels have no connection to different code.
Any difference comes from measurement variation rather than different code.

The analyses compare combined observations for each label.
They also compare the two halves of the window and each label's first and second blocks.

A gate is a limit that an experiment must pass.
Order drift compares early and late blocks.
The harness's default gate for small changes is 1%.
The Linux acceptance experiments here declared 2% for null and order drift.
Report the gate that the experiment actually used.

All fourteen included Linux confirmation windows passed their recorded null and order controls.
They also passed the tests for computer activity.
A preflight tests computer activity before measurement starts.
Failed preflights and interrupted windows remain in local archives.
They contribute no partial acceptance samples.
The extracts link the original reports of computer activity by checksum.

The extracts do not replace the complete monitoring reports.
Those reports contain the detailed record of computer activity.
The original reports remain necessary to examine that activity.

## A/B/B/A limits bias from the measurement order

Bias is a systematic error in a comparison.
Once the null control passes, A means the frozen baseline and B means the frozen candidate:

```text
baseline → candidate → candidate → baseline
 5 runs      5 runs      5 runs      5 runs
```

Each block also excludes two warmups.
Each implementation gets ten measured runs.
Both implementations appear before and after a neighboring alternative.
Averaging each implementation's two blocks balances a simple linear time trend.
It does not cancel arbitrary interference.
Tests for computer activity and block order remain necessary.

An arithmetic mean is the sum divided by the count.
Standard deviation measures the spread around the mean.
The coefficient of variation (CV) measures relative spread.
Its formula is $100s/\bar t$, where $s$ is sample standard deviation.
The robust-table change supplies this real example for the standard corpus:

| Variant | Measured runs | Arithmetic mean | CV |
| --- | ---: | ---: | ---: |
| Baseline | 10 | 2.587160 s | 0.412% |
| Table with split lookup | 10 | 2.443863 s | 0.724% |

A small CV for combined observations does not establish that block order was harmless.
That window records baseline/candidate order drift of `-0.061% / -0.322%`.
Both adjacent comparisons favor the candidate.
These comparisons support the result beyond the combined CV.

## Runtime reduction and speedup are different numbers

Runtime reduction expresses the saved time as a percentage.
Speedup is the ratio of baseline time to candidate time.
For a baseline time $t_A$ and candidate time $t_B$:

$$
\text{runtime reduction}=100\left(1-\frac{t_B}{t_A}\right)\%,
\qquad
\text{speedup}=\frac{t_A}{t_B}.
$$

The standard comparison for the robust table gives $1-2.443863/2.587160\approx0.05539$.
This means 5.54% less runtime.
Its speedup is about $1.0586\times$.
Throughput is the amount of work completed per second.
The speedup corresponds to 5.86% more throughput.
The chapters consistently report “runtime reduction” to distinguish these quantities.

The experiments require at least 1% improvement on standard or at least 3% on 10K.
Neither corpus can regress by more than 1%.
An effect of 1–3% requires a second independent quiet window.
Greater complexity can justify a larger required gain.

A confidence interval expresses uncertainty around an estimated effect.
These acceptance rules are engineering rules, not confidence intervals.
Passing a 2% drift gate does not establish arbitrary precision for every estimate below 2%.

## Read each comparison within its own window

The first temperature-decoder confirmation on 10K gives 1.89% less time.
Its independent repetition gives 3.83%.
The series retains both results.
Selecting only the larger result overstates the evidence.

The parser-word standard baseline is 2.444292 s in its window.
The previous table candidate was 2.443863 s in another window.
Their closeness is incidental.
The parser-word effect comes from its own baseline and candidate comparison.
It does not come from subtraction across sessions.

For each change, the figures show all included windows and individual observations.
The figures retain every observation and use arithmetic means.
An outlier is an observation far from the others.
The analyses remove no outliers.
The general harness also reports trimmed means, which omit some observations.
These acceptance charts do not use trimmed means.

## Reproduce the figures without rerunning the workload

The [timing extract](data/timings.json) keeps four confirmation blocks per window.
Each block contains five observations and the original variant names.
The [generated table](data/measurements.md) shows the recalculated means, percentages, null drift, and order drift.
The extract retains the report's archive path and checksums for the report and monitor.

Run this command from the repository root:

```sh
python3 docs/optimization-blog/tools/generate_assets.py
```

The script makes sure that the raw arrays reproduce the stored summaries.
It also makes sure that each block has the declared size and every output is exact.
It makes sure that the null and order results pass their declared limits.
The script regenerates figures from the existing observations.
It makes no performance claim for the computer where you run it.

Sources: [benchmark implementation](../../bench.sh),
[live-host monitor](../../benchmark_quiet.py),
[recorded experiment protocol](../../EXPERIMENTS.md#measurement-protocol).

[← Previous](01-correctness-and-early-changes.md) · [Next: eight-byte scanning →](03-swar-separator-scan.md)
