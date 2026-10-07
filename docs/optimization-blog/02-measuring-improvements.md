# 2. Measure the same work

A baseline is the old version, and a candidate is the proposed change.
The [benchmark runner](../../bench.sh) compares fixed binaries against identical input bytes and an independent Java answer.
The [main README](../../README.md) contains the operating commands.

## Fix the input and timing boundary

A corpus is the complete test input.
The generators are unseeded, so a new billion-row file has different bytes.
The retained Linux tests use these files:

| Corpus | Rows | Stations | File bytes |
| --- | ---: | ---: | ---: |
| Standard | 1,000,000,000 | 413 | 13,795,469,979 |
| 10K | 1,000,000,000 | 10,000 | 17,067,837,357 |

The [timing data](data/timings.json) records their checksums and the binaries used.
Every measured output must match its expected answer byte-for-byte.
Diagnostic runs also compare row counts and unrounded statistics.

The clock covers launch through process exit, including setup, output, and cleanup.
The external output comparison happens afterward.
The stored clock fields distinguish program exit from later observer bookkeeping:

| Field | Meaning |
| --- | --- |
| `seconds` | The original harness's launch-through-exit observation used for acceptance |
| `complete_process_seconds` | The observer's later completion point, including its child/cleanup bookkeeping |

The charts use `seconds` and include every observation.
Profiled runs remain separate from release timings.
Earlier Mac timings use different files or measurement methods.

## Compare a program with itself first

A null control runs one unchanged binary under two labels.
Differences reveal measurement variation rather than code changes.
Each fresh control uses this sequence:

```text
60 seconds conditioning
     ↓
A block → B block → B block → A block
2 warmups + 5 measured runs in each block
```

Drift is timing variation across a test period.
For the accepted Linux tests here, null and order drift must remain within 2%.
The runner also monitors background CPU, memory, and I/O activity.
Interrupted periods remain archived and contribute no partial acceptance samples.

## Balance the order

After the null control passes, A is the baseline and B is the candidate:

```text
baseline → candidate → candidate → baseline
 5 runs      5 runs      5 runs      5 runs
```

Warmups are untimed preparation runs.
Each block excludes two warmups.
The four blocks give ten observations per version.
This balances a simple time trend, but cannot cancel arbitrary interference.
The host and order controls remain necessary.

For example, the standard table comparison gives:

| Variant | Measured runs | Arithmetic mean | CV |
| --- | ---: | ---: | ---: |
| Baseline | 10 | 2.587160 s | 0.412% |
| Table with split lookup | 10 | 2.443863 s | 0.724% |

CV expresses the sample standard deviation as a percentage of the mean.
The baseline/candidate order drift is `-0.061% / -0.322%`.
Both adjacent comparisons favor the candidate.

## Report the effect without combining sessions

For the table, `2.587160 → 2.443863 s` means 5.54% less runtime, or a speedup of about 1.0586 times.
The acceptance rule requires at least 1% on standard or 3% on 10K, with neither corpus more than 1% slower.
Effects of 1–3% need an independent repeat, and extra complexity can require a larger gain.

These rules do not define statistical confidence intervals.
Keep every required repeat, including the temperature decoder's 1.89% and 3.83% 10K results.
Compare each candidate with its own baseline, rather than adding or multiplying gains from different periods.

The [data guide](data/README.md) gives reproduction commands.
The generator tests stored observations and null/order results, not the original reports of host activity.
The [experiment protocol](../../EXPERIMENTS.md#measurement-protocol) keeps those full records.

[← Previous](01-correctness-and-early-changes.md) · [Next: eight-byte scanning →](03-swar-separator-scan.md)
