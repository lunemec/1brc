# Numerical evidence retained with the blog

An observation is the measured runtime of one program run. A window is one
period of measurements under the same controls. `timings.json` contains 280
observations from fourteen complete windows that support the accepted changes.

A baseline is the version used for comparison. A candidate is the version
that we test. Each window retains four groups in A/B/B/A order. A represents
the baseline, and B represents the candidate. Each group contains five measured
runs and two excluded warmup runs.

A warmup prepares the program before measured runs. A screen is an initial
comparison before a final measurement. A null control compares one program
with itself. The original archives retain warmup values, screen timings, and
null observations. This extract retains summaries of their controls.

We read the source reports from the local `results/research/` archive on
2026-10-07. Git ignores that archive. We did not run timing tests to create
these extracts.

A checksum identifies file bytes for later comparisons. For each window,
`source_report` gives the report's path within the original archive.
`source_report_sha256` gives its checksum. `quiet_report_sha256` gives the
checksum of the adjacent report about host activity. The host is the computer
that runs the test.

A binary is the compiled program. The extract also preserves binary checksums
and the configuration used for measurements. A corpus is the test's input file.
An oracle is the expected answer from an independent implementation. The
extract identifies both the corpus and its oracle. For a full audit, copy the
original archives separately.

The accepted commit records the code that adopted the change. We measured
the fixed candidate binary before that commit existed. Its checksum identifies
the program that we measured. For AVX2, the extract uses all four final
comparisons against normal production. It excludes the more favorable
comparisons against a control with matching build flags.

`seconds` records the time from launch through program exit.
`complete_process_seconds` records the observer's later completion time. The
tools calculate arithmetic means from every `seconds` value. They do not remove
runs or exclude unusually fast or slow values.

The committed experiment records supply station counts and the host description.
The retained Linux checksum records identify the input files. These checksums
do not identify the files used in earlier Mac tests.

`memory.json` records memory measurements from the buffer-pool report. These
diagnostic runs collect extra data about the program. They are separate from
the release timing runs. [Part 7](../07-bounded-buffer-reuse.md) defines total
allocation, RSS, live heap, and garbage collection.

RSS is memory that the process keeps in physical RAM. The reported peak comes
from samples at specific times. It does not cover every instant between samples.
Part 7 also explains the runtime's outdated CPU ratios. The figures do not use
those ratios to claim CPU use over the full run.

Run this command from the repository root to recreate `measurements.md` and
the SVG figures from the JSON files:

```sh
python3 docs/optimization-blog/tools/generate_assets.py
```

The early Go and SWAR results have committed summaries. Their original timing
archives are unavailable in this checkout. We exclude them from the Linux
timing graph. We do not invent observation arrays for those results.
