# Data and reproduction

[timings.json](timings.json) retains 280 measured runs across fourteen accepted Linux comparison periods.
Each version gets ten observations in A/B/B/A order, with two excluded warmups per block.
It records source paths, report checksums, binary identities, input checksums, configuration, and control results.

The charts use `seconds`, from launch through program exit, without removing observations.
`complete_process_seconds` includes later observer bookkeeping.
[memory.json](memory.json) contains separate instrumented memory measurements.

Run these commands from the repository root:

```sh
python3 docs/optimization-blog/tools/generate_assets.py
python3 docs/optimization-blog/tools/trace_examples.py
```

They recreate the figures and make sure that the retained numbers and arithmetic examples match their expected results.
They do not rerun performance tests or inspect the original host-monitor reports.
The large archives remain in ignored `results/research/` and need a separate copy for a full audit.

The earlier Go and SWAR results have summaries but no available raw timing archives here.
They are excluded from the Linux graph.
[Part 2](../02-measuring-improvements.md) explains the measurement rules.
