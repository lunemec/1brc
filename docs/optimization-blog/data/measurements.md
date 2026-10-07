# Recomputed acceptance measurements

Each mean uses every retained `seconds` observation. The calculation excludes no values. Each variant has ten observations in an A/B/B/A comparison.

Each block excludes two warmup runs. All null controls and order results pass the declared 2% bound. Original reports establish whether host activity met the measurement rules.

| Change | Corpus / window | Baseline mean (s) | Candidate mean (s) | Runtime reduction | Null max drift | Baseline / candidate order drift |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Robust table | Standard / 9 | 2.587160 | 2.443863 | 5.539% | 0.062% | -0.061% / -0.322% |
| Robust table | 10K / 3 | 5.744953 | 3.309773 | 42.388% | 0.962% | +0.487% / -1.735% |
| First-word reuse | Standard / 6 | 2.444292 | 2.311884 | 5.417% | 0.299% | -0.013% / -0.016% |
| First-word reuse | 10K / 2 | 3.267638 | 3.172713 | 2.905% | 0.259% | -0.556% / +0.009% |
| First-word reuse | 10K / 3 | 3.269167 | 3.140641 | 3.931% | 0.777% | -0.146% / -0.667% |
| Temperature word | Standard / 1 | 2.291744 | 2.210153 | 3.560% | 0.548% | +0.545% / -0.973% |
| Temperature word | 10K / 1 | 3.130962 | 3.071864 | 1.888% | 1.172% | -0.219% / -0.437% |
| Temperature word | 10K / 2 | 3.142725 | 3.022448 | 3.827% | 0.464% | -0.042% / -0.698% |
| Buffer reuse | Standard / 1 | 2.204872 | 1.926155 | 12.641% | 0.184% | -0.035% / +0.003% |
| Buffer reuse | 10K / 1 | 3.049592 | 2.398277 | 21.357% | 0.637% | -0.501% / +0.953% |
| Pooled AVX2 | Standard / 3 | 1.917895 | 1.895101 | 1.189% | 0.482% | +0.131% / -0.111% |
| Pooled AVX2 | Standard / 4 | 1.911743 | 1.886221 | 1.335% | 0.287% | -0.235% / +0.051% |
| Pooled AVX2 | 10K / 3 | 2.415082 | 2.375341 | 1.646% | 0.853% | +1.174% / -0.708% |
| Pooled AVX2 | 10K / 4 | 2.376231 | 2.351397 | 1.045% | 0.220% | -0.852% / -0.258% |

[Part 2](../02-measuring-improvements.md) defines the table's measurement terms. [timings.json](timings.json) preserves the source observations and their report, binary, and input identities. [generate_assets.py](../tools/generate_assets.py) recreates the figures and this table.

The table keeps independent repeats separate. Each row compares a candidate with its baseline in the same window. These gains do not establish a total improvement across all changes.
