# 5. Keep the word we already loaded

[`51e20a1`](https://github.com/lunemec/1brc/commit/51e20a133a3aadecd947e3e1248676899fc9e9bc)
returns the first name word from the parser.
The hash then reuses those eight bytes instead of loading them again.
The table and fingerprint mapping stay fixed.

## Before and after

Before:

```text
input → load first eight bytes to scan for ';'
      → return name
      → load first name bytes again to hash them
      → lookup → update
```

After:

```text
input → load first eight bytes to scan for ';'
      → preserve and normalize that word
      → return name and word
      → hash cached word → lookup → update
```

The parser saves `firstWord` before XOR changes the bytes for delimiter detection.
For a short name, that word also contains the semicolon and temperature.
Those bytes must not enter the name's hash.

## Remove bytes after the name

For `Oslo;1.2\n`:

```text
Raw loaded word:  0x322e313b6f6c734f
Name length:      4 bytes
Required word:    0x000000006f6c734f
```

A mask keeps only the name bytes.

For four bytes, it is `0x00000000ffffffff`:

```go
firstWord &= (uint64(1) << (separatorIdx * 8)) - 1
```

The result equals `stationWord("Oslo")`.
Eight-byte names need no mask, and longer names continue hashing their remaining bytes.
Inputs shorter than eight bytes use the bounded `stationWord` helper instead of overreading.

For `abcdefghX`, reuse preserves the first word `0x6867666564636261` and the complete fingerprint `0x882cfcbff17ddf39`.
Thus, station identities and collision patterns do not change.
The comparison isolates removed parser work.

## Follow one row

The teaching input is `Oslo;-12.6\nParis;0.0\n`:

| Stage | Result |
| --- | --- |
| First 64-bit load | Name prefix plus `;-12` |
| Semicolon index | 4 |
| Normalized name word | `0x000000006f6c734f` |
| Scalar number decoder | `-126` tenths |
| Newline index | 10 |
| Parser return | `(10, "Oslo", -126, 0x6f6c734f)` |
| Fingerprint | `0xa618e03c65f6a782` |
| Home slot | 10114 |
| Next input view | Starts at byte 11: `Paris;0.0\n` |

Insertion copies a new name, and later rows update the same accumulator.
The parser's name still borrows input memory.
Returning the extra integer changes no ownership rule.

## Result

![Parser-word runtime observations, with both 10K repeats.](figures/parser-word-runtimes.svg)

| Corpus / window | Baseline | Reuse | Runtime reduction |
| --- | ---: | ---: | ---: |
| Standard / 6 | 2.444292 s | 2.311884 s | 5.417% |
| 10K / 2 | 3.267638 s | 3.172713 s | 2.905% |
| 10K / 3, independent repeat | 3.269167 s | 3.140641 s | 3.931% |

The initial 10K effect required an independent repeat, so both results remain visible.
All three periods pass their declared controls and exact-output comparisons.
The [acceptance record](../../EXPERIMENTS.md#accepted-parser-word-reuse-2026-10-06) contains the full evidence.

[← Previous](04-robust-station-table.md) · [Next: the temperature word →](06-temperature-word-decoder.md)
