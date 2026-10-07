# 1. Correctness fixes before more speed

These changes repair the answer and simplify the implementation.
Their individual edits have no controlled timing attribution in the available history.
Later performance tests must preserve the corrected behavior.

## Short rows and missing stations

The shortest legal row contains six bytes:

```text
A;0.0\n
012345       byte offsets
```

The original parser evaluates `data[newlineIdx-6]`, which becomes `data[-1]`.
Go stops with a bounds panic before reading it.
[`9233101`](https://github.com/lunemec/1brc/commit/9233101)
finds `;` at `5-4=1` first and guards the longer lookbacks.

The original merge also combines a missing station with zero-valued extrema:

```text
Source:       count=1, sum=150, min=150, max=150
Empty target: count=0, sum=0,   min=0,   max=0
Wrong merge:  min(0, 150) = 0
```

[`a1cd728`](https://github.com/lunemec/1brc/commit/a1cd728)
copies the complete source accumulator when the station is absent.
That commit also replaces the manual iterator with complete `iter.Seq2` traversal.
It sets chunks to 6 MiB and the station bound to 10,000.

## Pass the table descriptor by value

[`83098c9`](https://github.com/lunemec/1brc/commit/83098c9)
passes `simpleMap` by value instead of pointer.
The copy contains a slice descriptor and metadata, not all table entries:

```text
Descriptor A: [pointer to entries | length | capacity | metadata]
                        ↓
                   backing entries
                        ↑
Descriptor B: [same pointer       | length | capacity | copied metadata]
```

Both descriptors refer to the same entries, but each has its own metadata.
Workers finish their tables before handoff.
The standard-map experiment remains inactive in `main_stdmap.go.test`.

## Round negative ties correctly

The reference rounds exact ties toward positive infinity.
The [stored fixture](../../test/resources/samples/measurements-rounding-ties.txt) includes:

```text
NegativeTie;-1.6
NegativeTie;-1.7
PositiveTie;22.8
PositiveTie;22.9
```

For `NegativeTie`, the mean is `-16.5` tenths and must round to `-16`, which prints `-1.6`.
`math.Round` instead rounds that tie away from zero.
[`5a77773`](https://github.com/lunemec/1brc/commit/5a77773) replaces it with integer arithmetic:

| Operation for `S=-33`, `n=2` | Quotient | Remainder |
| --- | ---: | ---: |
| Go division and remainder | -16 | -1 |
| If remainder is negative: `q--`, `r+=n` | -17 | 1 |
| If `2*r >= n`: `q++` | -16 | 1 |
| Convert rounded tenths to degrees | -1.6 | — |

After the adjustment, the remainder is nonnegative and less than the count.
If twice the remainder is at least the count, increment the quotient.
This rounds exact ties toward positive infinity.
The earlier [oracle repair](https://github.com/lunemec/1brc/commit/010340d) supplies the independent expected answer.

## Match Java's name order

Go compares UTF-8 bytes, while the reference compares UTF-16 code units.
These orders differ for supplementary characters:

| Character | First UTF-8 byte | UTF-16 units |
| --- | --- | --- |
| `😀` | `0xf0` | `0xd83d`, `0xde00` |
| U+E000 | `0xee` | `0xe000` |

UTF-8 puts U+E000 first, but UTF-16 puts `😀` first because `0xd83d < 0xe000`.
[`6bfc2ec`](https://github.com/lunemec/1brc/commit/6bfc2ec) adds the required ordering and stronger output comparisons.
`javaStringLess` applies this order during final sorting, outside the billion-row loop.

[← Previous](00-first-go-solution.md) · [Next: learning to measure →](02-measuring-improvements.md)
