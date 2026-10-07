# 1. Making the answer correct before making it faster

SWAR compares several bytes through ordinary integer operations.
The commits before the SWAR parser fix errors and change the implementation.
We can reconstruct these changes from the differences between revisions.
The old README supplies some timing results.
The available history contains no controlled timing comparison for each individual change.

## A short row exposes an out-of-bounds lookup

An out-of-bounds lookup attempts an invalid array access.
The shortest legal row is:

```text
A;0.0\n
012345       byte offsets
```

The newline is at index 5.
The first parser evaluated both `data[newlineIdx-4]` and `data[newlineIdx-6]` before it chose the layout.
The second expression is `data[-1]`.
Go stops with a bounds panic (an error that stops execution).
The semicolon at index 1 already identifies the three-byte temperature.
Thus, the parser does not need the second lookup for this row.

[`9233101`](https://github.com/lunemec/1brc/commit/9233101acd7fa232d772e9736b28d8b74ec13fe3)
examines the first location before it evaluates the second.
It also tests the row length before the longer lookbacks.
For the shortest row, it finds `;` at `5-4=1` and never reads a negative index.
The commit's title mentions the 10K implementation.
The actual code change repairs these bounds.
It does not add the later 32K table.

## Copy an absent station's extrema during the merge

The initial merge created an accumulator with zero in every field for a station absent from the destination.
It then applied `min` and `max` against those zeros.
Suppose only one worker observes `Adelaide;15.0`:

```text
Source:       count=1, sum=150, min=150, max=150
Empty target: count=0, sum=0,   min=0,   max=0
Wrong merge:  min(0, 150) = 0
```

The extrema are the minimum and maximum values.
[`a1cd728`](https://github.com/lunemec/1brc/commit/a1cd72836776d8f42f8322274583e39c4a64be7b)
copies the source's entire accumulator when it inserts an absent station.
It then continues to the next entry.
The combination formula from part 0 applies only when both accumulators contain observations.

A merge must treat an empty accumulator separately.
Zero is not a correct initial minimum for an all-positive station.
It is also not a correct initial maximum for an all-negative station.

## Replace the manual iterator with a complete traversal

An iterator visits the entries in a collection.
A closure retains values from its enclosing function.
The same commit replaces a closure iterator with `iter.Seq2`.
The old iterator advanced across buckets with special end conditions.
The new implementation visits every bucket and every entry with nested loops.
It stops early only if the consumer returns `false`.

The code has this structure:

```go
return func(yield func(uint32, bucketItem) bool) {
    for bucketIndex, bucket := range m.data {
        for _, item := range bucket.items {
            if !yield(uint32(bucketIndex), item) {
                return
            }
        }
    }
}
```

The iterator visits all entries, including entries at the end of the table.
Go's range-over-function syntax makes the merge and output loops simpler.
The commit also changes chunks from 32 MiB to 6 MiB and sets the station bound to 10,000.
Its title says “3s”.
The title gives no benchmark evidence for the iterator, chunk size, or any other individual edit.

## Pass the small table descriptor by value

A descriptor stores information about an array.
[`83098c9`](https://github.com/lunemec/1brc/commit/83098c93f878c16c490894a63f6feff9675e3e86)
changes worker results and merge parameters from `*simpleMap` to `simpleMap`.
The descriptor holds a slice plus integer metadata.
Metadata describes the table's capacity and entry count.
A copy of the descriptor copies the slice header.
It does not copy every entry in the underlying array.

```text
Descriptor A: [pointer to entries | length | capacity | metadata]
                        ↓
                   backing entries
                        ↑
Descriptor B: [same pointer       | length | capacity | copied metadata]
```

Both descriptors refer to the same entries.
An update to an entry through either descriptor changes the shared storage.
An update to descriptor metadata changes only that descriptor.
A worker finishes its table before it sends the descriptor to the merge.
The metadata therefore describes the completed table at that point.

The change removes one pointer lookup from the interface.
The commit calls it faster, but this checkout contains no reproducible paired measurements.
It also retains a standard-map experiment in `main_stdmap.go.test`.
This file is not an active `.go` source file.
The production implementation still uses the custom map.

## Rounding is part of the answer

The intermediate version used `math.Round` on the mean in tenths.
A rounding tie falls exactly halfway between two integers.
The independent reference rounds to the nearest integer, with exact ties toward positive infinity.
`math.Round` differs from this rule for negative ties.

A fixture is a fixed input with an expected result.
The [rounding fixture](../../test/resources/samples/measurements-rounding-ties.txt)
contains:

```text
NegativeTie;-1.6
NegativeTie;-1.7
PositiveTie;22.8
PositiveTie;22.9
```

For `NegativeTie`, the integer sum is `-33` and the count is `2`.
The mean in tenths is `-16.5`.
The required rounding gives `-16`, which prints `-1.6`.
The required positive tie rounds `228.5` to `229`, which prints `22.9`.

[`5a77773`](https://github.com/lunemec/1brc/commit/5a77773)
replaces `math.Round` with integer arithmetic.
The quotient is the integer result of division.
The remainder is the amount left after division.
Go division truncates the quotient toward zero.
Floor division rounds the quotient toward negative infinity.
The fix first converts Go's result to floor division:

| Operation for `S=-33`, `n=2` | Quotient | Remainder |
| --- | ---: | ---: |
| Go division and remainder | -16 | -1 |
| If remainder is negative: `q--`, `r+=n` | -17 | 1 |
| If `2*r >= n`: `q++` | -16 | 1 |
| Convert rounded tenths to degrees | -1.6 | — |

After the correction, $S=qn+r$ with $0\le r<n$.
The fractional part is $r/n$.
If $2r\ge n$, the implementation increments $q$.
This applies the required tie rule without a floating-point quotient.

An oracle supplies an independently calculated expected answer.
The [oracle repair](https://github.com/lunemec/1brc/commit/010340d)
corrects the full-corpus oracle.
The [strict-verification changes](https://github.com/lunemec/1brc/commit/6bfc2ec)
add stronger comparisons and the Unicode ordering repair below.
The final [mean implementation](../../main.go) retains integer rounding.

Floating-point values can distinguish positive zero from negative zero.
The integer representation treats zero as a single value.
As a result, a zero mean prints `0.0`.

## “Alphabetical” needs an exact ordering rule

A code unit is one encoded storage value.
Go string comparison orders UTF-8 bytes.
The Java reference compares UTF-16 code units.
A basic-plane character has a value at most U+FFFF.
A supplementary character has a value above U+FFFF.
These orders can differ for a supplementary character such as `😀` (U+1F600) and a basic-plane character such as U+E000.

| Character | First UTF-8 byte | UTF-16 units |
| --- | --- | --- |
| `😀` | `0xf0` | `0xd83d`, `0xde00` |
| U+E000 | `0xee` | `0xe000` |

UTF-8 byte order puts U+E000 first.
UTF-16 order puts `😀` first because `0xd83d < 0xe000`.
The exact-output fixture
[defines that ordering](../../test/resources/samples/measurements-utf16-order.out).

A rune is one Unicode character value.
`javaStringLess` decodes each UTF-8 rune and compares its UTF-16 units for the final output.
This work occurs once per final sort comparison.
It takes place outside the billion-row parsing loop.

A faster program must still solve the same task.
If it skips rows, rounds differently, or prints names in the wrong order, its output is incorrect.
These repairs establish the answer that later performance comparisons must preserve.

[← Previous](00-first-go-solution.md) · [Next: learning to measure →](02-measuring-improvements.md)
