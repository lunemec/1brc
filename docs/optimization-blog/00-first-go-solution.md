# 0. The first Go solution: reduce the work per row

A commit is a saved revision in Git.
The first Go commit is
[`c27aa54`](https://github.com/lunemec/1brc/commit/c27aa549c3182866d56d71f1537ece3b900bde33).
It already reads several parts of the file at once and uses the known input format.
This repository contains no earlier basic Go version.

We can explain this design and report its historical result.
The history gives no earlier version for each design choice.
As a result, we cannot measure each choice's separate effect from these commits.

## Four integers describe a station

An accumulator stores measurement results for one station.
Consider these two rows from the original README:

```text
Istanbul;6.2
Istanbul;23.0
```

The program does not need either row after it updates the accumulator.
It stores temperatures in tenths of a degree.
Thus, `6.2` becomes `62` and `23.0` becomes `230`:

| Step | Count | Sum in tenths | Minimum | Maximum |
| --- | ---: | ---: | ---: | ---: |
| New station | 0 | 0 | Uninitialized | Uninitialized |
| Read `6.2` | 1 | 62 | 62 | 62 |
| Read `23.0` | 2 | 292 | 62 | 230 |

The mean is the sum divided by the count.
Here, the mean is `292 / 2 = 146` tenths.
These two rows produce `Istanbul=6.2/14.6/23.0` in the final output.

The extrema are the minimum and maximum values.
The first observation sets both extrema.
If the minimum starts at zero, a station with only positive temperatures gets an incorrect minimum of `0.0`.
The first-row branch in `updateStats` prevents this error.

For observations $x_1,\ldots,x_n$ represented in tenths:

$$
S=\sum_{i=1}^{n}x_i,\qquad
\text{min}=\min_i x_i,\qquad
\text{max}=\max_i x_i,\qquad
\text{mean in degrees}=\frac{S}{10n}.
$$

A bit holds either zero or one.
The implementation uses `int16` for temperatures and extrema.
It uses `uint32` for the count and `int64` for the sum.
The largest possible absolute sum is $999\times10^9=999{,}000{,}000{,}000$.
This sum needs more than 32 bits.
The count fits because $10^9<2^{32}$.

Floating-point numbers represent values with fractional parts.
The program converts integers to floating-point numbers when it formats the final result.
This conversion happens after the billion-row loop.

This approach avoids general floating-point parsing for every row.
It also keeps the integer sum exact.
Final rounding is a separate operation.
[Part 1](01-correctness-and-early-changes.md) explains a later correction to rounding.

## Split the file at complete rows

A producer reads data and sends it to workers.
A worker reads rows and updates station totals.
A chunk is a block of input bytes.
An offset is a position measured from the start.
One producer calls `ReadAt` to read a chunk at a known file offset.

An array stores values at numbered positions.
A pointer refers to a location in memory.
A slice describes part of an array.
Its descriptor stores a pointer, length, and capacity.

A channel sends values between parts of the program.
Workers receive byte-slice descriptors through a channel.
The channel does not copy the array that holds the bytes.
The producer searches backward for the last newline and sends only the complete prefix.

Use a tiny eight-byte read to see the boundary rule:

```text
Input:          A;1.0\nB;2.0\n
Byte offsets:   012345678901
First read:     A;1.0\nB;
Sent prefix:    A;1.0\n       offsets [0, 6)
Next read:      B;2.0\n       offsets [6, 12)
```

An interval describes a range of positions.
A half-open interval excludes its end position.
Thus, `[0, 6)` includes bytes 0 through 5.
The next read begins at 6 because the first read's `B;` suffix is an incomplete row.
The producer reads those two bytes again.
The worker processes each complete row once.

MiB means 1,048,576 bytes.
The initial source uses 32 MiB chunks.
Its README refers to 20 MiB.
This chapter follows the constant in the source.
The later working version changes the constant to 6 MiB.
The available history contains no separate acceptance experiment for either chunk size.

Every worker processes multiple chunks.
It produces one station table after its input channel closes.
A mutex prevents simultaneous access to shared data.
Each worker owns its table during parsing, so accumulator updates need no mutex.
The final merge reuses the first completed table and adds the others into it.
The addition rule is:

$$
(S_1,n_1,m_1,M_1)\oplus(S_2,n_2,m_2,M_2)
=(S_1+S_2,n_1+n_2,\min(m_1,m_2),\max(M_1,M_2)).
$$

For Istanbul, suppose worker A retains `(62, 1, 62, 62)` and worker B retains `(230, 1, 230, 230)`.
The merge produces `(292, 2, 62, 230)`.
This result matches the answer from a single worker.
Later fixes handle stations absent from the destination and entries that share a table position.

## Use the known temperature grammar

A parser converts input bytes into values.
A grammar defines the permitted input formats.
A general number parser supports many formats.
This input has only four:

| Layout | Example | Integer calculation |
| --- | --- | --- |
| `d.d` | `6.2` | $10\times6+2=62$ |
| `dd.d` | `23.0` | $100\times2+10\times3+0=230$ |
| `-d.d` | `-6.2` | $-(10\times6+2)=-62$ |
| `-dd.d` | `-12.6` | $-(100\times1+10\times2+6)=-126$ |

ASCII digits have consecutive values.
The digit `'0'` has value 48.
Thus, `line[i]-48` converts a digit byte to its numerical value.
The original `parseNumber` selects a branch from the sign and width.
It then loads the required digits directly.
For `-12.6`:

```go
// Teaching excerpt of the original five-byte negative case.
value := -(100*measurement(line[1]-48) +
           10*measurement(line[2]-48) +
              measurement(line[4]-48))
```

The decimal point at index 3 contributes no arithmetic.
The original source comment reports about 4% improvement over a loop.
This checkout does not contain the paired measurements behind that claim.
Treat the claim as a historical observation.

The initial row parser first uses `bytes.IndexByte` to locate `\n`.
A temperature occupies three, four, or five bytes.
As a result, the semicolon is four, five, or six bytes before the newline.
The parser looks backward a few positions and avoids a second full scan.
[Part 1](01-correctness-and-early-changes.md) explains the short-row bounds bug in this approach.
[Part 3](03-swar-separator-scan.md) replaces the newline-first scan.

## Hash once, then update in place

A hash converts a station name to a number.
A bucket groups entries with the same table position.
The original table is an array of buckets.
Each bucket contains a small slice of name/`*stats` pairs.
The hash chooses a bucket.
Lookup compares the names in that bucket and returns a pointer to the matching accumulator.

```text
name bytes → hash → bucket index → compare bucket names → *stats → update
```

If the name is absent, insertion uses the same bucket index.
This avoids another hash calculation.
The custom hash processes pairs of bytes:

```go
block := uint32(station[i]) | uint32(station[i+1])<<8
hash = hash*16777619 + block // wraps modulo 2^32
```

Little-endian packing puts the first byte in the lowest bits.
For `Oslo`, the pairs are `O,s` and `l,o`.
They become `0x734f` and `0x6f6c`.
The hash starts from `2166136261` and applies two multiply and add steps.
It then divides by the table capacity and uses the remainder.
This custom calculation uses an FNV prime, but it differs from standard FNV-1a.

A collision occurs when different names share a table position.
The original loop omits an odd trailing byte.
For example, `OsloX` and `OsloY` hash the same pairs.
This creates collisions, but exact name comparison keeps the answers correct.
More hash work can reduce collisions and increase the time for common names.
Later experiments measure both effects.

The parser uses `unsafe.String` to view name bytes without a separate string copy for each row.
The original design allocates new storage for each chunk and never overwrites it.
Stored strings keep that input storage alive.
The later table copies each newly inserted name into separate storage.
This change permits [buffer reuse](07-bounded-buffer-reuse.md).

## What was measured at the time

A benchmark measures the time for a fixed workload.
The first README records twenty Go benchmark repetitions on an Apple M1 MacBook Pro (2020, 16 GB):

| Implementation | Historical benchmark result |
| --- | ---: |
| Go `elh` | 7.363 s/op |
| Go `lunemec` | 7.099 s/op |

Its pasted `benchstat` output reports a 3.59% reduction against that Go comparison.
The benchmark called `run` inside `go test`.
It discarded formatted output through the `bench` flag.
Later benchmarks time the full executable with a different measurement procedure.

A checksum is a value that identifies file contents.
The original table compares languages without the later checksum records.
It also lacks controls for unchanged executables and monitoring of the computer.

The old README called this the fastest Go variant.
This series reports that historical claim.
It makes no claim about the current leaderboard.

Sources: [first Go source](https://github.com/lunemec/1brc/blob/c27aa549c3182866d56d71f1537ece3b900bde33/main.go),
[first README and its benchmark excerpt](https://github.com/lunemec/1brc/blob/c27aa549c3182866d56d71f1537ece3b900bde33/README.md).

[Next: correctness and early changes →](01-correctness-and-early-changes.md)
