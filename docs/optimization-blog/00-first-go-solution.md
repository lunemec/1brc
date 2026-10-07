# 0. The first Go solution

[`c27aa54`](https://github.com/lunemec/1brc/commit/c27aa549c3182866d56d71f1537ece3b900bde33)
already uses parallel workers and a parser for the known input format.
Git contains no earlier basic version of this implementation.
We can explain its choices, but cannot measure each choice separately from this history.

## Store temperatures as integers

An accumulator stores one station's running statistics.
Temperatures use integer tenths: `6.2` becomes `62`.
These rows from the original README show the update:

```text
Istanbul;6.2
Istanbul;23.0
```

| Step | Count | Sum in tenths | Minimum | Maximum |
| --- | ---: | ---: | ---: | ---: |
| New station | 0 | 0 | Uninitialized | Uninitialized |
| Read `6.2` | 1 | 62 | 62 | 62 |
| Read `23.0` | 2 | 292 | 62 | 230 |

The mean is `292 / 2 = 146` tenths.
The output is `Istanbul=6.2/14.6/23.0`.
The first observation must set both extrema, or an all-positive station gets an incorrect minimum of zero.

$$
S=\sum_{i=1}^{n}x_i,\qquad
\text{min}=\min_i x_i,\qquad
\text{max}=\max_i x_i,\qquad
\text{mean in degrees}=\frac{S}{10n}.
$$

`int16` holds temperatures and extrema, `uint32` holds the count, and `int64` holds the sum.
The largest possible absolute sum is $999\times10^9=999{,}000{,}000{,}000$, which exceeds 32 bits.
Floating-point conversion happens only during final formatting.

## Give each worker complete rows

A producer reads chunks with `ReadAt` and ends each chunk after its last complete newline.
The channel passes a slice descriptor rather than copying the bytes.
With an eight-byte read:

```text
Input:          A;1.0\nB;2.0\n
Byte offsets:   012345678901
First read:     A;1.0\nB;
Sent prefix:    A;1.0\n       offsets [0, 6)
Next read:      B;2.0\n       offsets [6, 12)
```

The next read starts at byte 6, so the incomplete `B;` suffix is read again.
Each complete row is processed once.
The original source uses 32 MiB chunks, despite its README's 20 MiB description.
The later working version changes this to 6 MiB.

Each worker owns its station table, so updates need no shared lock.
The merger reuses the first completed table and combines the others:

$$
(S_1,n_1,m_1,M_1)\oplus(S_2,n_2,m_2,M_2)
=(S_1+S_2,n_1+n_2,\min(m_1,m_2),\max(M_1,M_2)).
$$

For Istanbul, `(62, 1, 62, 62)` and `(230, 1, 230, 230)` combine into `(292, 2, 62, 230)`.
This matches the sequential result.
Later fixes handle previously absent stations correctly.

## Parse only the permitted layouts

The input has four temperature forms.
ASCII digits have consecutive values, with `'0'` equal to 48.
Subtracting 48 gives each digit's value:

| Layout | Example | Integer calculation |
| --- | --- | --- |
| `d.d` | `6.2` | $10\times6+2=62$ |
| `dd.d` | `23.0` | $100\times2+10\times3+0=230$ |
| `-d.d` | `-6.2` | $-(10\times6+2)=-62$ |
| `-dd.d` | `-12.6` | $-(100\times1+10\times2+6)=-126$ |

The five-byte negative case reads only the required digits:

```go
// Teaching excerpt of the original five-byte negative case.
value := -(100*measurement(line[1]-48) +
           10*measurement(line[2]-48) +
              measurement(line[4]-48))
```

The original row parser finds the newline first, then looks back four, five, or six bytes for `;`.
[Part 1](01-correctness-and-early-changes.md) repairs the short-row bounds bug.
[Part 3](03-swar-separator-scan.md) replaces this scan.

## Hash once and update in place

A hash converts name bytes to a table position.
The original table contains buckets of name/`*stats` pairs.
Lookup compares names in that bucket and returns the accumulator pointer:

```text
name bytes → hash → bucket index → compare bucket names → *stats → update
```

The hash processes two bytes at a time:

```go
block := uint32(station[i]) | uint32(station[i+1])<<8
hash = hash*16777619 + block // wraps modulo 2^32
```

For `Oslo`, the pairs become `0x734f` and `0x6f6c`.
The loop omits an odd trailing byte, so `OsloX` and `OsloY` share a hash.
Exact name comparison keeps their answers separate.

`unsafe.String` borrows the input bytes instead of copying each name.
These original buffers are never overwritten, and stored names keep them alive.
The later table copies new keys before [buffer reuse](07-bounded-buffer-reuse.md) begins.

## Historical result

The original README records twenty Go benchmark repetitions on an Apple M1 MacBook Pro (2020, 16 GB):

| Implementation | Historical benchmark result |
| --- | ---: |
| Go `elh` | 7.363 s/op |
| Go `lunemec` | 7.099 s/op |

Its `benchstat` excerpt reports 3.59% less time against `elh`.
The benchmark calls `run` inside `go test` and discards output.
This differs from the later full-process measurements.

[Original source](https://github.com/lunemec/1brc/blob/c27aa549c3182866d56d71f1537ece3b900bde33/main.go)
and [historical results](https://github.com/lunemec/1brc/blob/c27aa549c3182866d56d71f1537ece3b900bde33/README.md).

[Next: correctness and early changes →](01-correctness-and-early-changes.md)
