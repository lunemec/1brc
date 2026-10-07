# 3. Find the semicolon eight bytes at a time

[`84e642c`](https://github.com/lunemec/1brc/commit/84e642c)
replaces newline-first parsing with an eight-byte semicolon scan.
A grammar defines the permitted input formats.
The temperature's fixed grammar then gives the newline position.
The parser no longer needs to search for the newline first.

A register stores a value inside the processor.
SIMD applies one instruction to several data values.
SWAR means “SIMD within a register”.
A word is an integer that holds several bytes.
SWAR uses an ordinary 64-bit integer to compare several bytes together.

A vector instruction processes several values at once.
This technique needs no vector instruction set.
Its comparisons use ordinary integer operations.

## Begin with one concrete word

Hexadecimal is a number system with base sixteen.
A fixture is a fixed input with an expected result.
Take the teaching fixture `Oslo;1.2\n`.
Its first eight bytes are:

| Byte offset | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Character | `O` | `s` | `l` | `o` | `;` | `1` | `.` | `2` |
| Hex | `4f` | `73` | `6c` | `6f` | `3b` | `31` | `2e` | `32` |

A bit holds either zero or one.
The least significant bit has the smallest place value.
Little-endian packing puts the first byte in the lowest bits.
`binary.LittleEndian.Uint64` puts byte zero in the least significant eight bits.
The resulting word is `0x322e313b6f6c734f`.

Hexadecimal numbers print their most significant digits first.
Their printed byte order therefore reverses the order in the memory table.
This difference explains the order of bytes in the following calculations.

XOR sets bits where its two inputs differ.
Bitwise NOT reverses every bit in its input.
AND retains bits set in both inputs.
In Go, `a ^ b` means XOR, `^a` means NOT, and `a & b` means AND.
Wrapping arithmetic discards bits beyond the integer's width.

XOR the word with eight copies of the semicolon's byte `0x3b`.
Equal bytes turn into zero:

```go
x := binary.LittleEndian.Uint64(data) ^ 0x3b3b3b3b3b3b3b3b
matches := (x - 0x0101010101010101) & ^x & 0x8080808080808080
```

These are the intermediate values for the example:

| Operation | 64-bit hexadecimal result |
| --- | --- |
| Load `Oslo;1.2` | `322e313b6f6c734f` |
| XOR with repeated `3b` | `09150a0054574874` |
| Subtract repeated `01`, wrapping in 64 bits | `081408ff53564773` |
| Bitwise NOT of XOR result | `f6eaf5ffaba8b78b` |
| AND the last two values and repeated `80` | `0000008000000000` |

A lane is one byte position in this word.
The high bit is the highest-valued bit in a byte.
Bit positions start at zero, from the lowest bit.
The set bit is bit 39, the high bit of byte 4.
That bit locates the semicolon.

## Why subtraction can find zero

An unsigned value cannot represent negative numbers.
A borrow continues subtraction into the next byte.
Think first about an isolated unsigned byte $b$:

* If $b=0$, subtraction yields `0xff`, whose high bit is set. `~b` also has
  its high bit set, so the final AND retains that bit.
* If $1\le b\le127$ without an incoming borrow, $b-1$ has no high bit.
* If $128\le b\le255$, `~b` has no high bit.

The repeated constants apply the byte calculation to every lane in one word.
Whole-word subtraction can carry a borrow across adjacent bytes.
As a result, the expression reliably locates the first zero byte.
The remaining bits do not necessarily identify only zero bytes.

The first zero triggers the borrow.
A borrow cannot create a false marker in a lower byte position.
Little-endian packing places earlier input bytes in lower positions.
The first marked byte therefore identifies the first matching input position.

```go
separatorIdx := bits.TrailingZeros64(matches) / 8
```

The trailing-zero count gives the least significant set bit's position.
The example has 39 trailing zeros.
Integer division gives `39/8 = 4`, which is the semicolon's byte offset.

## A counterexample for multiple matches

A mask selects particular bits from a value.
Use the bytes `;:` followed by zero padding.
After XOR, their low lanes are `0x00, 0x01`.
The subtraction borrows through both lanes and produces `0xff, 0xff`:

```text
Original low bytes:     3b 3a       ; :
XOR low bytes:          00 01
After subtraction:      ff ff
Final marked lanes:     80 80
```

The mask is `0x8080`, although only offset 0 contains a semicolon.
The first position is still correct.
A loop over all marked positions also reports a delimiter at offset 1.
That delimiter does not exist in the input.

Scalar code uses ordinary integer instructions instead of vector instructions.
[Part 8](08-pooled-avx2-scanning.md) uses a vector mask that identifies every matching byte exactly.
That chapter also explains the exact scalar mask used for comparison.

## Advance by words, then finish the tail

A tail is the data left after complete words.
If a word contains no semicolon, the scan advances by eight bytes.
It repeats while at least eight bytes remain.
For the real sample `Cabo San Lucas;14.9\n`:

```text
offset 0:  Cabo San       no semicolon; continue
offset 8:   Lucas;1       semicolon is lane 6
separator index = 8 + 6 = 14
```

The first word contains exactly the first eight name bytes.
The second contains the final name bytes and the start of the temperature.
If fewer than eight bytes remain, an ordinary byte loop finds the delimiter.
Every 64-bit load stays within the slice.
The code does not assume readable memory beyond the slice.

## Recover the newline from the grammar

The semicolon position, sign, and integer width determine the newline position.
These four layouts give the possible offsets.
Each offset counts from the semicolon:

| Temperature | Newline position relative to semicolon |
| --- | ---: |
| `1.2` | `+4` |
| `12.6` | `+5` |
| `-1.2` | `+5` |
| `-12.6` | `+6` |

The parser starts with `separatorIdx+4`.
It adds one for `-`.
It adds another when the assumed decimal-point position is not `.`.
It makes sure that the computed position contains a newline.
An incomplete row returns `-1`.
This tells the worker to stop at the tail instead of accepting it as a complete measurement.

The original `parseNumber` still decodes temperatures with its four explicit branches.
This commit changes how the parser finds delimiters.
It does not change the station table or ownership of input storage.

## Why this can help, and what the experiments found

The new scan stops at the end of the name.
The old scan searches the whole row for a newline and then looks backward.
The new word loop handles eight bytes per iteration.
These differences explain the intended mechanism.
The selected instructions and actual benefit depend on the architecture and compiler.

The [committed experiment summary](../../EXPERIMENTS.md#results-to-date)
records 9.64% less runtime on standard and 7.12% on 10K.
It accepts the change.
A microbenchmark times one small operation in isolation.
A fused scan hashes bytes as it finds separators.
The summary also records a failed fused scan/hash experiment.
That experiment improved the microbenchmark but reduced whole-program time by only 0.92%, with bias from measurement order.

The historical `results/experiments/` timing archive is absent from this checkout.
The chapter therefore preserves the summary percentages.
It does not supply invented raw samples, exact times, or later null controls for these experiments.
This result belongs to the earlier Mac period.
It is separate from the later Linux confirmations.

From `docs/optimization-blog`, use Python to run `tools/trace_examples.py`.
The script regenerates the hexadecimal example and borrow counterexample.
It also makes sure that the real `Cabo San Lucas` sample gives the expected result.
Its fixture tests the first-match rule across all adjacent-byte combinations.

Sources: [original SWAR change](https://github.com/lunemec/1brc/commit/84e642c),
[current parser](../../main.go),
[parser boundary tests](../../parser_word_test.go).

[← Previous](02-measuring-improvements.md) · [Next: the station table →](04-robust-station-table.md)
