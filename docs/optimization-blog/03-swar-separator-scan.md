# 3. Find the semicolon eight bytes at a time

[`84e642c`](https://github.com/lunemec/1brc/commit/84e642c)
uses SWAR: ordinary integer operations compare several bytes in one register.
The parser finds `;` first, then derives the newline position from the temperature layout.
It no longer scans the full row for a newline.

## Follow one word

For `Oslo;1.2\n`, the first eight bytes are:

| Byte offset | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Character | `O` | `s` | `l` | `o` | `;` | `1` | `.` | `2` |
| Hex | `4f` | `73` | `6c` | `6f` | `3b` | `31` | `2e` | `32` |

Little-endian packing puts the first byte in the lowest bits.
The resulting word is `0x322e313b6f6c734f`, whose printed hexadecimal byte order runs right to left.
Bit positions start at zero from the lowest bit.

XOR sets differing bits, NOT reverses bits, and AND keeps bits set in both inputs.
In Go, these operations are `a ^ b`, `^a`, and `a & b`.
XOR with eight `0x3b` bytes turns each semicolon into zero:

```go
x := binary.LittleEndian.Uint64(data) ^ 0x3b3b3b3b3b3b3b3b
matches := (x - 0x0101010101010101) & ^x & 0x8080808080808080
```

| Operation | 64-bit hexadecimal result |
| --- | --- |
| Load `Oslo;1.2` | `322e313b6f6c734f` |
| XOR with repeated `3b` | `09150a0054574874` |
| Subtract repeated `01`, wrapping in 64 bits | `081408ff53564773` |
| Bitwise NOT of XOR result | `f6eaf5ffaba8b78b` |
| AND the last two values and repeated `80` | `0000008000000000` |

For a zero byte, subtraction by one produces `0xff` and retains its high bit under `~x`.
For bytes 1–127 without a borrow, subtraction leaves no high bit.
For bytes 128–255, `~x` has no high bit.

The first marked bit is 39, the high bit of byte 4:

```go
separatorIdx := bits.TrailingZeros64(matches) / 8
```

Thus `39/8 = 4`, the semicolon's position.
If no match exists, the parser advances eight bytes.
Fewer than eight remaining bytes use a bounded byte loop.

## The mask is only safe for the first match

Whole-word subtraction can borrow across bytes.
For `;:`, XOR gives `00 01`, and the borrow marks both:

```text
Original low bytes:     3b 3a       ; :
XOR low bytes:          00 01
After subtraction:      ff ff
Final marked lanes:     80 80
```

The mask becomes `0x8080`, although only byte zero is a semicolon.
The first match remains correct because the borrow cannot create a marker in an earlier byte.
[Part 8](08-pooled-avx2-scanning.md) uses an exact mask when it needs every delimiter.

## Derive the newline

The temperature has only four legal forms:

| Temperature | Newline position relative to semicolon |
| --- | ---: |
| `1.2` | `+4` |
| `12.6` | `+5` |
| `-1.2` | `+5` |
| `-12.6` | `+6` |

The parser starts at `separatorIdx+4` and adjusts for the sign and integer width.
It must find an actual newline at that position.
An incomplete row returns `-1` and ends work on that chunk.

The [historical log](../../EXPERIMENTS.md#results-to-date) reports 9.64% / 7.12% less standard / 10K runtime.
Its original timing archive is unavailable here, so this remains summary evidence from the earlier Mac period.
The [trace tool](tools/trace_examples.py) reproduces the arithmetic and borrow example.

[← Previous](02-measuring-improvements.md) · [Next: the station table →](04-robust-station-table.md)
