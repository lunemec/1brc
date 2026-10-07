# 6. Decode a temperature with one word

[`ef7418a`](https://github.com/lunemec/1brc/commit/ef7418a8be7fa57a02298fb49acbbfc3450b35e5)
handles all four temperature layouts with one 64-bit load.
We will follow `Oslo;-12.6\nParis;0.0\n` through each operation.
[Part 3](03-swar-separator-scan.md) explains the bit operators and byte order.

## Load only within the chunk

The word starts immediately after the semicolon:

| Offset | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Character | `-` | `1` | `2` | `.` | `6` | `\n` | `P` | `a` |
| Hex | `2d` | `31` | `32` | `2e` | `36` | `0a` | `50` | `61` |

Little-endian packing gives `0x61500a362e32312d`.
The load requires `len(data)-separatorIdx >= 9`: the semicolon plus eight following bytes.
A complete final row with fewer bytes still uses `parseNumber`.

## 1. Find the decimal point

Bit 4 is set in ASCII digits (`0x30`–`0x39`) and clear in `.` (`0x2e`).
The decimal point occurs at byte 1, 2, or 3.
The mask selects bit 4 only at those positions:

```go
dotPos := bits.TrailingZeros64(^temperatureWord & 0x10101000)
```

```text
~word & 0x10101000 = 0x0000000010000000
trailing-zero count = 28
dot byte = 28 / 8 = 3
```

The four layouts give:

| Temperature | Dot byte | `dotPos` | Alignment shift `28-dotPos` |
| --- | ---: | ---: | ---: |
| `1.2` | 1 | 12 | 16 |
| `12.6` | 2 | 20 | 8 |
| `-1.2` | 2 | 20 | 8 |
| `-12.6` | 3 | 28 | 0 |

This assumes legal temperature text, rather than validating arbitrary numbers.
The parser requires `dotPos <= 28` and an actual newline at the computed index:

$$
\text{newlineIdx}=\text{separatorIdx}+\lfloor\text{dotPos}/8\rfloor+3.
$$

For this row, the index is `4+3+3=10`.

## 2. Form the sign mask

The first byte is either a digit with bit 4 set or `-` with bit 4 clear.
Invert it, move that bit to position 63, and use signed right shift:

```go
signed := int64(^temperatureWord << 59) >> 63
```

The result is `0` for positive numbers and `-1` for negative numbers.
Signed right shift repeats the sign bit, so `-1` contains all one bits.
`uint64(signed)&0xff` therefore selects the sign byte only when the number is negative.

## 3. Align and select the digits

Clear the sign byte, move the decimal point to byte 3, and keep the digit positions:

```go
digits := ((temperatureWord & ^(uint64(signed) & 0xff)) <<
           (28 - dotPos)) & 0x0f000f0f00
```

A nibble is four bits.
ASCII digit low nibbles already hold their values: `'6' & 0x0f = 6`.
The mask keeps those nibbles at bytes 1, 2, and 4:

| Operation | Result |
| --- | --- |
| Original word | `61500a362e32312d` |
| Clear sign byte | `61500a362e323100` |
| Shift left by `28-28 = 0` | `61500a362e323100` |
| AND `0000000f000f0f00` | `0000000600020100` |

The retained memory-order bytes are `00 01 02 00 06 00 00 00`.
The decimal, newline, and following-row bytes are gone.
For `1.2`, the 16-bit shift leaves a zero hundreds digit and produces `0x0000000200010000`.

## 4. Combine the digits with a multiply

Let $h,t,u$ be the hundreds, tens, and units of integer tenths.
Their byte positions give:

$$
D=h\,2^8+t\,2^{16}+u\,2^{32}.
$$

The multiplier places their weighted contributions at bit 32:

$$
K=\mathtt{0x640a0001}=1+10\,2^{16}+100\,2^{24}.
$$

$$
(h\,2^8)(100\,2^{24})+
(t\,2^{16})(10\,2^{16})+
(u\,2^{32})(1)
=(100h+10t+u)\,2^{32}.
$$

For legal digits, lower terms cannot carry into bit 32.
After the shift, the extra term $100t\,2^8$ is a multiple of 1024.
The ten-bit mask removes it and higher terms, while retaining every magnitude up to 999:

```go
absolute := int64((digits * 0x640a0001) >> 32 & 0x3ff)
```

```text
64-bit wrapped product = 0x583cc87e0a020100
(product >> 32) & 0x3ff = 0x07e = 126
```

The operations wrap as unsigned 64-bit arithmetic.
The [Python trace](tools/trace_examples.py) uses explicit masks to reproduce that behavior.
The magnitude is 126.

## 5. Restore the sign

```go
value := measurement((absolute ^ signed) - signed)
```

With `signed=0`, the result stays positive.
With `signed=-1`, XOR complements the bits and subtraction adds one.
This is two's-complement negation.
The result is `-126`, and the parser returns `(10, "Oslo", -126, 0x6f6c734f)`.

## Result

![Temperature-decoder observations, with both 10K repeats.](figures/temperature-runtimes.svg)

| Corpus / window | Baseline | Word decoder | Runtime reduction |
| --- | ---: | ---: | ---: |
| Standard / 1 | 2.291744 s | 2.210153 s | 3.560% |
| 10K / 1 | 3.130962 s | 3.071864 s | 1.888% |
| 10K / 2, independent repeat | 3.142725 s | 3.022448 s | 3.827% |

The initial smaller 10K effect required an independent repeat.
Both results remain visible, and all periods pass their declared controls.
[Tests](../../temperature_word_test.go) cover all 1,999 values, `-0.0`, truncations, following bytes, and unaligned starts.

[Full acceptance record](../../EXPERIMENTS.md#accepted-bounded-temperature-decoder-2026-10-06).

[← Previous](05-reusing-the-first-word.md) · [Next: bounded buffer reuse →](07-bounded-buffer-reuse.md)
