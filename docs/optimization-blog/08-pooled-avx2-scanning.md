# 8. One delimiter mask for several rows

[`0e5dd35`](data/history.md#0e5dd35) adds the tested AVX2 scanner to the pooled version.
A vector operation processes several values in one instruction.
A mask records matching byte positions as set bits.
Two 32-byte vector comparisons produce one 64-bit mask.
Each set bit identifies a semicolon in the 64-byte input window.

A delimiter is a character that separates fields.
The worker uses every delimiter from the mask before it scans another window.
Two independent measurement periods per corpus show small gains.
Standard runtime falls by 1.19% / 1.34%, and 10K runtime falls by 1.65% / 1.05%.

## From vector lanes to a mask

A kernel is the function that performs vector operations.
This kernel compares 64 bytes with semicolons.
The complete function is:

```go
func semicolonMask64AVX2(data []byte) uint64 {
    _ = data[63]
    delimiter := archsimd.BroadcastUint8x32(';')
    low := archsimd.LoadUint8x32(data).Equal(delimiter).ToBits()
    high := archsimd.LoadUint8x32(data[32:]).Equal(delimiter).ToBits()
    return uint64(low) | uint64(high)<<32
}
```

A lane holds one value in a vector.
`Broadcast` puts a semicolon in every comparison lane.
Each vector load reads 32 bytes.
Equality marks the lanes that contain `;`.
`ToBits` turns those matches into a 32-bit mask of positions.

OR sets a bit when either input bit is set.
The expression shifts the second mask left by 32 bits, then combines both masks with OR.
The result records positions 0–63 within the window.

The `_ = data[63]` expression makes sure that the slice contains the complete window.
The worker calls the kernel only while at least 64 bytes remain.
Both loads therefore stay within the input.
The start does not need alignment to a 32-byte boundary.

## Follow two short rows through a mask

Consider `A;0.0\nB;1.0\n` at the start of a 64-byte teaching block.
The remaining bytes contain zeros.
The mask contains bits at the two semicolon positions:

```text
Bytes:    A ; 0 . 0 \n B ; 1 . 0 \n
Offsets:  0 1 2 3 4  5 6 7 8 9 10 11
Matches:    1           1
Mask:     (1<<1) | (1<<7) = 0x82
```

The trailing-zero count finds the lowest set bit.
The worker uses that position for the next delimiter.
It then clears the bit with `mask &= mask-1`:

| Operation | Before | After / result |
| --- | --- | --- |
| Find first set bit | `0x82` | offset 1 |
| Clear lowest set bit | `0x82 & 0x81` | `0x80` |
| Find next set bit | `0x80` | offset 7 |
| Clear lowest set bit | `0x80 & 0x7f` | `0x00` |

AND keeps a bit only when both input bits are set.
Subtracting one clears the lowest set bit and sets each lower bit.
AND with the original mask therefore removes exactly that lowest set bit.

An offset measures a position from a given start.
Here, offsets count bytes.
The first row starts at zero, so separator offset 1 is also offset 1 within that row.
Decoding `A;0.0\n` returns newline index 5 and value 0.
The next row starts at `0+5+1=6`.

The remaining separator is at offset 7 from the chunk start.
Its position within the second row is `7-6=1`.
Decoding B gives value 10 tenths and newline index 5.
The next row starts at 12.

An accumulator stores the running statistics for one station.
The worker updates the entry for A before the entry for B.
It does not delay or reorder accumulator updates when it uses a mask.
The same update order applies when two rows contain the same station.

## Keep block positions separate from row positions

Three offsets serve different purposes:

| Variable | Meaning |
| --- | --- |
| `rowStart` | Absolute start of the next row in the chunk |
| `blockStart` | Absolute start of the window that produced the current mask |
| `nextBlock` | Absolute start of the next unscanned 64-byte window |

The separator position from the chunk start is
`blockStart + bits.TrailingZeros64(delimiters)`.
The decoder instead expects a position within the current row.
Subtracting `rowStart` gives the index for `parseLineAtSeparator(chunkView, index)`.

A 100-byte station name crosses a window boundary.
The worker advances through windows with empty masks until it finds the semicolon.
The row view still starts at the original name.
For a name that starts at chunk offset zero, the semicolon is at offset 100.
Window 64–127 supplies bit 36.
The position within the row is `64+36-0=100`.

A delimiter near the end of a block can belong to a row that ends beyond that block.
The decoder reads within the complete chunk.
Legal temperature bytes contain no semicolons.
The remaining mask therefore contains no false delimiters inside those temperature bytes.

## Reuse the existing row decoder

The mask finds only separators.
`parseLineAtSeparator` returns the same borrowed name and normalized first word as the scalar parser.
It also uses the same temperature decoder and fallback for short final rows.
It makes sure that the exact newline is present.
The hash and table update are the same as before.

If no full window remains and no separator is cached, the original `parseLine` handles the remaining row view.
An incomplete final row stops work on that chunk.
The worker returns the buffer loan after all updates finish, including the final rows handled by `parseLine`.

## Why an exact mask needs a different scalar formula

A borrow moves subtraction across a bit or byte boundary.
The SWAR expression from [part 3](03-swar-separator-scan.md) finds the first match but can include extra markers from borrows.
A control that lists every delimiter needs an exact mask.
Its code uses a different calculation to find zero bytes:

```go
x := word ^ 0x3b3b3b3b3b3b3b3b
low := uint64(0x7f7f7f7f7f7f7f7f)
matches := ^(((x & low) + low) | x | low) & 0x8080808080808080
```

NOT changes each bit to its opposite value.
The mask keeps only seven bits of each byte before addition.
This limits each byte sum to 254, so no carry crosses a byte boundary.

A zero lane sums to `0x7f`.
Its high bit stays clear until NOT sets it.
A nonzero value in the low seven bits sets the high bit during addition.
OR with `x` rejects lanes whose original high bit is set.
The final result marks exactly the zero bytes.

The scalar helper compresses these markers into eight position bits.
It joins eight word masks to cover the complete 64-byte window.
This helper supplies another exact mask for comparison.
However, the scalar version that processes a complete mask runs slower than production.
Correct output does not guarantee lower runtime.

## Dispatch once per worker and retain the fallback

SIMD means one instruction that processes several data values.
The native file builds only for `amd64 && goexperiment.simd`.
Its feature test asks whether the CPU supports AVX2 once when each worker starts.
Unsupported CPUs or builds use `chunkReaderScalar`, the existing pooled parser.

The build adapter enables experimental SIMD compiler support when available and honors explicit `nosimd`.
These statements describe the repository at the documented revision.
They do not establish a public Go release schedule.

Tests cover exact masks, unaligned starts, protected-page boundaries, and disabled CPU features.
They also cover build fallbacks, short final rows, overwritten buffers, and the pool bound.
The acceptance record includes small and full comparisons with reference output for every variant.
It also includes independent billion-row counts.
Tests cover the fallback paths for other CPUs and builds.

## Separate algorithm changes from build flags

The experiment includes four variants: normal production, production with matching SIMD flags, scalar mask batching, and AVX2 mask batching.
The control that changes only the flags runs somewhat slower in the screening comparison.
Comparing AVX2 only with that control can overstate the practical gain against the normal build.

The adoption evidence therefore uses direct normal-production comparisons:

![All four direct normal-production versus pooled AVX2 confirmation windows.](figures/simd-runtimes.svg)

| Corpus / independent window | Normal production | AVX2 | Runtime reduction |
| --- | ---: | ---: | ---: |
| Standard / 3 | 1.917895 s | 1.895101 s | 1.189% |
| Standard / 4 | 1.911743 s | 1.886221 s | 1.335% |
| 10K / 3 | 2.415082 s | 2.375341 s | 1.646% |
| 10K / 4 | 2.376231 s | 2.351397 s | 1.045% |

All four comparisons pass fresh null, order, and quiet-host controls.
The inputs remain resident in memory, measured files stay unchanged, and outputs match exactly.
Both interrupted measurement periods are excluded in full.
Standard saves roughly 23–26 ms in these observations.
10K saves roughly 25–40 ms.

Dispatch selects the implementation for the current CPU and build.
The numerical acceptance rule passes.
The initial engineering decision keeps SIMD as research because duplicate parsing paths, dispatch, and experimental compiler support add complexity.
The user then requests promotion of this exact tested version.
The commit uses the existing measurements.
It does not claim a new performance run at promotion time.

The earlier SIMD experiment before the pool did not meet the gain threshold.
The pool changes producer and GC behavior, so another comparison becomes useful.
This history supports another test when surrounding conditions change.
It does not prove that allocation was the only reason the earlier attempt failed.

Evidence: [all direct confirmation observations](data/timings.json),
[promotion and controls](../../EXPERIMENTS.md#pooled-mask-retries-and-simd-promotion-2026-10-06),
[vector kernel](../../mask_simd_amd64.go),
[mask tests](../../delimiter_mask_test.go),
[source change](data/history.md#0e5dd35).

[← Previous](07-bounded-buffer-reuse.md) · [Next: experiments that did not win →](09-experiments-that-did-not-win.md)
