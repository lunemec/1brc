# 4. A table built for repeated names

[`f8a90a5`](https://github.com/lunemec/1brc/commit/f8a90a532e4f6ac4f6c66cc9a86f4e28c52c5c03)
replaces bucket slices and statistics pointers with 32,768 entries.
Each entry stores the statistics directly:

```go
type flatEntry struct {
    name  stationName // 16-byte string descriptor on 64-bit Go
    stats stats       // 16 bytes: sum, min, max, count
    hash  uint64      // 8 bytes
}                     // total: 40 bytes
```

The name descriptor is inline, but its bytes remain separate.
Each worker's entry array occupies 1,310,720 bytes.
At 10,000 names, 30.52% of the slots contain entries.

## Use the complete short name as a fingerprint

A fingerprint is the number used to match a name.
`stationWord` packs up to eight name bytes and fills unused bytes with zero:

```text
"Oslo" → bytes 4f 73 6c 6f → 0x000000006f6c734f
```

The fingerprint multiplies that word by `0x517cc1b727220a95`, then rotates left by 17 bits.

Multiplication wraps to 64 bits, and rotation moves departing bits back to the other end.
The odd multiplier is reversible in 64-bit arithmetic.
Both operations preserve distinct words.

For names of the same length up to eight bytes, distinct names therefore have distinct full fingerprints.
Length still matters: `"A"` and `"A\x00"` both pad to `0x41`.
Long names hash every byte and still need complete string equality.

## Probe only when the home slot misses

The home slot is the first position searched for a name:

```go
pos := uint32(hash) & 32767 // keep the lowest 15 bits
```

For Oslo, `0xa782 & 0x7fff = 10114`.
Different full fingerprints can share this 15-bit position.
On a collision, the table searches consecutive slots and wraps at the end:

```text
home 32767 occupied by another name
     ↓
slot 0 occupied by another name
     ↓
slot 1 empty: insert here
```

The row loop keeps its frequent path small enough for the compiler to inline:

```go
st := out.homeHit(name, hash) // check only the initial slot
if st == nil {
    st = out.findSlow(name, hash) // collisions or insertion
}
updateStats(st, value)
```

`homeHit` examines one entry, while `findSlow` handles insertion and collision probes.
This split preserves the prototype's hash and layout.
The accepted comparison measures the whole replacement, not inlining alone.

Workers can insert names in different orders, so their occupied slots can differ.
The merge must look up each name in the destination table.
It cannot reuse the source's final slot.

## Own each inserted name

The parser borrows bytes from a chunk.
Insertion copies a newly seen name:

```go
entry.name = stationName(strings.Clone(string(name)))
```

Repeated rows allocate no new key.
The stored name no longer keeps a whole input chunk alive.
This lifetime rule enables [buffer reuse](07-bounded-buffer-reuse.md).

## Result

![Robust-table runtime observations.](figures/table-runtimes.svg)

| Corpus / window | Old table mean | Accepted table mean | Runtime reduction |
| --- | ---: | ---: | ---: |
| Standard / 9 | 2.587160 s | 2.443863 s | 5.539% |
| 10K / 3 | 5.744953 s | 3.309773 s | 42.388% |

Both comparisons pass their fresh null, order, host, residency, and exact-output controls.
The null drift is 0.062% / 0.962% on standard / 10K.
The [acceptance record](../../EXPERIMENTS.md#accepted-robust-table-and-hotcold-split-2026-10-06) contains the full measurements and correctness evidence.

[← Previous](03-swar-separator-scan.md) · [Next: reuse the loaded word →](05-reusing-the-first-word.md)
