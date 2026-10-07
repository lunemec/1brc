#!/usr/bin/env python3
"""Print and verify the blog's word arithmetic using bounded teaching inputs."""

from pathlib import Path

MASK64 = (1 << 64) - 1
MULTIPLIER = 0x517CC1B727220A95
ROOT = Path(__file__).resolve().parents[3]


def rotate_left(word, count):
    return ((word << count) | (word >> (64 - count))) & MASK64


def station_word(name):
    return int.from_bytes(name[:8].ljust(8, b"\0"), "little")


def fingerprint(name):
    word = station_word(name)
    for offset in range(8, len(name), 8):
        word = rotate_left(((word * MULTIPLIER) & MASK64) ^ station_word(name[offset:]), 17)
    return rotate_left((word * MULTIPLIER) & MASK64, 17)


def trailing_zeros(word):
    return (word & -word).bit_length() - 1 if word else 64


def first_match(word):
    x = word ^ 0x3B3B3B3B3B3B3B3B
    return ((x - 0x0101010101010101) & MASK64) & (~x & MASK64) & 0x8080808080808080


def exact_mask(word):
    x = word ^ 0x3B3B3B3B3B3B3B3B
    low = 0x7F7F7F7F7F7F7F7F
    matches = (~(((x & low) + low) | x | low) & MASK64) & 0x8080808080808080
    return (((matches >> 7) * 0x0102040810204080) & MASK64) >> 56


def temperature_trace(eight_bytes):
    assert len(eight_bytes) == 8
    word = int.from_bytes(eight_bytes, "little")
    dot_mask = (~word & MASK64) & 0x10101000
    dot_pos = trailing_zeros(dot_mask)
    assert dot_pos in (12, 20, 28)
    sign_word = ((~word & MASK64) << 59) & MASK64
    signed = -1 if sign_word & (1 << 63) else 0
    cleared = word & (~(signed & 0xFF) & MASK64)
    aligned = (cleared << (28 - dot_pos)) & MASK64
    digits = aligned & 0x0F000F0F00
    product = (digits * 0x640A0001) & MASK64
    absolute = (product >> 32) & 0x3FF
    value = (absolute ^ signed) - signed
    return dict(word=word, dot_mask=dot_mask, dot_pos=dot_pos, signed=signed,
                cleared=cleared, aligned=aligned, digits=digits,
                product=product, absolute=absolute, value=value)


def print_trace(label, trace):
    print(label)
    for key, value in trace.items():
        if key in {"dot_pos", "signed", "absolute", "value"}:
            print(f"  {key:12} {value}")
        else:
            print(f"  {key:12} 0x{value:016x}")


def main():
    word = int.from_bytes(b"Oslo;1.2", "little")
    x = word ^ 0x3B3B3B3B3B3B3B3B
    matches = first_match(word)
    assert word == 0x322E313B6F6C734F
    assert matches == 0x0000008000000000
    assert trailing_zeros(matches) // 8 == 4
    print_trace("SWAR: Oslo;1.2", dict(word=word, xor=x,
                subtract=(x - 0x0101010101010101) & MASK64,
                inverted=~x & MASK64, matches=matches))

    borrow = int.from_bytes(b";:" + b"\0" * 6, "little")
    assert first_match(borrow) == 0x8080
    assert exact_mask(borrow) == 1
    print(f"Borrow example: first-match word mask=0x{first_match(borrow):x}; exact byte mask=0x{exact_mask(borrow):x}")

    # Independent byte comparisons verify first-match behavior and the exact
    # enumerating mask, including every adjacent pair and high-bit byte values.
    for first in range(256):
        for second in range(256):
            data = bytes([first, second]) + b";" + b"\0" * 5
            packed = int.from_bytes(data, "little")
            assert trailing_zeros(first_match(packed)) // 8 == data.index(b";")
            expected = sum(1 << i for i, value in enumerate(data) if value == ord(";"))
            assert exact_mask(packed) == expected

    real_rows = (ROOT / "test/resources/samples/measurements-10.txt").read_bytes().splitlines()
    real_row = next(row for row in real_rows if row.startswith(b"Cabo San Lucas;"))
    assert real_row == b"Cabo San Lucas;14.9"
    first = first_match(int.from_bytes(real_row[:8], "little"))
    second = first_match(int.from_bytes(real_row[8:16], "little"))
    assert first == 0 and 8 + trailing_zeros(second) // 8 == 14

    normalized = word & ((1 << (4 * 8)) - 1)
    assert normalized == station_word(b"Oslo") == 0x6F6C734F
    assert fingerprint(b"Oslo") == 0xA618E03C65F6A782
    assert fingerprint(b"Oslo") & 32767 == 10114
    assert fingerprint(b"abcdefghX") == 0x882CFCBFF17DDF39
    assert station_word(b"A") == station_word(b"A\0")
    print(f"Oslo: normalized=0x{normalized:016x}, fingerprint=0x{fingerprint(b'Oslo'):016x}, slot=10114")

    for spelling in ["1.2", "12.6", "-1.2", "-12.6"]:
        eight = (spelling + "\nPa").encode()[:8].ljust(8, b"\0")
        print_trace(f"Temperature {spelling}", temperature_trace(eight))

    for value in range(-999, 1000):
        absolute = abs(value)
        spelling = ("-" if value < 0 else "") + f"{absolute // 10}.{absolute % 10}"
        for suffix in [b"\0" * 8, b"Paris;0.0\n", b"\xff" * 8]:
            data = (spelling.encode() + b"\n" + suffix)[:8]
            trace = temperature_trace(data)
            assert trace["value"] == value
            assert trace["dot_pos"] // 8 + 2 == len(spelling)
    assert temperature_trace(b"-0.0\nPa\0")["value"] == 0

    block = (b"A;0.0\nB;1.0\n").ljust(64, b"\0")
    mask = sum(exact_mask(int.from_bytes(block[i:i + 8], "little")) << i for i in range(0, 64, 8))
    assert mask == 0x82
    positions = []
    while mask:
        positions.append(trailing_zeros(mask))
        mask &= mask - 1
    assert positions == [1, 7]
    print("Batch mask: 0x82 → separator 1 → 0x80 → separator 7 → 0")
    print("Verified 65,536 adjacent-byte cases, all 1,999 temperature values with three suffixes, -0.0, fingerprints, and mask positions.")


if __name__ == "__main__":
    main()
