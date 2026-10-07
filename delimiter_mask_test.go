package main

import (
	"encoding/binary"
	"math/rand"
	"testing"
)

func TestSemicolonBitsAllAdjacentBytes(t *testing.T) {
	for lane := 0; lane < 7; lane++ {
		for a := 0; a < 256; a++ {
			for b := 0; b < 256; b++ {
				word := uint64(0x4141414141414141)&^(uint64(0xffff)<<(lane*8)) | uint64(a)<<(lane*8) | uint64(b)<<((lane+1)*8)
				var want uint64
				if a == ';' {
					want |= uint64(1) << lane
				}
				if b == ';' {
					want |= uint64(1) << (lane + 1)
				}
				if got := semicolonBits(word); got != want {
					t.Fatalf("lane=%d bytes=%x,%x got=%x want=%x", lane, a, b, got, want)
				}
			}
		}
	}
}

func TestSemicolonMask64AgainstBytes(t *testing.T) {
	random := rand.New(rand.NewSource(20261006))
	for sample := 0; sample < 5000; sample++ {
		storage := make([]byte, 71)
		random.Read(storage)
		for alignment := 0; alignment < 8; alignment++ {
			data := storage[alignment : alignment+64]
			var want uint64
			for i, b := range data {
				if b == ';' {
					want |= uint64(1) << i
				}
			}
			if got := semicolonMask64(data); got != want {
				t.Fatalf("sample=%d alignment=%d got=%x want=%x", sample, alignment, got, want)
			}
		}
	}
	for position := 0; position < 64; position++ {
		for value := 0; value < 256; value++ {
			var data [64]byte
			data[position] = byte(value)
			var want uint64
			if value == ';' {
				want = uint64(1) << position
			}
			if got := semicolonMask64(data[:]); got != want {
				t.Fatalf("position=%d byte=%x got=%x want=%x", position, value, got, want)
			}
		}
	}
	var all [64]byte
	for i := range all {
		all[i] = ';'
	}
	if got := semicolonMask64(all[:]); got != ^uint64(0) {
		t.Fatalf("all separators=%x", got)
	}
	var word [8]byte
	copy(word[:], "A;0.0\nB;")
	if semicolonBits(binary.LittleEndian.Uint64(word[:])) != 0x82 {
		t.Fatal("documented mask example changed")
	}
}

func TestSIMDMask64AgainstBytes(t *testing.T) {
	if !supportsAVX2Masks() {
		t.Skip("AVX2 backend unavailable; consumer fallback is tested separately")
	}
	random := rand.New(rand.NewSource(20261006))
	for sample := 0; sample < 5000; sample++ {
		storage := make([]byte, 71)
		random.Read(storage)
		for alignment := 0; alignment < 8; alignment++ {
			data := storage[alignment : alignment+64]
			var want uint64
			for i, b := range data {
				if b == ';' {
					want |= uint64(1) << i
				}
			}
			if got := semicolonMask64AVX2(data); got != want {
				t.Fatalf("sample=%d alignment=%d got=%x want=%x", sample, alignment, got, want)
			}
		}
	}
	for position := 0; position < 64; position++ {
		for value := 0; value < 256; value++ {
			var data [64]byte
			data[position] = byte(value)
			var want uint64
			if value == ';' {
				want = uint64(1) << position
			}
			if got := semicolonMask64AVX2(data[:]); got != want {
				t.Fatalf("position=%d byte=%x got=%x want=%x", position, value, got, want)
			}
		}
	}
	var all [64]byte
	for i := range all {
		all[i] = ';'
	}
	if got := semicolonMask64AVX2(all[:]); got != ^uint64(0) {
		t.Fatalf("all separators=%x", got)
	}
	var word [8]byte
	copy(word[:], "A;0.0\nB;")
	if semicolonBits(binary.LittleEndian.Uint64(word[:])) != 0x82 {
		t.Fatal("documented mask example changed")
	}
}
