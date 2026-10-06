package main

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"math/rand"
	"testing"
	"unsafe"
)

func originalWord(name stationName) uint64 {
	if len(name) >= 8 {
		return binary.LittleEndian.Uint64([]byte(name))
	}
	var word uint64
	shift := 0
	if len(name) >= 4 {
		word = uint64(binary.LittleEndian.Uint32([]byte(name)))
		name = name[4:]
		shift = 32
	}
	if len(name) >= 2 {
		word |= uint64(binary.LittleEndian.Uint16([]byte(name))) << shift
		name = name[2:]
		shift += 16
	}
	if len(name) > 0 {
		word |= uint64(name[0]) << shift
	}
	return word
}
func originalFingerprint(name stationName) uint64 {
	word := originalWord(name)
	for offset := 8; offset < len(name); offset += 8 {
		word = bits.RotateLeft64(word*0x517cc1b727220a95^originalWord(name[offset:]), 17)
	}
	return bits.RotateLeft64(word*0x517cc1b727220a95, 17)
}

func TestHotpathFingerprintCompatibility(t *testing.T) {
	r := rand.New(rand.NewSource(20261005))
	for size := 0; size <= 100; size++ {
		for sample := 0; sample < 20; sample++ {
			b := make([]byte, size)
			r.Read(b)
			name := stationName(string(b))
			if got, want := stationFingerprint(name), originalFingerprint(name); got != want {
				t.Fatalf("fingerprint size=%d sample=%d: got %x want %x", size, sample, got, want)
			}
		}
	}
}

func wrapKeys(t *testing.T) []stationName {
	t.Helper()
	names := make([]stationName, 0, 3)
	for i := 0; len(names) < 3 && i < 2000000; i++ {
		name := stationName(fmt.Sprintf("wrap-%d", i))
		if uint32(stationFingerprint(name))&flatMask == flatMask {
			names = append(names, name)
		}
	}
	if len(names) != 3 {
		t.Fatal("could not construct wraparound collision fixture")
	}
	return names
}

func TestHotpathCollisionWrapAndMerge(t *testing.T) {
	names := wrapKeys(t)
	left, right := newSimpleMap(maxStations), newSimpleMap(maxStations)
	for i, name := range names {
		updateStats(left.find(name), measurement(i+1))
	}
	for i := len(names) - 1; i >= 0; i-- {
		updateStats(right.find(names[i]), measurement(-(i + 1)))
	}
	for _, m := range []simpleMap{left, right} {
		for _, slot := range []uint32{flatMask, 0, 1} {
			if m.data[slot].name == "" {
				t.Fatalf("wraparound slot %d remained empty", slot)
			}
		}
	}
	sumChunk(left, right)
	for i, name := range names {
		got, ok := left.get(left.pos(name), name)
		want := stats{sum: 0, min: minT(-(i + 1)), max: maxT(i + 1), count: 2}
		if !ok || *got != want {
			t.Fatalf("merged %q: got %v present=%v want %v", name, got, ok, want)
		}
		if left.find(name) != got {
			t.Fatalf("repeat lookup %q changed slot", name)
		}
	}
	if left.len() != 3 {
		t.Fatalf("merge introduced extra keys: %d", left.len())
	}
}

func TestHotpathOwnsInsertedNames(t *testing.T) {
	for _, text := range []string{"short", "abcdefghX", "abcdefghY", "12345678abcdefghX"} {
		b := []byte(text)
		borrowed := stationName(unsafe.String(unsafe.SliceData(b), len(b)))
		m := newSimpleMap(maxStations)
		updateStats(m.find(borrowed), 42)
		for i := range b {
			b[i] = '!'
		}
		key := stationName(text)
		got, ok := m.get(m.pos(key), key)
		if !ok || got.count != 1 || got.sum != 42 {
			t.Fatalf("name %q was not owned", text)
		}
	}
}

func TestHotpathEntrySize(t *testing.T) {
	if got := unsafe.Sizeof(flatEntry{}); got != 40 {
		t.Fatalf("entry size changed to %d", got)
	}
}
