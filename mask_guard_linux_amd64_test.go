//go:build linux && amd64 && goexperiment.simd

package main

import (
	"os"
	"syscall"
	"testing"
)

func TestSIMDMaskAtProtectedPageBoundaries(t *testing.T) {
	if !supportsAVX2Masks() {
		t.Skip("AVX2 backend unavailable")
	}
	page := os.Getpagesize()
	mapping, err := syscall.Mmap(-1, 0, 3*page, syscall.PROT_NONE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Munmap(mapping)
	writable := mapping[page : 2*page]
	if err := syscall.Mprotect(writable, syscall.PROT_READ|syscall.PROT_WRITE); err != nil {
		t.Fatal(err)
	}
	for offset := 0; offset < 32; offset++ {
		for _, start := range []int{offset, page - 64 - offset} {
			data := writable[start : start+64 : start+64]
			for i := range data {
				data[i] = byte(128 + i)
			}
			for _, i := range []int{0, 31, 32, 63} {
				data[i] = ';'
			}
			want := uint64(1) | uint64(1)<<31 | uint64(1)<<32 | uint64(1)<<63
			if got := semicolonMask64AVX2(data); got != want {
				t.Fatalf("start=%d got=%x want=%x", start, got, want)
			}
		}
	}
}

func TestSIMDMaskRejectsShortSlice(t *testing.T) {
	if !supportsAVX2Masks() {
		t.Skip("AVX2 backend unavailable")
	}
	for size := 0; size < 64; size++ {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("size=%d did not reject unsafe short load", size)
				}
			}()
			semicolonMask64AVX2(make([]byte, size, 64))
		}()
	}
}
