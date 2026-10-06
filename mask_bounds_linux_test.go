//go:build linux

package main

import (
	"os"
	"strings"
	"syscall"
	"testing"
)

func TestScalarMaskAtProtectedPageBoundaries(t *testing.T) {
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
	for offset := 0; offset < 64; offset++ {
		for _, start := range []int{offset, page - 64 - offset} {
			data := writable[start : start+64 : start+64]
			for i := range data {
				data[i] = byte(128 + i)
			}
			for _, i := range []int{0, 7, 8, 31, 32, 63} {
				data[i] = ';'
			}
			want := uint64(1) | uint64(1)<<7 | uint64(1)<<8 | uint64(1)<<31 | uint64(1)<<32 | uint64(1)<<63
			if got := semicolonMask64(data); got != want {
				t.Fatalf("start=%d got=%x want=%x", start, got, want)
			}
		}
	}
}

func TestScalarMaskRejectsShortSlice(t *testing.T) {
	for size := 0; size < 64; size++ {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("size=%d did not reject short load", size)
				}
			}()
			semicolonMask64(make([]byte, size, 64))
		}()
	}
}

func TestMaskReaderAtProtectedPageBounds(t *testing.T) {
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
	for rows := 0; rows <= 64; rows++ {
		for _, tail := range []string{"", "X;1.2", strings.Repeat("x", 100) + ";-99.9"} {
			text := strings.Repeat("A;0.0\n", rows) + tail
			for _, start := range []int{0, page - len(text)} {
				data := writable[start : start+len(text) : start+len(text)]
				copy(data, text)
				chunks := make(chan chunk, 1)
				chunks <- chunk{data: data}
				close(chunks)
				result := chunkReader(chunks)
				if rows == 0 {
					if result.len() != 0 {
						t.Fatalf("tail=%q accepted incomplete row", tail)
					}
					continue
				}
				got, present := result.get(result.pos("A"), "A")
				if result.len() != 1 || !present || got.count != countT(rows) || got.sum != 0 || got.min != 0 || got.max != 0 {
					t.Fatalf("rows=%d tail length=%d result=%v", rows, len(tail), got)
				}
			}
		}
	}
}
