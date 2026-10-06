package main

import (
	"strings"
	"testing"
	"time"
)

func testMaskPoolImmediateReuse(t *testing.T, reader func(chan chunk) simpleMap) {
	t.Helper()
	storage := make([]byte, 4096)
	recycled := make(chan []byte, 1)
	chunks := make(chan chunk)
	finished := make(chan simpleMap, 1)
	go func() { finished <- reader(chunks) }()
	long := strings.Repeat("x", 100)
	fixtures := []string{
		strings.Repeat("A;1.2\n", 32) + long + ";-99.9\nabcdefghX;9.9\né;0.0\ntail;-0.1\ncut;-99.9",
		strings.Repeat("B;-1.0\n", 9) + "last;9.9\n" + "unfinished",
		"",
		strings.Repeat("z", 100),
		"final;1.0\n",
	}
	for _, text := range fixtures {
		for i := range storage {
			storage[i] = "stale;9.9\n"[i%10]
		}
		copy(storage, text)
		select {
		case chunks <- chunk{data: storage[:len(text)], recycle: recycled}:
		case <-time.After(2 * time.Second):
			t.Fatal("worker did not accept next loan")
		}
		select {
		case returned := <-recycled:
			if len(returned) != len(storage) || cap(returned) != cap(storage) || &returned[0] != &storage[0] {
				t.Fatal("loan was not returned in full")
			}
			for i := range returned {
				returned[i] = 0xa5
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("loan with len=%d was not returned before next handoff", len(text))
		}
	}
	close(chunks)
	var result simpleMap
	select {
	case result = <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not finish after all returns")
	}
	select {
	case <-recycled:
		t.Fatal("a loan was returned more than once")
	default:
	}
	want := map[stationName]stats{
		"A":               {sum: 384, min: 12, max: 12, count: 32},
		stationName(long): {sum: -999, min: -999, max: -999, count: 1},
		"abcdefghX":       {sum: 99, min: 99, max: 99, count: 1},
		"é":               {sum: 0, min: 0, max: 0, count: 1},
		"tail":            {sum: -1, min: -1, max: -1, count: 1},
		"B":               {sum: -90, min: -10, max: -10, count: 9},
		"last":            {sum: 99, min: 99, max: 99, count: 1},
		"final":           {sum: 10, min: 10, max: 10, count: 1},
	}
	if result.len() != len(want) {
		t.Fatalf("stations=%d want=%d", result.len(), len(want))
	}
	for _, entry := range result.Iter() {
		expected, present := want[entry.name]
		if !present || *entry.stats != expected {
			t.Fatalf("owned key/statistics changed after reuse: name=%q got=%+v", entry.name, entry.stats)
		}
	}
}

func TestMaskPoolImmediateReuseAndOwnedKeys(t *testing.T) {
	testMaskPoolImmediateReuse(t, chunkReader)
}

func testMaskPoolAllShortEOFViews(t *testing.T, reader func(chan chunk) simpleMap) {
	t.Helper()
	for size := 0; size < 64; size++ {
		storage := make([]byte, 4096)
		for i := range storage {
			storage[i] = "stale;9.9\n"[i%10]
		}
		copy(storage, strings.Repeat("A;1.0\n", 11)[:size])
		recycled := make(chan []byte, 2)
		chunks := make(chan chunk, 1)
		chunks <- chunk{data: storage[:size], recycle: recycled}
		close(chunks)
		result := reader(chunks)
		if len(recycled) != 1 {
			t.Fatalf("EOF len=%d returns=%d want=1", size, len(recycled))
		}
		returned := <-recycled
		if len(returned) != len(storage) || cap(returned) != cap(storage) || &returned[0] != &storage[0] {
			t.Fatalf("EOF len=%d did not return complete backing storage", size)
		}
		for i := range returned {
			returned[i] = 0xa5
		}
		rows := size / 6
		if rows == 0 {
			if result.len() != 0 {
				t.Fatalf("EOF len=%d consumed bytes outside logical view", size)
			}
			continue
		}
		want := stats{sum: sumT(rows * 10), min: 10, max: 10, count: countT(rows)}
		for _, entry := range result.Iter() {
			if entry.name != "A" || *entry.stats != want {
				t.Fatalf("EOF len=%d got name=%q stats=%+v", size, entry.name, entry.stats)
			}
		}
		if result.len() != 1 {
			t.Fatalf("EOF len=%d stations=%d want=1", size, result.len())
		}
	}
}

func TestMaskPoolAllShortEOFViews(t *testing.T) {
	testMaskPoolAllShortEOFViews(t, chunkReader)
}
