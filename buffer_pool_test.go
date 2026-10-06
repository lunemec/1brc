package main

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBufferPoolBoundedLoansAndReuse(t *testing.T) {
	pool := newChunkBufferPool(128, 2)
	first, second := pool.take(), pool.take()
	if &first[0] == &second[0] {
		t.Fatal("outstanding loans share storage")
	}
	first[0], second[0] = 'A', 'B'
	waiting := make(chan struct{})
	acquired := make(chan []byte, 1)
	go func() { close(waiting); acquired <- pool.take() }()
	<-waiting
	select {
	case <-acquired:
		t.Fatal("third loan exceeded the pool bound")
	case <-time.After(20 * time.Millisecond):
	}
	pool.available <- first
	select {
	case reused := <-acquired:
		if &reused[0] != &first[0] || len(reused) != 128 || cap(reused) != 128 || reused[0] != 'A' {
			t.Fatal("returned buffer was not reused intact")
		}
		pool.available <- reused
	case <-time.After(2 * time.Second):
		t.Fatal("returned buffer did not wake producer")
	}
	pool.available <- second
	if pool.allocated != 2 {
		t.Fatalf("allocated=%d", pool.allocated)
	}
}

func TestBufferPoolPrefersReturns(t *testing.T) {
	pool := newChunkBufferPool(64, 17)
	first := pool.take()
	for i := 0; i < 1000; i++ {
		pool.available <- first
		next := pool.take()
		if &next[0] != &first[0] {
			t.Fatal("available buffer was replaced by an allocation")
		}
	}
	if pool.allocated != 1 {
		t.Fatalf("allocated=%d want1", pool.allocated)
	}
}

func TestBufferPoolReturnsEmptyAndTruncatedChunks(t *testing.T) {
	for _, text := range []string{"", "Oslo;-99.9", "A;0.0\nincomplete", "abcdefghX;1.2\n"} {
		storage := make([]byte, 64)
		copy(storage, text)
		recycled := make(chan []byte, 1)
		chunks := make(chan chunk, 1)
		chunks <- chunk{data: storage[:len(text)], recycle: recycled}
		close(chunks)
		result := chunkReader(chunks)
		select {
		case returned := <-recycled:
			if len(returned) != 64 || cap(returned) != 64 || &returned[0] != &storage[0] {
				t.Fatal("loan was not returned in full")
			}
			for i := range returned {
				returned[i] = '!'
			}
		default:
			t.Fatalf("chunk %q was not returned", text)
		}
		want := 0
		if strings.HasSuffix(text, "\n") || strings.HasPrefix(text, "A;") {
			want = 1
		}
		if result.len() != want {
			t.Fatalf("text=%q stations=%d want=%d", text, result.len(), want)
		}
		if want != 0 {
			key := stationName("A")
			sum := sumT(0)
			if strings.HasPrefix(text, "abcdefgh") {
				key = "abcdefghX"
				sum = 12
			}
			got, ok := result.get(result.pos(key), key)
			if !ok || got.count != 1 || got.sum != sum {
				t.Fatalf("returned input corrupted owned key %q: %v", key, got)
			}
		}
	}
}

type observedPoolReader struct {
	reader  *bytes.Reader
	buffers map[*byte][]byte
	mu      sync.Mutex
}

func (r *observedPoolReader) ReadAt(data []byte, offset int64) (int, error) {
	n, err := r.reader.ReadAt(data, offset)
	for i := n; i < len(data); i++ {
		data[i] = '?'
	}
	r.mu.Lock()
	r.buffers[&data[0]] = data
	r.mu.Unlock()
	return n, err
}

func TestBufferPoolPipelineOwnsNamesAndCounts(t *testing.T) {
	names := []string{"short", "abcdefghX", "abcdefghY", "é", "😀", strings.Repeat("x", 100), "abcdefgh\x00", "\ue000"}
	var data bytes.Buffer
	expected := make(map[stationName]stats)
	for i := 0; i < 12000; i++ {
		name := names[i%len(names)]
		value := i%1999 - 999
		absolute, sign := value, ""
		if value < 0 {
			absolute = -value
			sign = "-"
		}
		fmt.Fprintf(&data, "%s;%s%d.%d\n", name, sign, absolute/10, absolute%10)
		key := stationName(name)
		s, ok := expected[key]
		if !ok {
			s.min = minT(value)
			s.max = maxT(value)
		} else {
			if minT(value) < s.min {
				s.min = minT(value)
			}
			if maxT(value) > s.max {
				s.max = maxT(value)
			}
		}
		s.sum += sumT(value)
		s.count++
		expected[key] = s
	}
	for _, workers := range []int{1, 4, 16} {
		reader := &observedPoolReader{reader: bytes.NewReader(data.Bytes()), buffers: make(map[*byte][]byte)}
		chunks := chunkByBytes(reader, 256)
		results := make(chan simpleMap, workers)
		var group sync.WaitGroup
		group.Add(workers)
		for i := 0; i < workers; i++ {
			go func() { defer group.Done(); results <- chunkReader(chunks) }()
		}
		group.Wait()
		close(results)
		sum := newSimpleMap(maxStations)
		for result := range results {
			sumChunk(sum, result)
		}
		reader.mu.Lock()
		if len(reader.buffers) > chunkReaders+1 {
			t.Fatalf("workers=%d buffers=%d limit=%d", workers, len(reader.buffers), chunkReaders+1)
		}
		if len(reader.buffers) == 0 {
			t.Fatal("no reader buffers observed")
		}
		for _, buffer := range reader.buffers {
			for i := range buffer {
				buffer[i] = '!'
			}
		}
		reader.mu.Unlock()
		stations := 0
		for _, entry := range sum.Iter() {
			stations++
			if _, present := expected[entry.name]; !present {
				t.Fatalf("returned buffer changed stored name %q", entry.name)
			}
		}
		if stations != len(expected) {
			t.Fatalf("stations=%d want=%d", stations, len(expected))
		}
		var rows uint64
		for name, want := range expected {
			got, ok := sum.get(sum.pos(name), name)
			if !ok || *got != want {
				t.Fatalf("workers=%d name=%q got=%v want=%v", workers, name, got, want)
			}
			rows += uint64(got.count)
		}
		if rows != 12000 {
			t.Fatalf("workers=%d rows=%d", workers, rows)
		}
	}
}

func TestBufferPoolProducerEOFBoundaries(t *testing.T) {
	for _, text := range []string{"", strings.Repeat("A;0.0\n", 16), strings.Repeat("A;0.0\n", 16) + "B;-99.9\n", strings.Repeat("A;0.0\n", 16) + "B;-99.9"} {
		reader := &observedPoolReader{reader: bytes.NewReader([]byte(text)), buffers: make(map[*byte][]byte)}
		chunks := chunkByBytes(reader, 48)
		var actual bytes.Buffer
		for c := range chunks {
			actual.Write(c.data)
			c.release()
		}
		if actual.String() != text {
			t.Fatalf("EOF bytes changed: len=%d want=%d", actual.Len(), len(text))
		}
		if len(reader.buffers) > chunkReaders+1 {
			t.Fatal("EOF path exceeded bound")
		}
	}
}

var _ io.ReaderAt = (*observedPoolReader)(nil)
