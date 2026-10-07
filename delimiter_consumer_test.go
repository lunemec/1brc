package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestDelimiterConsumerBoundariesAndStatistics(t *testing.T) {
	var data bytes.Buffer
	expected := make(map[stationName]stats)
	add := func(name string, value int) {
		sign := ""
		absolute := value
		if value < 0 {
			sign = "-"
			absolute = -value
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
	for offset := 0; offset < 64; offset++ {
		for _, length := range []int{1, 7, 8, 9, 15, 16, 17, 31, 32, 63, 64, 65, 99, 100} {
			for _, value := range []int{-999, -1, 0, 999} {
				gap := (offset - data.Len()%64 + 64) % 64
				if gap < 6 {
					gap += 64
				}
				add(strings.Repeat("p", gap-5), 0)
				if data.Len()%64 != offset {
					t.Fatal("bad boundary fixture")
				}
				add(strings.Repeat("x", length), value)
				add("same", -value)
			}
		}
	}
	for _, name := range []string{"é", "😀", "abcdefg😀", "a:b:c", strings.Repeat("é", 50), "abcdefgh\x00", "abcdefgh\x00\x00"} {
		add(name, -126)
		add(name, 999)
	}
	for i := 0; i < 20; i++ {
		add("tiny", i)
	}
	raw := data.Bytes()
	chunks := make(chan chunk, 3)
	// Different chunk starts reset block alignment and include complete tails.
	start := 0
	for part := 1; part <= 3; part++ {
		end := len(raw)
		if part < 3 {
			end = part * len(raw) / 3
			end += bytes.IndexByte(raw[end:], '\n') + 1
		}
		chunks <- chunk{data: raw[start:end]}
		start = end
	}
	close(chunks)
	result := chunkReader(chunks)
	var rows uint64
	if result.len() != len(expected) {
		t.Fatalf("station count=%d want=%d", result.len(), len(expected))
	}
	for name, want := range expected {
		got, present := result.get(result.pos(name), name)
		if !present || *got != want {
			t.Fatalf("name=%q present=%v got=%v want=%v", name, present, got, want)
		}
		rows += uint64(got.count)
	}
	if rows != uint64(bytes.Count(raw, []byte{'\n'})) {
		t.Fatalf("consumed=%d complete rows=%d", rows, bytes.Count(raw, []byte{'\n'}))
	}
}

func TestDelimiterConsumerStopsAtTruncatedFinalRow(t *testing.T) {
	complete := strings.Repeat("A;0.0\n", 32)
	for _, name := range []string{"B", strings.Repeat("x", 63), strings.Repeat("x", 64), strings.Repeat("x", 100)} {
		row := name + ";-99.9\n"
		for cut := 0; cut < len(row); cut++ {
			chunks := make(chan chunk, 1)
			chunks <- chunk{data: []byte(complete + row[:cut])}
			close(chunks)
			result := chunkReader(chunks)
			got, ok := result.get(result.pos("A"), "A")
			if !ok || got.count != 32 || result.len() != 1 {
				t.Fatalf("name length=%d cut=%d count/table=%v/%d", len(name), cut, got, result.len())
			}
		}
	}
}
