package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestParserWordMatchesIndependentFingerprint(t *testing.T) {
	names := []string{"é", "€", "😀", "a\u0301", "A\x00", "A\x00\x00", "abcdefghX", "abcdefghY"}
	for size := 1; size <= 100; size++ {
		names = append(names, strings.Repeat("a", size))
	}
	for _, name := range names {
		for _, temperature := range []string{"0.0", "9.9", "99.9", "-0.1", "-9.9", "-99.9"} {
			line := name + ";" + temperature + "\n"
			for _, suffix := range []string{"", "X;0.0\n", "different;99.9\n"} {
				end, gotName, _, word := parseLine([]byte(line + suffix))
				if end != len(line)-1 || string(gotName) != name {
					t.Fatalf("name=%q temperature=%s suffix=%q: end=%d name=%q", name, temperature, suffix, end, gotName)
				}
				if want := originalWord(stationName(name)); word != want {
					t.Fatalf("name=%q temperature=%s suffix=%q: word=%x want=%x", name, temperature, suffix, word, want)
				}
				if got, want := stationFingerprintLongFromWord(gotName, word), originalFingerprint(stationName(name)); got != want {
					t.Fatalf("fingerprint %q: got=%x want=%x", name, got, want)
				}
			}
		}
	}
}

func TestParserWordTruncatedRows(t *testing.T) {
	for _, name := range []string{"A", "AB", "é", "12345678", "123456789", strings.Repeat("x", 100)} {
		for _, temperature := range []string{"0.0", "99.9", "-0.1", "-99.9"} {
			row := []byte(name + ";" + temperature + "\n")
			for end := 0; end < len(row); end++ {
				if got, _, _, _ := parseLine(row[:end:end]); got != -1 {
					t.Fatalf("accepted truncated row %q", row[:end])
				}
			}
		}
	}
}

func TestParserWordChunkStatistics(t *testing.T) {
	names := []string{"A", "AB", "é", "€", "😀", "A\x00", "A\x00\x00", "12345678", "abcdefghX", "abcdefghY", strings.Repeat("x", 100)}
	chunks := make(chan chunk, 3)
	for _, temperature := range []string{"-99.9", "0.0", "99.9"} {
		var data strings.Builder
		for _, name := range names {
			fmt.Fprintf(&data, "%s;%s\n", name, temperature)
		}
		chunks <- chunk{data: []byte(data.String())}
	}
	close(chunks)
	got := chunkReader(chunks)
	if got.len() != len(names) {
		t.Fatalf("station count=%d want=%d", got.len(), len(names))
	}
	for _, name := range names {
		key := stationName(name)
		actual, present := got.get(got.pos(key), key)
		want := stats{sum: 0, min: -999, max: 999, count: 3}
		if !present || *actual != want {
			t.Fatalf("station %q: present=%v stats=%v want=%v", name, present, actual, want)
		}
	}
}
