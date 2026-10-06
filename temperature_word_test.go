package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestTemperatureWordExhaustiveValuesAndTails(t *testing.T) {
	names := []string{"a", "seven77", "eight888", "fifteen12345678", "sixteen123456789", strings.Repeat("é", 50), "😀"}
	for _, n := range []int{1, 7, 8, 15, 16, 31, 63, 100} {
		names = append(names, strings.Repeat("x", n))
	}
	values := make([]string, 0, 2000)
	expected := make([]measurement, 0, 2000)
	for v := -999; v <= 999; v++ {
		sign := ""
		av := v
		if v < 0 {
			sign = "-"
			av = -v
		}
		values = append(values, fmt.Sprintf("%s%d.%d", sign, av/10, av%10))
		expected = append(expected, measurement(v))
	}
	values = append(values, "-0.0")
	expected = append(expected, 0)
	for _, name := range names {
		for i, number := range values {
			row := []byte(name + ";" + number + "\n")
			for pad := 0; pad <= 8; pad++ {
				data := append(append([]byte{}, row...), bytes.Repeat([]byte{'z'}, pad)...)
				end, gotName, gotValue, word := parseLine(data)
				if end != len(row)-1 || string(gotName) != name || gotValue != expected[i] || word != originalWord(stationName(name)) {
					t.Fatalf("row %q pad %d got %d %q %d expected %d", row, pad, end, gotName, gotValue, expected[i])
				}
			}
			for cut := 0; cut < len(row); cut++ {
				end, _, _, _ := parseLine(row[:cut:cut])
				if end != -1 {
					t.Fatalf("truncated row %q accepted at %d", row[:cut], end)
				}
			}
		}
	}
}

func TestTemperatureWordAllNameLengthsAndAlignment(t *testing.T) {
	for size := 1; size <= 100; size++ {
		name := strings.Repeat("a", size)
		for _, temperature := range []string{"0.0", "99.9", "-0.1", "-99.9"} {
			row := name + ";" + temperature + "\n"
			for _, suffix := range []string{"", "X;0.0\n", "z;99.9\n"} {
				for alignment := 0; alignment < 8; alignment++ {
					storage := []byte(strings.Repeat("!", alignment) + row + suffix)
					end, gotName, value, word := parseLine(storage[alignment:])
					expected := map[string]measurement{"0.0": 0, "99.9": 999, "-0.1": -1, "-99.9": -999}[temperature]
					if end != len(row)-1 || string(gotName) != name || value != expected || word != originalWord(stationName(name)) {
						t.Fatalf("row=%q suffix=%q alignment=%d got=%d,%q,%d,%x", row, suffix, alignment, end, gotName, value, word)
					}
				}
			}
		}
	}
}
