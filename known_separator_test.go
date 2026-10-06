package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestKnownSeparatorExhaustiveNumericAndTails(t *testing.T) {
	names := []string{"A", "seven77", "eight888", "abcdefghX", "abcdefgh\x00", "é", "😀", strings.Repeat("é", 50)}
	for _, n := range []int{15, 16, 31, 63, 64, 65, 100} {
		names = append(names, strings.Repeat("x", n))
	}
	for _, name := range names {
		for value := -999; value <= 999; value++ {
			absolute := value
			sign := ""
			if value < 0 {
				absolute = -value
				sign = "-"
			}
			row := name + ";" + fmt.Sprintf("%s%d.%d", sign, absolute/10, absolute%10) + "\n"
			for _, suffix := range []string{"", "X;0.0\n", "zzzzzzzz"} {
				end, n, v, w := parseLineAtSeparator([]byte(row+suffix), len(name))
				if end != len(row)-1 || string(n) != name || v != measurement(value) || w != originalWord(stationName(name)) {
					t.Fatalf("known row=%q suffix=%q got=%d,%q,%d,%x", row, suffix, end, n, v, w)
				}
			}
		}
		row := []byte(name + ";-0.0\n")
		for cut := 0; cut < len(row); cut++ {
			end, _, _, _ := parseLineAtSeparator(row[:cut:cut], len(name))
			if end != -1 {
				t.Fatalf("accepted truncated known row %q", row[:cut])
			}
		}
		end, _, value, _ := parseLineAtSeparator(row, len(name))
		if end != len(row)-1 || value != 0 {
			t.Fatalf("negative zero name=%q", name)
		}
	}
}
