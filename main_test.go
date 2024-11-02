package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	stationNames = []stationName{
		stationName("Port Moresby"),
		stationName("Seoul"),
		stationName("Libreville"),
		stationName("Mandalay"),
		stationName("Garissa"),
		stationName("Mek'ele"),
		stationName("Novosibirsk"),
		stationName("Mexico City"),
		stationName("Chișinău"),
		stationName("Portland (OR)"),
	}
	testData = []byte(`Nassau;22.7
Ljubljana;24.3
Bridgetown;9.3
Port Moresby;21.0
Ürümqi;-0.3
Jakarta;37.0
Bosaso;13.5
Ho Chi Minh City;46.2
Phnom Penh;27.0
Tromsø;18.8
Ljubljana;-24.3
Ljubljana;0.0
Ljubljana;-0.1
`)  // The file always ends with \n.
)

func TestChunkByBytes(t *testing.T) {
	want := []chunk{
		{data: testData[0:27]},
		{data: testData[27:42]},
		{data: testData[42:74]},
		{data: testData[74:99]},
		{data: testData[99:121]},
		{data: testData[121:150]},
		{data: testData[150:180]},
		{data: testData[180:195]},
	}

	indexes := chunkByBytes(bytes.NewReader(testData), 32)
	got := chanToSlice(indexes)

	require.Len(t, got, len(want))
	for i, w := range want {
		assert.Equal(t, w, got[i], "not equal on idx: %d", i)
	}
}

func chanToSlice[T any](c chan T) []T {
	var out = make([]T, 0)
	for i := range c {
		out = append(out, i)
	}
	return out
}

func TestParseLine(t *testing.T) {
	data := []byte(`Bridgetown;9.3
Ürümqi;-0.3
Ljubljana;-24.3
`)

	newlineIdx, name, msrmnt := parseLine(data)

	assert.Equal(t, 14, newlineIdx)
	assert.Equal(t, stationName("Bridgetown"), name)
	assert.Equal(t, measurement(93), msrmnt)

	data = data[newlineIdx+1:]
	newlineIdx, name, msrmnt = parseLine(data)

	assert.Equal(t, 13, newlineIdx)
	assert.Equal(t, stationName("Ürümqi"), name)
	assert.Equal(t, measurement(-3), msrmnt)

	data = data[newlineIdx+1:]
	newlineIdx, name, msrmnt = parseLine(data)

	assert.Equal(t, 15, newlineIdx)
	assert.Equal(t, stationName("Ljubljana"), name)
	assert.Equal(t, measurement(-243), msrmnt)
}

var (
	NewlineIdx  int
	Name        stationName
	Measurement measurement
)

func BenchmarkParseLine(b *testing.B) {
	var (
		newlineIdx int
		name       stationName
		msrmnt     measurement
	)
	data := testData

	for range b.N {
		newlineIdx, name, msrmnt = parseLine(data)
	}

	NewlineIdx = newlineIdx
	Name = name
	Measurement = msrmnt
}

func TestChunkReader(t *testing.T) {
	want := map[stationName]stats{
		"Bosaso": {
			min:   135,
			max:   135,
			sum:   135,
			count: 1,
		},
		"Bridgetown": {
			min:   93,
			max:   93,
			sum:   93,
			count: 1,
		},
		"Ho Chi Minh City": {
			min:   462,
			max:   462,
			sum:   462,
			count: 1,
		},
		"Jakarta": {
			min:   370,
			max:   370,
			sum:   370,
			count: 1,
		},
		"Ljubljana": {
			min:   -243,
			max:   243,
			sum:   -1,
			count: 4,
		},
		"Nassau": {
			min:   227,
			max:   227,
			sum:   227,
			count: 1,
		},
		"Phnom Penh": {
			min:   270,
			max:   270,
			sum:   270,
			count: 1,
		},
		"Port Moresby": {
			min:   210,
			max:   210,
			sum:   210,
			count: 1,
		},
		"Tromsø": {
			min:   188,
			max:   188,
			sum:   188,
			count: 1,
		},
		"Ürümqi": {
			min:   -3,
			max:   -3,
			sum:   -3,
			count: 1,
		},
	}
	chunksChan := chunkByBytes(bytes.NewReader(testData), 32)
	got := chunkReader(chunksChan)

	for k, v := range want {
		pos := got.pos(k)
		gotValue, ok := got.get(pos, k)

		assert.True(t, ok, "key: %s is not present in output", k)
		assert.Equal(t, v, *gotValue, "stats: %+v not equal to output: %+v", v, gotValue)
	}

	for _, item := range got.Iter() {
		gotName, gotValue := item.name, item.stats
		expectValue, ok := want[gotName]
		assert.True(t, ok, "extra key in output: %s", gotName)
		assert.Equal(t, expectValue, *gotValue)
	}
}

func TestParseNumer(t *testing.T) {
	want := measurement(-999)
	got := parseNumber([]byte("-99.9"))

	if want != got {
		t.Errorf("parseNumber, got: %+v, want: %+v", got, want)
	}
}

func TestParseNumer2(t *testing.T) {
	want := measurement(999)
	got := parseNumber([]byte("99.9"))

	if want != got {
		t.Errorf("parseNumber, got: %+v, want: %+v", got, want)
	}
}

func TestParseNumer3(t *testing.T) {
	want := measurement(0)
	got := parseNumber([]byte("0.0"))

	if want != got {
		t.Errorf("parseNumber, got: %+v, want: %+v", got, want)
	}
}

func TestParseNumer4(t *testing.T) {
	want := measurement(-1)
	got := parseNumber([]byte("-0.1"))

	if want != got {
		t.Errorf("parseNumber, got: %+v, want: %+v", got, want)
	}
}

func TestStatsMeasurement(t *testing.T) {
	want := stats{min: 10, max: 10, sum: 10, count: 1}
	var got stats
	updateStats(&got, 10)

	if want != got {
		t.Errorf("TestStatsMeasurement, got: %+v, want: %+v", got, want)
	}

	want = stats{min: -10, max: 10, sum: 0, count: 2}
	updateStats(&got, -10)
	if want != got {
		t.Errorf("TestStatsMeasurement, got: %+v, want: %+v", got, want)
	}
}

func TestSumStationData(t *testing.T) {
	want := newSimpleMap(10)
	pos := want.pos("station")
	want.set(pos, "station", &stats{min: -10, max: 20, sum: 0, count: 4})

	got := newSimpleMap(10)
	chunk1 := newSimpleMap(10)
	chunk1.set(pos, "station", &stats{min: -10, max: 10, sum: 10, count: 2})
	chunk2 := newSimpleMap(10)
	chunk2.set(pos, "station", &stats{min: 0, max: 20, sum: -10, count: 2})
	sumChunk(got, chunk1)
	sumChunk(got, chunk2)

	wantStats, ok := want.get(pos, "station")
	require.True(t, ok)
	gotStats, ok := got.get(pos, "station")
	require.True(t, ok)
	assert.Equal(t, wantStats, gotStats)
}

func TestMean(t *testing.T) {
	want := float64(18.1)
	got := mean(sumT(11277704), 62452)

	if want != got {
		t.Errorf("TestMean, got: %+v, want: %+v", got, want)
	}

	want = float64(1.3)
	got = mean(sumT(50), 4)

	if want != got {
		t.Errorf("TestMean, got: %+v, want: %+v", got, want)
	}
}

func TestSimpleMapSet(t *testing.T) {
	m := newSimpleMap(maxStations)

	pos := m.pos("testname")
	st := stats{sum: 10, min: 10, max: 10, count: 1}
	m.set(pos, "testname", &st)

	expect := bucket{
		items: []bucketItem{
			{
				name:  "testname",
				stats: &st,
			},
		},
	}
	assert.Equal(t, expect, m.data[pos])

	st = stats{sum: 20, min: 20, max: 20, count: 2}
	m.set(pos, "testname", &st)

	expect = bucket{
		items: []bucketItem{
			{
				name:  "testname",
				stats: &st,
			},
		},
	}
	assert.Equal(t, expect, m.data[pos])
}

func TestSimpleMapGet(t *testing.T) {
	m := newSimpleMap(maxStations)
	pos := m.pos("testname")

	st := stats{sum: 10, min: 10, max: 10, count: 1}
	m.data[pos] = bucket{
		items: []bucketItem{
			{
				name:  "testname",
				stats: &st,
			},
		},
	}

	expect := st
	got, ok := m.get(pos, "testname")
	assert.True(t, ok)
	assert.Equal(t, expect, *got)

	got, ok = m.get(pos, "")
	assert.False(t, ok)
	assert.Empty(t, got)
}

var Idx uint32

// BenchmarkStationIdx-8   	36248710	        31.03 ns/op	       0 B/op	       0 allocs/op
func BenchmarkStationIdx(b *testing.B) {
	var idx uint32
	for range b.N {
		for _, stationName := range stationNames {
			idx = stationPos(stationName, maxStations)
		}
	}

	Idx = idx
}

func BenchmarkRun(b *testing.B) {
	bench = true
	for range b.N {
		run(defaultMeasurementsFile)
	}
}
