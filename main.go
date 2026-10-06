package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"iter"
	"math/bits"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"
	"unsafe"
)

// MBP M1 16GB 2020
// hw.cachesize: 3708420096 65536 4194304 0 0 0 0 0 0 0
// hw.pagesize: 16384
// hw.pagesize32: 16384
// hw.cachelinesize: 128
// hw.l1icachesize: 131072
// hw.l1dcachesize: 65536
// hw.l2cachesize: 4194304

var (
	kiB = 1024
	MiB = kiB * kiB

	defaultMeasurementsFile = "measurements.txt"
	maxStations             = 10_000
	chunkSize               = 6 * MiB

	chunkReaders      = runtime.NumCPU()
	chunksChanBufSize = 0

	// Real measurement 11_025, we can add extra buffer.
	printBuilderCapacity = 16 * kiB
)

type (
	// Theoretically all 1B lines can be 1 station.
	countT uint32 // 1B max
	minT   int16  // [-99.9,99.9] * 10
	maxT   int16  // [-99.9,99.9] * 10
	// Theoretically all 1B lines can be 1 station.
	sumT        int64 // +/- 999 * n_measurements
	measurement int16 // [-99.9,99.9] * 10

	// Using []byte or string + unsafe (nocopy) makes no difference.
	stationName string // 100 bytes max

	stats struct {
		sum   sumT
		min   minT
		max   maxT
		count countT
	}
)

func main() {
	var measurementsFile string

	if len(os.Args) != 2 {
		measurementsFile = defaultMeasurementsFile
	} else {
		measurementsFile = os.Args[1]
	}
	err := run(measurementsFile)
	if err != nil {
		fmt.Printf("Error: %+v\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}

func run(file string) error {
	// We open the file and we use regular .ReadAt, so normal
	// syscalls. Mmap in Go is much slower compared to this (20s total vs 7s total).
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	var (
		dataChunkChan = make(chan simpleMap)
		wg            sync.WaitGroup
	)

	// Starts a new producer goroutine that reads 'chunkSize' bytes
	// from the file and sends those into the chunksChan.
	// We don't have to worry about having to copy all the data via the
	// chan, it sends a []byte slice (just a struct).
	chunksChan := chunkByBytes(f, chunkSize)

	// Spawn N CPUs readers that each reads from the chunks channel, each
	// producing 1 output hashmap after reading all of the chunks.
	wg.Add(chunkReaders)
	for range chunkReaders {
		go func() {
			defer wg.Done()
			// Reads the chunk and produces a *simpleMap[stationName, *stats] into the
			// channel (sends pointers over the chan).
			dataChunkChan <- chunkReader(chunksChan)
		}()
	}

	// Spawn a closer goroutine that waits until all the data
	// has been sent into stationDataChan and closes the channel
	// so the iteration below ends.
	go func() {
		wg.Wait()
		close(dataChunkChan)
	}()

	// Acumulate all of the chunk's processed maps into final map,
	// sums and counts along the way. We reuse 1st map so we don't
	// have to allocate and copy to the new one.
	stationData := <-dataChunkChan
	for dataChunk := range dataChunkChan {
		sumChunk(stationData, dataChunk)
	}

	// Formats and prints the output to stdout.
	printOutput(stationData)
	return nil
}

type chunk struct {
	data []byte
}

func chunkByBytes(f io.ReaderAt, chunkSize int) chan chunk {
	var (
		out = make(chan chunk, chunksChanBufSize)
	)
	go func() {
		defer close(out)
		var (
			prevEnd int
		)
		for {
			var (
				c          chunk
				start, end int

				data = make([]byte, chunkSize)
			)
			// Start idx is always previous chunk's end +1, except
			// for the 1st chunk.
			if prevEnd != 0 {
				start = prevEnd
			}

			end = int(chunkSize) - 1

			n, err := f.ReadAt(data, int64(start))
			if err != nil {
				if err == io.EOF {
					c.data = data[:n]
					out <- c
					return
				} else {
					panic(err)
				}
			}

			// backtrack until we find `\n`
			// TODO: is it faster to backtrack or go forward?
			// Measure. We should have a page cached, so hard to tell if it matters.
			// On the output file, this takes:
			//   1 chunk: 134us
			//   4 chunks: 999us
			//   10 chunks: 1.9ms
			// Even for 10 chunks it drops down to 100us, my guess is this is the
			// page cache warming up.
			chunkEnd := findEndIdx(data, end)
			prevEnd += chunkEnd
			c.data = data[:chunkEnd]
			out <- c
		}
	}()
	return out
}

func findEndIdx(data []byte, idx int) int {
	// Since we are looking for end idx in the slice of data
	// that will be used [:end], we want up to the \n, included.
	// IMPORTANT: the `\n` has to be included when doing slice [:end]
	// to correctly detect EOL and use that line.
	chunkEnd := bytes.LastIndexByte(data[:idx+1], '\n')
	if chunkEnd == -1 {
		return idx
	}
	return chunkEnd + 1
}

func chunkReader(chunks chan chunk) simpleMap {
	// Sadly even though we are reading much smaller chunk here,
	// it is still likely we get all the station names.
	out := newSimpleMap(maxStations)

	for chunk := range chunks {
		var (
			chunkView = chunk.data
		)
		for {
			newlineIdx, name, measurement := parseLine(chunkView)
			if newlineIdx == -1 {
				break
			}

			var hash uint64
			if len(name) <= 8 {
				hash = bits.RotateLeft64(stationWord(name)*0x517cc1b727220a95, 17)
			} else {
				hash = stationFingerprintLong(name)
			}
			stationStats := out.homeHit(name, hash)
			if stationStats == nil {
				stationStats = out.findSlow(name, hash)
			}
			updateStats(stationStats, measurement)
			// Save next line's start at current index+1 (step over \n).
			chunkView = chunkView[newlineIdx+1:]
		}
	}

	return out
}

func parseLine(data []byte) (int, stationName, measurement) {
	const (
		semicolon = uint64(0x3b3b3b3b3b3b3b3b)
		ones      = uint64(0x0101010101010101)
		highBits  = uint64(0x8080808080808080)
	)

	separatorIdx := 0
	for len(data)-separatorIdx >= 8 {
		word := binary.LittleEndian.Uint64(data[separatorIdx:]) ^ semicolon
		// A high bit remains in each byte where word was zero (the semicolon).
		matches := (word - ones) & ^word & highBits
		if matches != 0 {
			separatorIdx += bits.TrailingZeros64(matches) / 8
			break
		}
		separatorIdx += 8
	}
	for separatorIdx < len(data) && data[separatorIdx] != ';' {
		separatorIdx++
	}
	if separatorIdx+4 >= len(data) {
		return -1, "", 0
	}

	newlineIdx := separatorIdx + 4
	if data[separatorIdx+1] == '-' {
		newlineIdx++
	}
	if newlineIdx >= len(data) {
		return -1, "", 0
	}
	if data[newlineIdx-2] != '.' {
		newlineIdx++
	}
	if newlineIdx >= len(data) || data[newlineIdx] != '\n' {
		return -1, "", 0
	}

	name := stationName(unsafe.String(&data[0], len(data[:separatorIdx])))
	return newlineIdx, name, parseNumber(data[separatorIdx+1 : newlineIdx])
}

// parseNumber parses the bytes into a int16 multiplied by 10.
// Because we know exact layout of the data, which can be:
//
//	[9.9], [99.9], [-9.9], [-99.9]
//
// we can unroll by hand all the variants.
// This way is about 4% faster on full run than in a for loop.
func parseNumber(line []byte) measurement {
	if line[0] == '-' {
		// In this case the line can be 4 or 5 bytes.
		if len(line) == 4 {
			return -(10*measurement(line[1]-48) + measurement(line[3]-48))
		}
		// 5 bytes.
		return -(100*measurement(line[1]-48) + 10*measurement(line[2]-48) + measurement(line[4]-48))
	}

	// In this case the line can be 3 or 4 bytes.
	if len(line) == 3 {
		return 10*measurement(line[0]-48) + measurement(line[2]-48)
	}
	// 4 bytes.
	return 100*measurement(line[0]-48) + 10*measurement(line[1]-48) + measurement(line[3]-48)
}

func updateStats(stats *stats, measurement measurement) {
	// 1st temperature measurement must set all values
	// because min/max might not correctly get set with
	// default 0 (min(0, 10)).
	if stats.count == 0 {
		stats.count = 1
		stats.sum, stats.min, stats.max = sumT(measurement), minT(measurement), maxT(measurement)
		return
	}
	stats.count++
	stats.sum += sumT(measurement)
	stats.min = min(stats.min, minT(measurement))
	stats.max = max(stats.max, maxT(measurement))
}

// sumChunk merges the chunks from each worker into final output map.
// The 1st chunk is reused, and this function takes 150us in the worst case.
func sumChunk(sumStationData simpleMap, stationDataChunk simpleMap) {
	for _, bucketItem := range stationDataChunk.Iter() {
		stationName, stationStats := bucketItem.name, bucketItem.stats

		pos := sumStationData.pos(stationName)
		sumStationStats, ok := sumStationData.get(pos, stationName)
		if !ok {
			sumStationStats = &stats{
				count: stationStats.count,
				sum:   stationStats.sum,
				min:   stationStats.min,
				max:   stationStats.max,
			}
			sumStationData.set(pos, stationName, sumStationStats)
			continue
		}

		sumStationStats.count += stationStats.count
		sumStationStats.sum += stationStats.sum
		sumStationStats.min = min(sumStationStats.min, stationStats.min)
		sumStationStats.max = max(sumStationStats.max, stationStats.max)
	}
}

var bench bool

// printOutput: 1.521125ms - 2.49375ms
func printOutput(sumStationData simpleMap) {
	// We save the stationName with the position
	// and when we iterate the map, we can save the
	// bucket index to prevent yet another hashing
	// of the name.
	type nameWithPosition struct {
		name stationName
		pos  uint32
	}
	var names []nameWithPosition = make([]nameWithPosition, 0, sumStationData.len())
	for pos, bucketItem := range sumStationData.Iter() {
		names = append(names, nameWithPosition{name: bucketItem.name, pos: pos})
	}
	sort.Slice(names, func(i, j int) bool { return javaStringLess(names[i].name, names[j].name) })

	var builder strings.Builder
	builder.Grow(printBuilderCapacity)
	builder.WriteByte('{')
	for i, station := range names {
		stationStats, _ := sumStationData.get(station.pos, station.name)
		builder.WriteString(
			fmt.Sprintf(
				"%s=%.1f/%.1f/%.1f",
				station.name,
				correctMagnitude(stationStats.min),
				mean(stationStats.sum, stationStats.count),
				correctMagnitude(stationStats.max),
			))
		if i < len(names)-1 {
			builder.WriteString(", ")
		}
	}
	builder.WriteString("}\n")
	var writer io.Writer = os.Stdout
	if bench {
		writer = io.Discard
	}
	fmt.Fprint(writer, builder.String())
}

// javaStringLess matches String.compareTo, which orders UTF-16 code units.
// The challenge reference implementations use Java's TreeMap for output.
func javaStringLess(left, right stationName) bool {
	for len(left) > 0 && len(right) > 0 {
		leftRune, leftSize := utf8.DecodeRuneInString(string(left))
		rightRune, rightSize := utf8.DecodeRuneInString(string(right))

		leftHigh, leftLow := utf16Units(leftRune)
		rightHigh, rightLow := utf16Units(rightRune)
		if leftHigh != rightHigh {
			return leftHigh < rightHigh
		}
		if leftLow != rightLow {
			return leftLow < rightLow
		}

		left = left[leftSize:]
		right = right[rightSize:]
	}
	return len(left) < len(right)
}

func utf16Units(r rune) (high, low uint16) {
	if r <= 0xffff {
		return uint16(r), 0
	}
	r -= 0x10000
	return uint16(0xd800 + r>>10), uint16(0xdc00 + r&0x3ff)
}

// correctMagnitude fixes back our floating points which we save
// as multiply of 10 to speed up all of the calculations until
// we need to print and calculate mean.
func correctMagnitude[T minT | maxT | sumT](n T) float64 {
	return float64(n) / 10
}

func mean(sum sumT, count countT) float64 {
	divisor := sumT(count)
	quotient := sum / divisor
	remainder := sum % divisor
	if remainder < 0 {
		quotient--
		remainder += divisor
	}
	if remainder*2 >= divisor {
		quotient++
	}
	return float64(quotient) / 10
}

// simpleMap uses 32K inline entries and a complete short-name fingerprint.
const flatSlots = 32768
const flatMask = flatSlots - 1

type simpleMap struct {
	data     []flatEntry
	capacity int
	length   int
}
type flatEntry struct {
	name  stationName
	stats stats
	hash  uint64
}
type bucketItem struct {
	stats *stats
	name  stationName
}

func newSimpleMap(_ int) simpleMap {
	return simpleMap{capacity: flatSlots, data: make([]flatEntry, flatSlots)}
}
func (m *simpleMap) len() int { return m.length }
func (m *simpleMap) Iter() iter.Seq2[uint32, bucketItem] {
	return func(yield func(uint32, bucketItem) bool) {
		for i := range m.data {
			entry := &m.data[i]
			if entry.name != "" && !yield(uint32(i), bucketItem{stats: &entry.stats, name: entry.name}) {
				return
			}
		}
	}
}
func stationWord(name stationName) uint64 {
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
func stationFingerprint(name stationName) uint64 {
	if len(name) <= 8 {
		return bits.RotateLeft64(stationWord(name)*0x517cc1b727220a95, 17)
	}
	return stationFingerprintLong(name)
}

func stationFingerprintLong(name stationName) uint64 {
	word := stationWord(name)
	for offset := 8; offset < len(name); offset += 8 {
		word = bits.RotateLeft64(word*0x517cc1b727220a95^stationWord(name[offset:]), 17)
	}
	return bits.RotateLeft64(word*0x517cc1b727220a95, 17)
}

func (m *simpleMap) pos(name stationName) uint32 {
	return uint32(stationFingerprint(name)) & flatMask
}

// homeHit keeps the common probe independent of insertion and probe-loop work.
func (m *simpleMap) homeHit(name stationName, hash uint64) *stats {
	entry := &m.data[uint32(hash)&flatMask]
	if entry.hash == hash && len(entry.name) == len(name) && (len(name) <= 8 || entry.name == name) {
		return &entry.stats
	}
	return nil
}

func (m *simpleMap) find(name stationName) *stats {
	hash := stationFingerprint(name)
	if hit := m.homeHit(name, hash); hit != nil {
		return hit
	}
	return m.findSlow(name, hash)
}

// findSlow starts at the home slot, which may be empty, and never rehashes.
func (m *simpleMap) findSlow(name stationName, hash uint64) *stats {
	pos := uint32(hash) & flatMask
	for {
		entry := &m.data[pos]
		if entry.hash == hash && len(entry.name) == len(name) && (len(name) <= 8 || entry.name == name) {
			return &entry.stats
		}
		if entry.name == "" {
			entry.name = stationName(strings.Clone(string(name)))
			entry.hash = hash
			m.length++
			return &entry.stats
		}
		pos = (pos + 1) & flatMask
	}
}
func (m *simpleMap) get(pos uint32, name stationName) (*stats, bool) {
	hash := stationFingerprint(name)
	for {
		entry := &m.data[pos]
		if entry.name == "" {
			return nil, false
		}
		if entry.hash == hash && len(entry.name) == len(name) && (len(name) <= 8 || entry.name == name) {
			return &entry.stats, true
		}
		pos = (pos + 1) & flatMask
	}
}
func (m *simpleMap) set(_ uint32, name stationName, st *stats) {
	*m.find(name) = *st
}

// stationPos calculates position in slice of our simple hashmap
// given the stationName and capacity of the map.
//
// Original hashing function did 1 byte at a time (*101+byte)
// and this one just batches it into single uint32 2 bytes at a time.
// Thanks ChatGPT! And suprisingly it is much faster than the previous one
// and than fnv1a, because we have to % by capacity even with fnv1a.
//
// // BenchmarkStationIdx-8   	21225350	        50.76 ns/op	       0 B/op	       0 allocs/op
// // Benchmark101Hash-8   	20576145	        57.85 ns/op	       0 B/op	       0 allocs/op
// // BenchmarkFnv-8   	17671476	        60.78 ns/op	       0 B/op	       0 allocs/op
func stationPos(station stationName, capacity int) uint32 {
	var (
		hash uint32 = 2166136261
		// Prime number used also in fnv1a.
		prime32b uint32 = 16777619
		//prime64b uint64 = 1099511628211
	)
	n := len(station)

	// Process 2 bytes at a time.
	// We can also process 8 and 4 bytes at a time, however there are short
	// names (3 letters), and spec says names can be [1, 100] bytes.
	// Doing 8 bytes is faster, but produces over hundred collisions on
	// shorter names. This way it produces only 5 total collisions with
	// max 2 per bucket. That is acceptable and provides overall speedup
	// of 24% over the byte-by-byte hashing.
	for i := 0; i+2 <= n; i += 2 {
		// Load 2 bytes into a 64-bit integer.
		block := uint32(station[i]) | uint32(station[i+1])<<8

		// Hash calculation.
		hash = hash*prime32b + block
	}

	// I tried to use fnv1a hash with this variant
	// and fast modulo using bitwise operation (hash & capacity-1).
	return hash % uint32(capacity)
}
