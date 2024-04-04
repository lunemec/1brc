package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
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
	maxStations             = 1 << 14
	chunkSize               = 32 * MiB

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
		dataChunkChan = make(chan *simpleMap)
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

func chunkReader(chunks chan chunk) *simpleMap {
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

			pos := out.pos(name)
			stationStats, ok := out.get(pos, name)
			if !ok {
				stationStats = &stats{}
				out.set(pos, name, stationStats)
			}
			updateStats(stationStats, measurement)
			// Save next line's start at current index+1 (step over \n).
			chunkView = chunkView[newlineIdx+1:]
		}
	}

	return out
}

func parseLine(data []byte) (int, stationName, measurement) {
	newlineIdx := bytes.IndexByte(data, '\n')
	if newlineIdx == -1 {
		return -1, "", 0
	}
	// Because the measurement value can be 9.9 or -99.9 max, the ; must be 3 to 5 bytes before
	// the \n.
	// This way is ~20% faster than another bytes.IndexByte().
	var separatorIdx int
	check3, check5 := data[newlineIdx-4], data[newlineIdx-6]
	if check3 == ';' {
		separatorIdx = newlineIdx - 4
	} else if check5 == ';' {
		separatorIdx = newlineIdx - 6
	} else {
		// If its not 3th or 5th byte from the end, it must be 4th.
		separatorIdx = newlineIdx - 5
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
func sumChunk(sumStationData *simpleMap, stationDataChunk *simpleMap) {
	next := stationDataChunk.iter()
	for {
		pos, stationName, stationStats, ok := next()
		if !ok {
			break
		}
		sumStationStats, ok := sumStationData.get(pos, stationName)
		if !ok {
			sumStationStats = &stats{}
			sumStationData.set(pos, stationName, sumStationStats)
		}

		sumStationStats.count += stationStats.count
		sumStationStats.sum += stationStats.sum
		sumStationStats.min = min(sumStationStats.min, stationStats.min)
		sumStationStats.max = max(sumStationStats.max, stationStats.max)
	}
}

var bench bool

// printOutput: 1.521125ms - 2.49375ms
func printOutput(sumStationData *simpleMap) {
	// We save the stationName with the position
	// and when we iterate the map, we can save the
	// bucket index to prevent yet another hashing
	// of the name.
	type nameWithPosition struct {
		name stationName
		pos  uint32
	}
	var names []nameWithPosition = make([]nameWithPosition, 0, sumStationData.len())
	next := sumStationData.iter()
	for {
		pos, k, _, ok := next()
		if !ok {
			break
		}
		names = append(names, nameWithPosition{name: k, pos: pos})
	}
	sort.Slice(names, func(i, j int) bool { return names[i].name < names[j].name })

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
				mean(correctMagnitude(stationStats.sum), stationStats.count),
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

// correctMagnitude fixes back our floating points which we save
// as multiply of 10 to speed up all of the calculations until
// we need to print and calculate mean.
func correctMagnitude[T minT | maxT | sumT](n T) float64 {
	return float64(n) / 10
}

func mean(sum float64, count countT) float64 {
	return sum / float64(count)
}

// simpleMap is array backed map, it turns out that for this
// very specific and simple case it is faster than most implementations.
type simpleMap struct {
	data     []bucket
	capacity int
	length   int
}

type bucket struct {
	items []bucketItem
}

type bucketItem struct {
	stats *stats
	name  stationName
}

func newSimpleMap(capacity int) *simpleMap {
	m := simpleMap{
		capacity: capacity,
		data:     make([]bucket, capacity),
	}
	return &m
}

func (m *simpleMap) len() int {
	return m.length
}

func (m *simpleMap) iter() func() (uint32, stationName, *stats, bool) {
	var (
		bucketIndex int
		itemIndex   int
	)

	return func() (uint32, stationName, *stats, bool) {
		// Move to next bucket if we are over current bucket size.
		// And start from 0 of the bucket.
		if itemIndex >= len(m.data[bucketIndex].items) {
			for i := bucketIndex + 1; i <= len(m.data)-1; i++ {
				bucketIndex = i
				if len(m.data[i].items) != 0 {
					itemIndex = 0
					break
				}
			}
		}
		if bucketIndex >= len(m.data)-1 {
			// If all buckets are exhausted, return empty values.
			return 0, "", nil, false
		}

		// Retrieve current item.
		item := m.data[bucketIndex].items[itemIndex]
		itemIndex++
		return uint32(bucketIndex), item.name, item.stats, true
	}
}

// pos returns position in the data array so we can
// avoid re-hashing the same value when doing get/set
// in the same loop.
func (m *simpleMap) pos(name stationName) uint32 {
	return stationPos(name, m.capacity)
}

func (m *simpleMap) get(pos uint32, name stationName) (*stats, bool) {
	bucket := m.data[pos]
	// Fast-path for empty bucket.
	if len(bucket.items) == 0 {
		return nil, false
	}
	// Fast-path for bucket of 1.
	if len(bucket.items) == 1 {
		if bucket.items[0].name != name {
			return nil, false
		}

		return bucket.items[0].stats, true
	}

	for _, item := range bucket.items {
		if item.name == name {
			return item.stats, true
		}
	}

	return nil, false
}

func (m *simpleMap) set(pos uint32, name stationName, st *stats) {
	bucket := m.data[pos]
	if len(bucket.items) == 0 {
		// Empty bucket, add it there.
		bucket.items = make([]bucketItem, 0, 10)
		m.length++
		bucket.items = append(bucket.items, bucketItem{
			name:  name,
			stats: st,
		})
		m.data[pos] = bucket
		return
	}

	for i, item := range bucket.items {
		// Non-empty bucket, find which item in bucket are we
		// and set.
		if item.name == name {
			item.stats = st
			bucket.items[i] = item
			return
		}
	}

	// Non-empty bucket, not yet in any of the items,
	// append at the end.
	m.length++
	bucket.items = append(bucket.items, bucketItem{
		name:  name,
		stats: st,
	})
	m.data[pos] = bucket
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
