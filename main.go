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

// chunk borrows a buffer until its worker finishes all row updates. recycle is
// nil for caller-owned test chunks; producer chunks return data to that channel.
// For data="Oslo;1.2\n", the parser borrows "Oslo", the table owns its clone,
// and release can then make the buffer available for "Rome;9.9\n".
type chunk struct {
	data    []byte
	recycle chan []byte
}

// chunkBufferPool bounds the producer's allocations and accepts worker returns.
// Only the producer calls take and changes allocated; workers only send slices
// to available. With size=6 MiB and limit=17, at most 102 MiB of input buffers
// exist, instead of allocating another 6 MiB for every chunk in a 1B-row file.
type chunkBufferPool struct {
	available chan []byte
	allocated int
	size      int
}

// newChunkBufferPool creates a lazy pool with positive size and limit. For
// size=32, limit=2, the first two simultaneous loans allocate 64 bytes; a third
// waits for a return. Returned buffers can be reused without clearing because
// ReadAt fills them and EOF slices are bounded to the returned byte count.
func newChunkBufferPool(size, limit int) *chunkBufferPool {
	return &chunkBufferPool{available: make(chan []byte, limit), size: size}
}

// take lends one full-size buffer to the producer, preferring a returned one.
// After borrowing A and B from a two-buffer pool, another take waits until A
// or B is returned. No buffer is shared between outstanding chunks.
func (p *chunkBufferPool) take() []byte {
	select {
	case data := <-p.available:
		return data
	default:
	}
	if p.allocated < cap(p.available) {
		p.allocated++
		return make([]byte, p.size)
	}
	return <-p.available
}

// release ends a chunk's loan after its last row update. A 23-byte view with
// capacity=32 returns the complete 32-byte buffer, ready for the next ReadAt.
// The channel stays open because workers can finish after the producer reaches
// EOF. Station keys must already be owned; caller-owned chunks are unchanged.
func (c chunk) release() {
	if c.recycle != nil {
		c.recycle <- c.data[:cap(c.data)]
	}
}

// chunkByBytes keeps the existing sequential ReadAt/newline boundaries and
// unbuffered work handoff, using at most chunkReaders+1 input buffers. Workers
// release each loan after parsing. For "A;1.0\nB;2.0\n" with size=8, it sends
// "A;1.0\n" then the bounded EOF view "B;2.0\n", never exposing stale bytes.
func chunkByBytes(f io.ReaderAt, chunkSize int) chan chunk {
	var (
		out  = make(chan chunk, chunksChanBufSize)
		pool = newChunkBufferPool(chunkSize, chunkReaders+1)
	)
	go func() {
		defer close(out)
		var (
			prevEnd int
		)
		for {
			var (
				c          = chunk{recycle: pool.available}
				start, end int

				data = pool.take()
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

// chunkReader aggregates complete rows into a table owned by this worker.
// For "Oslo;1.2\nOslo;-0.2\n", Oslo ends with count=2, sum=10, min=-2,
// max=12; temperatures remain in tenths until output formatting.
// supportsAVX2Masks is an architecture/build-specific feature check, inlined
// here once per worker. When true, semicolonMask64AVX2 loads two bounded
// 32-byte vectors, compares each byte with ';' and joins the two masks.
// For "A;0.0\nB;1.0\n" followed by zero bytes, bits 1 and 7 produce 0x82.
// Both loads require 64 in-slice bytes; short tails use parseLine. SIMD-disabled
// and non-amd64 builds use the original pooled parser through chunkReaderScalar.
// All mask rows and the final scalar tail finish before the chunk loan is
// returned. A chunk ending in "A;0.0\nB;1." records only A, then releases
// its complete backing buffer; a later ReadAt cannot alter its owned key.
func chunkReader(chunks chan chunk) simpleMap {
	if !supportsAVX2Masks() {
		return chunkReaderScalar(chunks)
	}
	// Sadly even though we are reading much smaller chunk here,
	// it is still likely we get all the station names.
	out := newSimpleMap(maxStations)

	for chunk := range chunks {

		chunkView := chunk.data
		rowStart, nextBlock, blockStart := 0, 0, 0
		var delimiters uint64
		for {
			// Reuse every separator in the current sixty-four-byte block.
			// A row may end beyond the block; temperature bytes contain no ';'.
			for delimiters == 0 && len(chunk.data)-nextBlock >= 64 {
				blockStart = nextBlock
				delimiters = semicolonMask64AVX2(chunk.data[nextBlock:])
				nextBlock += 64
			}
			var newlineIdx int
			var name stationName
			var measurement measurement
			var firstWord uint64
			if delimiters != 0 {
				separator := blockStart + bits.TrailingZeros64(delimiters)
				delimiters &= delimiters - 1
				newlineIdx, name, measurement, firstWord = parseLineAtSeparator(chunkView, separator-rowStart)
			} else {
				// Keep an unfinished long name or a final short block intact.
				newlineIdx, name, measurement, firstWord = parseLine(chunkView)
			}
			if newlineIdx == -1 {
				break
			}

			var hash uint64
			if len(name) <= 8 {
				hash = bits.RotateLeft64(firstWord*0x517cc1b727220a95, 17)
			} else {
				hash = stationFingerprintLongFromWord(name, firstWord)
			}
			stationStats := out.homeHit(name, hash)
			if stationStats == nil {
				stationStats = out.findSlow(name, hash)
			}
			updateStats(stationStats, measurement)
			// Save next line's start at current index+1 (step over \n).
			rowStart += newlineIdx + 1
			chunkView = chunk.data[rowStart:]
		}
		chunk.release()
	}

	return out
}

// chunkReaderScalar aggregates complete rows into a table owned by this worker.
// For "Oslo;1.2\nOslo;-0.2\n", Oslo ends with count=2, sum=10, min=-2,
// max=12; temperatures remain in tenths until output formatting.
func chunkReaderScalar(chunks chan chunk) simpleMap {
	// Sadly even though we are reading much smaller chunk here,
	// it is still likely we get all the station names.
	out := newSimpleMap(maxStations)

	for chunk := range chunks {
		var (
			chunkView = chunk.data
		)
		for {
			newlineIdx, name, measurement, firstWord := parseLine(chunkView)
			if newlineIdx == -1 {
				break
			}

			var hash uint64
			if len(name) <= 8 {
				hash = bits.RotateLeft64(firstWord*0x517cc1b727220a95, 17)
			} else {
				hash = stationFingerprintLongFromWord(name, firstWord)
			}
			stationStats := out.homeHit(name, hash)
			if stationStats == nil {
				stationStats = out.findSlow(name, hash)
			}
			updateStats(stationStats, measurement)
			// Save next line's start at current index+1 (step over \n).
			chunkView = chunkView[newlineIdx+1:]
		}
		chunk.release()
	}

	return out
}

// parseLine decodes the first newline-terminated row and returns its newline
// index, a borrowed station name, temperature in tenths and normalized first
// name word. A bounded word decoder handles interior temperatures; short final
// rows retain the scalar fallback. For "Oslo;-12.6\nParis;0.0\n", it returns
// (10, "Oslo", -126, 0x6f6c734f); delimiter/temperature bytes are masked out.
// An incomplete row such as "Oslo;1.2" returns (-1, "", 0, 0). The name aliases
// data; the table clones it on insertion before the input buffer can be reused.
func parseLine(data []byte) (int, stationName, measurement, uint64) {
	const (
		semicolon = uint64(0x3b3b3b3b3b3b3b3b)
		ones      = uint64(0x0101010101010101)
		highBits  = uint64(0x8080808080808080)
	)

	separatorIdx := 0
	var firstWord uint64
	if len(data) >= 8 {
		// XOR turns a semicolon byte (0x3b) into zero; the zero-byte mask
		// locates its first occurrence, e.g. byte 4 in "Oslo;1.2".
		firstWord = binary.LittleEndian.Uint64(data)
		word := firstWord ^ semicolon
		matches := (word - ones) & ^word & highBits
		if matches != 0 {
			separatorIdx = bits.TrailingZeros64(matches) / 8
		} else {
			separatorIdx = 8
			for len(data)-separatorIdx >= 8 {
				word := binary.LittleEndian.Uint64(data[separatorIdx:]) ^ semicolon
				matches := (word - ones) & ^word & highBits
				if matches != 0 {
					separatorIdx += bits.TrailingZeros64(matches) / 8
					break
				}
				separatorIdx += 8
			}
		}
	}
	for separatorIdx < len(data) && data[separatorIdx] != ';' {
		separatorIdx++
	}
	if separatorIdx+4 >= len(data) {
		return -1, "", 0, 0
	}

	name := stationName(unsafe.String(&data[0], len(data[:separatorIdx])))
	// Preserve the exact first name word; delimiter, temperature and following
	// row bytes must not participate in its fingerprint.
	if separatorIdx < 8 {
		if len(data) < 8 {
			firstWord = stationWord(name)
		} else {
			// "Oslo;1.2" loads 0x322e313b6f6c734f; a four-byte mask
			// keeps only 0x6f6c734f for the name fingerprint.
			firstWord &= (uint64(1) << (separatorIdx * 8)) - 1
		}
	}

	// Decode all four legal temperature layouts with one bounded 64-bit load.
	// For "-12.6\n", the dot is byte 3 (bit 28 in the mask), signed=-1,
	// aligned digit nibbles multiply into absolute=126, then sign gives -126.
	// Extra bytes after the newline never enter the digit mask. Complete final
	// rows have fewer than eight remaining bytes and use parseNumber below.
	if len(data)-separatorIdx >= 9 {
		temperatureWord := binary.LittleEndian.Uint64(data[separatorIdx+1:])
		dotPos := bits.TrailingZeros64(^temperatureWord & 0x10101000)
		if dotPos <= 28 {
			newlineIdx := separatorIdx + dotPos/8 + 3
			if data[newlineIdx] != '\n' {
				return -1, "", 0, 0
			}
			signed := int64(^temperatureWord<<59) >> 63
			digits := ((temperatureWord & ^(uint64(signed) & 0xff)) << (28 - dotPos)) & 0x0f000f0f00
			absolute := int64((digits * 0x640a0001) >> 32 & 0x3ff)
			value := measurement((absolute ^ signed) - signed)
			return newlineIdx, name, value, firstWord
		}
	}

	newlineIdx := separatorIdx + 4
	if data[separatorIdx+1] == '-' {
		newlineIdx++
	}
	if newlineIdx >= len(data) {
		return -1, "", 0, 0
	}
	if data[newlineIdx-2] != '.' {
		newlineIdx++
	}
	if newlineIdx >= len(data) || data[newlineIdx] != '\n' {
		return -1, "", 0, 0
	}

	return newlineIdx, name, parseNumber(data[separatorIdx+1 : newlineIdx]), firstWord
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

// flatSlots keeps the table below one-third occupancy at the 10,000-station
// limit. flatMask reduces a hash to its home slot: Oslo's hash ends in 0xa782,
// so its home slot is 0xa782 & 0x7fff = 10114.
const flatSlots = 32768
const flatMask = flatSlots - 1

// simpleMap is a worker-local, open-addressed station table with owned keys.
// Repeated rows such as "Oslo;12.6\n" update one inline entry; other stations
// occupy separate entries, probing forward if their home slots collide.
type simpleMap struct {
	data     []flatEntry
	capacity int
	length   int
}

// flatEntry stores one owned station name, its running statistics and hash.
// After "Oslo;12.6\n", it holds name="Oslo", stats={sum:126, min:126,
// max:126, count:1}, hash=0xa618e03c65f6a782. An empty name marks a free slot;
// valid input names are nonempty. Each entry occupies 40 bytes on 64-bit Go.
type flatEntry struct {
	name  stationName
	stats stats
	hash  uint64
}

// bucketItem is an iterator view of an occupied entry, not a copied stats value.
// For Oslo, name is "Oslo" and stats points directly to that table's accumulator.
type bucketItem struct {
	stats *stats
	name  stationName
}

// newSimpleMap allocates an empty fixed-size table. The argument is retained
// for existing callers: newSimpleMap(10000) has 32768 slots and zero entries.
func newSimpleMap(_ int) simpleMap {
	return simpleMap{capacity: flatSlots, data: make([]flatEntry, flatSlots)}
}

// len counts distinct stations, not rows: two Oslo rows and one Paris row give 2.
func (m *simpleMap) len() int { return m.length }

// Iter visits occupied slots in table order and allows stopping via yield=false.
// Oslo at its home slot yields (10114, bucketItem{name:"Oslo", stats:...}).
// Collisions can move entries, so merging into another table must recompute
// that table's lookup position rather than reuse the yielded slot.
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

// stationWord packs the first eight name bytes into a little-endian word,
// zero-padding short names without reading past their end. For example,
// "Oslo" becomes 0x000000006f6c734f; "abcdefghX" becomes 0x6867666564636261.
// Length remains part of identity: "A" and "A\x00" both pack to 0x41.
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

// stationFingerprint hashes every name byte; "Oslo" gives 0xa618e03c65f6a782.
// Up to eight bytes, odd multiplication and rotation are reversible, so hash
// plus length identifies the whole name. Longer names also require exact string
// equality: a 64-bit hash alone cannot identify every possible long name.
func stationFingerprint(name stationName) uint64 {
	if len(name) <= 8 {
		return bits.RotateLeft64(stationWord(name)*0x517cc1b727220a95, 17)
	}
	return stationFingerprintLong(name)
}

// stationFingerprintLong mixes successive eight-byte words and a padded tail.
// "abcdefghX" and "abcdefghY" share their first word but include different
// tails (0x58 and 0x59), producing different complete fingerprints.
func stationFingerprintLong(name stationName) uint64 {
	return stationFingerprintLongFromWord(name, stationWord(name))
}

// stationFingerprintLongFromWord continues the full-name hash from a cached
// first word equal to stationWord(name), avoiding a second load of those bytes.
// For ("abcdefghX", 0x6867666564636261), it mixes the remaining byte 0x58 and
// returns 0x882cfcbff17ddf39, identical to stationFingerprint("abcdefghX").
func stationFingerprintLongFromWord(name stationName, word uint64) uint64 {
	for offset := 8; offset < len(name); offset += 8 {
		word = bits.RotateLeft64(word*0x517cc1b727220a95^stationWord(name[offset:]), 17)
	}
	return bits.RotateLeft64(word*0x517cc1b727220a95, 17)
}

// pos returns the initial probe slot, not necessarily the occupied slot after
// collisions. For example, pos("Oslo") is 10114 in every new table.
func (m *simpleMap) pos(name stationName) uint32 {
	return uint32(stationFingerprint(name)) & flatMask
}

// homeHit checks only the initial slot using an already computed fingerprint.
// It returns Oslo's accumulator when slot 10114 matches; an empty or colliding
// slot returns nil for findSlow to handle. Short names match by hash and length;
// long names additionally match the full string. Keeping this helper small
// lets the compiler inline the common lookup without insertion/probe-loop work.
func (m *simpleMap) homeHit(name stationName, hash uint64) *stats {
	entry := &m.data[uint32(hash)&flatMask]
	if entry.hash == hash && len(entry.name) == len(name) && (len(name) <= 8 || entry.name == name) {
		return &entry.stats
	}
	return nil
}

// find returns the existing accumulator or inserts an owned name with zero stats.
// Repeated find("Oslo") calls return the same pointer, ready for updateStats.
func (m *simpleMap) find(name stationName) *stats {
	hash := stationFingerprint(name)
	if hit := m.homeHit(name, hash); hit != nil {
		return hit
	}
	return m.findSlow(name, hash)
}

// findSlow handles insertion and collision probes without rehashing the name.
// For a home slot of 32767, occupied mismatches continue at 0, then 1, etc.
// A new "Oslo" key is cloned before storing it, so later changes to the input
// buffer cannot change the key. The input limit leaves empty slots available.
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

// get probes from pos (normally m.pos(name)) without inserting missing names.
// An existing Oslo returns its stats pointer and true; a missing Oslo reaches
// an empty slot and returns nil, false.
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

// set copies statistics into the station's owned entry, inserting if needed.
// For example, set(m.pos("Oslo"), "Oslo", &stats{sum:126, count:1}) stores
// those values independently of the supplied pointer. The old position
// argument is ignored because collision resolution belongs to find.
func (m *simpleMap) set(_ uint32, name stationName, st *stats) {
	*m.find(name) = *st
}

// parseLineAtSeparator decodes a row whose delimiter position was found by a
// block mask. For "Oslo;-12.6\n", separatorIdx=4 returns
// (10,"Oslo",-126,0x6f6c734f), just like parseLine. Names borrow data until
// insertion clones them. First-word normalization and bounded numeric decoding
// are identical to the scalar parser; short final rows keep its fallback.
func parseLineAtSeparator(data []byte, separatorIdx int) (int, stationName, measurement, uint64) {
	if separatorIdx < 0 {
		return -1, "", 0, 0
	}
	var firstWord uint64
	if len(data) >= 8 {
		firstWord = binary.LittleEndian.Uint64(data)
	}

	if separatorIdx+4 >= len(data) {
		return -1, "", 0, 0
	}

	name := stationName(unsafe.String(&data[0], len(data[:separatorIdx])))
	// Preserve the exact first name word; delimiter, temperature and following
	// row bytes must not participate in its fingerprint.
	if separatorIdx < 8 {
		if len(data) < 8 {
			firstWord = stationWord(name)
		} else {
			// "Oslo;1.2" loads 0x322e313b6f6c734f; a four-byte mask
			// keeps only 0x6f6c734f for the name fingerprint.
			firstWord &= (uint64(1) << (separatorIdx * 8)) - 1
		}
	}

	// Decode all four legal temperature layouts with one bounded 64-bit load.
	// For "-12.6\n", the dot is byte 3 (bit 28 in the mask), signed=-1,
	// aligned digit nibbles multiply into absolute=126, then sign gives -126.
	// Extra bytes after the newline never enter the digit mask. Complete final
	// rows have fewer than eight remaining bytes and use parseNumber below.
	if len(data)-separatorIdx >= 9 {
		temperatureWord := binary.LittleEndian.Uint64(data[separatorIdx+1:])
		dotPos := bits.TrailingZeros64(^temperatureWord & 0x10101000)
		if dotPos <= 28 {
			newlineIdx := separatorIdx + dotPos/8 + 3
			if data[newlineIdx] != '\n' {
				return -1, "", 0, 0
			}
			signed := int64(^temperatureWord<<59) >> 63
			digits := ((temperatureWord & ^(uint64(signed) & 0xff)) << (28 - dotPos)) & 0x0f000f0f00
			absolute := int64((digits * 0x640a0001) >> 32 & 0x3ff)
			value := measurement((absolute ^ signed) - signed)
			return newlineIdx, name, value, firstWord
		}
	}

	newlineIdx := separatorIdx + 4
	if data[separatorIdx+1] == '-' {
		newlineIdx++
	}
	if newlineIdx >= len(data) {
		return -1, "", 0, 0
	}
	if data[newlineIdx-2] != '.' {
		newlineIdx++
	}
	if newlineIdx >= len(data) || data[newlineIdx] != '\n' {
		return -1, "", 0, 0
	}

	return newlineIdx, name, parseNumber(data[separatorIdx+1 : newlineIdx]), firstWord
}

// semicolonBits returns one exact low-byte bit per semicolon in an eight-byte
// word: "A;0.0\nB;" produces 0x82 (byte offsets 1 and 7). For ";:", only
// bit zero is set; unlike first-match zero detection, addition here cannot
// propagate a borrow into the next byte and invent a delimiter.
func semicolonBits(word uint64) uint64 {
	const low = uint64(0x7f7f7f7f7f7f7f7f)
	const high = uint64(0x8080808080808080)
	word ^= 0x3b3b3b3b3b3b3b3b
	matches := ^(((word & low) + low) | word | low) & high
	return ((matches >> 7) * 0x0102040810204080) >> 56
}

// semicolonMask64 scans exactly sixty-four in-slice bytes and returns one bit
// per delimiter. With "A;0.0\nB;1.0\n" at the start, bits 1 and 7 are set.
// Eight exact byte masks are concatenated; no unsafe overread or native-endian
// assumption is used. Callers handle slices shorter than sixty-four bytes with
// the scalar parser. SIMD and scalar generators return the same exact bits.
func semicolonMask64(data []byte) uint64 {
	_ = data[63]
	return semicolonBits(binary.LittleEndian.Uint64(data)) |
		semicolonBits(binary.LittleEndian.Uint64(data[8:]))<<8 |
		semicolonBits(binary.LittleEndian.Uint64(data[16:]))<<16 |
		semicolonBits(binary.LittleEndian.Uint64(data[24:]))<<24 |
		semicolonBits(binary.LittleEndian.Uint64(data[32:]))<<32 |
		semicolonBits(binary.LittleEndian.Uint64(data[40:]))<<40 |
		semicolonBits(binary.LittleEndian.Uint64(data[48:]))<<48 |
		semicolonBits(binary.LittleEndian.Uint64(data[56:]))<<56
}
