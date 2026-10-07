# 7. Stop allocating the input again

[`57591e7`](data/history.md#57591e7) replaces fresh input buffers with a pool.
A buffer holds the input bytes for one chunk.
A pool keeps buffers available for reuse.
The producer owns the pool and limits its size.
It still reads sequentially with `ReadAt`.

The parser and table process the same ranges, which end at newline boundaries.
Workers return a buffer after they process its final row.
The change reduces measured total allocation by more than 99%.
Runtime falls by 12.64% on standard and 21.36% on 10K.

## Allocation volume is not a leak

Allocation reserves memory for a new object.
Initialization sets its first values.
Garbage collection, or GC, reclaims unreachable objects.
Before the pool, the producer calls `make([]byte, 6*MiB)` for every chunk.
For a file around 13.8 billion bytes, that creates thousands of large arrays in one run.

A heap holds objects that the program allocates.
A live object is still reachable by the program.
A small live heap at the end does not remove the earlier allocation, initialization, and garbage collection work.

The retained diagnostics show the distinction:

| Metric | Standard before → pool | 10K before → pool |
| --- | ---: | ---: |
| Total allocated, decimal GB | 13.822078 → 0.131817 | 17.098455 → 0.136554 |
| Automatic GC cycles | 130 → 3 | 152 → 3 |
| Sampled peak RSS, decimal MB | 291.992 → 147.841 | 288.616 → 150.209 |
| Live heap after GC, decimal MB | 0.625 → 0.616 | 0.632 → 0.613 |

Total allocation counts all allocations over time.
Live heap is the memory occupied by reachable objects.
RSS means the memory that a process holds in RAM.
It includes memory outside the Go heap.
These quantities describe different uses of memory.
High allocation volume does not imply a permanent leak.

![Total allocated bytes before and after buffer reuse, on a logarithmic GB scale.](figures/pool-allocations.svg)

The live heap after GC stays almost the same.
The improvement comes from avoiding repeated work during execution.
It does not come from reducing a growing live heap.

A logarithmic axis shows equal ratios as equal distances.
The graph uses that axis so both large and small values remain visible.
The bar lengths do not represent linear ratios.

## Give every outstanding chunk its own loan

A channel passes values between concurrent tasks.
A loan gives one task temporary ownership of a buffer.
The pool stores an available-buffer channel, an allocation count, and a fixed buffer size.
Only the producer takes buffers and updates the count.
Workers return completed loans:

```go
type chunkBufferPool struct {
    available chan []byte
    allocated int
    size      int
}

type chunk struct {
    data    []byte
    recycle chan []byte
}
```

`take` follows three rules:

1. If a returned buffer is available, reuse it immediately.
2. If no buffer is available and the count is below the limit, allocate one.
3. If neither condition holds, wait for a worker to return a buffer.

The pool allocates buffers as demand grows.
It does not allocate all buffers before the first read.
The available channel has capacity equal to the buffer limit.
Workers can therefore return loans without waiting for the producer to receive them.

## Why sixteen workers need at most seventeen buffers

An unbuffered work channel holds no queued chunks.
The sender waits until a receiver accepts each chunk.
The measured work channel uses this behavior.
Each of sixteen workers can own one chunk.
The producer can hold one additional buffer during a read or while it waits to send that buffer.
Thus:

$$
N_{\text{buffers}}\le W+1=17,
\qquad
M_{\text{input buffers}}\le17\times6\ \text{MiB}=102\ \text{MiB}.
$$

The backing array is the memory behind a buffer view.
The bound covers only the backing arrays of input buffers.
It excludes station tables, keys, channel metadata, and other process memory.
It relies on the measured unbuffered work channel.
Adding a queue requires a new review of the ownership bound and return protocol.

Backpressure means that slower consumers make the producer wait.
This keeps the producer from allocating buffers without a limit.
A two-worker example shows where the producer waits:

```text
Producer borrows A → reads → sends A to worker 0
Producer borrows B → reads → sends B to worker 1
Producer borrows C → reads → waits for an available worker
Worker 0 finishes A → returns A → accepts C
Producer next take reuses A
```

No buffer belongs to two workers at the same time.
When all loans are busy, the producer waits for a return.
That wait keeps the buffer count within its limit.

## Borrowed names must become owned keys

A string view refers to existing bytes without copying them.
The parser returns a string view into the input.
If the table stores only that view, the next read into the buffer can change the stored name.
The table from [part 4](04-robust-station-table.md) already copies newly inserted names:

```text
Buffer A contains:       Oslo;1.2\n
Parser borrows:          "Oslo" within A
Table inserts a clone:   separately owned "Oslo"
Worker finishes update:  count=1, sum=12, min=12, max=12
Worker returns A
Next read overwrites A:  Rome;9.9\n
Table still contains:    "Oslo" and its original accumulator
```

The scalar temperature decoder reads individual digits.
The worker waits until every row update finishes, including rows that use this decoder.
It then returns the buffer with its complete backing capacity:

```go
c.recycle <- c.data[:cap(c.data)]
```

A chunk view can be shorter than the backing buffer.
Restoring full capacity lets the next read fill the complete buffer again.
Test chunks owned by their caller have `recycle=nil`.
The worker does not return those chunks to the pool.

## Old bytes must never become new input

EOF means the end of the file.
Returning a buffer does not clear its bytes.
A later short `ReadAt` can overwrite only its first `n` bytes.
Earlier contents can remain beyond `n`.
At EOF, the producer sends `data[:n]`, never the full capacity.

```text
Old buffer:       A;1.0\nB;2.0\n...
New read:         C;3.0\n          n=6
Valid chunk view:  C;3.0\n          len=6
Stale suffix:     B;2.0\n...       outside the view
```

The parser cannot see those earlier bytes outside the view.
Tests cover partial EOF, exact EOF, empty or incomplete chunks, and full-capacity returns.
Other tests deliberately overwrite returned backing arrays.
This operation is called poisoning.

The tests inspect the actual stored name bytes after poisoning.
A successful short-key lookup alone does not prove ownership.
It can match by cached fingerprint and length even when the stored bytes contain different data.

The return channel stays open when the producer reaches EOF.
Workers can still finish and return loans afterward.
When the run finishes, the channel and buffers become unreachable.

## The measured result

![Individual observations before and after bounded buffer reuse.](figures/pool-runtimes.svg)

| Corpus / window | Fresh buffers | Bounded pool | Runtime reduction |
| --- | ---: | ---: | ---: |
| Standard / 1 | 2.204872 s | 1.926155 s | 12.641% |
| 10K / 1 | 3.049592 s | 2.398277 s | 21.357% |

Both independent measurement periods pass the host, fresh null, and order controls.
The inputs stay resident in memory, measured files stay unchanged, and outputs match exactly.
Neither gain requires the protocol's repeat for small improvements.
Separate diagnostic full runs record seventeen allocations against the seventeen-buffer limit.
Their independent row counts are exactly one billion.

The trace records Running as the state when a task executes.
Producer Running time falls `0.826 → 0.006` seconds on standard and `1.141 → 0.008` on 10K.
Its allocation and clearing work largely disappears.
Worker channel waits also shrink.
Their cumulative totals overlap across workers, so they do not equal savings in elapsed runtime.

CPU-class metrics divide CPU time into categories of runtime work.
The installed runtime refreshes some of these metrics at GC.
With only three startup collections, CPU ratios measured before cleanup GC contain older values.
The retained analysis uses a new measurement after GC and keeps the original values.
It also states that the later measurement includes cleanup and reporting.

The allocation-volume and GC-count differences above come directly from run measurements.
The older idle ratios do not establish how fully the program uses the CPU.
We do not use those ratios to make that claim.

Evidence: [timings](data/timings.json), [memory measurements](data/memory.json),
[pool tests](../../buffer_pool_test.go),
[full acceptance and diagnostic caveats](../../EXPERIMENTS.md#accepted-bounded-buffer-reuse-2026-10-06),
[source change](data/history.md#57591e7).

[← Previous](06-temperature-word-decoder.md) · [Next: pooled AVX2 →](08-pooled-avx2-scanning.md)
