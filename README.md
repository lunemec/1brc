# 1️⃣🐝🏎️ The One Billion Row Challenge

The **fast-er** Go variant of 1brc!

Because I also don't have access to the Hetzner Instance, here are measurements done locally (MBP M1 2020 16GB) on the generated 1B lines:

| Program | Time |
| ------- | ---- |
| Java thomaswue (TOP) | 15.350s |
| Go elh | 7.571s |
| Go AlexanderYastrebov | 17.966s |
| Go shraddhaag | 18.809s |
| **Go lunemec** | **7.128s** |
| **Go benhoyt** | **5.034s** |

I'm not including the latest variant by [Renato Pereira](https://r2p.dev/b/2024-03-18-1brc-go/) because even though it was faster (6.674s), 
it did not produce valid output on my machine (after copy+paste from the blog, I could not find repo). Also it did use swissmap, which is against the rules (deps here are strictly tests).

### Can we beat them? 🥁

Here are the results!
```shell
 λ hyperfine -w 2 'target/lunemec measurements.txt'
Benchmark 1: target/lunemec measurements.txt
  Time (mean ± σ):      7.128 s ±  0.019 s    [User: 47.592 s, System: 2.149 s]
  Range (min … max):    7.098 s …  7.163 s    10 runs
```
vs
```shell
 λ hyperfine -w 2 'target/elh measurements.txt'
Benchmark 1: target/elh measurements.txt
  Time (mean ± σ):      7.571 s ±  0.052 s    [User: 45.990 s, System: 2.245 s]
  Range (min … max):    7.498 s …  7.650 s    10 runs
```

**Whole 3.5% faster**! It ain't much, but its honest work!

However, there is new contender on the block:
```shell
 λ hyperfine -w 2 'target/benhoyt measurements.txt'
Benchmark 1: target/benhoyt measurements.txt
  Time (mean ± σ):      5.034 s ±  0.638 s    [User: 25.078 s, System: 2.786 s]
  Range (min … max):    4.548 s …  6.752 s    10 runs
```

### 10k unique station names
```shell
 λ hyperfine -w 2 'target/lunemec measurements3.txt'
Benchmark 1: target/lunemec measurements3.txt
  Time (mean ± σ):     14.423 s ±  0.083 s    [User: 101.053 s, System: 2.987 s]
  Range (min … max):   14.355 s … 14.584 s    10 runs
```
vs
```shell
 λ hyperfine -w 2 'target/elh measurements3.txt'
Benchmark 1: target/elh measurements3.txt
  Time (mean ± σ):     12.746 s ±  0.043 s    [User: 84.343 s, System: 2.981 s]
  Range (min … max):   12.672 s … 12.801 s    10 runs
```

and the fastest is:
```shell
 λ hyperfine -w 2 'target/benhoyt measurements3.txt'
Benchmark 1: target/benhoyt measurements3.txt
  Time (mean ± σ):      9.091 s ±  0.037 s    [User: 52.046 s, System: 4.699 s]
  Range (min … max):    9.038 s …  9.135 s    10 runs
```

### Details

There are few differences in my approach, where some time is saved:
1) Custom hash function that does 2 bytes at once while not having too many collisions.
2) Custom hashmap implementation that allows me to hash only 1x and use the hashed position.
3) Reading only `\n` from the data chunk using bytes.IndexByte, which uses optimised assembly instructions and is very fast, then finding `;` using byte index offset.
4) Unrolling the measurement by hand for all 4 variants.

There are some other general approaches that make it fast overall, but they were used in other implementations already:
1) Measurements (-99.9) are parsed into int16 (10x multiply), and transformed to float only 1x for the final print.
2) Using `*stats` in the hashmap so the values can be updated in-place.
3) Reading chunks of 20MiB per worker goroutine, mmap is 2-3x slower.
4) Using `unsafe.String` to avoid extra copies of the station name.


### Measuring

By compiling the source code, and using [hyperfine](https://github.com/sharkdp/hyperfine) benchmarking tool.

Or by running the test benchmark harness like this:
```go
func BenchmarkRun(b *testing.B) {
	bench = true
	for range b.N {
		run(defaultMeasurementsFile)
	}
}
```

This allows me to run each test 20x, and use benchstat to compare the versions.
```shell
go test -count 20 -run="^$" -bench "^BenchmarkRun$" . > full_lunemec.txt
```


## Original 1BRC description snippet

<img src="1brc.png" alt="1BRC" style="display: block; margin-left: auto; margin-right: auto; margin-bottom:1em; width: 50%;">

The text file contains temperature values for a range of weather stations.
Each row is one measurement in the format `<string: station name>;<double: measurement>`, with the measurement value having exactly one fractional digit.

The following shows ten rows as an example:

```
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
St. John's;15.2
Cracow;12.6
Bridgetown;26.9
Istanbul;6.2
Roseau;34.4
Conakry;31.2
Istanbul;23.0
```

The task is to write a program which reads the file, calculates the min, mean, and max temperature value per weather station, and emits the results on stdout like this
(i.e. sorted alphabetically by station name, and the result values per station in the format `<min>/<mean>/<max>`, rounded to one fractional digit):

```
{Abha=-23.0/18.0/59.2, Abidjan=-16.2/26.0/67.3, Abéché=-10.0/29.4/69.0, Accra=-10.1/26.4/66.4, Addis Ababa=-23.7/16.0/67.0, Adelaide=-27.8/17.3/58.5, ...}
```
