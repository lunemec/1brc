# Pinned reference implementations

These sources are kept in-tree so a benchmark does not silently change when an
upstream repository moves.

| Adapter | Source revision | License | Local correctness change |
| --- | --- | --- | --- |
| Corpus generators and baseline | [gunnarmorling/1brc@241d42c](https://github.com/gunnarmorling/1brc/commit/241d42ca6609b6bc32b403b1f4ee4d1fe6e325f8) | Apache-2.0; station data is CC BY 4.0 | Baseline uses fixed-point aggregation for exact challenge rounding |
| Upstream `test.sh` and `tocsv.sh` | [gunnarmorling/1brc@241d42c](https://github.com/gunnarmorling/1brc/commit/241d42ca6609b6bc32b403b1f4ee4d1fe6e325f8) | Apache-2.0 | GNU-only colored `diff` flag removed for Apple `diff`; runs in a disposable workspace |
| `java-thomaswue-jvm`, `java-thomaswue-native` | [gunnarmorling/1brc@241d42c](https://github.com/gunnarmorling/1brc/commit/241d42ca6609b6bc32b403b1f4ee4d1fe6e325f8) | Apache-2.0 | Exact integer rounding in output formatting |
| `c-matt-re` | [matt-re/1brc@1465eaf](https://github.com/matt-re/1brc/commit/1465eaf0b45db10554d3da014945c9a2d5739e4d) | MIT | Handle tiny inputs, implement challenge rounding, and match Java UTF-16 output ordering |
| `rust-mtopolnik` | [mtopolnik/rust-1brc@f5b1862](https://github.com/mtopolnik/rust-1brc/commit/f5b186298969b4ee3fc86da80f0e8531098f6a1f) | MIT | Use an `i64` sum, explicit challenge rounding, and Java UTF-16 output ordering |

The Java, C, and Rust rounding changes avoid floating-point edge cases and use
nearest rounding with ties toward positive. The C changes also handle tiny
inputs, and the Rust changes prevent overflow on legal skewed inputs. Go, C,
and Rust explicitly match the UTF-16 ordering used by the Java reference. These
changes are exercised by `./bench.sh validate`.
