# Pinned reference implementations

These sources are kept in-tree so a benchmark does not silently change when an
upstream repository moves.

| Adapter | Source revision | License | Local correctness change |
| --- | --- | --- | --- |
| Corpus generator | [gunnarmorling/1brc@241d42c](https://github.com/gunnarmorling/1brc/commit/241d42ca6609b6bc32b403b1f4ee4d1fe6e325f8) | Apache-2.0 | None |
| `java-thomaswue-jvm`, `java-thomaswue-native` | [gunnarmorling/1brc@241d42c](https://github.com/gunnarmorling/1brc/commit/241d42ca6609b6bc32b403b1f4ee4d1fe6e325f8) | Apache-2.0 | None |
| `c-matt-re` | [matt-re/1brc@1465eaf](https://github.com/matt-re/1brc/commit/1465eaf0b45db10554d3da014945c9a2d5739e4d) | MIT | Handle tiny inputs and implement challenge rounding |
| `rust-mtopolnik` | [mtopolnik/rust-1brc@f5b1862](https://github.com/mtopolnik/rust-1brc/commit/f5b186298969b4ee3fc86da80f0e8531098f6a1f) | MIT | Use an `i64` sum and explicit challenge rounding |

The C changes handle tiny validation inputs and replace unconditional ceiling
with nearest rounding, ties toward positive. The Rust changes prevent overflow
on legal skewed inputs and implement the same rounding rule. Both are exercised
by `./bench.sh validate`.
