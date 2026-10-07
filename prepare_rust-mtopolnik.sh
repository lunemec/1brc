#!/usr/bin/env bash
set -euo pipefail

CARGO_TARGET_DIR=build/rust-mtopolnik \
    cargo build --locked --release \
    --manifest-path third_party/rust-mtopolnik/Cargo.toml
