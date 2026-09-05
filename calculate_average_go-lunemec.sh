#!/usr/bin/env bash
set -euo pipefail

exec ./build/go-lunemec measurements.txt
