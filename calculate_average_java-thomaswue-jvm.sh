#!/usr/bin/env bash
set -euo pipefail

exec java --enable-preview \
    -cp build/java-thomaswue/classes \
    dev.morling.onebrc.CalculateAverage_thomaswue
