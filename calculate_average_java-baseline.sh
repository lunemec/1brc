#!/usr/bin/env bash
set -euo pipefail

exec java -cp build/java-baseline/classes dev.morling.onebrc.CalculateAverage_baseline
